import time
import json
from datetime import datetime
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from database import Incentive, SessionLocal
from llm_services import generate_structured_data_for_incentive

BASE_URL = "https://www.fundoambiental.pt"
TOP_LEVEL_MENU_TEXT = "Apoios PRR"

def get_all_incentive_links_from_category(driver, wait):
    """
    Navigates to the top-level menu, hovers over it to reveal sublinks,
    and collects all incentive URLs from second-level dropdowns under the category.
    """
    links_to_visit = set()
    print(f"--- Searching for all incentive links under category: '{TOP_LEVEL_MENU_TEXT}' ---")
    try:
        driver.get(BASE_URL)
        try:
            cookie_button = wait.until(EC.element_to_be_clickable((By.ID, "CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll")))
            cookie_button.click()
            print("Cookie policy accepted.")
        except TimeoutException:
            print("Cookie banner not found or already accepted.")

        actions = ActionChains(driver)
        main_menu_links = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div#navbar > ul > li > a")))
        top_menu_link = next((link for link in main_menu_links if link.text.strip() == TOP_LEVEL_MENU_TEXT), None)
        
        if not top_menu_link:
            raise NoSuchElementException(f"Could not find menu item '{TOP_LEVEL_MENU_TEXT}'")

        print(f"  Hovering over menu '{TOP_LEVEL_MENU_TEXT}' to reveal sublinks...")
        actions.move_to_element(top_menu_link).perform()
        time.sleep(2)

        parent_li = top_menu_link.find_element(By.XPATH, "./..")
        dropdown_ul = parent_li.find_element(By.TAG_NAME, "ul")
        first_level_lis = dropdown_ul.find_elements(By.TAG_NAME, "li")

        for li in first_level_lis:
            try:
                submenu_ul = li.find_element(By.TAG_NAME, "ul")
                sublink_a = li.find_element(By.TAG_NAME, "a")
                sublink_text = sublink_a.text.strip()
                print(f"    Hovering over sublink '{sublink_text}' to reveal its dropdown...")
                actions.move_to_element(sublink_a).perform()
                time.sleep(1)

                submenu_links = submenu_ul.find_elements(By.TAG_NAME, "a")
                for link in submenu_links:
                    href = link.get_attribute('href')
                    if href and href.startswith(BASE_URL) and ".aspx" in href:
                        links_to_visit.add(href)
                        print(f"      Added incentive link: {href}")
            except NoSuchElementException:
                try:
                    sublink_a = li.find_element(By.TAG_NAME, "a")
                    print(f"    Skipping sublink '{sublink_a.text.strip()}' (no submenu).")
                except NoSuchElementException:
                    print("    Skipping an invalid sublink (no <a> or <ul>).")
                continue

        print(f"Found {len(links_to_visit)} unique incentive links to process.")
        return list(links_to_visit)

    except Exception as e:
        print(f"[FATAL ERROR] Could not navigate the menu: {e}")
        return []

def run_scraper_and_processor():
    """
    Orchestrates scraping, AI processing, and database storage.
    """
    print("--- Starting Advanced Scraper with Menu Navigation ---")

    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("log-level=3")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    driver = webdriver.Chrome(service=service, options=options)
    wait = WebDriverWait(driver, 20)

    db = SessionLocal()
    try:
        incentive_urls = get_all_incentive_links_from_category(driver, wait)
        if not incentive_urls:
            print("No incentive URLs found. Terminating scraper.")
            return

        for url in incentive_urls:
            print(f"\n--- Processing URL: {url} ---")
            driver.get(url)
            time.sleep(2)

            try:
                wait.until(EC.presence_of_element_located((By.ID, "ctAreaConteudo")))
            except TimeoutException:
                print("  [WARNING] Content area not loaded in time. Skipping.")
                continue

            title = None
            try:
                title_element = wait.until(EC.visibility_of_element_located((By.TAG_NAME, "h1")))
                title = title_element.text.strip()
            except TimeoutException:
                print("  [WARNING] Could not find a title on the page. Using URL as title.")
                title = url.split('/')[-1].replace('.aspx', '')

            if db.query(Incentive).filter_by(title=title).first():
                print(f"  Incentive '{title}' already exists in the database. Skipping.")
                continue

            full_text, document_urls_str = "", ""
            try:
                content_area = driver.find_element(By.ID, "ctAreaConteudo")
                detail_soup = BeautifulSoup(content_area.get_attribute('outerHTML'), 'html.parser')
                full_text = detail_soup.get_text(separator='\n', strip=True)
                doc_links = [
                    urljoin(BASE_URL, a['href']) 
                    for a in detail_soup.find_all('a', href=True) 
                    if 'ficheiros/' in a['href']
                ]
                document_urls_str = ",".join(doc_links)
                print(f"  Found {len(doc_links)} document links.")
            except NoSuchElementException as e:
                print(f"  [!] Error locating content area: {e}. Skipping.")
                continue
            except Exception as e:
                print(f"  [!] Unexpected error extracting details: {e}. Skipping.")
                continue

            print("  Extracting structured data with AI...")
            ai_generated_json = generate_structured_data_for_incentive(full_text)

            publication_date = None
            start_date = None
            end_date = None
            total_budget = None
            if 'publication_date' in ai_generated_json and ai_generated_json['publication_date']:
                try:
                    publication_date = datetime.strptime(ai_generated_json['publication_date'], "%Y-%m-%d").date()
                except ValueError:
                    print("  [WARNING] Invalid publication_date format. Setting to None.")
            if 'start_date' in ai_generated_json and ai_generated_json['start_date']:
                try:
                    start_date = datetime.strptime(ai_generated_json['start_date'], "%Y-%m-%d").date()
                except ValueError:
                    print("  [WARNING] Invalid start_date format. Setting to None.")
            if 'end_date' in ai_generated_json and ai_generated_json['end_date']:
                try:
                    end_date = datetime.strptime(ai_generated_json['end_date'], "%Y-%m-%d").date()
                except ValueError:
                    print("  [WARNING] Invalid end_date format. Setting to None.")
            total_budget = ai_generated_json.get('total_budget')

            new_incentive = Incentive(
                title=title,
                description=full_text,
                ai_description=json.dumps(ai_generated_json, ensure_ascii=False),
                document_urls=document_urls_str,
                publication_date=publication_date,
                start_date=start_date,
                end_date=end_date,
                total_budget=total_budget,
                source_link=url
            )
            db.add(new_incentive)
            db.commit()
            print(f"  Successfully processed and saved: '{title}'")

            time.sleep(1)

    except Exception as e:
        print(f"\n[FATAL ERROR], Unexpected error during scraping: {e}")
        db.rollback()
    finally:
        print("\n--- Scraping process completed. ---")
        driver.quit()
        db.close()