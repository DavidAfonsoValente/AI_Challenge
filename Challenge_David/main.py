import os
import sys
import uvicorn
from database import create_database_and_tables, load_companies_from_csv, SessionLocal
from scraper import run_scraper_and_processor
from matching import find_and_store_matches
from chatbot import app as fastapi_app
from config import DB_FILE_NAME

COMPANY_CSV_PATH = 'companies_sample.csv'

def setup_initial_data():
    """Initial setup: create DB schema and load companies from CSV."""
    if os.path.exists(DB_FILE_NAME):
        print(f"Deleting existing database file: {DB_FILE_NAME}")
        os.remove(DB_FILE_NAME)

    print("\n--- 1. Setting up Database ---")
    create_database_and_tables()

    print("\n--- 2. Loading Company Data ---")
    if not os.path.exists(COMPANY_CSV_PATH):
        print(f"[FATAL ERROR] Company CSV file not found at '{COMPANY_CSV_PATH}'.")
        print("Please ensure the CSV file is in the project directory.")
        sys.exit(1)
    load_companies_from_csv(COMPANY_CSV_PATH)
    print("--- Setup Complete ---")

def run_pipeline():
    """Runs the main data processing pipeline: scrape and match."""
    print("\n--- 3. Scraping and Processing Incentives ---")
    run_scraper_and_processor()

    print("\n--- 4. Finding and Storing Company Matches ---")
    db_session = SessionLocal()
    try:
        find_and_store_matches(db_session, k=5)
    finally:
        db_session.close()
    print("--- Pipeline Complete ---")

def start_chatbot_server():
    """Starts the FastAPI server for the chatbot."""
    print("\n--- 5. Starting Chatbot API Server ---")
    print("The server is running. You can now send requests.")
    print("Access the interactive API documentation (Swagger UI) at: http://127.0.0.1:8000/docs")
    uvicorn.run(fastapi_app, host="127.0.0.1", port=8000)

def print_usage():
    """Prints the command-line usage instructions."""
    print("\nUsage: python main.py [command]")
    print("Commands:")
    print("  setup    : Creates DB schema and loads company CSV. Run this first.")
    print("  pipeline : Scrapes incentives and runs the matching algorithm.")
    print("  chatbot  : Starts the API server for the chatbot.")
    print("  all      : Runs 'setup', then 'pipeline', then starts the 'chatbot'.")
    print("\nExample: python main.py all\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == 'setup':
        setup_initial_data()
    elif command == 'pipeline':
        run_pipeline()
    elif command == 'chatbot':
        start_chatbot_server()
    elif command == 'all':
        setup_initial_data()
        run_pipeline()
        start_chatbot_server()
    else:
        print(f"Unknown command: '{command}'")
        print_usage()
