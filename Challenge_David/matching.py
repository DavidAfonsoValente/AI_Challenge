import json
from sqlalchemy.orm import Session
from database import Incentive, Company, Match
from llm_services import score_companies_for_incentive

def calculate_match_score(incentive_details: dict, company: Company) -> float:
    """
    Objective Evaluation Metric: Calculates a match score between 0.0 and 1.0.
    This function is deterministic, transparent, and tunable. It does NOT use an LLM,
    making it fast and free to run for millions of company-incentive pairs.
    """
    score = 0
    max_score = 100
    weights = {
        'cae': 50,
        'location': 30,
        'size': 20
    }
    
    caes_value = incentive_details.get('caes')
    incentive_caes = set(caes_value) if isinstance(caes_value, list) else set()
    company_caes = {company.cae_primary_code}
    if company.cae_secondary_codes:
        try:
            company_caes.update(json.loads(company.cae_secondary_codes))
        except (json.JSONDecodeError, TypeError):
            pass
    if not incentive_caes:
        score += weights['cae']
    elif company_caes.intersection(incentive_caes):
        score += weights['cae']

    location_value = incentive_details.get('geographic_location')
    incentive_loc = location_value.lower() if isinstance(location_value, str) else ''
    company_city = (company.city or "unknown").lower()
    if "nacional" in incentive_loc or not incentive_loc:
        score += weights['location']
    elif company_city != "unknown" and company_city in incentive_loc:
        score += weights['location']

    dimension_value = incentive_details.get('dimension')
    incentive_dim = dimension_value.lower() if isinstance(dimension_value, str) else 'não aplicável'
    employees = company.latest_number_of_employees
    is_pme = 0 < employees < 250
    is_large = employees >= 250
    if 'não aplicável' in incentive_dim or not incentive_dim:
        score += weights['size']
    elif 'pme' in incentive_dim and 'grande' not in incentive_dim and is_pme:
        score += weights['size']
    elif 'grande' in incentive_dim and 'pme' not in incentive_dim and is_large:
        score += weights['size']
    elif 'pme' in incentive_dim and 'grande' in incentive_dim and (is_pme or is_large):
        score += weights['size']
            
    return round(score / max_score, 4)

def find_and_store_matches(db: Session, k: int = 5):
    """
    Finds the top K matching companies for each incentive using a hybrid approach:
    1. Rule-based filtering to select top N candidates with scores above a threshold.
    2. LLM-based scoring to refine and select top K matches with scores greater than 0.00.

    Stores the matches in the 'matches' table.
    """
    print(f"\n--- Starting Hybrid Matching Process (Top {k}) ---")
    incentives = db.query(Incentive).all()
    companies = db.query(Company).all()

    if not incentives or not companies:
        print("Not enough data to perform matching. Please run scraper and load companies first.")
        return

    print("Clearing old matches from the database...")
    db.query(Match).delete()
    db.commit()

    N = 50
    MIN_RULE_SCORE = 0.1

    total_matches_found = 0

    for incentive in incentives:
        print(f"\n-> Matching for incentive: '{incentive.title}'")
        try:
            incentive_details = json.loads(incentive.ai_description)
        except (json.JSONDecodeError, TypeError):
            print(f"  [!] Could not parse AI description for this incentive. Skipping.")
            continue

        scores = [(company, calculate_match_score(incentive_details, company)) for company in companies]
        filtered_scores = [item for item in scores if item[1] > MIN_RULE_SCORE]
        top_n = sorted(filtered_scores, key=lambda x: x[1], reverse=True)[:N]
        top_n_companies = [company for company, _ in top_n]

        if not top_n_companies:
            print("  No companies meet the minimum rule-based score for this incentive.")
            continue

        print(f"  Scoring top {len(top_n_companies)} companies with LLM...")
        llm_scores = score_companies_for_incentive(incentive, top_n_companies)

        if llm_scores:
            llm_scores_filtered = [item for item in llm_scores if float(item['score']) > 0.00]
            top_k_scores = sorted(llm_scores_filtered, key=lambda x: float(x['score']), reverse=True)[:k]
            for item in top_k_scores:
                match = Match(incentive_id=incentive.incentive_id, company_nif=item['nif'], score=float(item['score']))
                db.add(match)
                total_matches_found += 1
                print(f"    - Company NIF: {item['nif']}, Score: {float(item['score']):.2f}")
        else:
            print("  No LLM scores returned for this incentive.")

    db.commit()
    print(f"\n--- Matching process complete. Stored {total_matches_found} new matches. ---")

if __name__ == "__main__":
    from database import create_database_and_tables, SessionLocal
    create_database_and_tables()
    db_session = SessionLocal()
    try:
        find_and_store_matches(db_session, k=5)
    finally:
        db_session.close()
        print("Database session closed.")