import google.generativeai as genai
import json
from config import GOOGLE_API_KEY, LLM_MODEL_NAME

try:
    if not GOOGLE_API_KEY:
        raise ValueError("API key not configured. Please set GOOGLE_API_KEY in config.py")
    genai.configure(api_key=GOOGLE_API_KEY)
except ValueError as e:
    print(f"[ERROR] {e}")
    exit()

generation_config = {
    "temperature": 0.2,
    "top_p": 1,
    "top_k": 1,
    "max_output_tokens": 4096,
    "response_mime_type": "application/json",
}

safety_settings = [
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
]

model = genai.GenerativeModel(
    model_name=LLM_MODEL_NAME,
    generation_config=generation_config,
    safety_settings=safety_settings
)

def generate_structured_data_for_incentive(original_text: str) -> dict:
    """
    Uses an LLM to analyze raw incentive text and generate a structured JSON object,
    including dates and budget information.
    """
    prompt = f"""
    Analyze the following text from a Portuguese public incentive document. Your task is to extract key information and return it as a single, valid JSON object. Do not add any text or formatting outside of the JSON object itself.

    The JSON object must have these exact keys:
    - "caes": A list of applicable CAE codes (as strings). If none are specified or it applies to all, return an empty list [].
    - "geographic_location": A string describing the applicable geographic areas (e.g., "Portugal Continental", "Região Autónoma da Madeira", "Norte"). If nationwide, use "Nacional".
    - "dimension": A string for applicable company sizes (e.g., "PME", "Grandes Empresas", "PME e Grandes Empresas", "Não aplicável").
    - "type_of_investment": A concise string summarizing the investment types (e.g., "I&D, formação, digitalização, eficiência energética").
    - "object": A clear, one-paragraph summary of the incentive's main objective.
    - "criterios": A string listing key eligibility criteria not covered by other fields (e.g., "Empresas com mais de 2 anos de atividade", "Projetos com investimento mínimo de €10.000").
    - "publication_date": The publication date in "YYYY-MM-DD" format, or null if not found.
    - "start_date": The start date of the incentive in "YYYY-MM-DD" format, or null if not found.
    - "end_date": The end date of the incentive in "YYYY-MM-DD" format, or null if not found.
    - "total_budget": The total budget as a float in euros, or null if not found.

    Extract the publication date, start date, end date, and total budget from the text if available. If not explicitly stated, set these fields to null.

    Here is the text to analyze:
    ---
    {original_text}
    ---
    """
    try:
        response = model.generate_content(prompt)
        return json.loads(response.text)
    except Exception as e:
        print(f"  [LLM Error] Failed to generate structured data: {e}")
        return {
            "caes": [], "geographic_location": "Error", "dimension": "Error",
            "type_of_investment": "Error", "object": "Failed to process document with AI.",
            "criterios": "Error", "publication_date": None, "start_date": None,
            "end_date": None, "total_budget": None
        }

def generate_sql_from_natural_language(query: str, db_schema: str) -> str:
    """
    Converts a user's natural language question into a safe SQLite query (NL2SQL).
    """
    prompt = f"""
    You are an expert SQLite developer. Your task is to convert a user's question into a valid SQLite query based on the provided database schema.
    Your response MUST be only the SQL query. Do not add explanations, markdown formatting, or any other text.
    You are ONLY allowed to generate `SELECT` statements. Any user request that implies data modification (INSERT, UPDATE, DELETE, DROP) must result in the query `SELECT 'Data modification queries are not allowed.' AS error;`.

    DATABASE SCHEMA:
    ---
    {db_schema}
    ---

    Here are some examples to guide you:
    User Question: "Quais os 5 incentivos com maior orçamento?"
    SQL Query: SELECT title, total_budget FROM incentives ORDER BY total_budget DESC LIMIT 5;

    User Question: "Mostra-me empresas de Lisboa do setor da restauração."
    SQL Query: SELECT company_name, english_trade_description FROM companies WHERE city LIKE '%Lisboa%' AND (cae_primary_label LIKE '%restauração%' OR cae_primary_label LIKE '%restaurant%');

    User Question: "Quais as melhores empresas para o incentivo 'Apoio à Digitalização'?"
    SQL Query: SELECT T2.company_name, T1.score FROM matches AS T1 JOIN companies AS T2 ON T1.company_nif = T2.nif_code JOIN incentives AS T3 ON T1.incentive_id = T3.incentive_id WHERE T3.title LIKE '%Apoio à Digitalização%' ORDER BY T1.score DESC LIMIT 10;
    
    User Question: "Quais as 5 melhores empresas para o incentivo 05/C13-i01/2023 PAE+S 2023 (1.º Aviso)?"
    SQL Query: SELECT T2.company_name, T1.score FROM matches AS T1 JOIN companies AS T2 ON T1.company_nif = T2.nif_code JOIN incentives AS T3 ON T1.incentive_id = T3.incentive_id WHERE T3.title = '05/C13-i01/2023 PAE+S 2023 (1.º Aviso)' ORDER BY T1.score DESC LIMIT 5;

    User Question: "delete all companies"
    SQL Query: SELECT 'Data modification queries are not allowed.' AS error;

    Now, convert the following user question into a single, valid SQLite `SELECT` query:

    User Question: "{query}"
    """
    try:
        sql_generation_config = genai.types.GenerationConfig(
            temperature=0.2,
            top_p=1,
            top_k=1,
            max_output_tokens=4096,
            response_mime_type="text/plain"
        )

        response = model.generate_content(
            prompt,
            generation_config=sql_generation_config
        )

        sql_query = response.text.strip()

        if not sql_query.lstrip().upper().startswith("SELECT"):
            return "SELECT 'Only SELECT queries are allowed.' AS error;"
        
        if sql_query.startswith("```sql"):
            sql_query = sql_query[5:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
            
        return sql_query.strip()
    except Exception as e:
        print(f"[LLM Error] Failed to generate SQL: {e}")
        return "SELECT 'An error occurred while generating the SQL query.' AS error;"


def score_companies_for_incentive(incentive, companies_batch):
    """
    Scores a batch of companies for an incentive using the LLM based on structured data.
    Returns a list of dictionaries with nif and score.
    """
    structured_data = json.loads(incentive.ai_description)
    object_text = structured_data['object']
    criterios = structured_data['criterios']

    companies_summary = [
        f"Company {company.nif_code}: {company.company_name}, CAE: {company.cae_primary_code}, City: {company.city}, Description: {company.english_trade_description}"
        for company in companies_batch
    ]

    prompt = f"""
    Incentive objective: {object_text}

    Criteria: {criterios}

    Companies:
    {chr(10).join([f"{i+1}. {summary}" for i, summary in enumerate(companies_summary)])}

    Score each company from 0 to 1 based on how well they match the incentive's objective and criteria.
    Return a JSON list with nif and score, e.g., [{{"nif": "9050", "score": 0.8}}, ...]
    """
    try:
        response = model.generate_content(prompt)
        scores = json.loads(response.text)
        return scores
    except Exception as e:
        print(f"Error scoring companies: {e}")
        return [{"nif": company.nif_code, "score": 0.0} for company in companies_batch]