from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
import pandas as pd

from database import get_db, engine
from llm_services import generate_sql_from_natural_language

app = FastAPI(
    title="AI Incentives Challenge API",
    description="An API for matching companies with public incentives and a chatbot for querying the data.",
    version="1.0.0",
)

def get_db_schema() -> str:
    """
    Extracts the CREATE TABLE statements from the SQLite database to provide
    context to the LLM for generating SQL queries.
    """
    try:
        with engine.connect() as connection:
            query = text("SELECT sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
            tables = connection.execute(query).fetchall()
            return "\n".join(table[0] for table in tables if table[0])
    except Exception as e:
        print(f"[ERROR] Failed to get DB schema: {e}")
        return ""

@app.post("/chat", summary="Query the database using natural language")
def chat_with_data(query: str, db: Session = Depends(get_db)):
    """
    This is the main chatbot endpoint. It takes a user's question in plain text,
    converts it to an SQL query using an LLM, executes it against the database,
    and returns the result in a structured format.

    - **query**: The user's question (e.g., "Quais as 5 melhores empresas para o incentivo X?").
    """
    print(f"Received chat query: '{query}'")

    schema = get_db_schema()
    if not schema:
        raise HTTPException(status_code=500, detail="Could not retrieve database schema.")

    sql_query = generate_sql_from_natural_language(query, schema)
    print(f"Generated SQL: {sql_query}")

    if "error" in sql_query.lower() or not sql_query.lstrip().upper().startswith("SELECT"):
        raise HTTPException(status_code=400, detail=f"Could not process query. Reason: {sql_query}")

    try:
        with engine.connect() as connection:
            result_df = pd.read_sql_query(sql_query, connection)

        if result_df.empty:
            return {
                "question": query,
                "response": "A sua pesquisa não encontrou resultados.",
                "data": []
            }

        response_data = {
            "question": query,
            "response": "Aqui estão os resultados da sua pesquisa:",
            "data": result_df.to_dict(orient='records')
        }
        return response_data

    except Exception as e:
        print(f"[ERROR] Error executing generated SQL: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred while executing the query: {str(e)}")

@app.get("/", summary="Root endpoint")
def read_root():
    """Provides a welcome message and directs users to the API documentation."""
    return {
        "message": "Welcome to the AI Incentives API.",
        "documentation": "Please visit /docs to see the interactive API documentation."
    }