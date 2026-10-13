import os
import sqlite3
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import openai  # or any preferred LLM client (e.g. Google Gemini, Anthropic)

app = FastAPI()

# Database setup
DB_FILE = "ledger.db"
SEED_URL = "YOUR_PROVIDED_QUESTION_DATA_URL"

def fetch_and_index_data():
    """Explores pagination and populates local SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Create table schema based on API fields
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY,
            date TEXT,
            region TEXT,
            product_name TEXT,
            revenue_usd REAL,
            refund_usd REAL,
            customer_id TEXT
        )
    """)
    
    # Fetch paginated data
    url = SEED_URL
    while url:
        resp = requests.get(url).json()
        orders = resp.get("data", []) # Adjust key according to API response structure
        for item in orders:
            cursor.execute("""
                INSERT OR REPLACE INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                item.get("id"),
                item.get("date"),
                item.get("region"),
                item.get("product_name"),
                item.get("revenue_usd", 0.0),
                item.get("refund_usd", 0.0),
                item.get("customer_id")
            ))
        url = resp.get("next_page_url")  # Follow pagination link
        
    conn.commit()
    conn.close()

# Index data on startup
@app.on_event("startup")
def startup_event():
    fetch_and_index_data()

class QuestionRequest(BaseModel):
    question: str

@app.post("/")
def answer_question(req: QuestionRequest):
    question = req.question
    
    # Convert Question -> SQL using LLM
    prompt = f"""
    Given the SQLite database schema 'orders(id TEXT, date TEXT, region TEXT, product_name TEXT, revenue_usd REAL, refund_usd REAL, customer_id TEXT)',
    write a valid SQL query to answer this question: "{question}".
    Return ONLY the raw SQL query without markdown or explanations.
    """
    
    # Call your LLM API here
    # sql_query = call_llm(prompt)
    
    # Execute SQL on local database
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    try:
        cursor.execute(sql_query)
        result = cursor.fetchone()[0]
        conn.close()
        
        # Round money/numbers if float
        if isinstance(result, float):
            result = round(result, 2)
            
        return {"answer": result}
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))
