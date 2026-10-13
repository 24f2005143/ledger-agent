import os
import sqlite3
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

DB_FILE = "ledger.db"
# Replace with your unique endpoint seed URL provided in the assignment portal
SEED_URL = os.getenv("SEED_URL", "YOUR_PROVIDED_QUESTION_DATA_URL")

def fetch_and_index_data():
    """Fetches all paginated order records and indexes them into an SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
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
    
    url = SEED_URL
    while url:
        try:
            resp = requests.get(url, timeout=10).json()
            orders = resp.get("data", [])
            for item in orders:
                cursor.execute("""
                    INSERT OR REPLACE INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(item.get("id")),
                    str(item.get("date")),
                    str(item.get("region")),
                    str(item.get("product_name")),
                    float(item.get("revenue_usd", 0.0)),
                    float(item.get("refund_usd", 0.0)),
                    str(item.get("customer_id"))
                ))
            # Handle pagination link or cursor if available
            url = resp.get("next_page_url")
        except Exception as e:
            print(f"Error fetching data page: {e}")
            break
            
    conn.commit()
    conn.close()

@app.on_event("startup")
def startup_event():
    fetch_and_index_data()

class QuestionRequest(BaseModel):
    question: str

@app.post("/")
def answer_question(req: QuestionRequest):
    question = req.question.strip()
    
    # ---------------------------------------------------------
    # Rule-based fallback & deterministic query construction 
    # to handle counts, refunds, regions, and dates accurately.
    # ---------------------------------------------------------
    q_lower = question.lower()
    sql_query = None
    
    # Example pattern handling for refund counts
    if "how many orders" in q_lower and "refund" in q_lower:
        # Extract region if specified
        region_filter = ""
        if "east" in q_lower:
            region_filter = "AND region = 'East'"
        elif "west" in q_lower:
            region_filter = "AND region = 'West'"
        elif "north" in q_lower:
            region_filter = "AND region = 'North'"
        elif "south" in q_lower:
            region_filter = "AND region = 'South'"
            
        # Extract date/month filter if specified (e.g., June 2026)
        date_filter = ""
        if "june 2026" in q_lower:
            date_filter = "AND strftime('%Y-%m', date) = '2026-06'"
        elif "may 2026" in q_lower:
            date_filter = "AND strftime('%Y-%m', date) = '2026-05'"
        # Add more flexible date mappings as needed
            
        sql_query = f"""
            SELECT COUNT(id) 
            FROM orders 
            WHERE refund_usd > 0 
            {region_filter} 
            {date_filter}
        """

    # If no specific pattern matched, you can route to your LLM API or custom query generator here:
    if not sql_query:
        # Placeholder or LLM code block to generate SQL if custom rules don't cover it:
        raise HTTPException(status_code=400, detail="Unhandled question format pattern.")

    # Execute SQL against the SQLite database
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    try:
        cursor.execute(sql_query)
        row = cursor.fetchone()
        conn.close()
        
        result = row[0] if row else 0
        
        # Format numeric outputs to 2 decimal places if float
        if isinstance(result, float):
            result = round(result, 2)
            
        return {"answer": result}
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))
