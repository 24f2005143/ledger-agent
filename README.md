# Acme Appliances Ledger Agent

A FastAPI POST endpoint that attempts to discover the user's private, undocumented ledger API through the IITM exam `questionData` gateway and answer common questions about revenue, refunds, products and customers.

## Environment

Set these environment variables on your host:

- `UPSTREAM_EMAIL=24f2005143@ds.study.iitm.ac.in`
- `UPSTREAM_QUIZ_SIGN=<the decoded quizSign value from your assigned URL>`
- `UPSTREAM_QUESTION_ID=q-ledger-agent-server`
- `CACHE_SECONDS=300` (optional)

Do not commit the quiz signature to a public repository. Render's Blueprint asks for `UPSTREAM_QUIZ_SIGN` during the initial setup because `sync: false` is set.

## Run locally

```bash
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:UPSTREAM_QUIZ_SIGN="YOUR_DECODED_QUIZ_SIGN"
uvicorn main:app --reload --port 8000
```

POST JSON to `http://127.0.0.1:8000/`:

```json
{"question":"What was the total revenue in USD from the North region in March 2026?"}
```

Inspect `/health` and `/debug/schema` after the service starts to confirm which API collections were discovered.
