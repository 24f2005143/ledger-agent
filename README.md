# Acme Appliances Ledger Agent

FastAPI endpoint for the assigned, undocumented Acme Appliances order-ledger API.

## Render environment variables

Set these in **Render → your service → Environment**:

- `UPSTREAM_EMAIL`: your assigned email
- `UPSTREAM_QUIZ_SIGN`: the `quizSign` value from your assigned `questionData` URL. The application accepts either the encoded value (with `%2F`, `%2B`, `%3D`, etc.) or the decoded value.
- `UPSTREAM_QUESTION_ID`: `q-ledger-agent-server`
- `CACHE_SECONDS`: `300`
- `UPSTREAM_TIMEOUT`: `8`

Do not commit your quiz signature to GitHub or share it publicly.

## Local run (Windows PowerShell)

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:UPSTREAM_EMAIL = "YOUR_ASSIGNED_EMAIL"
$env:UPSTREAM_QUIZ_SIGN = "YOUR_ASSIGNED_QUIZ_SIGN"
$env:UPSTREAM_QUESTION_ID = "q-ledger-agent-server"
uvicorn main:app --reload --port 8000
```

Check data loading:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
Invoke-RestMethod http://127.0.0.1:8000/debug/schema
```

Test a question:

```powershell
$body = @{ question = "What was the total revenue in USD from the North region in March 2026?" } | ConvertTo-Json
Invoke-RestMethod -Uri http://127.0.0.1:8000/ -Method Post -ContentType "application/json" -Body $body
```

## Notes

- The app discovers resources from the upstream root response and follows advertised pagination.
- It keeps the latest `updated_at` version of a repeated order ID.
- Revenue sums only orders whose current status is `paid`.
- `/health` reports whether the upstream signature is set and whether records have loaded. `/debug/schema` reports collection sizes and field names without exposing record contents.
