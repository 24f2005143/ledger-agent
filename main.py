import os
import re
import time
import calendar
from datetime import datetime, timezone
from urllib.parse import unquote, urljoin

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


app = FastAPI(title="Ledger Agent API")

# Configuration: retain the existing Render environment variables.
BASE_URL = os.getenv(
    "UPSTREAM_BASE_URL",
    "https://exam.sanand.workers.dev/questionData",
)
EMAIL = os.getenv("UPSTREAM_EMAIL", "")
QUIZ_SIGN = unquote(os.getenv("UPSTREAM_QUIZ_SIGN", ""))
QUESTION_ID = os.getenv(
    "UPSTREAM_QUESTION_ID",
    "q-ledger-agent-server",
)
CACHE_SECONDS = int(os.getenv("CACHE_SECONDS", "300"))
UPSTREAM_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "20"))

REGIONS = {"north", "south", "east", "west", "central"}

MONTHS = {
    name.lower(): number
    for number, name in enumerate(calendar.month_name)
    if name
}
MONTHS.update({
    name.lower(): number
    for number, name in enumerate(calendar.month_abbr)
    if name
})


class QuestionRequest(BaseModel):
    question: str


cache = {
    "orders": [],
    "rates": {},
    "loaded": False,
    "last_refresh": None,
    "last_error": None,
    "paths": ["/orders", "/rates"],
}
last_cache_time = 0.0


def upstream_get(path=None, url=None):
    if not EMAIL or not QUIZ_SIGN:
        raise RuntimeError(
            "UPSTREAM_EMAIL or UPSTREAM_QUIZ_SIGN is missing."
        )

    if url:
        # Support absolute and relative pagination links.
        request_url = urljoin(BASE_URL, url)
        response = httpx.get(
            request_url,
            timeout=UPSTREAM_TIMEOUT,
            follow_redirects=True,
        )
    else:
        response = httpx.get(
            BASE_URL,
            params={
                "email": EMAIL,
                "quizSign": QUIZ_SIGN,
                "questionId": QUESTION_ID,
                "path": path,
            },
            timeout=UPSTREAM_TIMEOUT,
            follow_redirects=True,
        )

    response.raise_for_status()
    return response.json()


def fetch_all_paginated_data(path):
    result = []
    next_url = None
    seen = set()

    while True:
        data = (
            upstream_get(url=next_url)
            if next_url
            else upstream_get(path=path)
        )

        if isinstance(data, list):
            result.extend(row for row in data if isinstance(row, dict))
            break

        if not isinstance(data, dict):
            raise RuntimeError(
                f"{path}: expected a JSON object or list."
            )

        # Rates may be a single object containing a currency map.
        if path == "/rates" and isinstance(
            data.get("usd_per_unit"), dict
        ):
            return data

        page = None
        for key in ("orders", "data", "items", "records"):
            if isinstance(data.get(key), list):
                page = data[key]
                break

        if page is None:
            raise RuntimeError(
                f"{path}: no list of records found. "
                f"Response keys: {list(data.keys())}"
            )

        result.extend(row for row in page if isinstance(row, dict))

        next_value = data.get("next")
        if not next_value:
            break

        next_url = str(next_value)
        if next_url in seen:
            raise RuntimeError("Pagination loop detected.")
        seen.add(next_url)

    return result


def parse_datetime(value):
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(
            str(value).strip().replace("Z", "+00:00")
        )
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except (ValueError, TypeError, OverflowError):
        return None


def deduplicate_orders(orders):
    """Keep the newest version of each order ID."""
    latest = {}
    no_id = []

    for order in orders:
        if not isinstance(order, dict):
            continue

        identifier = (
            order.get("id")
            or order.get("order_id")
            or order.get("order_number")
            or order.get("invoice_id")
        )

        if identifier is None:
            no_id.append(order)
            continue

        key = str(identifier)
        previous = latest.get(key)

        if previous is None:
            latest[key] = order
            continue

        old_time = parse_datetime(previous.get("updated_at"))
        new_time = parse_datetime(order.get("updated_at"))

        # If timestamps are unavailable, retain the first version.
        if new_time and (
            old_time is None or new_time > old_time
        ):
            latest[key] = order

    return list(latest.values()) + no_id


def load_data(force=False):
    global last_cache_time

    now = time.time()
    if (
        cache["loaded"]
        and not force
        and now - last_cache_time < CACHE_SECONDS
    ):
        return

    try:
        raw_orders = fetch_all_paginated_data("/orders")
        rates_data = fetch_all_paginated_data("/rates")

        if not isinstance(raw_orders, list):
            raise RuntimeError("/orders did not return a list.")

        if not isinstance(rates_data, dict):
            raise RuntimeError("/rates did not return a JSON object.")

        raw_rates = rates_data.get("usd_per_unit")
        if not isinstance(raw_rates, dict) or not raw_rates:
            raise RuntimeError(
                "Expected /rates to contain usd_per_unit mapping."
            )

        rates = {}
        for currency, value in raw_rates.items():
            rate = float(value)
            if rate <= 0:
                raise RuntimeError(f"Invalid rate for {currency}.")
            rates[str(currency).upper()] = rate

        if "USD" not in rates:
            raise RuntimeError("USD rate is missing.")

        # Replace the cache only after both resources validate.
        cache["orders"] = deduplicate_orders(raw_orders)
        cache["rates"] = rates
        cache["paths"] = ["/orders", "/rates"]
        cache["last_refresh"] = datetime.now(
            timezone.utc
        ).isoformat()
        cache["last_error"] = None
        cache["loaded"] = True
        last_cache_time = now

    except Exception as exc:
        cache["last_error"] = str(exc)
        if not cache["loaded"]:
            raise


@app.on_event("startup")
def startup_event():
    try:
        load_data()
    except Exception as exc:
        print(f"Initial data load failed: {exc}")


def find_month_year(question):
    q = question.lower()

    match = re.search(r"\b(20\d{2})-(0?[1-9]|1[0-2])\b", q)
    if match:
        return int(match.group(1)), int(match.group(2))

    month_pattern = "|".join(
        re.escape(name)
        for name in sorted(MONTHS, key=len, reverse=True)
    )

    match = re.search(rf"\b({month_pattern})\s+(20\d{{2}})\b", q)
    if match:
        return int(match.group(2)), MONTHS[match.group(1)]

    match = re.search(rf"\b(20\d{{2}})\s+({month_pattern})\b", q)
    if match:
        return int(match.group(1)), MONTHS[match.group(2)]

    return None


def requested_currency(question):
    q = question.lower()

    if (
        "usd" in q
        or "us dollar" in q
        or "american dollar" in q
        or "$" in q
    ):
        return "USD"

    if (
        "inr" in q
        or "indian rupee" in q
        or "rupees" in q
        or "₹" in q
    ):
        return "INR"

    if "eur" in q or "euro" in q:
        return "EUR"

    return "USD"


def convert_amount(amount, source_currency, target_currency):
    source = str(source_currency or "").upper()
    target = str(target_currency or "").upper()
    rates = cache["rates"]

    if source not in rates:
        raise ValueError(f"Missing exchange rate for {source}.")
    if target not in rates:
        raise ValueError(f"Missing exchange rate for {target}.")

    # usd_per_unit = USD value of one unit of the currency.
    usd_amount = float(amount) * rates[source]
    return usd_amount / rates[target]


def filter_orders(question):
    q = question.lower()
    orders = list(cache["orders"])

    # Region
    for region in REGIONS:
        if re.search(rf"\b{region}\b", q):
            orders = [
                row for row in orders
                if str(row.get("region", "")).strip().lower() == region
            ]
            break

    # Product
    products = {
        str(row.get("product", "")).strip()
        for row in cache["orders"]
        if row.get("product")
    }

    for product in sorted(products, key=len, reverse=True):
        if product.lower() in q:
            orders = [
                row for row in orders
                if str(row.get("product", "")).strip().lower()
                == product.lower()
            ]
            break

    # Customer ID, e.g. C1234
    match = re.search(r"\bc\d{3,}\b", q, re.I)
    if match:
        customer_id = match.group(0).upper()
        orders = [
            row for row in orders
            if str(row.get("customer", "")).strip().upper() == customer_id
        ]

    # Date filters use created_at, as specified by the question wording.
    month_year = find_month_year(q)
    if month_year:
        year, month = month_year
        orders = [
            row for row in orders
            if (
                (dt := parse_datetime(row.get("created_at"))) is not None
                and dt.year == year
                and dt.month == month
            )
        ]
    elif re.search(r"\b20\d{2}\b", q):
        year = int(re.search(r"\b(20\d{2})\b", q).group(1))
        orders = [
            row for row in orders
            if (
                (dt := parse_datetime(row.get("created_at"))) is not None
                and dt.year == year
            )
        ]

    # Explicit status filters. "Refunded" is checked before other statuses.
    if re.search(r"\b(refund|refunded|refunds)\b", q):
        orders = [
            row for row in orders
            if str(row.get("status", "")).strip().lower() == "refunded"
        ]
    elif re.search(r"\b(cancelled|canceled|cancellation)\b", q):
        orders = [
            row for row in orders
            if str(row.get("status", "")).strip().lower()
            in {"cancelled", "canceled"}
        ]
    elif re.search(r"\b(pending|unpaid)\b", q):
        orders = [
            row for row in orders
            if str(row.get("status", "")).strip().lower()
            in {"pending", "unpaid"}
        ]
    elif re.search(r"\bpaid\b", q) and not re.search(r"\bunpaid\b", q):
        orders = [
            row for row in orders
            if str(row.get("status", "")).strip().lower() == "paid"
        ]
    elif any(
        word in q for word in ("revenue", "sales", "earn", "earned", "income")
    ):
        orders = [
            row for row in orders
            if str(row.get("status", "")).strip().lower() == "paid"
        ]

    return orders


def is_count_question(question):
    """Detect count requests before checking for amount-related words."""
    q = question.lower().strip()

    # Explicit distinct/unique customer requests.
    if re.search(r"\b(distinct|unique|different)\s+customers?\b", q):
        return True

    # Questions phrased as "Count the ... orders", "How many orders ...",
    # "number of refunded orders", etc.
    count_intent = (
        re.search(r"\bcount\b", q)
        or re.search(r"\bhow many\b", q)
        or re.search(r"\bnumber of\b", q)
        or re.search(r"\bquantity of\b", q)
    )
    count_subject = (
        re.search(r"\borders?\b", q)
        or re.search(r"\bcustomers?\b", q)
        or re.search(r"\btransactions?\b", q)
        or re.search(r"\brecords?\b", q)
    )
    return bool(count_intent and count_subject)


def answer_question_text(question):
    q = question.lower().strip()

    if not cache["loaded"]:
        load_data()

    orders = filter_orders(q)

    # Count distinct customers (after applying all region/date/status/product filters).
    if re.search(r"\b(distinct|unique|different)\s+customers?\b", q):
        customers = {
            str(row.get("customer", "")).strip().upper()
            for row in orders
            if row.get("customer") is not None
            and str(row.get("customer", "")).strip()
        }
        return len(customers)

    # Handle all count questions before amount logic. This prevents words such
    # as "refund/refunded" from accidentally triggering an amount total.
    if is_count_question(q):
        return len(orders)

    is_amount_question = any(
        word in q
        for word in (
            "revenue", "sales", "earn", "earned", "income",
            "refund", "refunded", "how much", "total amount", "total value",
        )
    )

    if not is_amount_question:
        raise HTTPException(
            status_code=400,
            detail="Unsupported question type.",
        )

    target = requested_currency(q)
    total = 0.0

    for order in orders:
        amount = order.get("amount")
        if amount is None:
            continue

        try:
            total += convert_amount(
                amount,
                order.get("currency"),
                target,
            )
        except (ValueError, TypeError) as exc:
            raise HTTPException(status_code=422, detail=str(exc))

    return round(total, 2)


@app.post("/")
@app.post("/answer")
@app.post("/query")
def answer_question(req: QuestionRequest):
    try:
        return {"answer": answer_question_text(req.question)}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Could not process the question: {exc}",
        )


@app.post("/refresh")
def refresh():
    try:
        load_data(force=True)
        return {
            "ok": True,
            "orders": len(cache["orders"]),
            "rates": cache["rates"],
            "last_refresh": cache["last_refresh"],
        }
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@app.get("/")
def root():
    return {
        "service": "Ledger Agent API",
        "ok": True,
        "endpoints": ["/health", "/debug/schema", "/"],
    }


@app.get("/health")
def health():
    return {
        "ok": cache["loaded"],
        "upstream_configured": bool(EMAIL and QUIZ_SIGN),
        "data_loaded": cache["loaded"],
        "collections": {
            "orders": len(cache["orders"]),
        },
        "discovered_paths": cache["paths"],
        "last_refresh": cache["last_refresh"],
        "last_error": cache["last_error"],
    }


@app.get("/debug/schema")
def debug_schema():
    fields = sorted({
        key
        for row in cache["orders"]
        for key in row.keys()
    })

    return {
        "loaded": cache["loaded"],
        "collections": {
            "orders": {
                "count": len(cache["orders"]),
                "fields": fields,
            },
            "rates": {
                "count": len(cache["rates"]),
                "fields": sorted(cache["rates"].keys()),
            },
        },
        "usd_per_unit": cache["rates"],
        "last_error": cache["last_error"],
    }

@app.get("/debug/revenue-check")
def debug_revenue_check():
    if not cache["loaded"]:
        load_data()

    matching = []

    for row in cache["orders"]:
        dt = parse_datetime(row.get("created_at"))

        if (
            str(row.get("region", "")).strip().lower() == "north"
            and dt is not None
            and dt.year == 2026
            and dt.month == 1
            and str(row.get("status", "")).strip().lower() == "paid"
        ):
            matching.append(row)

    amount_by_currency = {}
    calculated_by_currency = {}
    converted_amount_total = 0.0
    converted_calculated_total = 0.0

    for row in matching:
        currency = str(row.get("currency", "")).strip().upper()

        try:
            amount = float(row.get("amount") or 0)
            calculated = (
                float(row.get("unit_price") or 0)
                * float(row.get("qty") or 0)
            )

            amount_by_currency[currency] = (
                amount_by_currency.get(currency, 0) + amount
            )
            calculated_by_currency[currency] = (
                calculated_by_currency.get(currency, 0) + calculated
            )

            converted_amount_total += convert_amount(
                amount, currency, "USD"
            )
            converted_calculated_total += convert_amount(
                calculated, currency, "USD"
            )
        except (ValueError, TypeError, KeyError) as exc:
            return {
                "error": str(exc),
                "currency": currency,
            }

    return {
        "matching_paid_orders": len(matching),
        "amount_by_currency": {
            k: round(v, 2) for k, v in amount_by_currency.items()
        },
        "unit_price_times_qty_by_currency": {
            k: round(v, 2) for k, v in calculated_by_currency.items()
        },
        "amount_converted_to_usd": round(converted_amount_total, 2),
        "unit_price_times_qty_converted_to_usd": round(
            converted_calculated_total, 2
        ),
    }
