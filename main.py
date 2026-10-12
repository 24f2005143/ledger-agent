"""Acme Appliances Ledger Agent.

A small, rule-based question-answering API that discovers an undocumented JSON
API through the exam gateway, caches discovered resources, and answers common
revenue/refund/product/customer questions.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
from collections import defaultdict
from datetime import date, datetime
from typing import Any
from urllib.parse import urlparse, parse_qs, unquote

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

UPSTREAM_URL = os.getenv("UPSTREAM_URL", "https://exam.sanand.workers.dev/questionData")
UPSTREAM_EMAIL = os.getenv("UPSTREAM_EMAIL", "24f2005143@ds.study.iitm.ac.in").strip().lower()
UPSTREAM_QUIZ_SIGN = unquote(os.getenv("UPSTREAM_QUIZ_SIGN", "").strip())
UPSTREAM_QUESTION_ID = os.getenv("UPSTREAM_QUESTION_ID", "q-ledger-agent-server").strip()
CACHE_SECONDS = int(os.getenv("CACHE_SECONDS", "300"))
REQUEST_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "2.5"))

app = FastAPI(title="Acme Appliances Ledger Agent", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=False, allow_methods=["*"], allow_headers=["*"]
)

class QuestionRequest(BaseModel):
    question: str = Field(min_length=1, max_length=2000)

DATA: dict[str, list[dict[str, Any]]] = {}
RESOURCE_PATHS: list[str] = []
LAST_REFRESH = 0.0
REFRESH_LOCK = asyncio.Lock()
HTTP: httpx.AsyncClient | None = None

CANDIDATE_PATHS = [
    "/", "/openapi.json", "/swagger.json", "/api-docs", "/docs.json", "/schema",
    "/api", "/api/", "/api/v1", "/api/v1/", "/health", "/status", "/routes",
    "/api/routes", "/help", "/api/help", "/ledger", "/api/ledger", "/orders",
    "/orders/", "/api/orders", "/api/v1/orders", "/v1/orders", "/order-ledger",
    "/api/order-ledger", "/products", "/api/products", "/api/v1/products",
    "/customers", "/api/customers", "/api/v1/customers", "/refunds", "/api/refunds",
    "/api/v1/refunds", "/transactions", "/api/transactions", "/sales", "/api/sales",
    "/order-items", "/order_items", "/api/order-items", "/invoices", "/api/invoices",
]

TABLE_ALIASES = {
    "order": "orders", "orders": "orders", "order_ledger": "orders", "orderledger": "orders",
    "ledger_entries": "ledger", "ledger": "ledger", "transactions": "transactions",
    "transaction": "transactions", "sales": "sales", "sale": "sales", "invoices": "invoices",
    "invoice": "invoices", "refund": "refunds", "refunds": "refunds", "products": "products",
    "product": "products", "customers": "customers", "customer": "customers",
    "order_items": "order_items", "orderitems": "order_items", "line_items": "order_items",
    "lineitems": "order_items", "items": "order_items",
}


def norm(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(s).strip().lower()).strip("_")


def canon_json(obj: Any) -> str:
    try:
        return json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str)
    except Exception:
        return repr(obj)


def get_client() -> httpx.AsyncClient:
    global HTTP
    if HTTP is None or HTTP.is_closed:
        HTTP = httpx.AsyncClient(timeout=httpx.Timeout(REQUEST_TIMEOUT), follow_redirects=True,
                                 headers={"User-Agent": "Acme-Ledger-Agent/1.0"})
    return HTTP

async def upstream_get(path: str) -> tuple[Any, str, int]:
    """Fetch an API path through the exam's gateway."""
    if not UPSTREAM_QUIZ_SIGN:
        return {"error": "UPSTREAM_QUIZ_SIGN is not configured"}, "application/json", 500
    # Keep the API path (including its query string) in the path query parameter.
    params = {
        "email": UPSTREAM_EMAIL,
        "quizSign": UPSTREAM_QUIZ_SIGN,
        "questionId": UPSTREAM_QUESTION_ID,
        "path": path or "/",
    }
    try:
        response = await get_client().get(UPSTREAM_URL, params=params)
        ctype = response.headers.get("content-type", "")
        text = response.text
        try:
            payload = response.json()
        except Exception:
            payload = text
        return payload, ctype, response.status_code
    except Exception as exc:
        return {"_fetch_error": str(exc)}, "application/json", 599


def add_path(paths: set[str], value: Any) -> None:
    if not isinstance(value, str):
        return
    value = value.strip()
    if not value:
        return
    # Strip host if an API responds with absolute links.
    if value.startswith("http://") or value.startswith("https://"):
        parsed = urlparse(value)
        value = parsed.path + (("?" + parsed.query) if parsed.query else "")
    if value.startswith("/") and len(value) < 500 and not value.startswith("//"):
        paths.add(value)


def discover_paths_from_payload(payload: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(payload, dict):
        # OpenAPI/Swagger route discovery.
        openapi_paths = payload.get("paths")
        if isinstance(openapi_paths, dict):
            for p, methods in openapi_paths.items():
                add_path(found, p)
                if isinstance(methods, dict):
                    for method, details in methods.items():
                        if isinstance(details, dict):
                            for param in details.get("parameters", []) or []:
                                if isinstance(param, dict) and param.get("in") == "path":
                                    pass
        for key, value in payload.items():
            kl = norm(key)
            if kl in {"href", "url", "path", "endpoint", "route", "uri", "next", "next_url", "self", "collection", "resource"}:
                if isinstance(value, str): add_path(found, value)
                elif isinstance(value, dict):
                    for subkey in ("href", "url", "path", "uri"):
                        add_path(found, value.get(subkey))
            found |= discover_paths_from_payload(value)
    elif isinstance(payload, list):
        for value in payload:
            found |= discover_paths_from_payload(value)
    elif isinstance(payload, str):
        # HTML link hrefs and path literals in a root/help response.
        for m in re.findall(r'(?:href|action)=["\']([^"\']+)', payload, flags=re.I):
            add_path(found, m)
        for m in re.findall(r'(?<![A-Za-z0-9])/(?:api/)?[A-Za-z0-9_.{}-]+(?:/[A-Za-z0-9_.{}-]+)*(?:\?[^\s"\']*)?', payload):
            add_path(found, m.rstrip(".,);"))
    return found


def table_key(name: str) -> str:
    n = norm(name)
    if n in TABLE_ALIASES:
        return TABLE_ALIASES[n]
    # Trim common wrappers and version markers.
    n = re.sub(r"^(api|v[0-9]+)_", "", n)
    return TABLE_ALIASES.get(n, n or "records")


def recordish_array(value: list[Any]) -> list[dict[str, Any]]:
    return [x for x in value if isinstance(x, dict)]


def extract_tables(payload: Any, path: str) -> dict[str, list[dict[str, Any]]]:
    """Collect arrays of objects from a response, keeping their semantic key."""
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    route_name = path.split("?", 1)[0].rstrip("/").split("/")[-1]
    route_table = table_key(route_name) if route_name else ""
    seen: set[tuple[str, str]] = set()

    def put(key: str, rows: list[dict[str, Any]]) -> None:
        k = table_key(key)
        if not rows:
            return
        for row in rows:
            # De-duplicate identical records within and across endpoints.
            identity = (k, canon_json(row))
            if identity not in seen:
                seen.add(identity)
                out[k].append(row)

    if isinstance(payload, list):
        rows = recordish_array(payload)
        if rows:
            put(route_table or "records", rows)
    elif isinstance(payload, dict):
        # A specific collection route is stronger evidence than generic `data`.
        recognized = False
        for key, value in payload.items():
            if isinstance(value, list):
                rows = recordish_array(value)
                if rows:
                    if norm(key) in {"data", "items", "results", "records", "rows", "entries"} and route_table:
                        put(route_table, rows)
                    else:
                        put(key, rows)
                    recognized = True
        # Generic paginated envelope where list is nested one level down.
        if not recognized and route_table and any(k in norm(key) for key in payload for k in ("data", "results", "items", "records")):
            for key in ("data", "results", "items", "records", "rows", "entries"):
                value = payload.get(key)
                if isinstance(value, list) and recordish_array(value):
                    put(route_table, recordish_array(value)); recognized = True
        if not recognized and any(k in payload for k in ("id", "order_id", "customer_id", "product_id")):
            put(route_table or "records", [payload])
        # Also collect nested table arrays, such as {ledger: {orders: [...]}}.
        for key, value in payload.items():
            if isinstance(value, (dict, list)):
                nested = extract_tables(value, key)
                for nk, rows in nested.items():
                    for row in rows:
                        identity = (nk, canon_json(row))
                        if identity not in seen:
                            seen.add(identity); out[nk].append(row)
    return dict(out)


def json_object_has_data(payload: Any) -> bool:
    if isinstance(payload, list):
        return bool(payload and isinstance(payload[0], dict))
    if isinstance(payload, dict):
        if any(isinstance(v, list) and v and isinstance(v[0], dict) for v in payload.values()):
            return True
        return any(k in payload for k in ("openapi", "swagger", "paths", "_links", "endpoints", "routes"))
    return False


def get_pagination(payload: Any, requested_path: str) -> list[str]:
    paths: list[str] = []
    if not isinstance(payload, dict):
        return paths
    for key in ("next", "next_url", "nextPageUrl", "next_page_url", "next_page"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            add_path_set = set(); add_path(add_path_set, value)
            paths.extend(add_path_set)
        elif isinstance(value, dict):
            for sub in ("href", "url", "path"):
                add_path_set = set(); add_path(add_path_set, value.get(sub)); paths.extend(add_path_set)
    # Numeric page/offset pagination.
    total_pages = payload.get("total_pages", payload.get("totalPages", payload.get("pages")))
    page = payload.get("page", payload.get("current_page", payload.get("currentPage")))
    if isinstance(total_pages, int) and 1 <= total_pages <= 100:
        current = int(page or 1)
        for next_page in range(current + 1, min(total_pages, current + 10) + 1):
            sep = "&" if "?" in requested_path else "?"
            paths.append(f"{requested_path}{sep}page={next_page}")
    elif payload.get("has_more") is True or payload.get("hasMore") is True:
        try: current = int(page or 1)
        except Exception: current = 1
        sep = "&" if "?" in requested_path else "?"
        paths.append(f"{requested_path}{sep}page={current + 1}")
    return paths

async def get_follow_pages(path: str, payload: Any) -> list[tuple[str, Any]]:
    """Follow explicit pagination cursors/pages with a small safety limit."""
    rows: list[tuple[str, Any]] = []
    visited = {path}
    todo = get_pagination(payload, path)[:10]
    for _ in range(10):
        todo = [p for p in todo if p not in visited]
        if not todo: break
        batch = todo[:4]
        visited.update(batch)
        results = await asyncio.gather(*(upstream_get(p) for p in batch), return_exceptions=True)
        todo = []
        for p, result in zip(batch, results):
            if isinstance(result, Exception): continue
            obj, _, status = result
            if status == 200 and not (isinstance(obj, dict) and "_fetch_error" in obj):
                rows.append((p, obj)); todo.extend(get_pagination(obj, p))
    return rows

async def refresh_data(force: bool = False) -> None:
    global DATA, RESOURCE_PATHS, LAST_REFRESH
    if not UPSTREAM_QUIZ_SIGN:
        return
    if not force and DATA and time.time() - LAST_REFRESH < CACHE_SECONDS:
        return
    async with REFRESH_LOCK:
        if not force and DATA and time.time() - LAST_REFRESH < CACHE_SECONDS:
            return
        collected: dict[str, list[dict[str, Any]]] = defaultdict(list)
        path_set = set(CANDIDATE_PATHS)
        fetched: dict[str, Any] = {}
        # Fetch root first because it often explains the undocumented API.
        root, _, root_status = await upstream_get("/")
        fetched["/"] = root
        path_set |= discover_paths_from_payload(root)
        # If a schema is linked by root, fetch it before route probing.
        schema_paths = [p for p in list(path_set) if "openapi" in p.lower() or "swagger" in p.lower()]
        for p in schema_paths[:5]:
            obj, _, code = await upstream_get(p)
            if code == 200:
                fetched[p] = obj; path_set |= discover_paths_from_payload(obj)
        # Parallel probes. Skip docs UI paths (their HTML rarely contains data).
        probe_paths = [p for p in sorted(path_set) if p not in fetched and p not in {"/docs", "/api-docs"}]
        sem = asyncio.Semaphore(10)
        async def probe(path: str):
            async with sem:
                return path, await upstream_get(path)
        results = await asyncio.gather(*(probe(p) for p in probe_paths[:55]), return_exceptions=True)
        newly_discovered: set[str] = set()
        for result in results:
            if isinstance(result, Exception): continue
            path, (obj, _, code) = result
            if code == 200 and not (isinstance(obj, dict) and ("_fetch_error" in obj or "error" in obj)):
                fetched[path] = obj
                discovered = discover_paths_from_payload(obj)
                newly_discovered |= discovered
                path_set |= discovered
        # Follow paths explicitly advertised by the root/API listing, even if
        # they do not resemble the common REST endpoint names used above.
        dynamic = [p for p in sorted(newly_discovered) if p not in fetched]
        if dynamic:
            results = await asyncio.gather(*(probe(p) for p in dynamic[:35]), return_exceptions=True)
            for result in results:
                if isinstance(result, Exception): continue
                path, (obj, _, code) = result
                if code == 200 and not (isinstance(obj, dict) and ("_fetch_error" in obj or "error" in obj)):
                    fetched[path] = obj
                    path_set |= discover_paths_from_payload(obj)
        # If an OpenAPI spec surfaced during probes, probe its listed GET paths too.
        extra = set()
        for p, obj in fetched.items():
            if isinstance(obj, dict) and isinstance(obj.get("paths"), dict):
                for route, methods in obj["paths"].items():
                    if isinstance(methods, dict) and "get" in methods:
                        add_path(extra, route)
        extras = [p for p in sorted(extra) if p not in fetched][:30]
        if extras:
            results = await asyncio.gather(*(probe(p) for p in extras), return_exceptions=True)
            for result in results:
                if isinstance(result, Exception): continue
                path, (obj, _, code) = result
                if code == 200 and not (isinstance(obj, dict) and ("_fetch_error" in obj or "error" in obj)):
                    fetched[path] = obj
        # Extract top-level collections and explicit pagination.
        for path, obj in list(fetched.items()):
            tables = extract_tables(obj, path)
            for t, rows in tables.items(): collected[t].extend(rows)
            if obj is not None:
                pages = await get_follow_pages(path, obj)
                for page_path, page_obj in pages:
                    for t, rows in extract_tables(page_obj, page_path).items(): collected[t].extend(rows)
        # Final cross-route dedupe.
        clean: dict[str, list[dict[str, Any]]] = {}
        for key, rows in collected.items():
            seen: set[str] = set(); clean_rows = []
            for row in rows:
                signature = canon_json(row)
                if signature not in seen:
                    seen.add(signature); clean_rows.append(row)
            if clean_rows: clean[key] = clean_rows
        DATA = clean
        RESOURCE_PATHS = sorted(fetched)
        LAST_REFRESH = time.time()


def key_values(obj: Any, wanted: set[str]) -> list[Any]:
    found: list[Any] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if norm(k) in wanted and v is not None and not isinstance(v, (dict, list)):
                found.append(v)
            if isinstance(v, (dict, list)):
                found.extend(key_values(v, wanted))
    elif isinstance(obj, list):
        for item in obj: found.extend(key_values(item, wanted))
    return found


def first_value(obj: Any, keys: list[str]) -> Any:
    vals = key_values(obj, {norm(k) for k in keys})
    return vals[0] if vals else None


def as_number(v: Any) -> float | None:
    if isinstance(v, bool) or v is None: return None
    if isinstance(v, (int, float)): return float(v)
    if isinstance(v, str):
        s = v.strip().replace(",", "")
        s = re.sub(r"[^0-9.\-]", "", s)
        if not s or s in {"-", ".", "-."}: return None
        try: return float(s)
        except ValueError: return None
    return None


def parse_date(v: Any) -> date | None:
    if not isinstance(v, (str, int, float)): return None
    s = str(v).strip()
    if not s: return None
    try:
        if re.fullmatch(r"\d{4}-\d{2}", s): return date.fromisoformat(s + "-01")
        if re.fullmatch(r"\d{4}", s): return date(int(s), 1, 1)
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        try: return date.fromisoformat(s[:10])
        except Exception: return None


def records_for_kind(kind: str) -> list[dict[str, Any]]:
    aliases = {
        "orders": ["orders", "order", "order_ledger", "orderledger", "invoices"],
        "refunds": ["refunds", "refund"],
        "products": ["products", "product"],
        "customers": ["customers", "customer"],
        "transactions": ["transactions", "transaction", "sales", "sale", "ledger"],
        "order_items": ["order_items", "items", "line_items", "orderitems"],
    }
    matches: list[dict[str, Any]] = []
    for alias in aliases.get(kind, [kind]):
        matches.extend(DATA.get(alias, []))
    # Deduplicate if aliases overlap.
    out=[]; seen=set()
    for row in matches:
        s=canon_json(row)
        if s not in seen: seen.add(s); out.append(row)
    return out


def all_records() -> list[dict[str, Any]]:
    out=[]; seen=set()
    for rows in DATA.values():
        for r in rows:
            s=canon_json(r)
            if s not in seen: seen.add(s); out.append(r)
    return out


def record_text(obj: Any) -> str:
    if isinstance(obj, dict):
        return " ".join(record_text(v) for v in obj.values())
    if isinstance(obj, list): return " ".join(record_text(v) for v in obj)
    return str(obj) if obj is not None else ""


def extract_month_filter(question: str) -> tuple[date | None, date | None]:
    months = {"january":1,"jan":1,"february":2,"feb":2,"march":3,"mar":3,"april":4,"apr":4,
              "may":5,"june":6,"jun":6,"july":7,"jul":7,"august":8,"aug":8,"september":9,
              "sep":9,"sept":9,"october":10,"oct":10,"november":11,"nov":11,"december":12,"dec":12}
    q=question.lower()
    for name, month in months.items():
        m=re.search(rf"\b{name}\s+(20\d{{2}})\b", q)
        if m:
            year=int(m.group(1)); start=date(year,month,1)
            end=date(year+1,1,1) if month==12 else date(year,month+1,1)
            return start,end
    # ISO date range: between/on/after/before YYYY-MM-DD
    m=re.search(r"\b(?:between|from)\s+(20\d{2}-\d{2}-\d{2})\s+(?:and|to)\s+(20\d{2}-\d{2}-\d{2})", q)
    if m:
        a=date.fromisoformat(m.group(1)); b=date.fromisoformat(m.group(2))
        from datetime import timedelta
        return a,b+timedelta(days=1)
    m=re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", q)
    if m:
        a=date.fromisoformat(m.group(1)); from datetime import timedelta
        return a,a+timedelta(days=1)
    return None,None


def get_region(obj: Any, customer_map: dict[str, dict[str, Any]]) -> str | None:
    # Prefer explicit region fields; recurse through nested addresses.
    vals=key_values(obj, {"region", "sales_region", "shipping_region", "billing_region", "territory"})
    if vals: return str(vals[0])
    cid=first_value(obj,["customer_id","customerId","customer"])
    if cid is not None:
        c=customer_map.get(str(cid))
        if c:
            vals=key_values(c,{"region","sales_region","shipping_region","billing_region","territory"})
            if vals: return str(vals[0])
    return None


def join_customer_map() -> dict[str, dict[str, Any]]:
    out={}
    for customer in records_for_kind("customers"):
        ident=first_value(customer,["customer_id","customerId","id","code"])
        if ident is not None: out[str(ident)]=customer
    return out


def record_date(obj: Any) -> date | None:
    value=first_value(obj,["order_date","orderDate","created_at","createdAt","transaction_date","transactionDate","date","issued_at","timestamp","purchase_date","refund_date"])
    return parse_date(value)


def amount_value(obj: Any) -> float | None:
    # Avoid a nested unit-price taking precedence over an explicitly recorded total.
    wanted = {"total_amount", "totalamount", "order_total", "ordertotal", "grand_total", "grandtotal",
              "net_amount", "netamount", "gross_amount", "grossamount", "amount_usd", "amountusd",
              "revenue_usd", "revenueusd", "total", "amount", "revenue", "subtotal", "sales_amount", "salesamount"}
    if isinstance(obj, dict):
        for k in ("total_amount","order_total","grand_total","net_amount","gross_amount","amount_usd","revenue_usd","total","amount","revenue","subtotal"):
            if k in obj:
                v=as_number(obj[k])
                if v is not None: return v
        for k,v in obj.items():
            if isinstance(v,dict):
                got=amount_value(v)
                if got is not None: return got
    vals=key_values(obj,wanted)
    for v in vals:
        n=as_number(v)
        if n is not None: return n
    return None


def product_name(obj: Any) -> str | None:
    v=first_value(obj,["product_name","productName","name","product","title","sku_name","item_name","itemName"])
    if isinstance(v,dict):
        v=first_value(v,["name","product_name","title","sku"])
    return str(v) if v is not None else None


def customer_name(obj: Any, customer_map: dict[str, dict[str, Any]]) -> str | None:
    v=first_value(obj,["customer_name","customerName","customer","buyer_name","buyerName","name"])
    if isinstance(v,dict): v=first_value(v,["name","full_name","fullName","email"])
    if v is not None and isinstance(v,(str,int,float)):
        # If customer is a foreign-key and table has details, resolve it.
        s=str(v)
        if s in customer_map:
            return str(first_value(customer_map[s],["name","full_name","fullName","email"]) or s)
        return s
    cid=first_value(obj,["customer_id","customerId"])
    if cid is not None and str(cid) in customer_map:
        c=customer_map[str(cid)]
        return str(first_value(c,["name","full_name","fullName","email"]) or cid)
    return None


def find_filter_values(question: str) -> dict[str, Any]:
    q=question.lower()
    start,end=extract_month_filter(q)
    region=None
    for r in ("north", "south", "east", "west", "central", "northeast", "northwest", "southeast", "southwest"):
        if re.search(rf"\b{r}\s+(?:region|area|territory)\b|\b(?:region|area|territory)\s+(?:of\s+)?{r}\b", q):
            region=r; break
    # Direct mention like "in North" is common, but avoid inferring arbitrary common words.
    if region is None:
        m=re.search(r"\b(?:in|from|for)\s+the\s+(north|south|east|west|central|northeast|northwest|southeast|southwest)\b", q)
        if m: region=m.group(1)
    currency="usd" if re.search(r"\b(?:usd|us dollars?|dollars?)\b", q) else None
    return {"start":start,"end":end,"region":region,"currency":currency}


def filtered(records: list[dict[str, Any]], question: str, *, customer_map=None) -> list[dict[str, Any]]:
    filt=find_filter_values(question); customer_map=customer_map or join_customer_map()
    result=[]
    for row in records:
        d=record_date(row)
        if filt["start"] and (d is None or d < filt["start"]): continue
        if filt["end"] and (d is None or d >= filt["end"]): continue
        if filt["region"]:
            rg=get_region(row,customer_map)
            # If a top-level row is a line item, its nested parent may include the region.
            if rg is None or filt["region"] not in rg.lower(): continue
        if filt["currency"]:
            cv=first_value(row,["currency","currency_code","currencyCode"]) 
            if cv is not None and str(cv).lower() not in {"usd","$","us dollars","dollar"}: continue
        result.append(row)
    return result


def discover_products_in_orders(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    lines=[]
    for order in orders:
        inherited={k:v for k,v in order.items() if not isinstance(v,(dict,list))}
        for key in ("items","line_items","lineItems","order_items","orderItems","products"):
            arr=order.get(key)
            if isinstance(arr,list):
                for item in arr:
                    if isinstance(item,dict):
                        row=dict(inherited); row.update(item)
                        lines.append(row)
    return lines


def sales_records(question: str) -> tuple[list[dict[str, Any]], str]:
    orders=records_for_kind("orders")
    lines=records_for_kind("order_items")
    sales=records_for_kind("sales")
    transactions=records_for_kind("transactions")
    ledger=DATA.get("ledger",[])
    # If order rows include total amounts, use their totals to avoid double-counting line items.
    if orders and any(amount_value(o) is not None for o in orders): return orders,"orders"
    embedded=discover_products_in_orders(orders)
    if lines: return lines,"order_items"
    if embedded: return embedded,"order_items"
    if sales: return sales,"sales"
    if transactions: return transactions,"transactions"
    if ledger: return ledger,"ledger"
    return all_records(),"records"


def amount_with_lines(row: dict[str, Any]) -> float | None:
    # If no explicit total but quantity x unit price exist, compute the line amount.
    direct=amount_value(row)
    if direct is not None: return direct
    qty=as_number(first_value(row,["quantity","qty","units","count"]))
    price=as_number(first_value(row,["unit_price","unitPrice","price","sale_price","salePrice"]))
    if qty is not None and price is not None: return qty*price
    return None


def status_of(row: dict[str, Any]) -> str:
    return str(first_value(row,["status","type","transaction_type","transactionType","kind","entry_type","entryType"]) or "").lower()


def answer_question(question: str) -> Any:
    q=question.lower().strip()
    if not DATA:
        raise HTTPException(status_code=503, detail="Ledger API data is not loaded yet. Check UPSTREAM_QUIZ_SIGN and /health.")
    customer_map=join_customer_map()
    all_rows=all_records()
    is_refund=any(w in q for w in ("refund", "refunded", "refunds"))
    is_customer=any(w in q for w in ("customer", "buyer", "client"))
    is_product=any(w in q for w in ("product", "item", "sku"))
    is_count=bool(re.search(r"\b(how many|number of|count|how much many)\b", q))
    is_average=any(w in q for w in ("average", "avg", "mean"))
    is_top=any(w in q for w in ("most", "highest", "top", "largest", "best-selling", "best selling", "least", "lowest"))
    is_revenue=any(w in q for w in ("revenue", "sales", "sold", "earned", "income", "turnover", "spend", "spent", "total amount", "total value", "order value"))
    is_order= "order" in q or "orders" in q

    # Explicit customer/product table questions.
    if is_count and is_customer and not is_revenue:
        rows=filtered(records_for_kind("customers"),question,customer_map=customer_map)
        return len(rows)
    if is_count and is_product and not is_revenue:
        rows=filtered(records_for_kind("products"),question,customer_map=customer_map)
        return len(rows)
    if is_count and ("order" in q or "orders" in q):
        rows=filtered(records_for_kind("orders") or records_for_kind("sales") or records_for_kind("transactions"),question,customer_map=customer_map)
        return len(rows)

    # Refund totals/records are separate when exposed by the API.
    if is_refund:
        refund_rows=records_for_kind("refunds")
        if not refund_rows:
            refund_rows=[r for r in all_rows if "refund" in status_of(r) or "refund" in record_text(r).lower()]
        refund_rows=filtered(refund_rows,question,customer_map=customer_map)
        if is_count: return len(refund_rows)
        amounts=[amount_with_lines(r) for r in refund_rows]
        amounts=[x for x in amounts if x is not None]
        if amounts: return round(sum(abs(x) for x in amounts),2)
        if is_top and refund_rows:
            return record_text(refund_rows[0])
        return 0.0

    # Product-specific aggregate: prefer order lines or embedded line items.
    if is_product and is_revenue:
        orders=records_for_kind("orders")
        line_rows=records_for_kind("order_items") or discover_products_in_orders(orders)
        rows=line_rows or orders or records_for_kind("sales") or records_for_kind("transactions")
        rows=filtered(rows,question,customer_map=customer_map)
        aggregates: dict[str,float]=defaultdict(float)
        counts: dict[str,int]=defaultdict(int)
        for row in rows:
            name=product_name(row)
            if not name: continue
            amt=amount_with_lines(row)
            qty=as_number(first_value(row,["quantity","qty","units"])) or 1
            if amt is not None: aggregates[name]+=amt
            counts[name]+=int(qty)
        if is_top and aggregates:
            reverse="least" not in q and "lowest" not in q
            return sorted(aggregates.items(),key=lambda x:x[1],reverse=reverse)[0][0]
        if is_count and counts:
            return sum(counts.values())
        # Extract a named product from the question and sum only it.
        matches=[name for name in set(list(aggregates)+list(counts)) if name.lower() in q]
        if matches:
            name=max(matches,key=len)
            return round(aggregates.get(name,0.0),2) if is_revenue else counts.get(name,0)
        if aggregates and ("by product" in q or "each product" in q or "per product" in q):
            return {k:round(v,2) for k,v in sorted(aggregates.items())}

    # Customer aggregate by spending/orders.
    if is_customer and (is_revenue or is_top):
        orders=records_for_kind("orders") or records_for_kind("sales") or records_for_kind("transactions")
        orders=filtered(orders,question,customer_map=customer_map)
        totals: dict[str,float]=defaultdict(float); counts: dict[str,int]=defaultdict(int)
        for row in orders:
            name=customer_name(row,customer_map)
            if not name: continue
            amt=amount_with_lines(row)
            if amt is not None: totals[name]+=amt
            counts[name]+=1
        if is_top:
            d=counts if is_count else totals
            if d:
                reverse="least" not in q and "lowest" not in q
                return sorted(d.items(),key=lambda x:x[1],reverse=reverse)[0][0]
        matches=[name for name in set(list(totals)+list(counts)) if name.lower() in q]
        if matches:
            name=max(matches,key=len)
            return round(totals.get(name,0.0),2) if is_revenue else counts.get(name,0)
        if "by customer" in q or "per customer" in q or "each customer" in q:
            return {k:round(v,2) for k,v in sorted(totals.items())}

    # Revenue/orders for a region, period, or generally.
    rows, kind=sales_records(question)
    rows=filtered(rows,question,customer_map=customer_map)
    # Revenue may have ledger transactions where refunds are represented as negative entries.
    amounts=[]
    for row in rows:
        st=status_of(row)
        if any(t in st for t in ("refund", "cancelled", "canceled", "void")) and "net" not in q:
            continue
        amount=amount_with_lines(row)
        if amount is not None: amounts.append(amount)
    if is_top and not is_product and not is_customer and rows:
        if "order" in q:
            candidates=[(amount_with_lines(r),r) for r in rows]
            candidates=[x for x in candidates if x[0] is not None]
            if candidates:
                return round(max(candidates,key=lambda x:x[0])[0],2)
    if is_average:
        if amounts: return round(sum(amounts)/len(amounts),2)
        return 0.0
    if amounts:
        return round(sum(amounts),2)
    if is_count: return len(rows)

    # If user asks for a direct lookup of a named product/customer/order, search all rows.
    for row in all_rows:
        text=record_text(row)
        m=re.search(r"\b(?:order|customer|product|sku)\s*(?:id|number|#)?\s*[:#]?\s*([A-Za-z0-9_-]+)", q)
        if m and m.group(1).lower() in text.lower():
            return row
    # Useful graceful fallback: number of records in the most relevant collection.
    if is_customer and DATA.get("customers"): return len(DATA["customers"])
    if is_product and DATA.get("products"): return len(DATA["products"])
    if "refund" in q and DATA.get("refunds"): return len(DATA["refunds"])
    if DATA.get("orders"): return len(DATA["orders"])
    return {"error":"Could not infer this question from discovered ledger data", "available_collections": {k:len(v) for k,v in DATA.items()}}

@app.on_event("startup")
async def startup() -> None:
    # Start warming the cache without making the ASGI startup health check wait.
    asyncio.create_task(refresh_data())

@app.get("/")
async def root():
    return {"service":"Acme Appliances Ledger Agent", "ready":bool(DATA), "answer_endpoint":"POST /", "health":"/health"}

@app.get("/health")
async def health():
    return {"ok":True, "upstream_configured":bool(UPSTREAM_QUIZ_SIGN), "data_loaded":bool(DATA),
            "collections":{k:len(v) for k,v in DATA.items()}, "discovered_paths":len(RESOURCE_PATHS),
            "last_refresh":datetime.fromtimestamp(LAST_REFRESH).isoformat() if LAST_REFRESH else None}

@app.post("/")
@app.post("/answer")
@app.post("/query")
async def ask(req: QuestionRequest):
    if not DATA or time.time()-LAST_REFRESH > CACHE_SECONDS:
        try:
            await asyncio.wait_for(refresh_data(force=False), timeout=12.0)
        except asyncio.TimeoutError:
            if not DATA:
                raise HTTPException(status_code=504, detail="Timed out discovering upstream ledger API")
    try:
        ans=answer_question(req.question)
        return {"answer":ans}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not answer question: {type(exc).__name__}: {exc}")

@app.post("/refresh")
async def refresh():
    await refresh_data(force=True)
    return {"ok":True,"collections":{k:len(v) for k,v in DATA.items()},"discovered_paths":len(RESOURCE_PATHS)}

# Useful when manually exploring, but does not expose ledger records.
@app.get("/debug/schema")
async def debug_schema():
    return {"collections":{k:len(v) for k,v in DATA.items()},"paths":RESOURCE_PATHS[:100],"loaded":bool(DATA)}
