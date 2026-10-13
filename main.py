from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import re
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
UPSTREAM_URL = os.getenv(
    "UPSTREAM_URL", "https://exam.sanand.workers.dev/questionData"
).strip()
UPSTREAM_EMAIL = os.getenv(
    "UPSTREAM_EMAIL", "24f2005143@ds.study.iitm.ac.in"
).strip()
# unquote() accepts either the percent-encoded query value copied from the URL
# or its decoded value. unquote (not unquote_plus) preserves literal '+' chars.
UPSTREAM_QUIZ_SIGN = unquote(os.getenv("UPSTREAM_QUIZ_SIGN", "").strip())
UPSTREAM_QUESTION_ID = os.getenv(
    "UPSTREAM_QUESTION_ID", "q-ledger-agent-server"
).strip()
CACHE_SECONDS = max(30, int(os.getenv("CACHE_SECONDS", "300")))
UPSTREAM_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "8"))
MAX_PAGES = min(1000, max(1, int(os.getenv("MAX_PAGES", "300"))))
LOCAL_TZ = ZoneInfo("Asia/Kolkata")

app = FastAPI(title="Acme Appliances Ledger Agent", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class QuestionRequest(BaseModel):
    question: str = Field(min_length=1, max_length=2000)


DATA: dict[str, list[dict[str, Any]]] = {}
RESOURCE_PATHS: list[str] = []
LAST_REFRESH = 0.0
LAST_ERROR: str | None = None
REFRESH_LOCK = asyncio.Lock()
HTTP: httpx.AsyncClient | None = None


# ---------------------------------------------------------------------------
# Generic field/data helpers
# ---------------------------------------------------------------------------
def norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def canon_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def table_key(value: str) -> str:
    n = norm(value)
    aliases = {
        "order": "orders", "orders": "orders", "order_ledger": "orders",
        "orderledger": "orders", "invoice": "orders", "invoices": "orders",
        "sale": "orders", "sales": "orders",
        "refund": "refunds", "refunds": "refunds", "return": "refunds",
        "returns": "refunds",
        "product": "products", "products": "products", "sku": "products",
        "skus": "products", "item": "order_items", "items": "order_items",
        "line_item": "order_items", "line_items": "order_items",
        "order_item": "order_items", "order_items": "order_items",
        "orderitems": "order_items",
        "customer": "customers", "customers": "customers", "buyers": "customers",
        "rates": "rates", "rate": "rates", "price_list": "rates", "prices": "rates",
        "transactions": "transactions", "transaction": "transactions",
        "data": "records", "results": "records", "records": "records",
        "rows": "records", "entries": "records", "export": "records",
    }
    return aliases.get(n, n or "records")


def likely_record(row: dict[str, Any]) -> bool:
    keys = {norm(k) for k in row}
    identifiers = {
        "id", "order_id", "customer_id", "product_id", "refund_id", "sku",
        "order_number", "invoice_id", "transaction_id",
    }
    return bool(keys & identifiers) or bool(keys & {"status", "order_date", "amount", "total_amount"})


def infer_table(row: dict[str, Any], fallback: str) -> str:
    keys = {norm(k) for k in row}
    record_type = norm(row.get("record_type", row.get("type", row.get("entity", ""))))
    if "refund" in record_type or keys & {"refund_id", "refund_amount", "refunded_amount"}:
        return "refunds"
    if "customer" in record_type or (keys & {"customer_id", "customer_name", "email"} and not keys & {"order_id", "order_number"}):
        return "customers"
    if "product" in record_type or (keys & {"product_id", "product_name", "sku"} and not keys & {"order_id", "order_number"}):
        return "products"
    if keys & {"order_id", "order_number", "invoice_id"}:
        return "orders"
    return table_key(fallback)


def csv_records(text: str) -> list[dict[str, Any]]:
    try:
        sample = text[:4096]
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
    except Exception:
        dialect = csv.excel
    try:
        return [dict(row) for row in csv.DictReader(io.StringIO(text), dialect=dialect)]
    except Exception:
        return []


def extract_tables(payload: Any, route_hint: str) -> dict[str, list[dict[str, Any]]]:
    """Extract collection-like arrays from JSON or a CSV export."""
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen: set[tuple[str, str]] = set()
    hint = table_key(route_hint)

    def put(table: str, rows: list[Any]) -> None:
        table = table_key(table)
        clean = [r for r in rows if isinstance(r, dict)]
        if not clean:
            return
        # An export can contain several entity types in one flat CSV/JSON array.
        if table in {"records", "export"}:
            buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in clean:
                buckets[infer_table(row, table)].append(row)
            if len(buckets) > 1 or next(iter(buckets), table) != table:
                for key, bucket in buckets.items():
                    put(key, bucket)
                return
        for row in clean:
            sig = (table, canon_json(row))
            if sig not in seen:
                seen.add(sig)
                result[table].append(row)

    def walk(obj: Any, current_hint: str, depth: int = 0) -> None:
        if depth > 9:
            return
        if isinstance(obj, str):
            stripped = obj.lstrip()
            if ("," in obj or "\t" in obj) and "\n" in obj and not stripped.startswith(("{", "[")):
                rows = csv_records(obj)
                if rows:
                    put(current_hint, rows)
            return
        if isinstance(obj, list):
            rows = [x for x in obj if isinstance(x, dict)]
            if rows and len(rows) >= max(1, len(obj) // 2):
                chosen = current_hint
                if chosen in {"records", "export"}:
                    chosen = infer_table(rows[0], chosen)
                put(chosen, rows)
                # Keep nested order line items, while avoiding re-adding parent objects.
                for row in rows:
                    for key, value in row.items():
                        if isinstance(value, (dict, list)):
                            walk(value, table_key(key), depth + 1)
            else:
                for item in obj:
                    if isinstance(item, (dict, list)):
                        walk(item, current_hint, depth + 1)
            return
        if not isinstance(obj, dict):
            return

        # Prefer named collections, then generic paginated envelopes.
        found_collection = False
        for key, value in obj.items():
            if isinstance(value, list) and any(isinstance(x, dict) for x in value):
                nkey = norm(key)
                if nkey in {"data", "results", "records", "rows", "entries", "items"}:
                    target = current_hint if current_hint not in {"records", "export"} else "records"
                else:
                    target = table_key(key)
                put(target, value)
                found_collection = True
                for row in value:
                    if isinstance(row, dict):
                        for child_key, child in row.items():
                            if isinstance(child, (dict, list)):
                                walk(child, table_key(child_key), depth + 1)
        if not found_collection and likely_record(obj):
            target = current_hint if current_hint not in {"records", "export"} else infer_table(obj, current_hint)
            put(target, [obj])
        # Follow nested named collections/envelopes (meta objects are harmless).
        for key, value in obj.items():
            if isinstance(value, dict):
                next_hint = table_key(key)
                if norm(key) in {"data", "result", "response", "payload"}:
                    next_hint = current_hint
                walk(value, next_hint, depth + 1)

    if isinstance(payload, str):
        rows = csv_records(payload)
        if rows:
            put(hint, rows)
        else:
            try:
                walk(json.loads(payload), hint)
            except Exception:
                pass
    else:
        walk(payload, hint)
    return dict(result)


def field(obj: Any, names: list[str] | tuple[str, ...], *, recursive: bool = True) -> Any:
    """Find a preferred field name, case-/punctuation-insensitively."""
    if not isinstance(obj, dict):
        return None
    wanted = [norm(x) for x in names]
    for name in wanted:
        for key, value in obj.items():
            if norm(key) == name and value is not None:
                return value
    if recursive:
        for value in obj.values():
            if isinstance(value, dict):
                got = field(value, names, recursive=True)
                if got is not None:
                    return got
    return None


def field_values(obj: Any, wanted: set[str]) -> list[Any]:
    found: list[Any] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            if norm(key) in wanted and value is not None and not isinstance(value, (dict, list)):
                found.append(value)
            if isinstance(value, (dict, list)):
                found.extend(field_values(value, wanted))
    elif isinstance(obj, list):
        for value in obj:
            found.extend(field_values(value, wanted))
    return found


def number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip().replace(",", "")
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1]
        s = re.sub(r"[^0-9.\-]", "", s)
        if not s or s in {"-", ".", "-."}:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            stamp = float(value)
            if stamp > 10_000_000_000:
                stamp /= 1000
            return datetime.fromtimestamp(stamp, tz=LOCAL_TZ).date()
        except Exception:
            return None
    s = str(value).strip()
    if not s:
        return None
    try:
        if re.fullmatch(r"\d{4}-\d{2}", s):
            return date.fromisoformat(s + "-01")
        if re.fullmatch(r"\d{4}", s):
            return date(int(s), 1, 1)
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(LOCAL_TZ)
        else:
            dt = dt.replace(tzinfo=LOCAL_TZ)
        return dt.date()
    except Exception:
        try:
            return date.fromisoformat(s[:10])
        except Exception:
            return None


def row_date(row: dict[str, Any]) -> date | None:
    value = field(row, [
        "order_date", "ordered_at", "purchase_date", "transaction_date", "created_at",
        "created_date", "date", "timestamp", "invoice_date", "issued_at",
        "refund_date", "updated_at",
    ])
    return parse_date(value)


def status(row: dict[str, Any]) -> str:
    return str(field(row, ["status", "order_status", "payment_status", "transaction_status"], recursive=False) or "").strip().lower()


def unique_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    out = []
    for row in rows:
        sig = canon_json(row)
        if sig not in seen:
            seen.add(sig)
            out.append(row)
    return out


def current_orders(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """For repeated order IDs, retain only the latest updated_at row."""
    latest: dict[str, tuple[datetime, int, dict[str, Any]]] = {}
    without_id: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        ident = field(row, ["order_id", "orderId", "order_number", "invoice_id", "id"], recursive=False)
        if ident is None:
            without_id.append(row)
            continue
        stamp_val = field(row, ["updated_at", "updatedAt", "last_updated", "modified_at"], recursive=False)
        stamp = None
        if stamp_val is not None:
            try:
                stamp = datetime.fromisoformat(str(stamp_val).replace("Z", "+00:00"))
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=LOCAL_TZ)
                else:
                    stamp = stamp.astimezone(LOCAL_TZ)
            except Exception:
                pass
        if stamp is None:
            stamp = datetime.min.replace(tzinfo=LOCAL_TZ)
        key = str(ident)
        if key not in latest or (stamp, index) > (latest[key][0], latest[key][1]):
            latest[key] = (stamp, index, row)
    return unique_rows([v[2] for v in latest.values()] + without_id)


# ---------------------------------------------------------------------------
# Upstream gateway and pagination
# ---------------------------------------------------------------------------
def client() -> httpx.AsyncClient:
    global HTTP
    if HTTP is None or HTTP.is_closed:
        HTTP = httpx.AsyncClient(
            timeout=httpx.Timeout(UPSTREAM_TIMEOUT, connect=min(4.0, UPSTREAM_TIMEOUT)),
            follow_redirects=True,
            headers={"User-Agent": "Acme-Ledger-Agent/2.0", "Accept": "application/json, text/csv, */*"},
        )
    return HTTP


def split_route_and_query(path: str, extra: dict[str, Any] | None = None) -> tuple[str, dict[str, Any]]:
    parsed = urlparse(path)
    route = parsed.path or "/"
    params: dict[str, Any] = {}
    for key, values in parse_qs(parsed.query, keep_blank_values=True).items():
        if values:
            params[key] = values[-1]
    if extra:
        params.update(extra)
    return route, params


async def upstream_get(path: str, extra: dict[str, Any] | None = None) -> tuple[Any, str, int, dict[str, Any]]:
    if not UPSTREAM_QUIZ_SIGN:
        return {"error": "UPSTREAM_QUIZ_SIGN is not configured"}, "application/json", 500, {}
    route, other_params = split_route_and_query(path, extra)
    params: dict[str, Any] = {
        "email": UPSTREAM_EMAIL,
        "quizSign": UPSTREAM_QUIZ_SIGN,
        "questionId": UPSTREAM_QUESTION_ID,
        "path": route,
    }
    params.update(other_params)
    try:
        response = await client().get(UPSTREAM_URL, params=params)
        content_type = response.headers.get("content-type", "")
        try:
            body: Any = response.json()
        except Exception:
            body = response.text
        return body, content_type, response.status_code, dict(response.headers)
    except Exception as exc:
        return {"_fetch_error": f"{type(exc).__name__}: {exc}"}, "application/json", 599, {}


def link_target(value: Any) -> tuple[str, dict[str, Any]] | None:
    """Turn a URL advertised by the gateway into a path plus non-auth query params."""
    if not isinstance(value, str) or not value.strip():
        return None
    parsed = urlparse(value.strip())
    if parsed.scheme and parsed.netloc:
        query = parse_qs(parsed.query, keep_blank_values=True)
        route = query.get("path", [parsed.path or "/"])[0]
        extras = {
            key: values[-1]
            for key, values in query.items()
            if values and key not in {"email", "quizSign", "questionId", "path"}
        }
        return split_route_and_query(route, extras)
    return split_route_and_query(value.strip())


def top_level_dicts(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    result: list[dict[str, Any]] = []
    for key in ("meta", "pagination", "page_info", "pageInfo", "paging", "links", "_links"):
        val = payload.get(key)
        if isinstance(val, dict):
            result.append(val)
    result.append(payload)
    return result


def pagination_details(payload: Any, path: str, params: dict[str, Any]) -> tuple[int | None, int, tuple[str, dict[str, Any]] | None, bool | None]:
    current = int(number(params.get("page")) or 1)
    total_pages: int | None = None
    has_more: bool | None = None
    next_target = None
    for block in top_level_dicts(payload):
        for name in ("next", "next_url", "nextUrl", "next_page_url", "nextPageUrl"):
            val = block.get(name)
            candidate = link_target(val)
            if candidate:
                next_target = candidate
                has_more = True
                break
        if next_target:
            break
        for name in ("has_more", "hasMore", "has_next", "hasNext"):
            if isinstance(block.get(name), bool):
                has_more = block[name]
        p = number(block.get("page", block.get("current_page", block.get("currentPage", block.get("page_number")))))
        if p is not None:
            current = int(p)
        tp_raw = block.get("total_pages")
        if tp_raw is None: tp_raw = block.get("totalPages")
        if tp_raw is None: tp_raw = block.get("last_page")
        if tp_raw is None: tp_raw = block.get("lastPage")
        if tp_raw is None: tp_raw = block.get("pages")
        tp = number(tp_raw)
        if tp is not None:
            total_pages = max(1, int(tp))
        if total_pages is None:
            total_raw = block.get("total", block.get("total_count", block.get("totalCount")))
            size_raw = block.get("per_page")
            if size_raw is None: size_raw = block.get("perPage")
            if size_raw is None: size_raw = block.get("page_size")
            if size_raw is None: size_raw = block.get("pageSize")
            if size_raw is None: size_raw = block.get("limit")
            total, size = number(total_raw), number(size_raw)
            if total is not None and size and size > 0:
                total_pages = max(1, int((total + size - 1) // size))
    return total_pages, current, next_target, has_more


async def fetch_resource(path: str, default_params: dict[str, Any], route_hint: str) -> dict[str, list[dict[str, Any]]]:
    first, content_type, code, _ = await upstream_get(path, default_params)
    if code != 200 or (isinstance(first, dict) and ("_fetch_error" in first or "error" in first)):
        return {}
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for key, rows in extract_tables(first, route_hint).items():
        result[key].extend(rows)

    # CSV exports are generally complete and do not use page-based pagination.
    if "csv" in content_type.lower() or isinstance(first, str):
        return {k: unique_rows(v) for k, v in result.items() if v}

    all_pages = [(path, default_params, first)]
    total_pages, current, next_target, has_more = pagination_details(first, path, default_params)
    if total_pages is not None and total_pages > current:
        page_params = []
        for page_num in range(current + 1, min(total_pages, MAX_PAGES) + 1):
            p = dict(default_params)
            p["page"] = page_num
            page_params.append((path, p))
        semaphore = asyncio.Semaphore(12)

        async def get_page(route: str, params: dict[str, Any]):
            async with semaphore:
                return route, params, await upstream_get(route, params)

        for start in range(0, len(page_params), 36):
            batch = page_params[start:start + 36]
            outcomes = await asyncio.gather(*(get_page(route, params) for route, params in batch))
            for route, params, (payload, ctype, status_code, _) in outcomes:
                if status_code == 200 and not (isinstance(payload, dict) and ("_fetch_error" in payload or "error" in payload)):
                    all_pages.append((route, params, payload))
    elif next_target or has_more:
        seen = {(path, canon_json(default_params))}
        route, params = next_target if next_target else (path, {**default_params, "page": current + 1})
        for _ in range(MAX_PAGES - 1):
            key = (route, canon_json(params))
            if key in seen:
                break
            seen.add(key)
            payload, ctype, status_code, _ = await upstream_get(route, params)
            if status_code != 200 or (isinstance(payload, dict) and ("_fetch_error" in payload or "error" in payload)):
                break
            all_pages.append((route, params, payload))
            next_total, next_current, next_link, next_more = pagination_details(payload, route, params)
            if next_link:
                route, params = next_link
            elif next_more:
                route, params = route, {**params, "page": next_current + 1}
            elif next_total and next_current < next_total:
                route, params = route, {**params, "page": next_current + 1}
            else:
                break

    for route, params, payload in all_pages[1:]:
        for key, rows in extract_tables(payload, route_hint).items():
            result[key].extend(rows)
    return {k: unique_rows(v) for k, v in result.items() if v}


def discover_root_links(payload: Any) -> dict[str, tuple[str, dict[str, Any]]]:
    links: dict[str, tuple[str, dict[str, Any]]] = {}
    if not isinstance(payload, dict):
        return links
    raw_links = payload.get("links", {})
    if isinstance(raw_links, dict):
        for label, value in raw_links.items():
            target = link_target(value)
            if target:
                # Preserve "export" as a route label; it is not the same thing
                # as a generic "records" collection.
                links[norm(str(label))] = target
    return links


async def refresh_data(force: bool = False) -> None:
    global DATA, RESOURCE_PATHS, LAST_REFRESH, LAST_ERROR
    if not UPSTREAM_QUIZ_SIGN:
        LAST_ERROR = "UPSTREAM_QUIZ_SIGN is not configured on the hosting service"
        return
    if not force and DATA and time.time() - LAST_REFRESH < CACHE_SECONDS:
        return
    async with REFRESH_LOCK:
        if not force and DATA and time.time() - LAST_REFRESH < CACHE_SECONDS:
            return
        try:
            root, _, code, _ = await upstream_get("/")
            if code != 200 or not isinstance(root, dict) or "error" in root:
                LAST_ERROR = f"Could not load API root (HTTP {code})"
                return
            links = discover_root_links(root)
            # Prefer API-advertised paths; fallbacks are used only if the root omits them.
            targets: list[tuple[str, dict[str, Any], str]] = []
            for key in ("orders", "rates", "records", "refunds", "products", "customers", "order_items"):
                if key in links:
                    route, params = links[key]
                    hint = "orders" if key == "orders" else key
                    targets.append((route, params, hint))
            if "records" in links and not any(t[2] == "records" for t in targets):
                targets.append((*links["records"], "records"))
            # Try /export too because it may expose the complete ledger in one response.
            if "export" in links:
                route, params = links["export"]
                targets.append((route, params, "records"))
            if not targets:
                targets = [("/orders", {"page": 1}, "orders"), ("/rates", {}, "rates"), ("/export", {}, "records")]

            sem = asyncio.Semaphore(4)
            async def load(target: tuple[str, dict[str, Any], str]):
                route, params, hint = target
                async with sem:
                    return route, await fetch_resource(route, params, hint)

            loaded = await asyncio.gather(*(load(t) for t in targets), return_exceptions=True)
            new_data: dict[str, list[dict[str, Any]]] = defaultdict(list)
            fetched_paths = ["/"]
            export_tables: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for item in loaded:
                if isinstance(item, Exception):
                    continue
                route, tables = item
                fetched_paths.append(route)
                is_export = norm(route.rstrip("/").split("/")[-1]) == "export"
                destination = export_tables if is_export else new_data
                for table, rows in tables.items():
                    destination[table].extend(rows)

            # Use /export as a fallback for tables that a dedicated resource did
            # not provide. This avoids mixing order-level and line-level rows if
            # the export uses a different row grain.
            for table, rows in export_tables.items():
                if table not in new_data and rows:
                    new_data[table].extend(rows)
            if not new_data and export_tables:
                for table, rows in export_tables.items():
                    new_data[table].extend(rows)

            clean: dict[str, list[dict[str, Any]]] = {}
            for key, rows in new_data.items():
                rows = unique_rows(rows)
                if key == "orders":
                    rows = current_orders(rows)
                if rows:
                    clean[key] = rows
            # An empty refresh must never erase a previously good cache.
            if not clean:
                LAST_ERROR = "API was reachable, but no order/rate/export records were extracted"
                return
            DATA = clean
            RESOURCE_PATHS = sorted(set(fetched_paths))
            LAST_REFRESH = time.time()
            LAST_ERROR = None
        except Exception as exc:
            LAST_ERROR = f"{type(exc).__name__}: {exc}"


# ---------------------------------------------------------------------------
# Business logic
# ---------------------------------------------------------------------------
def records(kind: str) -> list[dict[str, Any]]:
    aliases = {
        "orders": ("orders", "sales", "transactions", "records"),
        "refunds": ("refunds", "returns"),
        "products": ("products",),
        "customers": ("customers",),
        "rates": ("rates", "prices"),
        "order_items": ("order_items", "items", "line_items"),
    }
    out: list[dict[str, Any]] = []
    for key in aliases.get(kind, (kind,)):
        out.extend(DATA.get(key, []))
    return unique_rows(out)


def customer_map() -> dict[str, dict[str, Any]]:
    result = {}
    for row in DATA.get("customers", []):
        ident = field(row, ["customer_id", "customerId", "id", "customer_code"], recursive=False)
        if ident is not None:
            result[str(ident)] = row
    return result


def customer_name(row: dict[str, Any], cmap: dict[str, dict[str, Any]]) -> str | None:
    val = field(row, ["customer_name", "buyer_name", "full_name", "customer"], recursive=False)
    if isinstance(val, dict):
        val = field(val, ["name", "full_name", "email"], recursive=False)
    if val is not None:
        s = str(val)
        if s in cmap:
            return str(field(cmap[s], ["name", "customer_name", "full_name", "email"], recursive=False) or s)
        return s
    cid = field(row, ["customer_id", "customerId", "buyer_id"], recursive=False)
    if cid is not None and str(cid) in cmap:
        c = cmap[str(cid)]
        return str(field(c, ["name", "customer_name", "full_name", "email"], recursive=False) or cid)
    nested = field(row, ["customer"], recursive=False)
    if isinstance(nested, dict):
        return str(field(nested, ["name", "full_name", "email"], recursive=False) or "") or None
    return None


def product_name(row: dict[str, Any]) -> str | None:
    val = field(row, ["product_name", "item_name", "sku_name", "title", "product", "name"], recursive=False)
    if isinstance(val, dict):
        val = field(val, ["name", "product_name", "title", "sku"], recursive=False)
    if val is None:
        val = field(row, ["product_name", "item_name", "sku_name", "title", "product", "name"])
    return str(val) if val is not None else None


def order_id(row: dict[str, Any]) -> str | None:
    value = field(row, ["order_id", "orderId", "order_number", "invoice_id"], recursive=False)
    return str(value) if value is not None else None


def region_of(row: dict[str, Any], cmap: dict[str, dict[str, Any]]) -> str | None:
    val = field(row, ["region", "sales_region", "territory", "market_region"], recursive=False)
    if val is not None:
        return str(val)
    cid = field(row, ["customer_id", "customerId", "buyer_id"], recursive=False)
    if cid is not None and str(cid) in cmap:
        val = field(cmap[str(cid)], ["region", "sales_region", "territory"], recursive=False)
        if val is not None:
            return str(val)
    customer = row.get("customer")
    if isinstance(customer, dict):
        val = field(customer, ["region", "sales_region", "territory"], recursive=False)
        if val is not None:
            return str(val)
    return None


def filters(question: str) -> dict[str, Any]:
    q = question.lower()
    start: date | None = None
    end: date | None = None
    months = {
        "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
        "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6, "july": 7,
        "jul": 7, "august": 8, "aug": 8, "september": 9, "sep": 9, "sept": 9,
        "october": 10, "oct": 10, "november": 11, "nov": 11, "december": 12, "dec": 12,
    }
    for name, month in months.items():
        match = re.search(rf"\b{name}\s+(20\d{{2}})\b", q)
        if match:
            year = int(match.group(1))
            start = date(year, month, 1)
            end = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
            break
    if start is None:
        match = re.search(r"\b(?:between|from)\s+(20\d{2}-\d{2}-\d{2})\s+(?:and|to|through)\s+(20\d{2}-\d{2}-\d{2})", q)
        if match:
            start = date.fromisoformat(match.group(1))
            end = date.fromisoformat(match.group(2)) + timedelta(days=1)
        else:
            match = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", q)
            if match:
                start = date.fromisoformat(match.group(1))
                end = start + timedelta(days=1)
            else:
                match = re.search(r"\b(?:in|during|for|year)\s+(20\d{2})\b", q)
                if match:
                    year = int(match.group(1))
                    start, end = date(year, 1, 1), date(year + 1, 1, 1)
    regions = ("north", "south", "east", "west", "central", "northeast", "northwest", "southeast", "southwest")
    region = None
    for candidate in regions:
        if re.search(rf"\b{candidate}\s+(?:region|area|territory)\b|\b(?:region|area|territory)\s+(?:of\s+)?{candidate}\b", q):
            region = candidate
            break
    if region is None:
        match = re.search(r"\b(?:in|from|for|within)\s+(?:the\s+)?(north|south|east|west|central|northeast|northwest|southeast|southwest)\b", q)
        if match:
            region = match.group(1)
    return {"start": start, "end": end, "region": region}


def matches_filters(row: dict[str, Any], flt: dict[str, Any], cmap: dict[str, dict[str, Any]]) -> bool:
    if flt["start"] is not None:
        d = row_date(row)
        if d is None or d < flt["start"]:
            return False
    if flt["end"] is not None:
        d = row_date(row)
        if d is None or d >= flt["end"]:
            return False
    if flt["region"]:
        r = region_of(row, cmap)
        if r is None or flt["region"] not in r.lower():
            return False
    return True


def get_rates() -> tuple[dict[str, float], dict[str, float]]:
    by_id: dict[str, float] = {}
    by_name: dict[str, float] = {}
    for row in records("rates") + records("products"):
        price = number(field(row, ["unit_price", "price_usd", "price", "rate", "unit_rate", "amount", "cost"], recursive=False))
        if price is None:
            continue
        pid = field(row, ["product_id", "productId", "sku", "sku_id", "id"], recursive=False)
        pname = product_name(row)
        if pid is not None:
            by_id[str(pid)] = price
        if pname:
            by_name[pname.lower()] = price
    return by_id, by_name


def get_line_rows(cmap: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    order_lookup = {order_id(o): o for o in records("orders") if order_id(o) is not None}
    for line in DATA.get("order_items", []):
        oid = order_id(line) or field(line, ["order_id", "orderId"], recursive=False)
        parent = order_lookup.get(str(oid)) if oid is not None else None
        merged = dict(parent or {})
        merged.update(line)
        result.append(merged)
    # Use a dedicated line-item collection when available; otherwise derive lines
    # from embedded arrays. Avoid double-counting when both formats are exposed.
    if not result:
        for order in records("orders"):
            for key in ("items", "line_items", "lineItems", "order_items", "orderItems", "products"):
                arr = order.get(key)
                if isinstance(arr, list):
                    for item in arr:
                        if isinstance(item, dict):
                            merged = dict(order)
                            # Avoid letting a nested order total masquerade as a line amount.
                            for total_key in ("total", "amount", "total_amount", "order_total", "grand_total", "net_amount"):
                                merged.pop(total_key, None)
                            merged.update(item)
                            result.append(merged)
    # Some ledger APIs use one row per order line instead of nested line_items.
    # In that schema, the order row itself has product + quantity + price fields.
    if not result:
        for order in records("orders"):
            has_product = product_name(order) is not None or field(order, ["product_id", "sku"], recursive=False) is not None
            has_quantity = field(order, ["quantity", "qty", "units"], recursive=False) is not None
            if has_product and has_quantity:
                result.append(dict(order))
    return unique_rows(result)


def line_amount(row: dict[str, Any], rates_id: dict[str, float], rates_name: dict[str, float]) -> float | None:
    direct = field(row, ["line_total", "line_amount", "extended_price", "item_total", "subtotal", "sales_amount", "amount"], recursive=False)
    value = number(direct)
    if value is not None:
        return value
    qty = number(field(row, ["quantity", "qty", "units", "quantity_sold"], recursive=False))
    price = number(field(row, ["unit_price", "unitPrice", "sale_price", "price", "rate", "unit_rate"], recursive=False))
    if price is None:
        pid = field(row, ["product_id", "productId", "sku", "sku_id"], recursive=False)
        pname = product_name(row)
        if pid is not None:
            price = rates_id.get(str(pid))
        if price is None and pname:
            price = rates_name.get(pname.lower())
    if qty is not None and price is not None:
        return qty * price
    return None


def order_amount(row: dict[str, Any], lines_by_order: dict[str, list[dict[str, Any]]], rates_id: dict[str, float], rates_name: dict[str, float]) -> float | None:
    direct = field(row, ["order_total", "total_amount", "grand_total", "total_usd", "amount_usd", "revenue_usd", "net_amount", "gross_amount", "total", "amount", "revenue", "order_value"], recursive=False)
    value = number(direct)
    if value is not None:
        return value
    oid = order_id(row)
    if oid and oid in lines_by_order:
        line_values = [line_amount(line, rates_id, rates_name) for line in lines_by_order[oid]]
        valid = [v for v in line_values if v is not None]
        if valid:
            return sum(valid)
    return line_amount(row, rates_id, rates_name)


def line_index(lines: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for line in lines:
        oid = order_id(line)
        if oid:
            out[oid].append(line)
    return dict(out)


def order_is_paid(row: dict[str, Any]) -> bool:
    # Revenue policy from the upstream root: only status='paid' is revenue.
    st = status(row)
    if st:
        return st == "paid"
    # Some exports separate order status from payment status.
    pay = str(field(row, ["payment_status", "financial_status"], recursive=False) or "").lower()
    return pay == "paid"


def is_count_question(q: str) -> bool:
    return bool(re.search(r"\b(how many|number of|count of|count|quantity of)\b", q))


def is_average_question(q: str) -> bool:
    return bool(re.search(r"\b(average|avg|mean)\b", q))


def is_top_question(q: str) -> bool:
    return bool(re.search(r"\b(top|most|highest|largest|greatest|best|least|lowest|smallest|fewest|maximum|max|min|minimum)\b", q))


def asks_money(q: str) -> bool:
    return bool(re.search(r"\b(revenue|sales|earned|income|turnover|spend|spent|spending|paid|payment|money|worth|value|amount|dollars?|usd|profit|total cost|order value)\b", q))


def named_entity(question: str, names: list[str]) -> str | None:
    q = question.casefold()
    candidates = [name for name in names if name and name.casefold() in q]
    return max(candidates, key=len) if candidates else None


def product_aggregate(question: str, lines: list[dict[str, Any]], cmap: dict[str, dict[str, Any]], rates_id: dict[str, float], rates_name: dict[str, float]) -> Any:
    q = question.lower()
    flt = filters(question)
    paid_lines = [r for r in lines if order_is_paid(r) and matches_filters(r, flt, cmap)]
    revenue_by: dict[str, float] = defaultdict(float)
    qty_by: dict[str, float] = defaultdict(float)
    orders_by: dict[str, set[str]] = defaultdict(set)
    for row in paid_lines:
        name = product_name(row)
        if not name:
            # Resolve a product ID against the product/rates resources.
            pid = field(row, ["product_id", "productId", "sku", "sku_id"], recursive=False)
            if pid is not None:
                for product in records("products") + records("rates"):
                    product_id_value = field(product, ["product_id", "productId", "sku", "sku_id", "id"], recursive=False)
                    if str(product_id_value) == str(pid):
                        name = product_name(product)
                        if name:
                            break
        if not name:
            continue
        qty = number(field(row, ["quantity", "qty", "units", "quantity_sold"], recursive=False)) or 1.0
        amount = line_amount(row, rates_id, rates_name)
        if amount is not None:
            revenue_by[name] += amount
        qty_by[name] += qty
        oid = order_id(row)
        if oid:
            orders_by[name].add(oid)
    names = sorted(set(revenue_by) | set(qty_by))
    exact = named_entity(question, names)
    if exact:
        if count_q and re.search(r"\b(order|orders)\b", q) and not re.search(r"\b(unit|units|quantity|pieces|items)\b", q):
            return len(orders_by.get(exact, set()))
        if count_q:
            qty = qty_by.get(exact, 0.0)
            return int(qty) if qty.is_integer() else round(qty, 2)
        if asks_money(q):
            return round(revenue_by.get(exact, 0.0), 2)
        qty = qty_by.get(exact, 0.0)
        return int(qty) if qty.is_integer() else round(qty, 2)
    if is_top_question(q) and names:
        if re.search(r"\b(least|lowest|smallest|fewest|min|minimum)\b", q):
            score = qty_by if not asks_money(q) else revenue_by
            return min(names, key=lambda name: score.get(name, 0.0))
        # Most popular / most sold means quantity; highest revenue means money.
        score = revenue_by if asks_money(q) else qty_by
        return max(names, key=lambda name: score.get(name, 0.0))
    if is_count_question(q):
        if re.search(r"\b(unique|different|distinct)\s+(products|items|skus)\b", q):
            return len(names)
        return int(sum(qty_by.values())) if sum(qty_by.values()).is_integer() else round(sum(qty_by.values()), 2)
    if re.search(r"\b(by product|per product|each product|every product|all products)\b", q):
        if asks_money(q):
            return {name: round(revenue_by[name], 2) for name in sorted(revenue_by)}
        return {name: (int(qty_by[name]) if qty_by[name].is_integer() else round(qty_by[name], 2)) for name in sorted(qty_by)}
    # Asking for products without a named aggregate: unique product names.
    if re.search(r"\b(list|which|what are|name)\b", q) and names:
        return names
    return None


def answer_question(question: str) -> Any:
    if not DATA:
        raise HTTPException(status_code=503, detail=f"Ledger data is not loaded. Check /health. Detail: {LAST_ERROR or 'no data'}")
    q = question.strip().lower()
    cmap = customer_map()
    flt = filters(question)
    count_q = is_count_question(q)
    avg_q = is_average_question(q)
    top_q = is_top_question(q)
    money_q = asks_money(q)
    refund_q = bool(re.search(r"\b(refund|refunded|refunds|reimbursement|returned money)\b", q))
    customer_q = bool(re.search(r"\b(customer|customers|buyer|buyers|client|clients)\b", q))
    product_q = bool(re.search(r"\b(product|products|item|items|sku|skus)\b", q))
    order_q = bool(re.search(r"\b(order|orders|invoice|invoices)\b", q))

    orders = current_orders(records("orders"))
    lines = get_line_rows(cmap)
    rates_id, rates_name = get_rates()
    lines_by_order = line_index(lines)

    # Customer-specific questions must be answered before the general revenue
    # total, otherwise "revenue generated by Alice" would incorrectly return
    # the revenue for every customer.
    if customer_q and not refund_q:
        activity_question = bool(re.search(
            r"\b(ordered|orders|order|purchased|purchase|bought|spent|spending|revenue|sales|paid|placed|generated|sold)\b", q
        ))
        customer_orders = [r for r in orders if matches_filters(r, flt, cmap)]
        wants_customer_metric = money_q or top_q or (count_q and activity_question)
        if wants_customer_metric:
            totals_by_customer: dict[str, float] = defaultdict(float)
            counts_by_customer: dict[str, int] = defaultdict(int)
            labels_by_key: dict[str, str] = {}
            for row in customer_orders:
                # Revenue/spending only counts paid orders; order-count questions
                # count the current order rows regardless of payment status.
                if money_q and not count_q and not order_is_paid(row):
                    continue
                label = customer_name(row, cmap)
                cid = field(row, ["customer_id", "customerId", "buyer_id"], recursive=False)
                key = str(cid) if cid is not None else (label or "")
                if not key:
                    continue
                if not label:
                    label = key
                labels_by_key[key] = label
                counts_by_customer[key] += 1
                if order_is_paid(row):
                    amount = order_amount(row, lines_by_order, rates_id, rates_name)
                    if amount is not None:
                        totals_by_customer[key] += amount
            by_label_totals: dict[str, float] = defaultdict(float)
            by_label_counts: dict[str, int] = defaultdict(int)
            for key, label in labels_by_key.items():
                by_label_totals[label] += totals_by_customer.get(key, 0.0)
                by_label_counts[label] += counts_by_customer.get(key, 0)
            candidates = sorted(set(by_label_totals) | set(by_label_counts))
            exact_customer = named_entity(question, candidates)
            if exact_customer:
                if count_q:
                    return by_label_counts.get(exact_customer, 0)
                if money_q:
                    return round(by_label_totals.get(exact_customer, 0.0), 2)
                return by_label_counts.get(exact_customer, 0)
            if count_q and activity_question:
                return len(candidates)
            if top_q and candidates:
                wants_order_count = bool(re.search(r"\b(order|orders|frequently|most orders)\b", q)) and not money_q
                metric = by_label_counts if wants_order_count else by_label_totals
                reverse = not bool(re.search(r"\b(least|lowest|fewest|smallest|min|minimum)\b", q))
                return (max if reverse else min)(candidates, key=lambda name: metric.get(name, 0.0))
            if re.search(r"\b(by customer|per customer|each customer|every customer|all customers)\b", q):
                if money_q:
                    return {name: round(by_label_totals[name], 2) for name in sorted(by_label_totals)}
                return {name: by_label_counts[name] for name in sorted(by_label_counts)}

        # Questions about the customer directory itself (e.g. how many customers
        # are in the South region) use the dedicated collection, if present.
        people = records("customers")
        if not people:
            unique_people: dict[str, dict[str, Any]] = {}
            for row in orders:
                label = customer_name(row, cmap)
                ident = field(row, ["customer_id", "customerId", "buyer_id"], recursive=False)
                key = str(ident if ident is not None else label)
                if key.strip() and key != "None":
                    unique_people[key] = row
            people = list(unique_people.values())
        # Date filters apply to purchasing activity, not a static customer master.
        directory_filter = dict(flt)
        if not activity_question:
            people = [p for p in people if matches_filters(p, directory_filter, cmap)]
        if count_q and not activity_question:
            return len(people)
        if re.search(r"\b(list|which|what are|name)\b", q):
            names = sorted({customer_name(p, cmap) for p in people if customer_name(p, cmap)})
            if names:
                return names

    if product_q and not refund_q:
        product_answer = product_aggregate(question, lines, cmap, rates_id, rates_name)
        if product_answer is not None:
            return product_answer
        products = records("products") + records("rates")
        names = sorted({product_name(p) for p in products if product_name(p)})
        if count_q:
            return len(names)
        if re.search(r"\b(list|which|what are|name)\b", q) and names:
            return names

    # Refunds: prefer explicit refund records/fields over assuming an order's full total.
    if refund_q:
        refund_rows = records("refunds")
        if refund_rows:
            rows = [r for r in refund_rows if matches_filters(r, flt, cmap)]
            if count_q:
                return len(rows)
            amounts = []
            for row in rows:
                value = number(field(row, ["refund_amount", "refunded_amount", "amount_refunded", "refund_total", "amount", "total"], recursive=False))
                if value is not None:
                    amounts.append(abs(value))
            if amounts:
                return round(sum(amounts), 2)
            return 0.0
        candidates = []
        for row in orders:
            ref_value = number(field(row, ["refund_amount", "refunded_amount", "amount_refunded", "refund_total"], recursive=False))
            if "refund" in status(row) or (ref_value is not None and ref_value != 0):
                if matches_filters(row, flt, cmap):
                    candidates.append((row, ref_value))
        if count_q:
            return len(candidates)
        total = 0.0
        got_any = False
        for row, ref_value in candidates:
            value = ref_value
            if value is None:
                value = number(field(row, ["order_total", "total_amount", "grand_total", "amount", "total"], recursive=False))
            if value is not None:
                total += abs(value)
                got_any = True
        return round(total, 2) if got_any else 0.0

    # Revenue and order totals. Only latest order versions with status paid count.
    eligible_orders = [r for r in orders if order_is_paid(r) and matches_filters(r, flt, cmap)]
    if not eligible_orders and not orders:
        eligible_orders = [r for r in records("transactions") if order_is_paid(r) and matches_filters(r, flt, cmap)]
    per_order: list[tuple[dict[str, Any], float]] = []
    for row in eligible_orders:
        amount = order_amount(row, lines_by_order, rates_id, rates_name)
        if amount is not None:
            per_order.append((row, amount))

    if re.search(r"\b(by region|per region|each region|which region|region generated|region had)\b", q) and money_q:
        totals: dict[str, float] = defaultdict(float)
        for row, amount in per_order:
            region = region_of(row, cmap)
            if region:
                totals[region] += amount
        if totals:
            if top_q or "which region" in q:
                reverse = not bool(re.search(r"\b(least|lowest|smallest|min|minimum)\b", q))
                return (max if reverse else min)(totals, key=totals.get)
            return {k: round(v, 2) for k, v in sorted(totals.items())}

    if count_q and order_q and not money_q:
        return len([r for r in orders if matches_filters(r, flt, cmap)])
    if count_q and not money_q and not customer_q and not product_q:
        return len([r for r in orders if matches_filters(r, flt, cmap)])
    if top_q and order_q and per_order and money_q:
        reverse = not bool(re.search(r"\b(least|lowest|smallest|min|minimum)\b", q))
        return round((max if reverse else min)(per_order, key=lambda x: x[1])[1], 2)
    values = [amount for _, amount in per_order]
    if avg_q:
        return round(sum(values) / len(values), 2) if values else 0.0
    if values and (money_q or not count_q):
        return round(sum(values), 2)
    if count_q:
        return len(eligible_orders)

    # Direct lookup by product/customer/order id or a named customer.
    all_names = [customer_name(r, cmap) for r in orders]
    exact_customer = named_entity(question, [x for x in all_names if x])
    if exact_customer:
        total = 0.0
        for row, amount in per_order:
            if customer_name(row, cmap) == exact_customer:
                total += amount
        return round(total, 2) if money_q else sum(1 for r in eligible_orders if customer_name(r, cmap) == exact_customer)
    match = re.search(r"\b(?:order|customer|product|sku|invoice)\s*(?:id|number|#)?\s*[:#]?\s*([a-z0-9_-]+)", q)
    if match:
        token = match.group(1).lower()
        for row in orders + records("products") + records("customers"):
            if token in canon_json(row).lower():
                return row
    # No plausible result is safer than silently returning an unrelated total.
    return {"error": "Could not infer the requested calculation", "available_collections": {k: len(v) for k, v in DATA.items()}}


# ---------------------------------------------------------------------------
# HTTP routes
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def startup() -> None:
    # Load before the server reports itself ready, so the first grading request
    # does not spend its 15-second budget discovering pages. Missing/bad env vars
    # are recorded by refresh_data and the service still starts for diagnosis.
    await refresh_data(force=True)


@app.get("/")
async def root() -> dict[str, Any]:
    return {
        "service": "Acme Appliances Ledger Agent",
        "ready": bool(DATA),
        "answer_endpoint": "POST /",
        "health": "/health",
    }


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "ok": True,
        "upstream_configured": bool(UPSTREAM_QUIZ_SIGN),
        "data_loaded": bool(DATA),
        "collections": {k: len(v) for k, v in DATA.items()},
        "discovered_paths": RESOURCE_PATHS,
        "last_refresh": datetime.fromtimestamp(LAST_REFRESH, tz=LOCAL_TZ).isoformat() if LAST_REFRESH else None,
        "last_error": LAST_ERROR,
    }


@app.get("/debug/schema")
async def debug_schema() -> dict[str, Any]:
    return {
        "loaded": bool(DATA),
        "collections": {
            key: {"count": len(rows), "fields": sorted({str(k) for row in rows[:100] for k in row.keys()})}
            for key, rows in DATA.items()
        },
        "paths": RESOURCE_PATHS,
        "last_error": LAST_ERROR,
    }


@app.post("/")
@app.post("/answer")
@app.post("/query")
async def ask(req: QuestionRequest) -> dict[str, Any]:
    if not DATA or time.time() - LAST_REFRESH > CACHE_SECONDS:
        try:
            await asyncio.wait_for(refresh_data(force=not bool(DATA)), timeout=13.0)
        except asyncio.TimeoutError:
            if not DATA:
                raise HTTPException(status_code=504, detail="Timed out loading the ledger API; check /health and Render logs")
    if not DATA:
        raise HTTPException(status_code=503, detail=f"Ledger data is unavailable: {LAST_ERROR or 'unknown upstream error'}")
    try:
        return {"answer": answer_question(req.question)}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Answering failed: {type(exc).__name__}: {exc}")


@app.post("/refresh")
async def refresh() -> dict[str, Any]:
    await refresh_data(force=True)
    return {"ok": bool(DATA), "collections": {k: len(v) for k, v in DATA.items()}, "paths": RESOURCE_PATHS, "last_error": LAST_ERROR}
