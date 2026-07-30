from __future__ import annotations

import json
import re
from io import StringIO

import pandas as pd

from collectors.vn_derivatives.source_gates import ProviderFetchResult, classify_http_status, empty_ohlcv_frame, normalize_ohlcv_frame
from collectors.vn_derivatives.web_cache import get_public

BASE_URL = "https://finance.vietstock.vn/chung-khoan-phai-sinh/{symbol}/hop-dong-tuong-lai.htm"
SEARCH_URL = "https://finance.vietstock.vn/search/{query}/3"
TABLE_RE = re.compile(r"<table[\s\S]*?</table>", re.IGNORECASE)


def overview_url(symbol: str) -> str:
    return BASE_URL.format(symbol=symbol.upper())


def _tables_from_html(html: str) -> list[pd.DataFrame]:
    if not TABLE_RE.search(html):
        return []
    try:
        return pd.read_html(StringIO(html))
    except Exception:
        return []


def _search_symbol_once(query: str, symbol: str) -> tuple[bool, str | None, str | None]:
    url = SEARCH_URL.format(query=query.upper())
    http_status, text, cache_path, error = get_public(url, cache_namespace="vietstock")
    if error:
        return False, cache_path or url, f"search {classify_http_status(http_status)}: {error}"
    try:
        payload = json.loads(text)
    except Exception as exc:
        return False, cache_path or url, f"search json error: {type(exc).__name__}: {exc}"
    lines = str(payload.get("data", "")).splitlines() if isinstance(payload, dict) else []
    symbol_upper = symbol.upper()
    for line in lines:
        parts = line.split("|")
        if parts and parts[0].upper() == symbol_upper:
            return True, cache_path or url, None
    return False, cache_path or url, "symbol not found in public search"


def _search_symbol(symbol: str) -> tuple[bool, str | None, str | None]:
    found, source_url, error = _search_symbol_once(symbol, symbol)
    if found or not symbol.upper().startswith("VN30F"):
        return found, source_url, error
    fallback_found, fallback_url, fallback_error = _search_symbol_once("VN30F", symbol)
    if fallback_found:
        return True, fallback_url, None
    return False, fallback_url or source_url, fallback_error or error


def fetch_daily(symbol: str) -> ProviderFetchResult:
    provider = "vietstock"
    search_found, search_url, search_error = _search_symbol(symbol)
    url = overview_url(symbol)
    http_status, text, cache_path, error = get_public(url, cache_namespace="vietstock")
    if error:
        return ProviderFetchResult(
            provider=provider,
            canonical_symbol=symbol,
            requested_symbol=symbol,
            resolved_symbol=None,
            resolution="1D",
            status=classify_http_status(http_status),
            rows=empty_ohlcv_frame(),
            http_status=http_status,
            error=error,
            endpoint_type="public_html",
            source_url=url,
        )
    lowered = text.lower()
    if "access denied" in lowered or "request blocked" in lowered:
        return ProviderFetchResult(provider, symbol, symbol, None, "1D", "blocked", empty_ohlcv_frame(), http_status=http_status, source_url=url, endpoint_type="public_html", error="blocked/access-denied marker")

    tables = _tables_from_html(text)
    best = empty_ohlcv_frame()
    schema_error = "no parseable OHLCV table found"
    for table in tables:
        frame, err = normalize_ohlcv_frame(table, resolution="1D", source=provider, source_symbol=symbol, derivative_session=False)
        if not frame.empty and len(frame) > len(best):
            best = frame
            schema_error = err

    status = "success" if not best.empty else "empty_confirmed" if search_found else "schema_error"
    if best.empty and search_found:
        schema_error = "public search resolved symbol but no parseable OHLCV table found"
    elif best.empty and search_error:
        schema_error = f"{schema_error}; {search_error}"
    return ProviderFetchResult(
        provider=provider,
        canonical_symbol=symbol,
        requested_symbol=symbol,
        resolved_symbol=symbol,
        resolution="1D",
        status=status,
        rows=best,
        http_status=http_status,
        first_bar=best["time"].min() if not best.empty else None,
        last_bar=best["time"].max() if not best.empty else None,
        error=None if not best.empty else schema_error,
        endpoint_type="public_html_search",
        source_url=cache_path or search_url or url,
    )
