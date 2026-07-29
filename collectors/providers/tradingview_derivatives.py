from __future__ import annotations

import pandas as pd

from collectors.vn_derivatives.source_gates import ProviderFetchResult, classify_http_status, empty_ohlcv_frame
from collectors.vn_derivatives.web_cache import get_public

BASE_URL = "https://vn.tradingview.com/symbols/HNX-VN301%21/"


def probe_public_page(symbol: str = "HNX:VN301!", resolution: str = "1D") -> ProviderFetchResult:
    http_status, text, cache_path, error = get_public(BASE_URL, cache_namespace="tradingview")
    if error:
        return ProviderFetchResult(
            provider="tradingview",
            canonical_symbol=symbol,
            requested_symbol=symbol,
            resolved_symbol=None,
            resolution=resolution,
            status=classify_http_status(http_status),
            rows=empty_ohlcv_frame(),
            http_status=http_status,
            error=error,
            endpoint_type="public_page",
            source_url=BASE_URL,
        )
    lowered = text.lower()
    if "captcha" in lowered or "sign in" in lowered or "login" in lowered:
        status = "blocked"
        err = "public page indicates captcha/login requirement"
    elif "vn301" in lowered:
        status = "empty_confirmed"
        err = "public page reachable but no public OHLC endpoint integrated"
    else:
        status = "schema_error"
        err = "symbol marker not found in public page"
    return ProviderFetchResult(
        provider="tradingview",
        canonical_symbol=symbol,
        requested_symbol=symbol,
        resolved_symbol=symbol if status == "empty_confirmed" else None,
        resolution=resolution,
        status=status,
        rows=pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"]),
        http_status=http_status,
        error=err,
        endpoint_type="public_page",
        source_url=cache_path or BASE_URL,
    )
