from __future__ import annotations

import hashlib
import time
from pathlib import Path

import requests

from collectors.common.env import state_root
from collectors.vn_derivatives.source_gates import classify_http_status

DEFAULT_USER_AGENT = "pool-alpha-get-data/1.0 (+https://github.com/BobbyAxerol/trading-historical-data)"


def cache_root() -> Path:
    return state_root() / "vn_derivatives" / "web_cache"


def cache_key(url: str, params: dict[str, object] | None = None) -> str:
    raw = url
    if params:
        raw += "?" + "&".join(f"{key}={params[key]}" for key in sorted(params))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def get_public(
    url: str,
    *,
    params: dict[str, object] | None = None,
    cache_namespace: str,
    timeout: int = 30,
    sleep_seconds: float = 0.0,
) -> tuple[int | None, str, str | None, str | None]:
    root = cache_root() / cache_namespace
    root.mkdir(parents=True, exist_ok=True)
    key = cache_key(url, params)
    path = root / f"{key}.html"
    meta_path = root / f"{key}.url"
    if path.exists():
        return 200, path.read_text(errors="ignore"), str(path), None
    if sleep_seconds:
        time.sleep(sleep_seconds)
    try:
        response = requests.get(
            url,
            params=params,
            headers={"User-Agent": DEFAULT_USER_AGENT, "Accept": "text/html,application/json;q=0.9,*/*;q=0.8"},
            timeout=timeout,
        )
    except Exception as exc:
        return None, "", None, f"{type(exc).__name__}: {exc}"
    status = classify_http_status(response.status_code)
    if status == "success":
        path.write_text(response.text)
        meta_path.write_text(response.url)
        return response.status_code, response.text, str(path), None
    return response.status_code, response.text[:500], None, f"HTTP {response.status_code}: {response.text[:200]}"
