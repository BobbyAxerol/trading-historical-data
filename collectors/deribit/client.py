from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

import requests

from collectors.deribit.config import DeribitConfig
from collectors.deribit.rate_limit import SyncRateLimiter, backoff_sleep, parse_retry_after

USER_AGENT = {"User-Agent": "pool-alpha-get-data/deribit-probe-v1"}


@dataclass
class DeribitApiResult:
    method: str
    params: dict[str, Any]
    ok: bool
    status_code: int | None
    result: Any = None
    error: Any = None
    error_type: str | None = None
    retry_after_seconds: float | None = None
    latency_ms: float | None = None
    response_bytes: int | None = None
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def trades(self) -> list[dict[str, Any]]:
        if not isinstance(self.result, dict):
            return []
        trades = self.result.get("trades")
        return trades if isinstance(trades, list) else []

    @property
    def has_more(self) -> bool | None:
        if not isinstance(self.result, dict):
            return None
        value = self.result.get("has_more")
        return bool(value) if value is not None else None

    def classification(self) -> str:
        if not self.ok:
            return "UNKNOWN"
        if self.trades:
            return "SUCCESS_WITH_DATA"
        if isinstance(self.result, dict) and isinstance(self.result.get("trades"), list):
            return "EMPTY_CONFIRMED"
        if self.result is not None:
            return "SUCCESS"
        return "UNKNOWN"

    def summary(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "status_code": self.status_code,
            "classification": self.classification(),
            "error_type": self.error_type,
            "error": self.error,
            "retry_after_seconds": self.retry_after_seconds,
            "latency_ms": self.latency_ms,
            "response_bytes": self.response_bytes,
            "has_more": self.has_more,
            "trade_rows": len(self.trades),
        }


class DeribitHistoryClient:
    def __init__(
        self,
        config: DeribitConfig,
        *,
        requests_per_second: float | None = None,
        session: requests.Session | None = None,
    ):
        self.config = config
        self.base_url = str(config.raw["api"]["base_url"]).rstrip("/")
        self.timeout = float(config.raw["api"].get("timeout_seconds", 60))
        rps = requests_per_second or float(config.raw["api"].get("target_requests_per_second", 5))
        self.limiter = SyncRateLimiter(rps)
        self.session = session or requests.Session()

    def public_get(self, method: str, params: dict[str, Any] | None = None, *, retry: bool = True) -> DeribitApiResult:
        params = dict(params or {})
        retry_cfg = self.config.raw["api"].get("retry", {})
        max_attempts = int(retry_cfg.get("max_attempts", 1 if not retry else 3)) if retry else 1
        min_delay = float(retry_cfg.get("min_delay_seconds", 1))
        max_delay = float(retry_cfg.get("max_delay_seconds", 60))
        jitter = bool(retry_cfg.get("jitter", True))

        last: DeribitApiResult | None = None
        for attempt in range(1, max_attempts + 1):
            result = self._public_get_once(method, params)
            last = result
            if result.ok:
                return result
            retryable = result.status_code in {429, 500, 502, 503, 504} or result.error_type in {"timeout", "connection_error"}
            if not retryable or attempt >= max_attempts:
                return result
            if result.retry_after_seconds is not None:
                time.sleep(result.retry_after_seconds)
            else:
                backoff_sleep(attempt, min_delay=min_delay, max_delay=max_delay, jitter=jitter)
        assert last is not None
        return last

    def _public_get_once(self, method: str, params: dict[str, Any]) -> DeribitApiResult:
        url = f"{self.base_url}/{method.lstrip('/')}"
        self.limiter.wait()
        started = time.perf_counter()
        try:
            response = self.session.get(url, params=params, timeout=self.timeout, headers=USER_AGENT)
            latency_ms = (time.perf_counter() - started) * 1000.0
        except requests.Timeout as exc:
            return DeribitApiResult(method, params, False, None, error=str(exc), error_type="timeout")
        except requests.RequestException as exc:
            return DeribitApiResult(method, params, False, None, error=str(exc), error_type="connection_error")

        headers = {str(k): str(v) for k, v in response.headers.items()}
        retry_after = parse_retry_after(headers.get("Retry-After") or headers.get("retry-after"))
        response_bytes = len(response.content or b"")

        if response.status_code != 200:
            return DeribitApiResult(
                method=method,
                params=params,
                ok=False,
                status_code=response.status_code,
                error=response.text[:500],
                error_type="http_error",
                retry_after_seconds=retry_after,
                latency_ms=latency_ms,
                response_bytes=response_bytes,
                headers=headers,
            )

        try:
            payload = response.json()
        except (json.JSONDecodeError, ValueError) as exc:
            return DeribitApiResult(
                method=method,
                params=params,
                ok=False,
                status_code=response.status_code,
                error=str(exc),
                error_type="malformed_json",
                retry_after_seconds=retry_after,
                latency_ms=latency_ms,
                response_bytes=response_bytes,
                headers=headers,
            )

        if not isinstance(payload, dict):
            return DeribitApiResult(method, params, False, response.status_code, error=payload, error_type="invalid_payload", latency_ms=latency_ms, response_bytes=response_bytes, headers=headers)
        if payload.get("error") is not None:
            return DeribitApiResult(method, params, False, response.status_code, error=payload.get("error"), error_type="jsonrpc_error", retry_after_seconds=retry_after, latency_ms=latency_ms, response_bytes=response_bytes, headers=headers)
        if "result" not in payload:
            return DeribitApiResult(method, params, False, response.status_code, error=payload, error_type="missing_result", latency_ms=latency_ms, response_bytes=response_bytes, headers=headers)
        return DeribitApiResult(method, params, True, response.status_code, result=payload.get("result"), retry_after_seconds=retry_after, latency_ms=latency_ms, response_bytes=response_bytes, headers=headers)

    def get_instruments(self, *, expired: bool) -> DeribitApiResult:
        return self.public_get(
            str(self.config.raw["api"]["instruments_method"]),
            {"currency": self.config.currency, "kind": "option", "expired": str(bool(expired)).lower()},
        )

    def get_last_trades_by_instrument(self, instrument_name: str, *, retry: bool = True, **params: Any) -> DeribitApiResult:
        payload = {"instrument_name": instrument_name, **params}
        return self.public_get(str(self.config.raw["api"]["trades_method"]), payload, retry=retry)
