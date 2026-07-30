from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd

from collectors.common.env import state_root
from collectors.common.manifest import utc_now_iso
from collectors.providers.vndirect_dchart_derivatives import DChartFetchResult, VndirectDChartProvider


@dataclass(frozen=True)
class VndirectProbeOptions:
    recent_days: int = 5
    old_start: str = "2018-08-01"
    old_end: str = "2018-09-01"
    daily_start: str = "2017-08-10"
    fail_on_gate: bool = True


def vndirect_probe_path() -> Path:
    return state_root() / "vn_derivatives" / "vndirect_dchart_probe.json"


def _result_summary(result: DChartFetchResult) -> dict[str, object]:
    return {
        "status": result.status,
        "row_count": result.row_count,
        "requested_start": result.requested_start,
        "requested_end": result.requested_end,
        "first_bar": result.first_bar,
        "last_bar": result.last_bar,
        "http_status": result.http_status,
        "error": result.error,
    }


def _json_default(value: object) -> str:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return str(value)


def _gate(
    *,
    recent_1m: DChartFetchResult,
    old_1m: DChartFetchResult,
    daily: DChartFetchResult,
) -> tuple[Literal["PASS", "FAIL"], list[str]]:
    errors: list[str] = []
    if recent_1m.status != "success":
        errors.append(f"recent_1m status is {recent_1m.status}")
    elif recent_1m.row_count <= 100:
        errors.append(f"recent_1m row_count <= 100: {recent_1m.row_count}")

    if daily.status != "success":
        errors.append(f"daily status is {daily.status}")
    elif daily.row_count <= 500:
        errors.append(f"daily row_count <= 500: {daily.row_count}")

    if old_1m.status not in {"success", "no_data"}:
        errors.append(f"old_1m status is {old_1m.status}")

    return ("PASS" if not errors else "FAIL", errors)


def run_vndirect_probe(options: VndirectProbeOptions | None = None) -> dict[str, object]:
    opts = options or VndirectProbeOptions()
    provider = VndirectDChartProvider()
    now = pd.Timestamp.now(tz="Asia/Ho_Chi_Minh")
    recent_start = now - pd.Timedelta(days=opts.recent_days)

    recent_1m = provider.fetch(start=recent_start, end=now, resolution="1m")
    old_1m = provider.fetch(start=pd.Timestamp(opts.old_start), end=pd.Timestamp(opts.old_end), resolution="1m")
    daily = provider.fetch(start=pd.Timestamp(opts.daily_start), end=now, resolution="1d")
    production_gate, gate_errors = _gate(recent_1m=recent_1m, old_1m=old_1m, daily=daily)

    payload = {
        "provider": VndirectDChartProvider.SOURCE,
        "symbol": VndirectDChartProvider.SYMBOL,
        "recent_1m": _result_summary(recent_1m),
        "old_1m": _result_summary(old_1m),
        "daily": _result_summary(daily),
        "production_gate": production_gate,
        "gate_errors": gate_errors,
        "updated_at": utc_now_iso(),
    }
    path = vndirect_probe_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=_json_default))
    if production_gate != "PASS" and opts.fail_on_gate:
        raise RuntimeError("; ".join(gate_errors) or "VNDIRECT DChart probe failed")
    return payload
