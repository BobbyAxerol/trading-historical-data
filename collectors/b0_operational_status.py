"""Read-only B0 operational monitoring status.

This command never starts collectors or changes runtime state.  It provides the
operator-visible evidence required before scheduled writers are enabled and
fails closed when a required heartbeat or alert channel is absent.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from collectors.production_preflight import POLICY_PATH, _load_policy


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    return payload if isinstance(payload, dict) else None


def _parse_utc(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _check(name: str, passed: bool, message: str, **details: Any) -> dict[str, Any]:
    return {"name": name, "status": "pass" if passed else "blocked", "message": message, **details}


def operational_status(policy: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    """Return disk, alert-channel, and heartbeat readiness without side effects."""

    current = now or _utc_now()
    runtime_root = Path(policy["runtime"]["root"])
    state = runtime_root / "state"
    monitoring = policy["monitoring"]
    checks: list[dict[str, Any]] = []

    usage = os.statvfs(runtime_root)
    free_bytes = usage.f_bavail * usage.f_frsize
    free_gib = free_bytes / (1024**3)
    inode_free_pct = (usage.f_favail / usage.f_files * 100.0) if usage.f_files else 0.0
    checks.append(
        _check(
            "free_disk_low_water",
            free_gib >= float(monitoring["free_disk_low_water_gib"]),
            "Free disk must remain above the configured low-water mark.",
            free_gib=round(free_gib, 2),
            threshold_gib=monitoring["free_disk_low_water_gib"],
        )
    )
    checks.append(
        _check(
            "free_inode_low_water",
            inode_free_pct >= float(monitoring["inode_low_water_pct"]),
            "Free inode percentage must remain above the configured low-water mark.",
            free_inode_pct=round(inode_free_pct, 4),
            threshold_pct=monitoring["inode_low_water_pct"],
        )
    )

    heartbeat_max_age = float(monitoring["heartbeat_max_age_minutes"])
    heartbeat_details: list[dict[str, Any]] = []
    for name in monitoring["expected_heartbeat_datasets"]:
        payload = _read_json(state / "heartbeats" / f"{name}.json")
        updated_at = None if payload is None else _parse_utc(payload.get("updated_at"))
        age_minutes = None if updated_at is None else (current - updated_at).total_seconds() / 60.0
        fresh = bool(payload and payload.get("status") == "ok" and age_minutes is not None and age_minutes <= heartbeat_max_age)
        heartbeat_details.append(
            {
                "dataset": name,
                "status": "pass" if fresh else "blocked",
                "heartbeat_status": None if payload is None else payload.get("status"),
                "age_minutes": None if age_minutes is None else round(age_minutes, 2),
            }
        )
    checks.append(
        _check(
            "expected_heartbeats",
            bool(heartbeat_details) and all(item["status"] == "pass" for item in heartbeat_details),
            "Each enabled scheduled dataset needs a recent successful heartbeat.",
            max_age_minutes=heartbeat_max_age,
            heartbeats=heartbeat_details,
        )
    )

    evidence = _read_json(state / "bootstrap" / "monitoring.json") or {}
    for name, key in (
        ("collector_exit_alert", "collector_exit_alert"),
        ("retry_rate_limit_alert", "retry_rate_limit_alert"),
        ("validation_repair_alert", "validation_repair_alert"),
        ("rss_alert", "rss_alert"),
        ("backup_failure_alert", "backup_failure_alert"),
    ):
        value = evidence.get(key)
        passed = isinstance(value, dict) and value.get("status") == "pass"
        checks.append(
            _check(
                name,
                passed,
                "Operator-visible alert evidence must be recorded and passing.",
                evidence=value,
            )
        )

    return {
        "schema_version": 1,
        "observed_at": current.replace(microsecond=0).isoformat(),
        "runtime_root": str(runtime_root),
        "status": "pass" if all(item["status"] == "pass" for item in checks) else "blocked",
        "checks": checks,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only B0 operational monitoring status")
    parser.add_argument("--policy", type=Path, default=POLICY_PATH)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args(argv)
    payload = operational_status(_load_policy(args.policy))
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"B0 operational monitoring: {payload['status']}")
        for check in payload["checks"]:
            print(f"{check['status']:<7} {check['name']}: {check['message']}")
    return 2 if args.strict and payload["status"] != "pass" else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
