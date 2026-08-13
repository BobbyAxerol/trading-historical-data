"""Read-only B0 operational monitoring status.

The status command deliberately separates the alert channel's readiness from
the condition it watches.  A Discord monitor may be healthy while reporting a
real collector failure; the latter must keep this command fail-closed.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from collectors.production_preflight import (
    POLICY_PATH,
    _load_policy,
    _parse_utc,
    _runtime_paths,
    accepted_technical_debt_waiver,
)


ALERT_CATEGORIES = (
    "collector_exit_alert",
    "retry_rate_limit_alert",
    "validation_repair_alert",
    "rss_alert",
    "backup_failure_alert",
)
HEALTHY_HEARTBEAT_STATUSES = {"ok", "success", "sleeping"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    return payload if isinstance(payload, dict) else None


def _check(name: str, passed: bool, message: str, *, status: str | None = None, **details: Any) -> dict[str, Any]:
    return {"name": name, "status": status or ("pass" if passed else "blocked"), "message": message, **details}


def _accepted_status(status: str) -> bool:
    return status in {"pass", "waived"}


def _active_heartbeat_datasets(policy: dict[str, Any], state: Path) -> tuple[list[str], list[str]]:
    """Return explicit scheduled datasets and any names outside the policy list.

    A one-shot B0 seed writes a heartbeat as useful telemetry, but it is not a
    scheduled service and therefore must not become a false stale alert.  The
    runtime registry is updated before long-lived services are enabled.
    """

    configured = [str(name) for name in policy["monitoring"].get("expected_heartbeat_datasets", [])]
    evidence = _read_json(state / "bootstrap" / "monitoring.json") or {}
    requested = evidence.get("active_heartbeat_datasets")
    if requested is None:
        requested = configured  # backward-compatible, fail-closed default
    active = [str(name) for name in requested] if isinstance(requested, list) else []
    unknown = sorted(set(active) - set(configured))
    return sorted(dict.fromkeys(active)), unknown


def _heartbeat_details(state: Path, active: list[str], now: datetime, max_age_minutes: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    expected: list[dict[str, Any]] = []
    error_heartbeats: list[dict[str, Any]] = []
    heartbeat_root = state / "heartbeats"
    all_payloads: dict[str, dict[str, Any]] = {}
    if heartbeat_root.is_dir():
        for path in sorted(heartbeat_root.glob("*.json")):
            payload = _read_json(path)
            if payload is not None:
                all_payloads[path.stem] = payload

    for name in active:
        payload = all_payloads.get(name)
        updated_at = None if payload is None else _parse_utc(payload.get("updated_at"))
        age_minutes = None if updated_at is None else (now - updated_at).total_seconds() / 60.0
        heartbeat_status = None if payload is None else str(payload.get("status", ""))
        fresh = bool(
            payload
            and heartbeat_status in HEALTHY_HEARTBEAT_STATUSES
            and age_minutes is not None
            and age_minutes <= max_age_minutes
        )
        expected.append(
            {
                "dataset": name,
                "status": "pass" if fresh else "blocked",
                "heartbeat_status": heartbeat_status or None,
                "age_minutes": None if age_minutes is None else round(age_minutes, 2),
            }
        )

    for name, payload in all_payloads.items():
        status = str(payload.get("status", ""))
        if status and status not in HEALTHY_HEARTBEAT_STATUSES:
            updated_at = _parse_utc(payload.get("updated_at"))
            error_heartbeats.append(
                {
                    "dataset": name,
                    "heartbeat_status": status,
                    "age_minutes": None if updated_at is None else round((now - updated_at).total_seconds() / 60.0, 2),
                    "error_class": _error_class(payload.get("error")),
                }
            )
    return expected, error_heartbeats


def _error_class(value: object) -> str:
    text = str(value or "").lower()
    if any(marker in text for marker in ("validat", "repair", "gap")):
        return "validation_repair"
    if any(marker in text for marker in ("429", "rate limit", "retry-after", "too many requests")):
        return "retry_rate_limit"
    return "collector_exit"


def _event_is_active(state: Path, category: str, now: datetime, window_minutes: float) -> tuple[bool, dict[str, Any] | None]:
    payload = _read_json(state / "operational-events" / f"{category}.json")
    if payload is None or payload.get("status") != "alert":
        return False, payload
    updated_at = _parse_utc(payload.get("updated_at"))
    if updated_at is None:
        return True, payload
    return (now - updated_at).total_seconds() <= window_minutes * 60, payload


def _alert_channel_ready(monitor_state: dict[str, Any] | None, category: str) -> bool:
    if not isinstance(monitor_state, dict):
        return False
    categories = monitor_state.get("alert_categories")
    category_state = categories.get(category) if isinstance(categories, dict) else None
    return isinstance(category_state, dict) and category_state.get("status") == "pass"


def _rss_violations(state: Path, active: list[str], policy: dict[str, Any]) -> list[dict[str, Any]]:
    monitoring = policy["monitoring"]
    default_limit = float(monitoring.get("default_rss_limit_mb", 1024))
    per_dataset = monitoring.get("rss_limit_mb_by_dataset") or {}
    violations: list[dict[str, Any]] = []
    for name in active:
        payload = _read_json(state / "heartbeats" / f"{name}.json")
        if payload is None:
            continue
        value = payload.get("peak_rss_mb")
        try:
            observed = float(value)
        except (TypeError, ValueError):
            continue
        limit = float(per_dataset.get(name, default_limit))
        if observed > limit:
            violations.append({"dataset": name, "peak_rss_mb": round(observed, 2), "limit_mb": limit})
    return violations


def operational_status(
    policy: dict[str, Any],
    *,
    now: datetime | None = None,
    require_monitor_service: bool = True,
) -> dict[str, Any]:
    """Return disk, Discord channel, and collector health readiness safely."""

    current = now or _utc_now()
    runtime = _runtime_paths(policy)
    runtime_root = runtime["root"]
    state = runtime["state"]
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

    active, unknown_active = _active_heartbeat_datasets(policy, state)
    heartbeat_max_age = float(monitoring["heartbeat_max_age_minutes"])
    heartbeat_details, error_heartbeats = _heartbeat_details(state, active, current, heartbeat_max_age)
    heartbeats_ok = not unknown_active and all(item["status"] == "pass" for item in heartbeat_details)
    checks.append(
        _check(
            "expected_heartbeats",
            heartbeats_ok,
            "Each explicitly active scheduled dataset needs a recent successful heartbeat.",
            max_age_minutes=heartbeat_max_age,
            active_datasets=active,
            unknown_active_datasets=unknown_active,
            heartbeats=heartbeat_details,
        )
    )

    monitor_relative = str(monitoring.get("discord_monitor_state_relative_path", "state/monitoring/discord_monitor.json"))
    monitor_state_path = runtime_root / monitor_relative
    monitor_state = _read_json(monitor_state_path)
    monitor_updated_at = None if monitor_state is None else _parse_utc(monitor_state.get("updated_at"))
    monitor_max_age = float(monitoring.get("discord_monitor_max_age_minutes", 3))
    monitor_fresh = bool(monitor_updated_at and (current - monitor_updated_at).total_seconds() <= monitor_max_age * 60)
    monitor_ok = bool(monitor_state and monitor_fresh and monitor_state.get("status") in {"pass", "pass_with_accepted_waivers"})
    if require_monitor_service:
        checks.append(
            _check(
                "discord_monitor_service",
                monitor_ok,
                "The read-only Discord monitor must report a recent successful cycle.",
                state_path=str(monitor_state_path),
                monitor_status=None if monitor_state is None else monitor_state.get("status"),
                updated_at=None if monitor_updated_at is None else monitor_updated_at.isoformat(),
                max_age_minutes=monitor_max_age,
            )
        )

    retry_window_minutes = float(monitoring.get("retry_rate_limit_window_minutes", 10))
    collector_exit_active, collector_exit_event = _event_is_active(state, "collector_exit", current, retry_window_minutes)
    retry_active, retry_event = _event_is_active(state, "retry_rate_limit", current, retry_window_minutes)
    validation_active, validation_event = _event_is_active(state, "validation_repair", current, retry_window_minutes)
    backup_active, backup_event = _event_is_active(state, "backup_failure", current, retry_window_minutes)
    validation_from_heartbeats = [item for item in error_heartbeats if item["error_class"] == "validation_repair"]
    retry_from_heartbeats = [item for item in error_heartbeats if item["error_class"] == "retry_rate_limit"]
    collector_exit_from_heartbeats = [item for item in error_heartbeats if item["error_class"] == "collector_exit"]
    rss_violations = _rss_violations(state, active, policy)

    category_conditions = {
        "collector_exit_alert": collector_exit_active or bool(error_heartbeats),
        "retry_rate_limit_alert": retry_active or bool(retry_from_heartbeats),
        "validation_repair_alert": validation_active or bool(validation_from_heartbeats),
        "rss_alert": bool(rss_violations),
        "backup_failure_alert": backup_active,
    }
    category_details = {
        "collector_exit_alert": {
            "event": collector_exit_event,
            "error_heartbeats": error_heartbeats,
            "collector_exit_heartbeats": collector_exit_from_heartbeats,
            "window_minutes": retry_window_minutes,
        },
        "retry_rate_limit_alert": {"event": retry_event, "error_heartbeats": retry_from_heartbeats, "window_minutes": retry_window_minutes},
        "validation_repair_alert": {"event": validation_event, "error_heartbeats": validation_from_heartbeats, "window_minutes": retry_window_minutes},
        "rss_alert": {"violations": rss_violations},
        "backup_failure_alert": {"event": backup_event, "window_minutes": retry_window_minutes},
    }
    backup_waiver = accepted_technical_debt_waiver(policy, "backup_and_restore")
    for category in ALERT_CATEGORIES:
        condition_active = bool(category_conditions[category])
        waiver = backup_waiver if category == "backup_failure_alert" and not condition_active else None
        channel_ready = _alert_channel_ready(monitor_state, category)
        passed = channel_ready and not condition_active
        status_override = "waived" if waiver is not None and channel_ready else None
        checks.append(
            _check(
                category,
                passed,
                "Discord alert routing must be tested and any active condition must remain unresolved until repaired.",
                status=status_override,
                channel_ready=channel_ready,
                condition_active=condition_active,
                accepted_technical_debt=waiver,
                **category_details[category],
            )
        )

    accepted = all(_accepted_status(str(item["status"])) for item in checks)
    has_waiver = any(item["status"] == "waived" for item in checks)
    return {
        "schema_version": 2,
        "observed_at": current.replace(microsecond=0).isoformat(),
        "runtime_root": str(runtime_root),
        "status": "pass_with_accepted_waivers" if accepted and has_waiver else "pass" if accepted else "blocked",
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
    return 2 if args.strict and payload["status"] == "blocked" else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
