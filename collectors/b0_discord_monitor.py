"""Read-only Discord delivery loop for Phase B0 operational alerts.

Webhook credentials are read only from a Docker secret file.  This module never
prints the URL, serializes it into state, or accepts arbitrary webhook hosts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from collectors.b0_operational_status import ALERT_CATEGORIES, operational_status
from collectors.production_preflight import POLICY_PATH, _load_policy, _runtime_paths


DISCORD_ALLOWED_HOSTS = {"discord.com", "discordapp.com"}
DEFAULT_SECRET_PATH = Path("/run/secrets/primus_hmd_discord_webhook")
Sender = Callable[[str, str], None]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.stem}-", suffix=".json", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _state_path(policy: dict[str, Any], override: Path | None) -> Path:
    if override is not None:
        return override
    runtime = _runtime_paths(policy)
    relative = str(policy["monitoring"].get("discord_monitor_state_relative_path", "state/monitoring/discord_monitor.json"))
    return runtime["root"] / relative


def _webhook_secret_path() -> Path:
    configured = os.getenv("DISCORD_WEBHOOK_URL_FILE", "").strip()
    return Path(configured) if configured else DEFAULT_SECRET_PATH


def load_discord_webhook_url(path: Path | None = None) -> str:
    secret_path = path or _webhook_secret_path()
    try:
        value = secret_path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise RuntimeError("Discord webhook secret is unavailable") from exc
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise RuntimeError("Discord webhook secret is not an allowed HTTPS Discord webhook URL") from exc
    if (
        parsed.scheme != "https"
        or parsed.hostname not in DISCORD_ALLOWED_HOSTS
        or port is not None
        or not parsed.path.startswith("/api/webhooks/")
        or not parsed.path[len("/api/webhooks/") :].strip("/")
    ):
        raise RuntimeError("Discord webhook secret is not an allowed HTTPS Discord webhook URL")
    return value


def send_discord_message(webhook_url: str, content: str, *, timeout_seconds: float = 10.0) -> None:
    """Post a bounded, no-mention Discord message without exposing the URL."""

    payload = json.dumps(
        {
            "content": str(content)[:1900],
            "allowed_mentions": {"parse": []},
        }
    ).encode("utf-8")
    request = Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "primus-hmd-b0-monitor/1"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 - URL is validated above.
            if not 200 <= int(response.status) < 300:
                raise RuntimeError("Discord webhook returned a non-success status")
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        raise RuntimeError("Discord webhook delivery failed") from exc


def _fingerprint(status_payload: dict[str, Any]) -> str:
    compact = {
        "status": status_payload.get("status"),
        "checks": [(item.get("name"), item.get("status")) for item in status_payload.get("checks", [])],
    }
    return hashlib.sha256(json.dumps(compact, sort_keys=True).encode("utf-8")).hexdigest()


def _summary(status_payload: dict[str, Any]) -> str:
    blocked = [str(item.get("name")) for item in status_payload.get("checks", []) if item.get("status") == "blocked"]
    status = str(status_payload.get("status", "blocked"))
    if blocked:
        return f"[Primus HMD][B0] monitor status={status}; active alerts: {', '.join(blocked[:8])}."
    return f"[Primus HMD][B0] monitor status={status}; monitored controls are healthy."


def _test_alerts(webhook_url: str, state: dict[str, Any], sender: Sender) -> tuple[dict[str, Any], bool]:
    categories: dict[str, dict[str, Any]] = {}
    now = _utc_now_iso()
    all_ok = True
    for category in ALERT_CATEGORIES:
        try:
            sender(webhook_url, f"[Primus HMD][B0][TEST] Discord routing verified for {category}.")
            categories[category] = {"status": "pass", "tested_at": now, "provider": "discord-webhook"}
        except Exception:
            categories[category] = {"status": "blocked", "tested_at": now, "provider": "discord-webhook"}
            all_ok = False
    state["alert_categories"] = categories
    state["webhook"] = {"status": "pass" if all_ok else "blocked", "provider": "discord-webhook", "last_tested_at": now}
    return state, all_ok


def run_cycle(
    policy: dict[str, Any],
    *,
    state_file: Path | None = None,
    test_alerts: bool = False,
    sender: Sender = send_discord_message,
) -> bool:
    """Deliver tests/changes once and persist only non-secret monitor state."""

    path = _state_path(policy, state_file)
    state = _read_json(path)
    try:
        webhook_url = load_discord_webhook_url()
    except RuntimeError:
        state.update(
            {
                "schema_version": 1,
                "service": "b0-discord-monitor",
                "status": "blocked",
                "updated_at": _utc_now_iso(),
                "webhook": {"status": "blocked", "provider": "discord-webhook"},
            }
        )
        _write_json(path, state)
        return False

    if test_alerts:
        state, tests_ok = _test_alerts(webhook_url, state, sender)
        state.update({"schema_version": 1, "service": "b0-discord-monitor", "updated_at": _utc_now_iso()})
        _write_json(path, state)
        if not tests_ok:
            state["status"] = "blocked"
            _write_json(path, state)
            return False

    status_payload = operational_status(policy, require_monitor_service=False)
    fingerprint = _fingerprint(status_payload)
    previous_fingerprint = state.get("operational_fingerprint")
    delivered_at: str | None = None
    try:
        if previous_fingerprint and previous_fingerprint != fingerprint:
            sender(webhook_url, _summary(status_payload))
            delivered_at = _utc_now_iso()
        elif not previous_fingerprint and not test_alerts:
            sender(webhook_url, "[Primus HMD][B0] Discord monitor is online and will alert on control changes.")
            delivered_at = _utc_now_iso()
    except Exception:
        state.update(
            {
                "schema_version": 1,
                "service": "b0-discord-monitor",
                "status": "blocked",
                "updated_at": _utc_now_iso(),
                "webhook": {"status": "blocked", "provider": "discord-webhook"},
            }
        )
        _write_json(path, state)
        return False

    previous_webhook = state.get("webhook")
    webhook_state = {
        "status": "pass",
        "provider": "discord-webhook",
    }
    if isinstance(previous_webhook, dict) and isinstance(previous_webhook.get("last_tested_at"), str):
        webhook_state["last_tested_at"] = previous_webhook["last_tested_at"]
    if isinstance(previous_webhook, dict) and isinstance(previous_webhook.get("last_delivery_at"), str):
        webhook_state["last_delivery_at"] = previous_webhook["last_delivery_at"]
    if delivered_at is not None:
        webhook_state["last_delivery_at"] = delivered_at

    state.update(
        {
            "schema_version": 1,
            "service": "b0-discord-monitor",
            "status": "pass",
            "updated_at": _utc_now_iso(),
            "webhook": webhook_state,
            "operational_status": status_payload.get("status"),
            "operational_fingerprint": fingerprint,
            "blocked_checks": [item["name"] for item in status_payload.get("checks", []) if item.get("status") == "blocked"],
        }
    )
    _write_json(path, state)
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Primus B0 Discord operational monitor")
    parser.add_argument("--policy", type=Path, default=POLICY_PATH)
    parser.add_argument("--state-file", type=Path, default=None)
    parser.add_argument("--loop-seconds", type=int, default=60)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--test-alerts", action="store_true")
    args = parser.parse_args(argv)
    if args.loop_seconds < 10:
        parser.error("--loop-seconds must be at least 10")

    policy = _load_policy(args.policy)
    first = True
    while True:
        ok = run_cycle(policy, state_file=args.state_file, test_alerts=bool(args.test_alerts and first))
        if args.once:
            return 0 if ok else 2
        first = False
        time.sleep(args.loop_seconds)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
