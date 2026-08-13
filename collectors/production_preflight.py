"""Phase B0 production preflight evidence and fail-closed status checks.

This module intentionally never starts a collector, opens a source connection,
or writes market-data partitions.  It only creates the dedicated runtime
directory skeleton and validates B0 evidence written by the operator.
"""

from __future__ import annotations

import argparse
import grp
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from collectors.common.storage_manifest import StorageManifestError, validate_accepted_release_manifest


REPO_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = REPO_ROOT / "configs" / "primus_hmd_b0.yml"
SOURCE_INVENTORY_TEMPLATE_PATH = REPO_ROOT / "docs" / "b0" / "source_inventory.template.json"
SCHEMA_VERSION = 1
REQUIRED_RUNTIME_DIRECTORIES = ("storage", "state", "logs", "releases")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def _load_policy(path: Path = POLICY_PATH) -> dict[str, Any]:
    with path.open() as handle:
        policy = yaml.safe_load(handle) or {}
    if int(policy.get("schema_version", 0)) != SCHEMA_VERSION:
        raise ValueError(f"Unsupported B0 policy schema: {policy.get('schema_version')!r}")
    runtime = policy.get("runtime") or {}
    if not runtime.get("root"):
        raise ValueError("B0 policy requires runtime.root")
    return policy


def _runtime_paths(policy: dict[str, Any]) -> dict[str, Path]:
    runtime_root = Path(str(policy["runtime"]["root"])).resolve()
    return {
        "root": runtime_root,
        "storage": runtime_root / "storage",
        "state": runtime_root / "state",
        "logs": runtime_root / "logs",
        "releases": runtime_root / "releases",
        "bootstrap": runtime_root / "state" / "bootstrap",
        "metadata": runtime_root / "storage" / "_primus_metadata",
    }


def _contains_required(value: Any) -> bool:
    return isinstance(value, str) and value.strip() and not value.startswith("REQUIRED:")


def _check(name: str, ok: bool, detail: str, **evidence: Any) -> dict[str, Any]:
    return {"name": name, "status": "pass" if ok else "blocked", "detail": detail, "evidence": evidence}


def _filesystem_snapshot(path: Path) -> dict[str, Any]:
    stats = os.statvfs(path)
    return {
        "path": str(path),
        "total_bytes": stats.f_frsize * stats.f_blocks,
        "free_bytes": stats.f_frsize * stats.f_bfree,
        "available_bytes": stats.f_frsize * stats.f_bavail,
        "total_inodes": stats.f_files,
        "free_inodes": stats.f_ffree,
        "available_inodes": stats.f_favail,
    }


def _capacity_report(policy: dict[str, Any], runtime: dict[str, Path]) -> dict[str, Any]:
    capacity = policy["capacity"]
    root = runtime["root"]
    snapshot = _filesystem_snapshot(root)
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "draft",
        "generated_at": utc_now_iso(),
        "filesystem": snapshot,
        "mount_options": "REQUIRED: record from findmnt -T runtime root",
        "docker_image_cache_budget_gib": capacity["docker_cache_reserve_gib"],
        "state_log_reserve_gib": capacity["state_log_reserve_gib"],
        "os_reserve_gib": capacity["os_reserve_gib"],
        "largest_expected_compaction_or_repair_temp_gib": "REQUIRED: bounded seed measurement",
        "datasets": capacity["datasets"],
        "concurrency": policy["concurrency"],
        "approval": {"status": "pending", "approved_by": None, "approved_at": None},
    }


def _release_manifest(policy: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "draft",
        "environment_id": policy["environment"]["id"],
        "created_at": utc_now_iso(),
        "git": {"commit": "REQUIRED", "tag": "REQUIRED"},
        "build": {
            "python_base_image": "REQUIRED: digest-pinned image reference",
            "collector_image": "REQUIRED: image digest",
            "docker_engine": "REQUIRED",
            "docker_compose": "REQUIRED",
            "python": "REQUIRED",
            "duckdb": "REQUIRED",
            "pyarrow": "REQUIRED",
            "timezone_and_ntp_evidence": "REQUIRED: state/bootstrap/clock.json",
        },
        "artifacts": {
            "reader_lock_sha256": "REQUIRED",
            "collector_lock_sha256": "REQUIRED",
            "wheel_filename": "PENDING_PHASE_C",
            "wheel_sha256": "PENDING_PHASE_C",
            "clean_rebuild_verified_at": "REQUIRED",
        },
        "config_sha256": "REQUIRED",
        "datasets": [],
        "storage": {
            "schema_migration_policy": "new layout versions are additive; incompatible readers fail clearly",
            "supported_loader_contract_versions": policy["storage_manifest"]["supported_loader_contract_versions"],
            "incompatible_reader_policy": policy["storage_manifest"]["incompatible_reader_policy"],
        },
    }


def initialize_runtime(policy: dict[str, Any], *, overwrite_drafts: bool = False) -> dict[str, Any]:
    paths = _runtime_paths(policy)
    for name in REQUIRED_RUNTIME_DIRECTORIES:
        paths[name].mkdir(parents=True, exist_ok=True)
    paths["bootstrap"].mkdir(parents=True, exist_ok=True)
    paths["metadata"].mkdir(parents=True, exist_ok=True)

    source_inventory_path = paths["bootstrap"] / "source_inventory.json"
    if overwrite_drafts or not source_inventory_path.exists():
        template = json.loads(SOURCE_INVENTORY_TEMPLATE_PATH.read_text())
        template["environment_id"] = policy["environment"]["id"]
        _atomic_write_json(source_inventory_path, template)

    capacity_path = paths["bootstrap"] / "capacity_report.json"
    if overwrite_drafts or not capacity_path.exists():
        _atomic_write_json(capacity_path, _capacity_report(policy, paths))

    release_path = paths["metadata"] / "release_manifest.json"
    if overwrite_drafts or not release_path.exists():
        _atomic_write_json(release_path, _release_manifest(policy))

    clock_path = paths["bootstrap"] / "clock.json"
    if overwrite_drafts or not clock_path.exists():
        _atomic_write_json(
            clock_path,
            {
                "schema_version": SCHEMA_VERSION,
                "status": "draft",
                "observed_at": utc_now_iso(),
                "host_timezone": datetime.now().astimezone().tzname(),
                "utc_time": datetime.now(timezone.utc).isoformat(),
                "required_timezones": policy["environment"]["required_timezones"],
                "ntp_synchronized": "REQUIRED: operator evidence",
            },
        )

    monitoring_path = paths["bootstrap"] / "monitoring.json"
    if overwrite_drafts or not monitoring_path.exists():
        _atomic_write_json(
            monitoring_path,
            {
                "schema_version": SCHEMA_VERSION,
                "status": "draft",
                "observed_at": utc_now_iso(),
                "disk_low_water_gib": policy["monitoring"]["free_disk_low_water_gib"],
                "inode_low_water_pct": policy["monitoring"]["inode_low_water_pct"],
                "heartbeat_max_age_minutes": policy["monitoring"]["heartbeat_max_age_minutes"],
                "operator_status_command": policy["monitoring"]["operator_status_command"],
                "collector_exit_alert": "REQUIRED: operator-visible evidence",
                "retry_rate_limit_alert": "REQUIRED: operator-visible evidence",
                "validation_repair_alert": "REQUIRED: operator-visible evidence",
                "backup_failure_alert": "REQUIRED: operator-visible evidence",
                "resource_policy": policy["monitoring"]["resource_policy"],
            },
        )

    restore_path = paths["bootstrap"] / "restore_drill.json"
    if overwrite_drafts or not restore_path.exists():
        _atomic_write_json(
            restore_path,
            {
                "schema_version": SCHEMA_VERSION,
                "status": "pending",
                "observed_at": None,
                "backup_destination": policy["backup"]["destination"],
                "restored_partition": None,
                "restored_checkpoint": None,
                "loader_or_validator_smoke": None,
                "evidence": None,
            },
        )

    access_path = paths["bootstrap"] / "access_control.json"
    if overwrite_drafts or not access_path.exists():
        _atomic_write_json(
            access_path,
            {
                "schema_version": SCHEMA_VERSION,
                "status": "draft",
                "reader_group": policy["runtime"]["reader_group"],
                "collector_identity": {
                    "uid": policy["runtime"]["collector_uid"],
                    "gid": policy["runtime"]["collector_gid"],
                },
                "storage_default_acl": "REQUIRED: reader group has read/traverse only",
                "reader_read_probe": "REQUIRED",
                "reader_write_rejected": "REQUIRED",
                "state_logs_secrets_rejected": "REQUIRED",
            },
        )

    compose_path = paths["bootstrap"] / "compose_contract.json"
    if overwrite_drafts or not compose_path.exists():
        _atomic_write_json(
            compose_path,
            {
                "schema_version": SCHEMA_VERSION,
                "status": "draft",
                "resolved_at": None,
                "compose_config_evidence": "REQUIRED: docker compose --env-file <protected file> config",
                "container_mount_evidence": "REQUIRED: inspect a bounded non-collector container before service start",
                "runtime_root": str(paths["root"]),
                "required_mounts": {"storage": "/app/storage", "state": "/app/state", "logs": "/app/logs"},
                "no_writable_checkout_mount": "REQUIRED",
                "no_source_code_bind_mount": "REQUIRED",
            },
        )
    return {"status": "ok", "runtime_root": str(paths["root"]), "created_at": utc_now_iso()}


def _status(policy: dict[str, Any]) -> dict[str, Any]:
    paths = _runtime_paths(policy)
    checks: list[dict[str, Any]] = []
    root = paths["root"]
    required_dirs = list(policy["runtime"].get("required_directories", REQUIRED_RUNTIME_DIRECTORIES))
    actual_required = [root / name for name in required_dirs]
    dirs_ok = root.is_dir() and all(path.is_dir() for path in actual_required)
    checks.append(
        _check(
            "runtime_layout",
            dirs_ok,
            "Dedicated runtime root and required directories must exist outside the Git checkout.",
            root=str(root),
            required_directories=[str(path) for path in actual_required],
        )
    )

    source_checkout = Path(str(policy["runtime"]["source_checkout"])).resolve()
    isolated = source_checkout != root and source_checkout not in root.parents and root not in source_checkout.parents
    checks.append(
        _check(
            "runtime_checkout_isolation",
            isolated,
            "Runtime root must be separate from the source checkout.",
            runtime_root=str(root),
            source_checkout=str(source_checkout),
        )
    )

    expected_uid = int(policy["runtime"]["collector_uid"])
    expected_gid = int(policy["runtime"]["collector_gid"])
    ownership = []
    for path in actual_required:
        if path.exists():
            stat = path.stat()
            ownership.append({"path": str(path), "uid": stat.st_uid, "gid": stat.st_gid, "mode": oct(stat.st_mode & 0o777)})
    ownership_ok = bool(ownership) and all(item["uid"] == expected_uid and item["gid"] == expected_gid for item in ownership)
    checks.append(
        _check(
            "runtime_ownership",
            ownership_ok,
            "Collector UID/GID must own every runtime directory before containers start.",
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            observed=ownership,
        )
    )

    reader_group = str(policy["runtime"]["reader_group"])
    try:
        group = grp.getgrnam(reader_group)
    except KeyError:
        group = None
    access_path = paths["bootstrap"] / "access_control.json"
    access = _read_json(access_path)
    access_ok = bool(group and access and access.get("status") == "pass")
    checks.append(
        _check(
            "reader_group_and_acl",
            access_ok,
            "Reader group and read-only ACL evidence must protect storage while denying state, logs, and secrets.",
            reader_group=reader_group,
            group_gid=None if group is None else group.gr_gid,
            path=str(access_path),
        )
    )

    capacity_path = paths["bootstrap"] / "capacity_report.json"
    capacity = _read_json(capacity_path)
    capacity_ok = bool(capacity and capacity.get("status") == "pass" and capacity.get("approval", {}).get("status") == "approved")
    checks.append(_check("capacity_and_concurrency", capacity_ok, "Capacity report and concurrency matrix require explicit approval.", path=str(capacity_path)))

    inventory_path = paths["bootstrap"] / "source_inventory.json"
    inventory = _read_json(inventory_path)
    source_statuses = [item.get("source_probe", {}).get("status") for item in (inventory or {}).get("datasets", [])]
    inventory_ok = bool(source_statuses) and all(status == "pass" for status in source_statuses) and bool(inventory and inventory.get("environment_id") == policy["environment"]["id"])
    checks.append(_check("source_inventory", inventory_ok, "Every enabled dataset needs a passing bounded source probe from this environment.", path=str(inventory_path), source_statuses=source_statuses, environment_id=None if inventory is None else inventory.get("environment_id")))

    release_path = paths["metadata"] / "release_manifest.json"
    release = _read_json(release_path)
    release_error = None
    try:
        if release is None:
            raise StorageManifestError("release manifest is missing or invalid JSON")
        validate_accepted_release_manifest(release)
        release_required = True
    except StorageManifestError as exc:
        release_required = False
        release_error = str(exc)
    checks.append(
        _check(
            "reproducible_release_and_storage_manifest",
            release_required,
            "Release/storage manifest requires resolved hashes, image digest, build evidence, and compatibility contract.",
            path=str(release_path),
            validation_error=release_error,
        )
    )

    compose_path = paths["bootstrap"] / "compose_contract.json"
    compose = _read_json(compose_path)
    compose_ok = bool(compose and compose.get("status") == "pass")
    checks.append(_check("production_compose_contract", compose_ok, "Resolved Compose and mount inspection evidence must prove the dedicated runtime root and non-root ownership.", path=str(compose_path)))

    backup = policy["backup"]
    restore_path = paths["bootstrap"] / "restore_drill.json"
    restore = _read_json(restore_path)
    backup_ok = _contains_required(backup.get("destination")) and _contains_required(backup.get("retention")) and bool(restore and restore.get("status") == "pass")
    checks.append(_check("backup_and_restore", backup_ok, "An off-host destination, retention policy, and successful restore drill are required.", path=str(restore_path)))

    monitoring_path = paths["bootstrap"] / "monitoring.json"
    monitoring = _read_json(monitoring_path)
    monitoring_ok = bool(monitoring and monitoring.get("status") == "pass")
    checks.append(_check("monitoring_and_resource_policy", monitoring_ok, "Disk/inode, heartbeat, error, retry, validation, RSS, and backup monitoring must be active.", path=str(monitoring_path)))

    clock_path = paths["bootstrap"] / "clock.json"
    clock = _read_json(clock_path)
    clock_ok = bool(clock and clock.get("status") == "pass" and clock.get("ntp_synchronized") is True)
    rollback = policy["environment"]["rollback"]
    rollback_ok = _contains_required(rollback.get("previous_approved_release")) and _contains_required(rollback.get("previous_approved_data_root"))
    checks.append(_check("clock_environment_and_rollback", clock_ok and rollback_ok, "NTP evidence, environment identity, and an explicit single-root rollback path are required.", clock_path=str(clock_path), environment_id=policy["environment"]["id"], rollback=rollback))

    return {
        "schema_version": SCHEMA_VERSION,
        "phase": "B0",
        "generated_at": utc_now_iso(),
        "runtime_root": str(root),
        "status": "pass" if all(check["status"] == "pass" for check in checks) else "blocked",
        "checks": checks,
    }


def _print(payload: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    print(f"Phase {payload.get('phase', 'B0')} status: {payload['status']}")
    for check in payload.get("checks", []):
        print(f"{check['status']:7} {check['name']}: {check['detail']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Primus Historical Market Data Phase B0 preflight")
    parser.add_argument("--policy", type=Path, default=POLICY_PATH)
    parser.add_argument("--json", action="store_true")
    subparsers = parser.add_subparsers(dest="command", required=True)
    init = subparsers.add_parser("init-runtime", help="Create only the empty runtime metadata/evidence skeleton; never collect data.")
    init.add_argument("--json", action="store_true", default=argparse.SUPPRESS)
    init.add_argument("--overwrite-drafts", action="store_true")
    status = subparsers.add_parser("status", help="Report B0 gate status without starting any collector.")
    status.add_argument("--json", action="store_true", default=argparse.SUPPRESS)
    status.add_argument("--strict", action="store_true", help="Document that any incomplete B0 gate returns a non-zero exit code.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    policy = _load_policy(args.policy)
    if args.command == "init-runtime":
        _print(initialize_runtime(policy, overwrite_drafts=bool(args.overwrite_drafts)), args.json)
        return 0
    payload = _status(policy)
    _print(payload, args.json)
    return 0 if payload["status"] == "pass" else 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
