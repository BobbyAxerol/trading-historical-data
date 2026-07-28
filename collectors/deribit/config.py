from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from collectors.common.env import config_root, data_root, state_root

DEFAULT_CONFIG_NAME = "deribit_historical_v1.yml"
SUPPORTED_VERSION = "v1"

REQUIRED_TOP_LEVEL = (
    "dataset",
    "scope",
    "api",
    "runtime",
    "memory",
    "checkpoint",
    "instrument_discovery",
    "broad_ingestion",
    "staging",
    "canonical_trades",
    "compaction",
    "snapshot_5m",
    "snapshot_1m",
    "pricing",
    "execution_proxy",
    "held_overlay",
    "disk_budget",
    "cleanup",
    "validation",
    "monitoring",
)

REQUIRED_VERSION_KEYS = (
    "version",
    "universe_version",
    "schema_version",
    "snapshot_version",
    "pricing_version",
    "execution_proxy_version",
)


class DeribitConfigError(ValueError):
    """Raised when the Deribit V1 config is missing required interface fields."""


@dataclass(frozen=True)
class DeribitConfig:
    raw: dict[str, Any]
    config_path: Path
    config_hash: str

    @property
    def version(self) -> str:
        return str(self.raw["dataset"]["version"])

    @property
    def currency(self) -> str:
        return str(self.raw["scope"]["currency"]).upper()

    @property
    def checkpoint_path(self) -> Path:
        configured = Path(str(self.raw["checkpoint"]["path"]))
        if configured.is_absolute():
            return configured
        parts = configured.parts
        if parts and parts[0] == "state":
            return state_root().joinpath(*parts[1:])
        return state_root() / configured

    @property
    def staging_root(self) -> Path:
        return _resolve_storage_path(str(self.raw["staging"]["root"]))

    @property
    def canonical_trades_root(self) -> Path:
        return _resolve_storage_path(str(self.raw["canonical_trades"]["root"]))

    @property
    def snapshot_5m_root(self) -> Path:
        return _resolve_storage_path(str(self.raw["snapshot_5m"]["root"]))

    def to_storage_reference(self, path: str | Path | None) -> str | None:
        """Return a DATA_ROOT-portable storage reference when possible."""
        if path is None:
            return None
        return storage_reference(path) or str(path)

    def resolve_storage_reference(self, path: str | Path) -> Path:
        return resolve_storage_reference(path)


def _resolve_storage_path(value: str) -> Path:
    configured = Path(value)
    if configured.is_absolute():
        return configured
    parts = configured.parts
    if parts and parts[0] == "storage":
        return data_root().joinpath(*parts[1:])
    return data_root() / configured


def storage_reference(path: str | Path) -> str | None:
    """Normalize a storage path to a portable ``storage/...`` reference.

    Historical pilot runs wrote host absolute paths into the checkpoint. Docker
    jobs mount the same directory at /app/storage, so checkpoint references must
    be portable across runtimes.
    """
    value = str(path)
    configured = Path(value)
    parts = configured.parts
    if parts and parts[0] == "storage":
        return Path(*parts).as_posix()
    try:
        relative = configured.resolve().relative_to(data_root())
        return Path("storage", relative).as_posix()
    except ValueError:
        pass
    if "storage" in parts:
        index = parts.index("storage")
        trailing = parts[index + 1 :]
        if trailing:
            return Path("storage", *trailing).as_posix()
    return None


def resolve_storage_reference(path: str | Path) -> Path:
    reference = storage_reference(path)
    if reference:
        relative_parts = Path(reference).parts[1:]
        return data_root().joinpath(*relative_parts)
    return Path(str(path))


def _stable_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_deribit_config(payload: dict[str, Any], *, expected_version: str = SUPPORTED_VERSION) -> None:
    missing = [key for key in REQUIRED_TOP_LEVEL if key not in payload]
    if missing:
        raise DeribitConfigError(f"Missing top-level config sections: {missing}")

    dataset = payload.get("dataset") or {}
    missing_versions = [key for key in REQUIRED_VERSION_KEYS if key not in dataset]
    if missing_versions:
        raise DeribitConfigError(f"Missing dataset version keys: {missing_versions}")
    if str(dataset.get("version")) != expected_version:
        raise DeribitConfigError(f"Unsupported Deribit config version: {dataset.get('version')!r}")

    scope = payload.get("scope") or {}
    if str(scope.get("currency", "")).upper() != "BTC":
        raise DeribitConfigError("Deribit V1 supports only scope.currency=BTC")
    if scope.get("kind") != "option":
        raise DeribitConfigError("Deribit V1 supports only scope.kind=option")
    if scope.get("orderbook") is not False:
        raise DeribitConfigError("Deribit V1 must not enable historical orderbook")

    checkpoint = payload.get("checkpoint") or {}
    if checkpoint.get("backend") != "sqlite":
        raise DeribitConfigError("Deribit V1 checkpoint.backend must be sqlite")
    if not checkpoint.get("disk_before_checkpoint"):
        raise DeribitConfigError("Deribit V1 requires disk_before_checkpoint=true")

    snapshot_5m = payload.get("snapshot_5m") or {}
    if int(snapshot_5m.get("max_rows_per_timestamp", 0)) > 64:
        raise DeribitConfigError("snapshot_5m.max_rows_per_timestamp must be <=64")
    if int(snapshot_5m.get("max_total_expiries", 0)) > 7:
        raise DeribitConfigError("snapshot_5m.max_total_expiries must be <=7")

    snapshot_1m = payload.get("snapshot_1m") or {}
    if snapshot_1m.get("persistent_full_history") is not False:
        raise DeribitConfigError("Deribit V1 forbids persistent full-history 1m snapshots")

    disk_budget = payload.get("disk_budget") or {}
    if float(disk_budget.get("post_cleanup_filesystem_limit_gib", 0)) > 10.0:
        raise DeribitConfigError("post-cleanup filesystem budget must be <=10 GiB")


def load_deribit_config(path: str | Path | None = None, *, version: str = SUPPORTED_VERSION) -> DeribitConfig:
    config_path = Path(path).resolve() if path is not None else (config_root() / DEFAULT_CONFIG_NAME).resolve()
    if not config_path.exists():
        raise FileNotFoundError(config_path)
    with config_path.open() as fh:
        payload = yaml.safe_load(fh) or {}
    validate_deribit_config(payload, expected_version=version)
    return DeribitConfig(raw=payload, config_path=config_path, config_hash=_stable_hash(payload))
