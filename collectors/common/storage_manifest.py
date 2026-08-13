"""Atomic storage release-manifest and reader compatibility contract.

The manifest lives outside canonical partitions at
``storage/_primus_metadata/release_manifest.json``.  Collectors must update it
only after their own canonical-data validation succeeds.  Package loaders will
call :func:`assert_loader_compatible` in Phase C before querying a dataset.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
MANIFEST_RELATIVE_PATH = Path("_primus_metadata") / "release_manifest.json"


class StorageManifestError(RuntimeError):
    """The storage release manifest is absent, malformed, or incomplete."""


class StorageCompatibilityError(StorageManifestError):
    """A reader cannot safely interpret the declared dataset layout/schema."""


def release_manifest_path(storage_root: str | Path) -> Path:
    return Path(storage_root) / MANIFEST_RELATIVE_PATH


def _require_nonempty_text(payload: dict[str, Any], key: str, *, context: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise StorageManifestError(f"{context}.{key} must be a non-empty string")
    return value


def _require_text_list(payload: dict[str, Any], key: str, *, context: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list) or not value or not all(isinstance(item, str) and item.strip() for item in value):
        raise StorageManifestError(f"{context}.{key} must be a non-empty list of strings")
    return [str(item) for item in value]


def validate_release_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate the B0 manifest shape without accepting a release as usable."""

    if not isinstance(payload, dict):
        raise StorageManifestError("release manifest must be a JSON object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise StorageManifestError(f"unsupported release manifest schema_version: {payload.get('schema_version')!r}")
    _require_nonempty_text(payload, "environment_id", context="release_manifest")
    _require_nonempty_text(payload, "created_at", context="release_manifest")
    _require_nonempty_text(payload, "source_inventory_reference", context="release_manifest")

    git = payload.get("git")
    if not isinstance(git, dict):
        raise StorageManifestError("release_manifest.git must be an object")
    _require_nonempty_text(git, "commit", context="release_manifest.git")
    _require_nonempty_text(git, "tag", context="release_manifest.git")

    build = payload.get("build")
    if not isinstance(build, dict):
        raise StorageManifestError("release_manifest.build must be an object")
    for key in ("collector_image", "python_base_image", "python", "duckdb", "pyarrow"):
        _require_nonempty_text(build, key, context="release_manifest.build")

    storage = payload.get("storage")
    if not isinstance(storage, dict):
        raise StorageManifestError("release_manifest.storage must be an object")
    _require_text_list(storage, "supported_loader_contract_versions", context="release_manifest.storage")
    _require_nonempty_text(storage, "schema_migration_policy", context="release_manifest.storage")
    _require_nonempty_text(storage, "incompatible_reader_policy", context="release_manifest.storage")

    datasets = payload.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise StorageManifestError("release_manifest.datasets must be a non-empty list")
    seen: set[str] = set()
    for dataset in datasets:
        if not isinstance(dataset, dict):
            raise StorageManifestError("each release_manifest.datasets item must be an object")
        dataset_id = _require_nonempty_text(dataset, "dataset_id", context="release_manifest.datasets[]")
        if dataset_id in seen:
            raise StorageManifestError(f"duplicate release manifest dataset_id: {dataset_id}")
        seen.add(dataset_id)
        _require_nonempty_text(dataset, "canonical_schema_version", context=f"dataset[{dataset_id}]")
        _require_nonempty_text(dataset, "partition_layout_version", context=f"dataset[{dataset_id}]")
        _require_text_list(dataset, "supported_loader_contract_versions", context=f"dataset[{dataset_id}]")
        _require_nonempty_text(dataset, "source_report_reference", context=f"dataset[{dataset_id}]")
    return payload


def read_release_manifest(storage_root: str | Path) -> dict[str, Any]:
    path = release_manifest_path(storage_root)
    if not path.exists():
        raise StorageManifestError(f"missing storage release manifest: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StorageManifestError(f"cannot read storage release manifest {path}: {type(exc).__name__}: {exc}") from exc
    return validate_release_manifest(payload)


def write_release_manifest(storage_root: str | Path, payload: dict[str, Any]) -> Path:
    """Atomically replace a complete release manifest after validation succeeds."""

    validated = validate_release_manifest(payload)
    path = release_manifest_path(storage_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".release-manifest-", suffix=".json", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(validated, handle, indent=2, sort_keys=True)
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
    return path


def validate_accepted_release_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate a manifest that is eligible for reader use or B0 acceptance."""

    validated = validate_release_manifest(payload)
    if validated.get("status") != "pass":
        raise StorageCompatibilityError(f"storage release is not accepted (status={validated.get('status')!r})")
    return validated


def assert_loader_compatible(
    storage_root: str | Path,
    *,
    dataset_id: str,
    loader_contract_version: str,
) -> dict[str, Any]:
    """Return the declared dataset contract or raise a clear safe-read error."""

    manifest = validate_accepted_release_manifest(read_release_manifest(storage_root))
    storage_contracts = set(manifest["storage"]["supported_loader_contract_versions"])
    if loader_contract_version not in storage_contracts:
        raise StorageCompatibilityError(
            f"loader contract {loader_contract_version!r} is unsupported by storage release; supported={sorted(storage_contracts)!r}"
        )
    dataset = next((item for item in manifest["datasets"] if item["dataset_id"] == dataset_id), None)
    if dataset is None:
        raise StorageCompatibilityError(f"dataset {dataset_id!r} is not declared in the storage release manifest")
    dataset_contracts = set(dataset["supported_loader_contract_versions"])
    if loader_contract_version not in dataset_contracts:
        raise StorageCompatibilityError(
            f"loader contract {loader_contract_version!r} cannot read dataset {dataset_id!r}; "
            f"dataset supports={sorted(dataset_contracts)!r}, "
            f"schema={dataset['canonical_schema_version']!r}, layout={dataset['partition_layout_version']!r}"
        )
    return dataset
