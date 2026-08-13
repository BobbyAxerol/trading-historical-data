from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from collectors.common.storage_manifest import (
    StorageCompatibilityError,
    StorageManifestError,
    assert_loader_compatible,
    read_release_manifest,
    release_manifest_path,
    validate_accepted_release_manifest,
    write_release_manifest,
)


def manifest(*, status: str = "pass", contracts: list[str] | None = None) -> dict:
    supported = contracts or ["hmd-loader-v1"]
    return {
        "schema_version": 1,
        "status": status,
        "environment_id": "test-environment",
        "created_at": "2026-08-13T00:00:00+00:00",
        "source_inventory_reference": "state/bootstrap/source_inventory.json",
        "git": {"commit": "abc123", "tag": "primus-historical-market-data-v0.1.0rc1"},
        "build": {
            "collector_image": "example@sha256:abc",
            "python_base_image": "python@sha256:def",
            "python": "3.12.13",
            "duckdb": "1.5.5",
            "pyarrow": "20.0.0",
        },
        "storage": {
            "supported_loader_contract_versions": supported,
            "schema_migration_policy": "additive layouts only",
            "incompatible_reader_policy": "raise clear compatibility error",
        },
        "datasets": [
            {
                "dataset_id": "deribit_btc_options_v1_compact_liquid",
                "canonical_schema_version": "trade_schema_v1",
                "partition_layout_version": "deribit-trades-hive-day-v1",
                "supported_loader_contract_versions": supported,
                "source_report_reference": "state/deribit_options/version=v1/api_probe_report.json",
            }
        ],
    }


class TestStorageManifest(unittest.TestCase):
    def test_writer_reader_and_matching_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "storage"
            path = write_release_manifest(root, manifest())
            self.assertEqual(path, release_manifest_path(root))
            self.assertEqual(read_release_manifest(root)["environment_id"], "test-environment")
            dataset = assert_loader_compatible(
                root,
                dataset_id="deribit_btc_options_v1_compact_liquid",
                loader_contract_version="hmd-loader-v1",
            )
            self.assertEqual(dataset["canonical_schema_version"], "trade_schema_v1")
            self.assertFalse(path.with_suffix(".json.tmp").exists())

    def test_reader_refuses_draft_release_and_unsupported_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "storage"
            write_release_manifest(root, manifest(status="draft"))
            with self.assertRaisesRegex(StorageCompatibilityError, "not accepted"):
                validate_accepted_release_manifest(read_release_manifest(root))
            with self.assertRaisesRegex(StorageCompatibilityError, "not accepted"):
                assert_loader_compatible(root, dataset_id="deribit_btc_options_v1_compact_liquid", loader_contract_version="hmd-loader-v1")
            write_release_manifest(root, manifest())
            with self.assertRaisesRegex(StorageCompatibilityError, "unsupported"):
                assert_loader_compatible(root, dataset_id="deribit_btc_options_v1_compact_liquid", loader_contract_version="hmd-loader-v2")

    def test_reader_rejects_malformed_or_undeclared_dataset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "storage"
            path = release_manifest_path(root)
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps({"schema_version": 1}), encoding="utf-8")
            with self.assertRaisesRegex(StorageManifestError, "environment_id"):
                read_release_manifest(root)
            write_release_manifest(root, manifest())
            with self.assertRaisesRegex(StorageCompatibilityError, "not declared"):
                assert_loader_compatible(root, dataset_id="missing", loader_contract_version="hmd-loader-v1")


if __name__ == "__main__":
    unittest.main()
