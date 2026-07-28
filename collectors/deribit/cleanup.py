from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from collectors.common.manifest import utc_now_iso
from collectors.common.storage import release_unused_memory
from collectors.deribit.config import DeribitConfig
from collectors.deribit.parquet_parts import file_checksum
from collectors.deribit.validate import DeribitValidator


@dataclass(frozen=True)
class CleanupReport:
    status: str
    phase: str
    dry_run: bool
    files_seen: int
    files_deleted: int
    bytes_seen: int
    validation_status: str

    def as_payload(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "phase": self.phase,
            "dry_run": self.dry_run,
            "files_seen": self.files_seen,
            "files_deleted": self.files_deleted,
            "bytes_seen": self.bytes_seen,
            "validation_status": self.validation_status,
        }


class DeribitCleanup:
    def __init__(self, config: DeribitConfig):
        self.config = config

    def run(self, *, confirm: bool = False) -> dict[str, Any]:
        validation = DeribitValidator(self.config).run()
        files = self._staging_files()
        bytes_seen = sum(path.stat().st_size for path in files if path.exists())
        if validation["status"] != "ok":
            return CleanupReport("blocked", "Phase 4", not confirm, len(files), 0, bytes_seen, str(validation["status"])).as_payload()
        deleted = 0
        if confirm:
            manifest_files = self._load_manifest_files()
            deleted_at = utc_now_iso()
            for path in files:
                manifest_files[str(path)] = {
                    "path": str(path),
                    "checksum": file_checksum(path),
                    "bytes": path.stat().st_size,
                    "deleted_at": deleted_at,
                }
            self._write_manifest(status="pending", files=manifest_files)
            for path in files:
                path.unlink()
                deleted += 1
            self._write_manifest(status="ok", files=manifest_files)
            release_unused_memory()
        return CleanupReport("ok", "Phase 4", not confirm, len(files), deleted, bytes_seen, str(validation["status"])).as_payload()

    def _staging_files(self) -> list[Path]:
        if not self.config.staging_root.exists():
            return []
        return sorted(path for path in self.config.staging_root.rglob("*.parquet") if not path.name.endswith(".tmp"))

    def _manifest_path(self) -> Path:
        return self.config.checkpoint_path.parent / "staging_cleanup_manifest.json"

    def _load_manifest_files(self) -> dict[str, Any]:
        path = self._manifest_path()
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            return {}
        files = payload.get("files")
        return dict(files) if isinstance(files, dict) else {}

    def _write_manifest(self, *, status: str, files: dict[str, Any]) -> None:
        path = self._manifest_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "status": status,
            "phase": "Phase 4",
            "updated_at": utc_now_iso(),
            "staging_root": str(self.config.staging_root),
            "files_deleted": len(files),
            "files": files,
        }
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
        tmp.replace(path)
