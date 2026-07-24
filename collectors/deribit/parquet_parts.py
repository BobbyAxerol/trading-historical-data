from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def write_parquet_atomic(table: pa.Table, path: Path, *, metadata: dict[str, Any] | None = None, compression: str = "zstd") -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    if tmp.exists():
        tmp.unlink()
    write_table = _with_metadata(table, metadata or {})
    pq.write_table(write_table, tmp, compression=compression)
    _fsync_file(tmp)
    os.replace(tmp, path)
    _fsync_directory(path.parent)
    checksum = file_checksum(path)
    return {"path": path, "bytes": path.stat().st_size, "checksum": checksum}


def file_checksum(path: Path) -> str:
    h = hashlib.blake2b(digest_size=16)
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return f"blake2b_128:{h.hexdigest()}"


def _with_metadata(table: pa.Table, metadata: dict[str, Any]) -> pa.Table:
    existing = dict(table.schema.metadata or {})
    encoded = {str(key).encode("utf-8"): str(value).encode("utf-8") for key, value in metadata.items()}
    return table.replace_schema_metadata({**existing, **encoded})


def _fsync_file(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
