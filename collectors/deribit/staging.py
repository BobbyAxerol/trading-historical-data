from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa

from collectors.deribit.config import DeribitConfig
from collectors.deribit.parquet_parts import write_parquet_atomic
from collectors.deribit.schema import staging_trade_schema
from collectors.deribit.tasks import DownloadTask


@dataclass(frozen=True)
class StagingWriteResult:
    path: Path
    checksum: str
    bytes: int
    rows: int


class DeribitStagingWriter:
    def __init__(self, config: DeribitConfig, *, run_id: str):
        self.config = config
        self.run_id = run_id
        self.shard_count = int(config.raw["staging"].get("shard_count", 64))
        self.compression = str(config.raw["canonical_trades"].get("compression", "zstd"))

    def path_for_task(self, task: DownloadTask) -> Path:
        shard = int(task.instrument_id) % max(1, self.shard_count)
        return (
            self.config.staging_root
            / f"currency={self.config.currency}"
            / f"shard={shard:02d}"
            / f"run_id={self.run_id}"
            / f"instrument={int(task.instrument_id)}"
            / f"seq_{int(task.start_seq):012d}_{int(task.end_seq):012d}.parquet"
        )

    def write_chunk(self, rows: list[dict[str, Any]], *, task: DownloadTask, metadata: dict[str, Any]) -> StagingWriteResult | None:
        if not rows:
            return None
        table = pa.Table.from_pylist(rows, schema=staging_trade_schema())
        path = self.path_for_task(task)
        result = write_parquet_atomic(table, path, metadata=metadata, compression=self.compression)
        return StagingWriteResult(path=Path(result["path"]), checksum=str(result["checksum"]), bytes=int(result["bytes"]), rows=len(rows))
