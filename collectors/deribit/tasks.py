from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from collectors.deribit.checkpoints import DeribitCheckpointStore
from collectors.deribit.config import DeribitConfig

TERMINAL_STATUSES = {"COMPLETE_EXPIRED", "EMPTY_CONFIRMED", "DEAD_LETTER"}


@dataclass(frozen=True)
class DownloadTask:
    instrument_name: str
    instrument_id: int
    start_seq: int
    end_seq: int
    is_expired: bool
    status: str


def plan_sequence_tasks(config: DeribitConfig, store: DeribitCheckpointStore | None = None, *, limit: int | None = None) -> list[DownloadTask]:
    checkpoint = store or DeribitCheckpointStore(config)
    chunk_size = int(config.raw["api"].get("chunk_size", 5000))
    rows = checkpoint.instrument_states()
    tasks: list[DownloadTask] = []
    for row in rows:
        status = str(row["status"])
        if status in TERMINAL_STATUSES:
            continue
        start_seq = int(row["last_processed_seq"]) + 1
        tasks.append(
            DownloadTask(
                instrument_name=str(row["instrument_name"]),
                instrument_id=int(row["instrument_id"]),
                start_seq=start_seq,
                end_seq=start_seq + chunk_size - 1,
                is_expired=bool(row["is_expired"]),
                status=status,
            )
        )
        if limit is not None and len(tasks) >= limit:
            break
    return tasks


def task_names(tasks: Iterable[DownloadTask]) -> list[str]:
    return [task.instrument_name for task in tasks]
