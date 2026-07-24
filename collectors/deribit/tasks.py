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
    activated_at_ms: int | None = None
    activation_seq: int | None = None


def plan_sequence_tasks(
    config: DeribitConfig,
    store: DeribitCheckpointStore | None = None,
    *,
    limit: int | None = None,
    symbols: list[str] | None = None,
    chunk_size: int | None = None,
) -> list[DownloadTask]:
    checkpoint = store or DeribitCheckpointStore(config)
    page_size = int(chunk_size or config.raw["api"].get("chunk_size", 5000))
    symbol_set = {item.upper() for item in symbols} if symbols else None
    rows = checkpoint.instrument_states()
    tasks: list[DownloadTask] = []
    for row in rows:
        instrument_name = str(row["instrument_name"]).upper()
        if symbol_set is not None and instrument_name not in symbol_set:
            continue
        status = str(row["status"])
        if status in TERMINAL_STATUSES:
            continue
        start_seq = int(row["last_processed_seq"]) + 1
        tasks.append(
            DownloadTask(
                instrument_name=instrument_name,
                instrument_id=int(row["instrument_id"]),
                start_seq=start_seq,
                end_seq=start_seq + page_size - 1,
                is_expired=bool(row["is_expired"]),
                status=status,
                activated_at_ms=row.get("activated_at_ms"),
                activation_seq=row.get("activation_seq"),
            )
        )
        if limit is not None and len(tasks) >= limit:
            break
    return tasks


def task_names(tasks: Iterable[DownloadTask]) -> list[str]:
    return [task.instrument_name for task in tasks]
