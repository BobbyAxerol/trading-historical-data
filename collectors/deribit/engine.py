from __future__ import annotations

import json
import logging
import resource
from dataclasses import dataclass, field
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from collectors.common.env import state_root
from collectors.common.manifest import utc_now_iso
from collectors.common.storage import release_unused_memory
from collectors.deribit.checkpoints import DeribitCheckpointStore
from collectors.deribit.client import DeribitHistoryClient
from collectors.deribit.config import DeribitConfig
from collectors.deribit.instruments import DeribitInstrumentDiscovery, instrument_dimension_path
from collectors.deribit.normalize import ActivationState, normalize_trade_chunk
from collectors.deribit.staging import DeribitStagingWriter
from collectors.deribit.tasks import DownloadTask, plan_sequence_tasks

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class DownloaderOptions:
    mode: str = "backfill"
    max_tasks: int = 1
    symbols: list[str] | None = None
    run_id: str | None = None
    discover_first: bool = False
    allow_unprobed: bool = False
    require_pilot_pass: bool = False
    allow_blocked_pilot: bool = False
    expiry_start_ms: int | None = None
    expiry_end_ms: int | None = None
    progress_every: int = 25


@dataclass
class DownloadSummary:
    status: str = "ok"
    phase: str = "Phase 3"
    mode: str = "backfill"
    run_id: str = ""
    tasks_planned: int = 0
    tasks_attempted: int = 0
    tasks_succeeded: int = 0
    tasks_failed: int = 0
    files_written: int = 0
    retained_rows: int = 0
    discarded_rows: int = 0
    response_rows: int = 0
    peak_rss_mb: float | None = None
    errors: list[str] = field(default_factory=list)

    def as_payload(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "phase": self.phase,
            "mode": self.mode,
            "run_id": self.run_id,
            "tasks_planned": self.tasks_planned,
            "tasks_attempted": self.tasks_attempted,
            "tasks_succeeded": self.tasks_succeeded,
            "tasks_failed": self.tasks_failed,
            "files_written": self.files_written,
            "retained_rows": self.retained_rows,
            "discarded_rows": self.discarded_rows,
            "response_rows": self.response_rows,
            "peak_rss_mb": self.peak_rss_mb,
            "errors": self.errors,
        }


class DeribitTradeDownloader:
    def __init__(
        self,
        config: DeribitConfig,
        *,
        client: Any | None = None,
        store: DeribitCheckpointStore | None = None,
        options: DownloaderOptions | None = None,
    ):
        self.config = config
        self.options = options or DownloaderOptions()
        self.store = store or DeribitCheckpointStore(config)
        self.client = client or DeribitHistoryClient(config, requests_per_second=self._safe_trade_rps())
        self.run_id = self.options.run_id or _run_id()
        self.writer = DeribitStagingWriter(config, run_id=self.run_id)

    def run(self) -> dict[str, Any]:
        self.store.initialize()
        if not self.options.allow_unprobed:
            probe = self._load_probe_report()
            if not probe.get("production_backfill_allowed"):
                return {
                    "status": "blocked",
                    "phase": "Phase 3",
                    "reason": "api_probe_report.json is missing or production_backfill_allowed=false",
                    "probe_report_path": str(self._probe_report_path()),
                }
            pilot_block = self._pilot_block_reason()
            if pilot_block is not None:
                if not self.options.allow_blocked_pilot:
                    return {
                        "status": "blocked",
                        "phase": "Phase 3",
                        "reason": pilot_block,
                        "pilot_summary_path": str(self._pilot_summary_path()),
                    }
                if self.options.max_tasks > 20 or not self.options.symbols:
                    return {
                        "status": "blocked",
                        "phase": "Phase 3",
                        "reason": "blocked pilot override is limited to explicit symbols and max_tasks<=20",
                        "pilot_summary_path": str(self._pilot_summary_path()),
                    }

        if self.options.discover_first or not self.store.instrument_states():
            discovery = DeribitInstrumentDiscovery(self.config).run()
            instruments = pq.ParquetFile(discovery.instrument_path).read().to_pylist()
            self.store.upsert_instruments(instruments)

        instruments = self._load_instrument_dimension()
        symbols = self._eligible_symbols(instruments)
        page_size = self._page_size()
        tasks = plan_sequence_tasks(
            self.config,
            self.store,
            limit=max(1, int(self.options.max_tasks)),
            symbols=symbols,
            chunk_size=page_size,
        )
        summary = DownloadSummary(mode=self.options.mode, run_id=self.run_id, tasks_planned=len(tasks))
        LOGGER.info(
            "deribit_backfill_start mode=%s run_id=%s tasks_planned=%s max_tasks=%s page_size=%s expiry_start_ms=%s expiry_end_ms=%s symbols_filter=%s",
            self.options.mode,
            self.run_id,
            len(tasks),
            self.options.max_tasks,
            page_size,
            self.options.expiry_start_ms,
            self.options.expiry_end_ms,
            "explicit" if self.options.symbols else "all",
        )
        for task in tasks:
            summary.tasks_attempted += 1
            if self._should_log_progress(summary.tasks_attempted):
                LOGGER.info(
                    "deribit_task_start run_id=%s task=%s/%s instrument=%s start_seq=%s end_seq=%s status=%s",
                    self.run_id,
                    summary.tasks_attempted,
                    len(tasks),
                    task.instrument_name,
                    task.start_seq,
                    task.end_seq,
                    task.status,
                )
            try:
                item = self._process_task(task, instruments[str(task.instrument_name)], page_size=page_size)
            except Exception as exc:
                summary.tasks_failed += 1
                summary.errors.append(f"{task.instrument_name}: {type(exc).__name__}: {exc}")
                LOGGER.exception(
                    "deribit_task_failed run_id=%s task=%s/%s instrument=%s error=%s",
                    self.run_id,
                    summary.tasks_attempted,
                    len(tasks),
                    task.instrument_name,
                    exc,
                )
                self.store.record_failure(
                    instrument_name=task.instrument_name,
                    error_code=type(exc).__name__,
                    error_message=str(exc),
                    attempted_at=utc_now_iso(),
                )
                release_unused_memory()
                continue
            summary.tasks_succeeded += 1
            summary.files_written += int(item["files_written"])
            summary.retained_rows += int(item["retained_rows"])
            summary.discarded_rows += int(item["discarded_rows"])
            summary.response_rows += int(item["response_rows"])
            if self._should_log_progress(summary.tasks_attempted):
                LOGGER.info(
                    "deribit_task_done run_id=%s task=%s/%s instrument=%s files_written=%s response_rows=%s retained_rows=%s discarded_rows=%s totals_retained=%s",
                    self.run_id,
                    summary.tasks_attempted,
                    len(tasks),
                    task.instrument_name,
                    item["files_written"],
                    item["response_rows"],
                    item["retained_rows"],
                    item["discarded_rows"],
                    summary.retained_rows,
                )
            release_unused_memory()

        if summary.tasks_failed:
            summary.status = "partial" if summary.tasks_succeeded else "blocked"
        summary.peak_rss_mb = _peak_rss_mb()
        LOGGER.info(
            "deribit_backfill_done run_id=%s status=%s tasks_attempted=%s tasks_succeeded=%s tasks_failed=%s files_written=%s response_rows=%s retained_rows=%s discarded_rows=%s peak_rss_mb=%s",
            self.run_id,
            summary.status,
            summary.tasks_attempted,
            summary.tasks_succeeded,
            summary.tasks_failed,
            summary.files_written,
            summary.response_rows,
            summary.retained_rows,
            summary.discarded_rows,
            summary.peak_rss_mb,
        )
        return summary.as_payload()

    def _process_task(self, task: DownloadTask, instrument: dict[str, Any], *, page_size: int) -> dict[str, int]:
        started_at = utc_now_iso()
        result = self.client.get_last_trades_by_instrument(
            task.instrument_name,
            start_seq=task.start_seq,
            end_seq=task.end_seq,
            count=page_size,
            sorting="asc",
        )
        if not result.ok:
            raise RuntimeError(f"Deribit API {result.classification()}: {result.summary()}")

        activation = ActivationState(
            activated_at_ms=_int_or_none(task.activated_at_ms),
            activation_seq=_int_or_none(task.activation_seq),
        )
        chunk = normalize_trade_chunk(result.trades, task=task, instrument=instrument, config=self.config, activation_state=activation)

        output_file = None
        output_checksum = None
        files_written = 0
        if chunk.rows:
            metadata = {
                "dataset_version": self.config.version,
                "config_hash": self.config.config_hash,
                "instrument_name": task.instrument_name,
                "requested_start_seq": task.start_seq,
                "requested_end_seq": task.end_seq,
                "response_count": chunk.response_count,
                "retained_count": len(chunk.rows),
                "created_at": utc_now_iso(),
            }
            written = self.writer.write_chunk(chunk.rows, task=task, metadata=metadata)
            if written is not None:
                output_file = self.config.to_storage_reference(written.path)
                output_checksum = written.checksum
                files_written = 1

        completed_at = utc_now_iso()
        next_status, advance_to = self._next_state(task, result.has_more, chunk.response_count, chunk.response_max_seq)
        range_status = self._range_status(task, chunk.response_count, len(chunk.rows), next_status)
        self.store.commit_success_range(
            instrument_name=task.instrument_name,
            requested_start_seq=task.start_seq,
            requested_end_seq=task.end_seq,
            response_min_seq=chunk.response_min_seq,
            response_max_seq=chunk.response_max_seq,
            response_trade_count=chunk.response_count,
            retained_trade_count=len(chunk.rows),
            discarded_trade_count=chunk.discarded_count,
            output_file=output_file,
            output_checksum=output_checksum,
            range_status=range_status,
            started_at=started_at,
            completed_at=completed_at,
            next_status=next_status,
            advance_to_seq=advance_to,
            activated_at_ms=chunk.activated_at_ms,
            activation_seq=chunk.activation_seq,
        )
        return {
            "files_written": files_written,
            "retained_rows": len(chunk.rows),
            "discarded_rows": chunk.discarded_count,
            "response_rows": chunk.response_count,
        }

    def _next_state(self, task: DownloadTask, has_more: bool | None, response_count: int, response_max_seq: int | None) -> tuple[str, int | None]:
        if response_count == 0:
            if task.is_expired:
                status = "EMPTY_CONFIRMED" if task.start_seq <= 1 else "COMPLETE_EXPIRED"
                return status, task.end_seq
            return "CAUGHT_UP_ACTIVE", None
        if response_count < self._page_size() or has_more is False:
            return ("COMPLETE_EXPIRED" if task.is_expired else "CAUGHT_UP_ACTIVE"), response_max_seq
        return "IN_PROGRESS", response_max_seq

    def _range_status(self, task: DownloadTask, response_count: int, retained_count: int, next_status: str) -> str:
        if response_count == 0:
            return next_status
        return "SUCCESS_WITH_DATA" if retained_count else "SUCCESS_DISCARDED"

    def _load_instrument_dimension(self) -> dict[str, dict[str, Any]]:
        path = instrument_dimension_path(self.config)
        if not path.exists():
            raise FileNotFoundError(f"instrument dimension missing: {path}")
        rows = pq.ParquetFile(path).read().to_pylist()
        return {str(row["instrument_name"]): row for row in rows}

    def _eligible_symbols(self, instruments: dict[str, dict[str, Any]]) -> list[str] | None:
        explicit = {item.upper() for item in self.options.symbols} if self.options.symbols else None
        if self.options.expiry_start_ms is None and self.options.expiry_end_ms is None:
            return sorted(explicit) if explicit is not None else None
        selected: list[str] = []
        for name, row in instruments.items():
            instrument_name = str(name).upper()
            if explicit is not None and instrument_name not in explicit:
                continue
            expiry = row.get("expiry_timestamp_ms")
            if expiry is None:
                continue
            expiry_ms = int(expiry)
            if self.options.expiry_start_ms is not None and expiry_ms < self.options.expiry_start_ms:
                continue
            if self.options.expiry_end_ms is not None and expiry_ms > self.options.expiry_end_ms:
                continue
            selected.append(instrument_name)
        return sorted(selected)

    def _should_log_progress(self, attempted: int) -> bool:
        every = max(1, int(self.options.progress_every))
        return attempted == 1 or attempted % every == 0 or attempted == int(self.options.max_tasks)

    def _page_size(self) -> int:
        probe = self._load_probe_report()
        selected = int(probe.get("selected_page_size") or self.config.raw["api"].get("chunk_size", 5000))
        maximum = int(self.config.raw["api"].get("max_supported_chunk_size", selected))
        return max(1, min(selected, maximum))

    def _safe_trade_rps(self) -> float:
        probe = self._load_probe_report()
        return float(probe.get("safe_trade_rps") or self.config.raw["api"].get("target_requests_per_second", 5))

    def _load_probe_report(self) -> dict[str, Any]:
        path = self._probe_report_path()
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text())
        except json.JSONDecodeError:
            return {}

    def _probe_report_path(self) -> Path:
        return state_root() / "deribit_options" / f"version={self.config.version}" / "api_probe_report.json"

    def _pilot_block_reason(self) -> str | None:
        if not self.options.require_pilot_pass:
            return None
        path = self._pilot_summary_path()
        if not path.exists():
            return "pilot_summary.json is missing; run Phase 5 pilot before broad backfill"
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            return "pilot_summary.json is malformed; rerun Phase 5 pilot"
        if payload.get("status") != "ok":
            return "pilot_summary.json status is not ok; Phase 6 full historical backfill remains blocked"
        return None

    def _pilot_summary_path(self) -> Path:
        return state_root() / "deribit_options" / f"version={self.config.version}" / "pilot_summary.json"


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def date_boundary_ms(value: str | None, *, end: bool = False) -> int | None:
    if not value:
        return None
    text = str(value).strip()
    if len(text) == 10:
        day = datetime.fromisoformat(text).date()
        dt = datetime.combine(day, time.max if end else time.min, tzinfo=timezone.utc)
    else:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        dt = parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
        if end and parsed.time() == time.min:
            dt = datetime.combine(parsed.date(), time.max, tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _peak_rss_mb() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(float(usage) / 1024.0, 2)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
