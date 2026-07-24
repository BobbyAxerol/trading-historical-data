from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from collectors.deribit.config import DeribitConfig

SCHEMA_VERSION = 2


@dataclass(frozen=True)
class CheckpointSummary:
    path: Path
    schema_version: int
    instrument_states: int
    download_ranges: int


class DeribitCheckpointStore:
    """SQLite checkpoint store for Deribit V1.

    Phase 0 only initializes and validates the interface. Later phases add state
    transition helpers, but they must keep the disk-before-checkpoint invariant.
    """

    def __init__(self, config: DeribitConfig):
        self.config = config
        self.path = config.checkpoint_path

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        return con

    def initialize(self) -> CheckpointSummary:
        with self.connect() as con:
            self._configure_connection(con)
            self._create_schema(con)
            self._upsert_metadata(con)
            con.commit()
        return self.summary()

    def summary(self) -> CheckpointSummary:
        with self.connect() as con:
            instrument_states = int(con.execute("SELECT COUNT(*) FROM instrument_state").fetchone()[0])
            download_ranges = int(con.execute("SELECT COUNT(*) FROM download_ranges").fetchone()[0])
        return CheckpointSummary(
            path=self.path,
            schema_version=SCHEMA_VERSION,
            instrument_states=instrument_states,
            download_ranges=download_ranges,
        )

    def metadata(self) -> dict[str, Any]:
        with self.connect() as con:
            rows = con.execute("SELECT key, value FROM metadata").fetchall()
        return {str(row["key"]): row["value"] for row in rows}

    def upsert_instruments(self, instruments: list[dict[str, Any]]) -> int:
        if not instruments:
            return 0
        with self.connect() as con:
            self._configure_connection(con)
            self._create_schema(con)
            self._upsert_metadata(con)
            rows = [
                (
                    str(item["instrument_name"]),
                    int(item["instrument_id"]),
                    1 if bool(item["is_expired"]) else 0,
                    "NEW",
                    self.config.version,
                    self.config.config_hash,
                )
                for item in instruments
            ]
            con.executemany(
                """
                INSERT INTO instrument_state (
                    instrument_name,
                    instrument_id,
                    is_expired,
                    status,
                    dataset_version,
                    config_hash
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_name) DO UPDATE SET
                    instrument_id=excluded.instrument_id,
                    is_expired=excluded.is_expired,
                    dataset_version=excluded.dataset_version,
                    config_hash=excluded.config_hash,
                    status=CASE
                        WHEN excluded.is_expired = 0
                             AND instrument_state.status IN ('COMPLETE_EXPIRED', 'EMPTY_CONFIRMED')
                        THEN 'CAUGHT_UP_ACTIVE'
                        ELSE instrument_state.status
                    END
                """,
                rows,
            )
            con.commit()
            return len(rows)

    def instrument_states(self) -> list[dict[str, Any]]:
        with self.connect() as con:
            self._configure_connection(con)
            self._create_schema(con)
            rows = con.execute(
                """
                SELECT
                    instrument_name,
                    instrument_id,
                    is_expired,
                    status,
                    last_processed_seq,
                    activated_at_ms,
                    activation_seq,
                    failure_count,
                    dataset_version,
                    config_hash
                FROM instrument_state
                ORDER BY instrument_name
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def record_failure(self, *, instrument_name: str, error_code: str | None, error_message: str | None, attempted_at: str) -> None:
        with self.connect() as con:
            self._configure_connection(con)
            self._create_schema(con)
            con.execute(
                """
                UPDATE instrument_state
                SET
                    status='RETRYABLE_ERROR',
                    last_attempt_at=?,
                    failure_count=failure_count + 1,
                    last_error_code=?,
                    last_error_message=?,
                    dataset_version=?,
                    config_hash=?
                WHERE instrument_name=?
                """,
                (
                    attempted_at,
                    error_code,
                    (error_message or "")[:1000],
                    self.config.version,
                    self.config.config_hash,
                    instrument_name,
                ),
            )
            con.commit()

    def commit_success_range(
        self,
        *,
        instrument_name: str,
        requested_start_seq: int,
        requested_end_seq: int,
        response_min_seq: int | None,
        response_max_seq: int | None,
        response_trade_count: int,
        retained_trade_count: int,
        discarded_trade_count: int,
        output_file: str | None,
        output_checksum: str | None,
        range_status: str,
        started_at: str,
        completed_at: str,
        next_status: str,
        advance_to_seq: int | None,
        activated_at_ms: int | None = None,
        activation_seq: int | None = None,
    ) -> None:
        with self.connect() as con:
            self._configure_connection(con)
            self._create_schema(con)
            con.execute(
                """
                INSERT INTO download_ranges (
                    instrument_name,
                    requested_start_seq,
                    requested_end_seq,
                    response_min_seq,
                    response_max_seq,
                    response_trade_count,
                    retained_trade_count,
                    discarded_trade_count,
                    output_file,
                    output_checksum,
                    status,
                    started_at,
                    completed_at,
                    dataset_version,
                    config_hash
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_name, requested_start_seq, requested_end_seq, dataset_version)
                DO UPDATE SET
                    response_min_seq=excluded.response_min_seq,
                    response_max_seq=excluded.response_max_seq,
                    response_trade_count=excluded.response_trade_count,
                    retained_trade_count=excluded.retained_trade_count,
                    discarded_trade_count=excluded.discarded_trade_count,
                    output_file=excluded.output_file,
                    output_checksum=excluded.output_checksum,
                    status=excluded.status,
                    completed_at=excluded.completed_at,
                    config_hash=excluded.config_hash
                """,
                (
                    instrument_name,
                    int(requested_start_seq),
                    int(requested_end_seq),
                    response_min_seq,
                    response_max_seq,
                    int(response_trade_count),
                    int(retained_trade_count),
                    int(discarded_trade_count),
                    output_file,
                    output_checksum,
                    range_status,
                    started_at,
                    completed_at,
                    self.config.version,
                    self.config.config_hash,
                ),
            )

            params = {
                "status": next_status,
                "last_success_at": completed_at,
                "last_attempt_at": completed_at,
                "last_error_code": None,
                "last_error_message": None,
                "dataset_version": self.config.version,
                "config_hash": self.config.config_hash,
                "instrument_name": instrument_name,
            }
            assignments = [
                "status=:status",
                "last_success_at=:last_success_at",
                "last_attempt_at=:last_attempt_at",
                "failure_count=0",
                "last_error_code=:last_error_code",
                "last_error_message=:last_error_message",
                "dataset_version=:dataset_version",
                "config_hash=:config_hash",
            ]
            if advance_to_seq is not None:
                params["advance_to_seq"] = int(advance_to_seq)
                assignments.append("last_processed_seq=MAX(last_processed_seq, :advance_to_seq)")
            if activated_at_ms is not None:
                params["activated_at_ms"] = int(activated_at_ms)
                assignments.append("activated_at_ms=COALESCE(activated_at_ms, :activated_at_ms)")
            if activation_seq is not None:
                params["activation_seq"] = int(activation_seq)
                assignments.append("activation_seq=COALESCE(activation_seq, :activation_seq)")

            con.execute(
                f"UPDATE instrument_state SET {', '.join(assignments)} WHERE instrument_name=:instrument_name",
                params,
            )
            con.commit()

    def _configure_connection(self, con: sqlite3.Connection) -> None:
        checkpoint = self.config.raw["checkpoint"]
        con.execute(f"PRAGMA journal_mode={checkpoint.get('journal_mode', 'WAL')}")
        con.execute(f"PRAGMA synchronous={checkpoint.get('synchronous', 'NORMAL')}")
        con.execute("PRAGMA foreign_keys=ON")

    def _create_schema(self, con: sqlite3.Connection) -> None:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS instrument_state (
                instrument_name TEXT PRIMARY KEY,
                instrument_id INTEGER,
                is_expired INTEGER NOT NULL,
                status TEXT NOT NULL,
                last_processed_seq INTEGER NOT NULL DEFAULT 0,
                activated_at_ms INTEGER,
                activation_seq INTEGER,
                last_success_at TEXT,
                last_attempt_at TEXT,
                failure_count INTEGER NOT NULL DEFAULT 0,
                last_error_code TEXT,
                last_error_message TEXT,
                dataset_version TEXT NOT NULL,
                config_hash TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS download_ranges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument_name TEXT NOT NULL,
                requested_start_seq INTEGER NOT NULL,
                requested_end_seq INTEGER NOT NULL,
                response_min_seq INTEGER,
                response_max_seq INTEGER,
                response_trade_count INTEGER NOT NULL DEFAULT 0,
                retained_trade_count INTEGER NOT NULL DEFAULT 0,
                discarded_trade_count INTEGER NOT NULL DEFAULT 0,
                output_file TEXT,
                output_checksum TEXT,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                dataset_version TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                UNIQUE (
                    instrument_name,
                    requested_start_seq,
                    requested_end_seq,
                    dataset_version
                )
            );

            CREATE INDEX IF NOT EXISTS idx_download_ranges_instrument
                ON download_ranges (instrument_name, requested_start_seq, requested_end_seq);

            CREATE INDEX IF NOT EXISTS idx_download_ranges_status
                ON download_ranges (status);
            """
        )
        self._ensure_column(con, "instrument_state", "activated_at_ms", "INTEGER")
        self._ensure_column(con, "instrument_state", "activation_seq", "INTEGER")

    def _upsert_metadata(self, con: sqlite3.Connection) -> None:
        values = {
            "schema_version": str(SCHEMA_VERSION),
            "dataset_version": self.config.version,
            "currency": self.config.currency,
            "config_hash": self.config.config_hash,
        }
        con.executemany(
            "INSERT INTO metadata(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            sorted(values.items()),
        )

    def _ensure_column(self, con: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        columns = {str(row["name"]) for row in con.execute(f"PRAGMA table_info({table})").fetchall()}
        if column not in columns:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
