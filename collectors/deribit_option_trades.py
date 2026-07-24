from __future__ import annotations

import argparse
import json
from typing import Any

from collectors.common.env import load_environment
from collectors.deribit.checkpoints import DeribitCheckpointStore
from collectors.deribit.config import SUPPORTED_VERSION, load_deribit_config
from collectors.deribit.engine import DeribitTradeDownloader, DownloaderOptions
from collectors.deribit.instruments import DeribitInstrumentDiscovery
from collectors.deribit.probe import DeribitApiProbeRunner, ProbeOptions
from collectors.deribit.tasks import plan_sequence_tasks

RESERVED_COMMANDS = {
    "compact": "Phase 4",
    "validate": "Phase 4",
    "repair": "Phase 4",
    "pilot": "Phase 5",
    "build-snapshot-5m": "Phase 7",
    "build-snapshot-1m": "Phase 9",
    "fit-execution": "Phase 8",
    "cleanup": "Phase 10",
    "storage-report": "Phase 10",
}


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--version", default=SUPPORTED_VERSION)
    parser.add_argument("--config", default=None)
    parser.add_argument("--json", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deribit BTC options historical data V1 CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Initialize Phase 0 config paths and SQLite checkpoint schema.")
    _add_common_args(init_parser)

    probe_parser = subparsers.add_parser("probe", help="Run Phase 1 Deribit API behavior probe.")
    _add_common_args(probe_parser)
    probe_parser.add_argument("--sample-instruments", type=int, default=24)
    probe_parser.add_argument("--rate-ramp", action="store_true")
    probe_parser.add_argument("--max-rps", type=float, default=5.0)
    probe_parser.add_argument("--requests-per-rps", type=int, default=2)

    discover_parser = subparsers.add_parser("discover", help="Discover Deribit BTC option instruments and initialize checkpoint state.")
    _add_common_args(discover_parser)

    for command in ("backfill", "sync-once"):
        ingest_parser = subparsers.add_parser(command, help=f"Run Deribit Phase 3 {command} staging downloader.")
        _add_common_args(ingest_parser)
        ingest_parser.add_argument("--max-tasks", type=int, default=1)
        ingest_parser.add_argument("--symbols", default=None, help="Comma-separated instrument names. Default scans checkpoint order.")
        ingest_parser.add_argument("--run-id", default=None)
        ingest_parser.add_argument("--discover-first", action="store_true")
        ingest_parser.add_argument("--allow-unprobed", action="store_true")

    for command, phase in RESERVED_COMMANDS.items():
        sub = subparsers.add_parser(command, help=f"Reserved command for {phase}.")
        _add_common_args(sub)
        if command == "repair":
            sub.add_argument("--only-unresolved", action="store_true")
        elif command == "build-snapshot-1m":
            sub.add_argument("--start", default=None)
            sub.add_argument("--end", default=None)
    return parser


def _config_from_args(args: argparse.Namespace):
    return load_deribit_config(args.config, version=args.version)


def _run_init(args: argparse.Namespace) -> dict[str, Any]:
    config = _config_from_args(args)
    summary = DeribitCheckpointStore(config).initialize()
    return {
        "status": "ok",
        "phase": "Phase 0",
        "config_path": str(config.config_path),
        "config_hash": config.config_hash,
        "dataset_version": config.version,
        "currency": config.currency,
        "checkpoint_path": str(summary.path),
        "sqlite_schema_version": summary.schema_version,
        "instrument_states": summary.instrument_states,
        "download_ranges": summary.download_ranges,
        "staging_root": str(config.staging_root),
        "canonical_trades_root": str(config.canonical_trades_root),
        "snapshot_5m_root": str(config.snapshot_5m_root),
    }


def _run_probe(args: argparse.Namespace) -> dict[str, Any]:
    config = _config_from_args(args)
    options = ProbeOptions(
        sample_instruments=max(1, int(args.sample_instruments)),
        rate_ramp=bool(args.rate_ramp),
        max_rps=max(1.0, float(args.max_rps)),
        requests_per_rps=max(1, int(args.requests_per_rps)),
    )
    return DeribitApiProbeRunner(config, options=options).run()


def _run_discover(args: argparse.Namespace) -> dict[str, Any]:
    config = _config_from_args(args)
    result = DeribitInstrumentDiscovery(config).run()
    store = DeribitCheckpointStore(config)
    import pyarrow.parquet as pq

    table = pq.ParquetFile(result.instrument_path).read()
    instruments = table.to_pylist()
    checkpoint_rows = store.upsert_instruments(instruments)
    planned_tasks = len(plan_sequence_tasks(config, store))
    payload = result.as_payload()
    payload["checkpoint_path"] = str(store.path)
    payload["checkpoint_rows_upserted"] = checkpoint_rows
    payload["planned_initial_tasks"] = planned_tasks
    return payload


def _run_downloader(args: argparse.Namespace) -> dict[str, Any]:
    config = _config_from_args(args)
    symbols = _parse_symbols(args.symbols)
    options = DownloaderOptions(
        mode=str(args.command),
        max_tasks=max(1, int(args.max_tasks)),
        symbols=symbols,
        run_id=args.run_id,
        discover_first=bool(args.discover_first) or str(args.command) == "sync-once",
        allow_unprobed=bool(args.allow_unprobed),
    )
    return DeribitTradeDownloader(config, options=options).run()


def _parse_symbols(value: str | None) -> list[str] | None:
    if value is None:
        return None
    symbols = [item.strip().upper() for item in value.split(",") if item.strip()]
    return symbols or None


def _not_implemented(command: str) -> dict[str, Any]:
    return {
        "status": "blocked",
        "command": command,
        "reason": f"{command} is reserved for {RESERVED_COMMANDS[command]} and is not implemented yet.",
    }


def main(argv: list[str] | None = None) -> int:
    load_environment()
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "init":
        payload = _run_init(args)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(f"Deribit {payload['dataset_version']} initialized")
            print(f"config: {payload['config_path']}")
            print(f"checkpoint: {payload['checkpoint_path']}")
        return 0

    if args.command == "probe":
        payload = _run_probe(args)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(f"Deribit API probe status: {payload['status']}")
            print(f"report: {DeribitApiProbeRunner(_config_from_args(args)).report_path}")
            print(f"selected_page_size: {payload.get('selected_page_size')}")
            print(f"safe_trade_rps: {payload.get('safe_trade_rps')}")
        return 0

    if args.command == "discover":
        payload = _run_discover(args)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(f"Deribit instrument discovery status: {payload['status']}")
            print(f"instruments: {payload['instrument_path']}")
            print(f"checkpoint: {payload['checkpoint_path']}")
            print(f"total_rows: {payload['total_rows']}")
            print(f"planned_initial_tasks: {payload['planned_initial_tasks']}")
        return 0

    if args.command in {"backfill", "sync-once"}:
        payload = _run_downloader(args)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(f"Deribit {args.command} status: {payload['status']}")
            print(f"run_id: {payload.get('run_id')}")
            print(f"tasks_attempted: {payload.get('tasks_attempted')}")
            print(f"files_written: {payload.get('files_written')}")
            print(f"retained_rows: {payload.get('retained_rows')}")
        return 0 if payload.get("status") in {"ok", "partial"} else 2

    payload = _not_implemented(args.command)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(payload["reason"])
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
