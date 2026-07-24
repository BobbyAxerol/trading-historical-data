from __future__ import annotations

import argparse
import json
from typing import Any

from collectors.common.env import load_environment
from collectors.deribit.checkpoints import DeribitCheckpointStore
from collectors.deribit.config import SUPPORTED_VERSION, load_deribit_config

PHASE0_ONLY_COMMANDS = {
    "probe": "Phase 1",
    "discover": "Phase 2",
    "backfill": "Phase 3",
    "sync-once": "Phase 3",
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

    for command, phase in PHASE0_ONLY_COMMANDS.items():
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


def _not_implemented(command: str) -> dict[str, Any]:
    return {
        "status": "blocked",
        "command": command,
        "reason": f"{command} is reserved for {PHASE0_ONLY_COMMANDS[command]} and is not implemented in Phase 0.",
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

    payload = _not_implemented(args.command)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(payload["reason"])
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
