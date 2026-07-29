from __future__ import annotations

import argparse
import json

from collectors.common.env import load_environment
from collectors.vn_derivatives.contracts import backfill_contracts, options_from_config
from collectors.vn_derivatives.instruments import build_initial_instrument_dimension
from collectors.vn_derivatives.probe import PROBE_CONTRACTS, run_provider_probe
from collectors.vn_derivatives.validate import validate_storage


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m collectors.vn_derivatives")
    sub = parser.add_subparsers(dest="command", required=True)

    instruments = sub.add_parser("discover", help="Build VN30 futures instrument dimension skeleton.")
    instruments.add_argument("--version", default="v1")
    instruments.add_argument("--start", default="2017-08-10")
    instruments.add_argument("--end", default=None)
    instruments.add_argument("--horizon-months", type=int, default=6)
    instruments.add_argument("--json", action="store_true")

    probe = sub.add_parser("probe", help="Probe KBS/DNSE VN30 futures coverage without publishing canonical bars.")
    probe.add_argument("--version", default="v1")
    probe.add_argument("--contracts", default=",".join(PROBE_CONTRACTS), help="Comma-separated canonical symbols.")
    probe.add_argument("--providers", default="kbs,dnse", help="Comma-separated providers to probe: kbs,dnse.")
    probe.add_argument("--window-days", type=int, default=30)
    probe.add_argument("--json", action="store_true")

    backfill = sub.add_parser("backfill", help="Backfill individual VN30 futures contracts into canonical contract storage.")
    backfill.add_argument("--version", default="v1")
    backfill.add_argument("--start", default=None)
    backfill.add_argument("--end", default=None)
    backfill.add_argument("--symbols", default=None, help="Comma-separated canonical contract symbols.")
    backfill.add_argument("--resolutions", default="1m,1d")
    backfill.add_argument("--max-contracts", type=int, default=None)
    backfill.add_argument("--max-windows", type=int, default=None)
    backfill.add_argument("--sleep-seconds", type=float, default=0.0)
    backfill.add_argument("--json", action="store_true")

    validate = sub.add_parser("validate", help="Validate canonical VN30 futures contract storage.")
    validate.add_argument("--version", default="v1")
    validate.add_argument("--symbols", default=None)
    validate.add_argument("--resolutions", default="1m,1d")
    validate.add_argument("--json", action="store_true")
    return parser


def main() -> None:
    load_environment()
    args = build_parser().parse_args()
    if args.command == "discover":
        df = build_initial_instrument_dimension(start=args.start, end=args.end, horizon_months=args.horizon_months, version=args.version)
        payload = {"status": "ok", "contracts": int(len(df)), "first": str(df["canonical_symbol"].iloc[0]) if not df.empty else None, "last": str(df["canonical_symbol"].iloc[-1]) if not df.empty else None}
    elif args.command == "probe":
        contracts = [item.strip().upper() for item in args.contracts.split(",") if item.strip()]
        providers = [item.strip().lower() for item in args.providers.split(",") if item.strip()]
        payload = run_provider_probe(contracts=contracts, window_days=args.window_days, providers=providers, version=args.version)
    elif args.command == "backfill":
        symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()] if args.symbols else None
        resolutions = [item.strip().lower() for item in args.resolutions.split(",") if item.strip()]
        options = options_from_config(
            version=args.version,
            start=args.start,
            end=args.end,
            resolutions=resolutions,
            symbols=symbols,
            max_contracts=args.max_contracts,
            max_windows=args.max_windows,
            sleep_seconds=args.sleep_seconds,
        )
        payload = backfill_contracts(options)
    elif args.command == "validate":
        symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()] if args.symbols else None
        resolutions = [item.strip().lower() for item in args.resolutions.split(",") if item.strip()]
        payload = validate_storage(version=args.version, resolutions=resolutions, symbols=symbols)
    else:  # pragma: no cover
        raise RuntimeError(f"Unsupported command: {args.command}")

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(payload)


if __name__ == "__main__":
    main()
