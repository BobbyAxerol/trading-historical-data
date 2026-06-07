from __future__ import annotations

import argparse
import json
from pathlib import Path

from collectors.common.env import load_environment, state_root


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    root = state_root()
    payload = {
        "heartbeats": {},
        "manifests": {},
    }
    for path in (root / "heartbeats").glob("*.json"):
        payload["heartbeats"][path.stem] = json.loads(path.read_text())
    for path in (root / "manifests").glob("*.json"):
        manifest = json.loads(path.read_text())
        payload["manifests"][path.stem] = {
            "symbols": len(manifest.get("symbols", {})),
            "updated_symbols": {
                symbol: state.get("latest_time")
                for symbol, state in manifest.get("symbols", {}).items()
            },
        }

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return

    for name, heartbeat in payload["heartbeats"].items():
        print(f"heartbeat {name}: {heartbeat.get('status')} at {heartbeat.get('updated_at')}")
    for name, manifest in payload["manifests"].items():
        print(f"manifest {name}: {manifest['symbols']} symbols")


if __name__ == "__main__":
    main()

