#!/usr/bin/env bash
set -euo pipefail
if [[ "$#" -ne 1 || "$1" != "phase-g-binance-futures-metrics-5m-ethusdt" ]]; then
  echo "usage: $0 phase-g-binance-futures-metrics-5m-ethusdt" >&2; exit 64
fi
hmd_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
hmd_env="/srv/primus/historical-market-data/deploy/compose.env"
hmd_image="primus-historical-market-data:phase-g-$(git -C "$hmd_root" rev-parse --short=12 HEAD)"
sudo -n docker image inspect "$hmd_image" >/dev/null || { echo "missing reviewed image: $hmd_image" >&2; exit 69; }
if [[ -n "$(sudo -n docker ps --format '{{.Names}}' --filter 'name=primus-historical-market-data-phase-[efg]-')" ]]; then echo "another historical job is running" >&2; exit 75; fi
exec sudo -n env PRIMUS_HMD_IMAGE_REF="$hmd_image" PRIMUS_HMD_PHASE_G_BINANCE_FUTURES_METRICS_5M_ETHUSDT_APPROVED=approved docker compose --project-directory "$hmd_root" --env-file "$hmd_env" -f "$hmd_root/docker-compose.yml" up -d --no-deps phase-g-binance-futures-metrics-5m-ethusdt
