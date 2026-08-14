#!/usr/bin/env bash
# Launch one exact, detached Phase E historical/probe service.
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 <reviewed-phase-e-service>" >&2
  exit 64
fi

case "$1" in
  phase-e-binance-usdm-core-perpetual-1m)
    hmd_approval="PRIMUS_HMD_PHASE_E_BINANCE_USDM_CORE_PERPETUAL_1M_APPROVED"
    ;;
  phase-e-binance-orderbook-history-1h)
    hmd_approval="PRIMUS_HMD_PHASE_E_BINANCE_ORDERBOOK_HISTORY_1H_APPROVED"
    ;;
  phase-e-vn-daily-universe-1d)
    hmd_approval="PRIMUS_HMD_PHASE_E_VN_DAILY_UNIVERSE_1D_APPROVED"
    ;;
  phase-e-vn-daily-matrix-rebuild)
    hmd_approval="PRIMUS_HMD_PHASE_E_VN_DAILY_MATRIX_REBUILD_APPROVED"
    ;;
  phase-e-vn30f1m-vndirect-1m)
    hmd_approval="PRIMUS_HMD_PHASE_E_VN30F1M_VNDIRECT_1M_APPROVED"
    ;;
  phase-e-vn30-contract-source-probe)
    hmd_approval="PRIMUS_HMD_PHASE_E_VN30_CONTRACT_SOURCE_PROBE_APPROVED"
    ;;
  *)
    echo "unreviewed Phase E service: $1" >&2
    exit 64
    ;;
esac

hmd_service="$1"
hmd_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
hmd_repo_root="$(cd -- "${hmd_script_dir}/.." && pwd)"
hmd_deploy_env="/srv/primus/historical-market-data/deploy/compose.env"
hmd_revision="$(git -C "$hmd_repo_root" rev-parse --short=12 HEAD)"
hmd_image_ref="primus-historical-market-data:phase-e-${hmd_revision}"

if [[ ! -r "$hmd_deploy_env" ]]; then
  echo "protected Phase E Compose environment is unavailable" >&2
  exit 66
fi
if ! sudo -n docker image inspect "$hmd_image_ref" >/dev/null; then
  echo "required reviewed Phase E image is unavailable: $hmd_image_ref" >&2
  exit 69
fi
if [[ -n "$(sudo -n docker ps --format '{{.Names}}' --filter 'name=primus-historical-market-data-phase-e-')" ]]; then
  echo "another Phase E historical service is already running" >&2
  exit 75
fi

exec sudo -n env \
  PRIMUS_HMD_IMAGE_REF="$hmd_image_ref" \
  "$hmd_approval=approved" \
  docker compose \
  --project-directory "$hmd_repo_root" \
  --env-file "$hmd_deploy_env" \
  -f "$hmd_repo_root/docker-compose.yml" \
  up -d --no-deps "$hmd_service"
