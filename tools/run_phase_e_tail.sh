#!/usr/bin/env bash
# Launch one exact Phase E live tail after its matching historical gate passes.
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 <reviewed-phase-e-tail>" >&2
  exit 64
fi

case "$1" in
  crypto-1m-core-live)
    hmd_approval="PRIMUS_HMD_STAGED_CRYPTO_CORE_1M_APPROVED"
    ;;
  binance-usdm-quarterly-next-1m)
    hmd_approval="PRIMUS_HMD_STAGED_BINANCE_USDM_QUARTERLY_NEXT_1M_APPROVED"
    ;;
  binance-orderbook-expanded-1h)
    hmd_approval="PRIMUS_HMD_STAGED_BINANCE_ORDERBOOK_EXPANDED_1H_APPROVED"
    ;;
  vn30f1m-vndirect-1m)
    hmd_approval="PRIMUS_HMD_STAGED_VN30F1M_VNDIRECT_1M_APPROVED"
    ;;
  *)
    echo "unreviewed Phase E tail: $1" >&2
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

exec sudo -n env \
  PRIMUS_HMD_IMAGE_REF="$hmd_image_ref" \
  "$hmd_approval=approved" \
  docker compose \
  --project-directory "$hmd_repo_root" \
  --env-file "$hmd_deploy_env" \
  -f "$hmd_repo_root/docker-compose.yml" \
  up -d --no-deps "$hmd_service"
