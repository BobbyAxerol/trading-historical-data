#!/usr/bin/env bash
# Launch exactly the reviewed, detached Phase D BTCUSDT perpetual rebuild.
# No arguments or arbitrary image/collector command are accepted.
set -euo pipefail

if [[ "$#" -ne 0 ]]; then
  echo "usage: $0" >&2
  exit 64
fi

hmd_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
hmd_repo_root="$(cd -- "${hmd_script_dir}/.." && pwd)"
hmd_deploy_env="/srv/primus/historical-market-data/deploy/compose.env"
hmd_revision="$(git -C "$hmd_repo_root" rev-parse --short=12 HEAD)"
hmd_image_ref="primus-historical-market-data:phase-d-${hmd_revision}"

if [[ ! -r "$hmd_deploy_env" ]]; then
  echo "protected Phase D Compose environment is unavailable" >&2
  exit 66
fi

if ! sudo -n docker image inspect "$hmd_image_ref" >/dev/null; then
  echo "required reviewed Phase D image is unavailable: $hmd_image_ref" >&2
  exit 69
fi

exec sudo -n env \
  PRIMUS_HMD_IMAGE_REF="$hmd_image_ref" \
  PRIMUS_HMD_PHASE_D_BINANCE_USDM_PERPETUAL_1M_APPROVED=approved \
  docker compose \
  --project-directory "$hmd_repo_root" \
  --env-file "$hmd_deploy_env" \
  -f "$hmd_repo_root/docker-compose.yml" \
  up -d --no-deps phase-d-binance-usdm-perpetual-1m
