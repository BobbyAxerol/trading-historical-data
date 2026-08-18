#!/usr/bin/env bash
# Launch one exact, detached Phase F DNSE historical/proof service.
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 <reviewed-phase-f-service>" >&2
  exit 64
fi

case "$1" in
  phase-f-vn30f1m-dnse-probe)
    hmd_approval="PRIMUS_HMD_PHASE_F_VN30F1M_DNSE_PROBE_APPROVED"
    ;;
  phase-f-vn30f1m-dnse-backfill)
    hmd_approval="PRIMUS_HMD_PHASE_F_VN30F1M_DNSE_BACKFILL_APPROVED"
    ;;
  phase-f-vn30f1m-csv-bridge)
    hmd_approval="PRIMUS_HMD_PHASE_F_VN30F1M_CSV_BRIDGE_APPROVED"
    ;;
  *)
    echo "unreviewed Phase F service: $1" >&2
    exit 64
    ;;
esac

hmd_service="$1"
hmd_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
hmd_repo_root="$(cd -- "${hmd_script_dir}/.." && pwd)"
hmd_deploy_env="/srv/primus/historical-market-data/deploy/compose.env"
hmd_revision="$(git -C "$hmd_repo_root" rev-parse --short=12 HEAD)"
hmd_image_ref="primus-historical-market-data:phase-f-${hmd_revision}"

if [[ ! -r "$hmd_deploy_env" ]]; then
  echo "protected Phase F Compose environment is unavailable" >&2
  exit 66
fi
if ! sudo -n docker image inspect "$hmd_image_ref" >/dev/null; then
  echo "required reviewed Phase F image is unavailable: $hmd_image_ref" >&2
  exit 69
fi
if [[ -n "$(sudo -n docker ps --format '{{.Names}}' --filter 'name=primus-historical-market-data-phase-e-' --filter 'name=primus-historical-market-data-phase-f-')" ]]; then
  echo "another Phase E/F historical service is already running" >&2
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
