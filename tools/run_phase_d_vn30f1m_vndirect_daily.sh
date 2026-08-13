#!/usr/bin/env bash
# Launch exactly the reviewed, detached Phase D VNDIRECT VN30F1M daily rebuild.
set -euo pipefail

if [[ "$#" -ne 0 ]]; then
  echo "usage: $0" >&2
  exit 64
fi

hmd_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
hmd_repo_root="$(cd -- "${hmd_script_dir}/.." && pwd)"
hmd_deploy_env="/srv/primus/historical-market-data/deploy/compose.env"
hmd_service="phase-d-vn30f1m-vndirect-daily"
hmd_container="primus-historical-market-data-${hmd_service}-1"
hmd_revision="$(git -C "$hmd_repo_root" rev-parse --short=12 HEAD)"
hmd_head_image="primus-historical-market-data:phase-d-${hmd_revision}"
hmd_prior_image="$(sudo -n docker inspect --format '{{.Config.Image}}' "$hmd_container" 2>/dev/null || true)"

# Prefer the reviewed image for the current source commit.  A doc-only commit
# need not force an image build: then preserve the evidence container's image
# as a safe fallback.  This prevents an exited one-shot from silently rerunning
# superseded collector code after a real source fix.
if sudo -n docker image inspect "$hmd_head_image" >/dev/null 2>&1; then
  hmd_image_ref="$hmd_head_image"
else
  hmd_image_ref="$hmd_prior_image"
fi

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
  PRIMUS_HMD_PHASE_D_VN30F1M_VNDIRECT_DAILY_APPROVED=approved \
  docker compose \
  --project-directory "$hmd_repo_root" \
  --env-file "$hmd_deploy_env" \
  -f "$hmd_repo_root/docker-compose.yml" \
  up -d --no-deps "$hmd_service"
