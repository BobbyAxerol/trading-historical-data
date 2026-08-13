#!/usr/bin/env bash
# Execute the single fixed, owner-approved B0 seed.  This script deliberately
# accepts no arguments: changing a backfill window belongs in reviewed source,
# never in an ad-hoc operational command.
set -euo pipefail

if [[ "$#" -ne 0 ]]; then
  echo "usage: $0" >&2
  exit 64
fi

hmd_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
hmd_repo_root="$(cd -- "${hmd_script_dir}/.." && pwd)"
hmd_deploy_env="/srv/primus/historical-market-data/deploy/compose.env"

if [[ ! -r "$hmd_deploy_env" ]]; then
  echo "protected B0 Compose environment is unavailable" >&2
  exit 66
fi

exec sudo -n env \
  PRIMUS_HMD_B0_SEED_APPROVED=approved \
  PRIMUS_HMD_B0_SEED_RUNNER=bounded-v1 \
  docker compose \
  --project-directory "$hmd_repo_root" \
  --env-file "$hmd_deploy_env" \
  -f "$hmd_repo_root/docker-compose.yml" \
  run --rm --no-deps crypto-1m-live \
  python -m collectors.b0_bounded_seed
