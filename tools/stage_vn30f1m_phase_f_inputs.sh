#!/usr/bin/env bash
# Stage owner-provided CSV inputs under the dedicated runtime root for the
# read-only Phase F bridge mount.  This script never deletes or overwrites a
# differing staged input.
set -euo pipefail

if [[ "$#" -ne 0 ]]; then
  echo "usage: $0" >&2
  exit 64
fi

hmd_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
hmd_repo_root="$(cd -- "${hmd_script_dir}/.." && pwd)"
hmd_runtime_dir="/srv/primus/historical-market-data/state/migration_inputs"
hmd_deploy_env="/srv/primus/historical-market-data/deploy/compose.env"
hmd_collector_uid="$(awk -F= '/^PRIMUS_HMD_COLLECTOR_UID=/{print $2; exit}' "$hmd_deploy_env")"
hmd_collector_gid="$(awk -F= '/^PRIMUS_HMD_COLLECTOR_GID=/{print $2; exit}' "$hmd_deploy_env")"

if [[ ! "$hmd_collector_uid" =~ ^[0-9]+$ || ! "$hmd_collector_gid" =~ ^[0-9]+$ ]]; then
  echo "collector UID/GID are unavailable from protected compose environment" >&2
  exit 66
fi

sudo -n install -d -o "$hmd_collector_uid" -g "$hmd_collector_gid" -m 0750 "$hmd_runtime_dir"
for hmd_name in vn30f1m_raw_1m.csv vn30f1m_1m.csv; do
  hmd_source="${hmd_repo_root}/${hmd_name}"
  hmd_target="${hmd_runtime_dir}/${hmd_name}"
  if [[ ! -f "$hmd_source" ]]; then
    echo "missing approved input: $hmd_source" >&2
    exit 66
  fi
  if [[ -e "$hmd_target" ]]; then
    if ! sudo -n cmp -s "$hmd_source" "$hmd_target"; then
      echo "refusing to overwrite different staged input: $hmd_target" >&2
      exit 73
    fi
    continue
  fi
  sudo -n install -o "$hmd_collector_uid" -g "$hmd_collector_gid" -m 0640 "$hmd_source" "$hmd_target"
done

echo "staged VN30F1M Phase F inputs under $hmd_runtime_dir"
