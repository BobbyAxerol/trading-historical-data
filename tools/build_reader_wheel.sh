#!/usr/bin/env bash
# Build the code-only reader wheel in a pinned, hash-locked Python 3.12 builder.
#
# The source checkout is mounted read-only. Setuptools receives an ephemeral
# copy in the container because it must create egg-info while building.
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 OUTPUT_DIRECTORY" >&2
  exit 64
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
output_dir="$(realpath -e -- "$1")"

if [[ ! -d "$output_dir" ]]; then
  echo "output directory must already exist: $output_dir" >&2
  exit 66
fi

base_image="python@sha256:d657ab0ade19f404a6ccc883ab399540de667aff751748ce23c07330c5a89e64"
source_date_epoch="$(git -C "$repo_root" log -1 --format=%ct)"

if [[ ! "$source_date_epoch" =~ ^[0-9]+$ ]]; then
  echo "could not resolve a commit timestamp for reproducible wheel metadata" >&2
  exit 70
fi

sudo -n docker run --rm \
  --read-only \
  --cap-drop ALL \
  --pids-limit 256 \
  --cpus 1 \
  --memory 1024m \
  --user 1000:1000 \
  --tmpfs /tmp:rw,exec,nosuid,size=512m \
  -e HOME=/tmp \
  -e PIP_CACHE_DIR=/tmp/pip-cache \
  -e SOURCE_DATE_EPOCH="$source_date_epoch" \
  --mount "type=bind,src=${repo_root},dst=/src,readonly" \
  --mount "type=bind,src=${output_dir},dst=/dist" \
  "$base_image" sh -lc '
    cp -a /src/. /tmp/build-src
    python -m venv /tmp/build-venv
    /tmp/build-venv/bin/python -m pip install \
      --disable-pip-version-check --no-cache-dir --require-hashes \
      -r /src/requirements-build.lock
    /tmp/build-venv/bin/python -m build --wheel --no-isolation \
      --outdir /dist /tmp/build-src
  '
