#!/bin/sh
set -eu

# This image contains writers.  A Compose invocation cannot accidentally start
# one while the new VPS is still in its B0 preflight window.  The approval value
# lives only in the protected host deployment env file after every B0 gate has
# been recorded as passed; it is never committed to Git.
if [ "${PRIMUS_HMD_B0_APPROVED:-}" != "approved" ]; then
  echo "refusing to start a collector: Phase B0 is not approved" >&2
  exit 64
fi

exec "$@"
