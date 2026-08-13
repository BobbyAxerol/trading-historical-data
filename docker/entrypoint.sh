#!/bin/sh
set -eu

# This image contains writers.  A Compose invocation cannot accidentally start
# one while the new VPS is still in its B0 preflight window.  The approval value
# lives only in the protected host deployment env file after B0 has an accepted
# status; it is never committed to Git.
if [ "${PRIMUS_HMD_B0_APPROVED:-}" = "approved" ]; then
  exec "$@"
fi

# The only pre-B0 writer exception is the owner-approved, one-shot bounded
# seed.  `tools/run_b0_seed.sh` supplies both values transiently and invokes
# one fixed runner with no operator-provided collector arguments.  The runner
# owns the small time windows and never enables Deribit historical backfill.
if [ "${PRIMUS_HMD_B0_SEED_APPROVED:-}" = "approved" ] \
  && [ "${PRIMUS_HMD_B0_SEED_RUNNER:-}" = "bounded-v1" ] \
  && [ "${1:-}" = "python" ] \
  && [ "${2:-}" = "-m" ] \
  && [ "${3:-}" = "collectors.b0_bounded_seed" ] \
  && [ "$#" -eq 3 ]; then
  exec "$@"
fi

echo "refusing to start a collector: Phase B0 is not approved and no bounded-seed authorization matched" >&2
exit 64
