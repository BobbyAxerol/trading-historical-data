#!/bin/sh
set -eu

# This image contains writers.  A Compose invocation cannot accidentally start
# one while the new VPS is still in its B0 preflight window.  There is no
# blanket B0 approval: the protected host deployment file can inject the
# following value only into `crypto-1m-live` after B0 has an accepted staged
# status.  Even there, accept only that service's immutable command.  This is
# defense in depth for Compose operations, not a boundary against a privileged
# Docker administrator who can override an entrypoint manually.
if [ "${PRIMUS_HMD_STAGED_CRYPTO_1M_APPROVED:-}" = "approved" ] \
  && [ "${1:-}" = "python" ] \
  && [ "${2:-}" = "-m" ] \
  && [ "${3:-}" = "collectors.crypto_1m" ] \
  && [ "${4:-}" = "--mode" ] \
  && [ "${5:-}" = "live" ] \
  && [ "$#" -eq 5 ]; then
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

echo "refusing to start a collector: no service-scoped staged authorization or bounded-seed authorization matched" >&2
exit 64
