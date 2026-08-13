#!/bin/sh
set -eu

# This image contains writers.  A Compose invocation cannot accidentally start
# one while the new VPS is still in its B0 preflight window.  There is no
# blanket B0 approval: the protected host deployment file can inject an
# approval only into its own reviewed staged service.  Every clause below
# accepts one immutable B0-seeded tail command, never a default symbol list,
# archive sync, repair job, broad backfill, or Deribit invocation.  This is
# defense in depth for Compose operations, not a boundary against a privileged
# Docker administrator who can override an entrypoint manually.
if [ "${PRIMUS_HMD_STAGED_CRYPTO_1M_APPROVED:-}" = "approved" ] \
  && [ "${1:-}" = "python" ] \
  && [ "${2:-}" = "-m" ] \
  && [ "${3:-}" = "collectors.crypto_1m" ] \
  && [ "${4:-}" = "--mode" ] \
  && [ "${5:-}" = "live" ] \
  && [ "${6:-}" = "--symbols" ] \
  && [ "${7:-}" = "BTCUSDT" ] \
  && [ "$#" -eq 7 ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_STAGED_BINANCE_USDM_QUARTERLY_1M_APPROVED:-}" = "approved" ] \
  && [ "${1:-}" = "python" ] \
  && [ "${2:-}" = "-m" ] \
  && [ "${3:-}" = "collectors.binance_usdm_quarterly_1m" ] \
  && [ "${4:-}" = "--mode" ] \
  && [ "${5:-}" = "live" ] \
  && [ "${6:-}" = "--pairs" ] \
  && [ "${7:-}" = "BTCUSDT" ] \
  && [ "${8:-}" = "--symbols" ] \
  && [ "${9:-}" = "BTCUSDT_260925" ] \
  && [ "${10:-}" = "--no-archive-discovery" ] \
  && [ "${11:-}" = "--no-monthly" ] \
  && [ "${12:-}" = "--no-daily" ] \
  && [ "${13:-}" = "--sleep" ] \
  && [ "${14:-}" = "21600" ] \
  && [ "$#" -eq 14 ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_STAGED_BINANCE_SPOT_1M_APPROVED:-}" = "approved" ] \
  && [ "${1:-}" = "python" ] \
  && [ "${2:-}" = "-m" ] \
  && [ "${3:-}" = "collectors.binance_spot_1m" ] \
  && [ "${4:-}" = "--mode" ] \
  && [ "${5:-}" = "live" ] \
  && [ "${6:-}" = "--symbols" ] \
  && [ "${7:-}" = "BTCUSDT" ] \
  && [ "${8:-}" = "--no-monthly" ] \
  && [ "${9:-}" = "--no-daily" ] \
  && [ "${10:-}" = "--no-validate" ] \
  && [ "${11:-}" = "--sleep" ] \
  && [ "${12:-}" = "75" ] \
  && [ "$#" -eq 12 ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_STAGED_BINANCE_ORDERBOOK_SNAPSHOT_1H_APPROVED:-}" = "approved" ] \
  && [ "${1:-}" = "python" ] \
  && [ "${2:-}" = "-m" ] \
  && [ "${3:-}" = "collectors.binance_orderbook_snapshot_1h" ] \
  && [ "${4:-}" = "--mode" ] \
  && [ "${5:-}" = "live" ] \
  && [ "${6:-}" = "--symbols" ] \
  && [ "${7:-}" = "BTCUSDT" ] \
  && [ "${8:-}" = "--no-vision" ] \
  && [ "${9:-}" = "--no-validate" ] \
  && [ "${10:-}" = "--sleep" ] \
  && [ "${11:-}" = "3600" ] \
  && [ "$#" -eq 11 ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_STAGED_BINANCE_FUTURES_METRICS_5M_APPROVED:-}" = "approved" ] \
  && [ "${1:-}" = "python" ] \
  && [ "${2:-}" = "-m" ] \
  && [ "${3:-}" = "collectors.binance_futures_metrics_5m" ] \
  && [ "${4:-}" = "--mode" ] \
  && [ "${5:-}" = "live" ] \
  && [ "${6:-}" = "--symbols" ] \
  && [ "${7:-}" = "BTCUSDT" ] \
  && [ "${8:-}" = "--no-legacy" ] \
  && [ "${9:-}" = "--no-vision" ] \
  && [ "${10:-}" = "--rest-tail-days" ] \
  && [ "${11:-}" = "1" ] \
  && [ "${12:-}" = "--rest-overlap-hours" ] \
  && [ "${13:-}" = "1" ] \
  && [ "${14:-}" = "--no-validate" ] \
  && [ "${15:-}" = "--sleep" ] \
  && [ "${16:-}" = "21600" ] \
  && [ "$#" -eq 16 ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_STAGED_VN_DAILY_APPROVED:-}" = "approved" ] \
  && [ "${1:-}" = "python" ] \
  && [ "${2:-}" = "-m" ] \
  && [ "${3:-}" = "collectors.vn_daily" ] \
  && [ "${4:-}" = "--mode" ] \
  && [ "${5:-}" = "live" ] \
  && [ "${6:-}" = "--symbols" ] \
  && [ "${7:-}" = "FPT" ] \
  && [ "${8:-}" = "--schedule" ] \
  && [ "${9:-}" = "16:30" ] \
  && [ "${10:-}" = "--skip-derived" ] \
  && [ "$#" -eq 10 ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_STAGED_VN30F1M_VNDIRECT_APPROVED:-}" = "approved" ] \
  && [ "${1:-}" = "python" ] \
  && [ "${2:-}" = "-m" ] \
  && [ "${3:-}" = "collectors.vn_derivatives" ] \
  && [ "${4:-}" = "sync-vndirect" ] \
  && [ "${5:-}" = "--resolution" ] \
  && [ "${6:-}" = "1d" ] \
  && [ "${7:-}" = "--mode" ] \
  && [ "${8:-}" = "live" ] \
  && [ "${9:-}" = "--schedule" ] \
  && [ "${10:-}" = "16:30" ] \
  && [ "${11:-}" = "--overlap-days" ] \
  && [ "${12:-}" = "14" ] \
  && [ "$#" -eq 12 ]; then
  exec "$@"
fi

# Phase D preserves the B0 default-deny model: an owner-approved one-shot
# archive rebuild has a dedicated service variable and literal command.  It
# cannot turn a generic backfill, another symbol, a repair sweep, or Deribit
# invocation into an approved writer.
if [ "${PRIMUS_HMD_PHASE_D_BINANCE_USDM_PERPETUAL_1M_APPROVED:-}" = "approved" ] \
  && [ "${1:-}" = "python" ] \
  && [ "${2:-}" = "-m" ] \
  && [ "${3:-}" = "collectors.binance_usdm_perpetual_1m" ] \
  && [ "${4:-}" = "--mode" ] \
  && [ "${5:-}" = "once" ] \
  && [ "${6:-}" = "--symbols" ] \
  && [ "${7:-}" = "BTCUSDT" ] \
  && [ "${8:-}" = "--start-month" ] \
  && [ "${9:-}" = "2020-01" ] \
  && [ "${10:-}" = "--daily-bridge-days" ] \
  && [ "${11:-}" = "35" ] \
  && [ "${12:-}" = "--rest-bridge-days" ] \
  && [ "${13:-}" = "35" ] \
  && [ "${14:-}" = "--rest-window-minutes" ] \
  && [ "${15:-}" = "10080" ] \
  && [ "$#" -eq 15 ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_PHASE_D_BINANCE_SPOT_1M_APPROVED:-}" = "approved" ] \
  && [ "${1:-}" = "python" ] \
  && [ "${2:-}" = "-m" ] \
  && [ "${3:-}" = "collectors.binance_spot_1m" ] \
  && [ "${4:-}" = "--mode" ] \
  && [ "${5:-}" = "once" ] \
  && [ "${6:-}" = "--symbols" ] \
  && [ "${7:-}" = "BTCUSDT" ] \
  && [ "${8:-}" = "--backfill-start" ] \
  && [ "${9:-}" = "2018-01-01" ] \
  && [ "${10:-}" = "--max-workers" ] \
  && [ "${11:-}" = "1" ] \
  && [ "${12:-}" = "--repair-gaps" ] \
  && [ "$#" -eq 12 ]; then
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

echo "refusing to start a collector: no service-scoped staged authorization, phase-D authorization, or bounded-seed authorization matched" >&2
exit 64
