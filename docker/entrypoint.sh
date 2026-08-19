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
  && [ "${8:-}" = "--lookback-days" ] \
  && [ "${9:-}" = "2500" ] \
  && [ "${10:-}" = "--no-vision" ] \
  && [ "${11:-}" = "--no-validate" ] \
  && [ "${12:-}" = "--sleep" ] \
  && [ "${13:-}" = "3600" ] \
  && [ "$#" -eq 13 ]; then
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

if [ "${PRIMUS_HMD_PHASE_D_VN30F1M_VNDIRECT_DAILY_APPROVED:-}" = "approved" ] \
  && [ "${1:-}" = "python" ] \
  && [ "${2:-}" = "-m" ] \
  && [ "${3:-}" = "collectors.vn_derivatives" ] \
  && [ "${4:-}" = "sync-vndirect" ] \
  && [ "${5:-}" = "--resolution" ] \
  && [ "${6:-}" = "1d" ] \
  && [ "${7:-}" = "--mode" ] \
  && [ "${8:-}" = "once" ] \
  && [ "${9:-}" = "--start" ] \
  && [ "${10:-}" = "2017-08-10" ] \
  && [ "${11:-}" = "--overlap-days" ] \
  && [ "${12:-}" = "14" ] \
  && [ "${13:-}" = "--audit-phase-d" ] \
  && [ "${14:-}" = "--json" ] \
  && [ "$#" -eq 14 ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_PHASE_D_BINANCE_FUTURES_METRICS_5M_APPROVED:-}" = "approved" ] \
  && [ "${1:-}" = "python" ] \
  && [ "${2:-}" = "-m" ] \
  && [ "${3:-}" = "collectors.binance_futures_metrics_5m" ] \
  && [ "${4:-}" = "--mode" ] \
  && [ "${5:-}" = "once" ] \
  && [ "${6:-}" = "--symbols" ] \
  && [ "${7:-}" = "BTCUSDT" ] \
  && [ "${8:-}" = "--start-date" ] \
  && [ "${9:-}" = "2020-01-01" ] \
  && [ "${10:-}" = "--max-workers" ] \
  && [ "${11:-}" = "2" ] \
  && [ "${12:-}" = "--no-legacy" ] \
  && [ "${13:-}" = "--rest-tail-days" ] \
  && [ "${14:-}" = "7" ] \
  && [ "${15:-}" = "--rest-overlap-hours" ] \
  && [ "${16:-}" = "24" ] \
  && [ "${17:-}" = "--audit-phase-d" ] \
  && [ "$#" -eq 17 ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_PHASE_D_BINANCE_USDM_QUARTERLY_1M_APPROVED:-}" = "approved" ] \
  && [ "${1:-}" = "python" ] \
  && [ "${2:-}" = "-m" ] \
  && [ "${3:-}" = "collectors.binance_usdm_quarterly_1m" ] \
  && [ "${4:-}" = "--mode" ] \
  && [ "${5:-}" = "once" ] \
  && [ "${6:-}" = "--pairs" ] \
  && [ "${7:-}" = "BTCUSDT" ] \
  && [ "${8:-}" = "--start-month" ] \
  && [ "${9:-}" = "2021-02" ] \
  && [ "${10:-}" = "--rest-bridge-days" ] \
  && [ "${11:-}" = "7" ] \
  && [ "${12:-}" = "--repair-gaps" ] \
  && [ "${13:-}" = "--max-gap-minutes" ] \
  && [ "${14:-}" = "5" ] \
  && [ "${15:-}" = "--audit-phase-d" ] \
  && [ "$#" -eq 15 ]; then
  exec "$@"
fi

# Phase E remains service-scoped.  The string comparisons below intentionally
# accept only one literal argv vector per approval; no generic Phase E writer
# flag exists, and Deribit is still absent from every permitted command.
if [ "${PRIMUS_HMD_PHASE_E_BINANCE_USDM_CORE_PERPETUAL_1M_APPROVED:-}" = "approved" ] \
  && [ "$*" = "python -m collectors.binance_usdm_perpetual_1m --mode once --symbols ETHUSDT,SOLUSDT,BNBUSDT,DOGEUSDT --start-month 2020-01 --daily-bridge-days 35 --rest-bridge-days 35 --rest-window-minutes 10080 --phase-label e --allow-later-start" ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_PHASE_E_BINANCE_DAILY_MATRIX_APPROVED:-}" = "approved" ] \
  && [ "$*" = "python -m collectors.binance_daily_matrix --mode once --backfill-start 2020-01-01 --top-n 400 --overlap-days 5 --min-history-days 365 --phase-e-audit" ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_PHASE_E_BINANCE_ORDERBOOK_HISTORY_1H_APPROVED:-}" = "approved" ] \
  && [ "$*" = "python -m collectors.binance_orderbook_snapshot_1h --mode once --symbols BTCUSDT,BTCUSDT_260925,BTCUSDT_261225 --lookback-days 2500 --phase-e-audit --fail-on-symbol-error" ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_PHASE_E_VN_DAILY_UNIVERSE_1D_APPROVED:-}" = "approved" ] \
  && [ "$*" = "python -m collectors.vn_daily --mode once --configured-universe --backfill-start 2016-01-01 --force-history --resume-success-after 2026-08-14T05:25:00+00:00 --audit-phase-e --fail-on-symbol-error" ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_PHASE_E_VN_DAILY_MATRIX_REBUILD_APPROVED:-}" = "approved" ] \
  && [ "$*" = "python -m collectors.vn_daily_matrix" ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_PHASE_E_VN30F1M_VNDIRECT_1M_APPROVED:-}" = "approved" ] \
  && [ "$*" = "python -m collectors.vn_derivatives sync-vndirect --resolution 1m --mode once --start 2017-08-10 --window-days 31 --min-window-days 7 --require-source-proof --audit-phase-e --json" ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_PHASE_E_VN30_CONTRACT_SOURCE_PROBE_APPROVED:-}" = "approved" ] \
  && [ "$*" = "python -m collectors.vn_derivatives probe --version v1 --contracts VN30F1709,VN30F2406,VN30F2508,VN30F2608 --providers kbs,dnse --window-days 30 --json" ]; then
  exec "$@"
fi

# Phase F is a separately owner-approved DNSE legacy-alias rebuild.  Its
# source proof cannot publish Parquet, while its backfill cannot run until the
# persisted proof is accepted by the collector itself.  Keep both argv vectors
# literal so no broad DNSE collector invocation becomes implicitly approved.
if [ "${PRIMUS_HMD_PHASE_F_VN30F1M_DNSE_PROBE_APPROVED:-}" = "approved" ] \
  && [ "$*" = "python -m collectors.vn30f1m_dnse_phase_f --mode probe --symbols VN30F1M --probe-start 2025-01-06 --probe-end 2025-01-10 --json" ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_PHASE_F_VN30F1M_DNSE_BACKFILL_APPROVED:-}" = "approved" ] \
  && [ "$*" = "python -m collectors.vn30f1m_dnse_phase_f --mode backfill --symbols VN30F1M --backfill-start 2025-01-01 --backfill-end 2026-08-18 --window-days 5 --require-probe --audit-phase-f --json" ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_PHASE_F_VN30F1M_CSV_BRIDGE_APPROVED:-}" = "approved" ] \
  && [ "$*" = "python -m collectors.vn30f1m_csv_bridge_phase_f --base-raw-path /input/vn30f1m_raw_1m.csv --extended-raw-path /input/vn30f1m_raw_1m_from_2024.csv --adjusted-path /input/vn30f1m_1m.csv --start 2018-01-02 --end 2026-08-18 --json" ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_PHASE_G_BINANCE_FUTURES_METRICS_5M_ETHUSDT_APPROVED:-}" = "approved" ] \
  && [ "$*" = "python -m collectors.binance_futures_metrics_5m --mode once --symbols ETHUSDT --start-date 2020-01-01 --max-workers 2 --no-legacy --rest-tail-days 7 --rest-overlap-hours 24 --audit-phase-g" ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_STAGED_BINANCE_FUTURES_METRICS_5M_ETHUSDT_APPROVED:-}" = "approved" ] \
  && [ "$*" = "python -m collectors.binance_futures_metrics_5m --mode live --symbols ETHUSDT --no-legacy --no-vision --rest-tail-days 1 --rest-overlap-hours 1 --no-validate --sleep 21600" ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_STAGED_CRYPTO_CORE_1M_APPROVED:-}" = "approved" ] \
  && [ "$*" = "python -m collectors.crypto_1m --mode live --symbols ETHUSDT,SOLUSDT,BNBUSDT,DOGEUSDT" ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_STAGED_BINANCE_USDM_QUARTERLY_NEXT_1M_APPROVED:-}" = "approved" ] \
  && [ "$*" = "python -m collectors.binance_usdm_quarterly_1m --mode live --pairs BTCUSDT --symbols BTCUSDT_261225 --no-archive-discovery --no-monthly --no-daily --sleep 21600" ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_STAGED_BINANCE_ORDERBOOK_EXPANDED_1H_APPROVED:-}" = "approved" ] \
  && [ "$*" = "python -m collectors.binance_orderbook_snapshot_1h --mode live --symbols BTCUSDT,BTCUSDT_260925,BTCUSDT_261225 --lookback-days 2500 --no-vision --no-validate --sleep 3600" ]; then
  exec "$@"
fi

if [ "${PRIMUS_HMD_STAGED_VN30F1M_VNDIRECT_1M_APPROVED:-}" = "approved" ] \
  && [ "$*" = "python -m collectors.vn_derivatives sync-vndirect --resolution 1m --mode live --overlap-minutes 10 --sleep 60" ]; then
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

echo "refusing to start a collector: no service-scoped staged authorization, phase-D/Phase-E authorization, or bounded-seed authorization matched" >&2
exit 64
