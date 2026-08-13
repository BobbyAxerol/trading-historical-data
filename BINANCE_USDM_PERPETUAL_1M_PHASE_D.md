# Binance USD-M Perpetual 1m — Phase D

## Approved initial scope

The first Phase D run rebuilds only `BTCUSDT` perpetual 1m data on the clean
new VPS. It starts at `2020-01`, writes only under the canonical
`storage/crypto/binance_futures/1m/symbol=BTCUSDT/` root, and never imports
old-VPS runtime data. Deribit is not part of this run.

## Sources and bounded-memory behavior

1. Completed Binance Vision monthly USD-M archives are listed and handled one
   month at a time. An existing partition is skipped, so a rerun resumes.
2. The latest 35 completed UTC days are checked against daily Vision archives.
   A partially populated B0 tail day is not treated as complete.
3. The latest 35 days are fetched again through REST in seven-day maximum
   windows (`10,080` minutes), immediately appended/deduplicated, then memory
   is released. This bridges archive publication lag and protects the live
   tail without loading historical years into RAM.
4. A streaming per-partition validation records rows, first/latest time,
   duplicates, OHLC/negative checks, continuity gaps, and tail lag. It does
   not concatenate the historical dataset in memory.

Current-day partial *daily* files are excluded. Closed 1m candles may be
written by the bounded REST bridge and the B0 live tail.

## Launch and observe

After the reviewed image for the current commit exists, launch only through:

```bash
./tools/run_phase_d_binance_usdm_perpetual_1m.sh
```

The job runs detached as `phase-d-binance-usdm-perpetual-1m`; host-visible
logs are written to:

```text
/srv/primus/historical-market-data/logs/phase_d_binance_usdm_perpetual_1m.log
```

Its durable state and audit are:

```text
state/phase_d/phase_d_binance_usdm_perpetual_1m.json
state/audits/crypto_binance_futures_1m_BTCUSDT_phase_d.json
```

An exited one-shot container with exit code `0` means the job completed its
idempotent batch. A nonzero exit or `requires_repair` state is an anomaly to
inspect and repair through a separately reviewed bounded action; do not delete
or overwrite canonical partitions manually.
