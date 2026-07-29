# _get_data Implementation Plan

This file is the consolidated implementation tracker for `_get_data` jobs. Detailed job guides may live in separate markdown files, but the executable phase plan and implementation logs should be summarized here to avoid fragmented state.

## Active Job: VN Daily Universe Upgrade

Source guide: `implementation.md`

Status: Phase 1 in progress

Branch: `feat/vn-daily-universe-upgrade`

Important adaptation:

- The source guide mentions `csv.gz` paths for raw daily and matrix outputs.
- Current `_get_data` storage architecture has already migrated these datasets to Parquet.
- This implementation keeps the current storage contract:
  - raw VN equity daily: `storage/vn/equity/1d/symbol={SYMBOL}/year={YYYY}/part.parquet`;
  - raw VN futures daily: `storage/vn/futures/1d/symbol=VN30F1M/year={YYYY}/part.parquet`;
  - VN daily matrix: `storage/vn/equity/daily_matrix/{open,high,low,close,volume}.parquet`;
  - universe report: `state/vn_daily_universe_report.csv.gz`.

### Goal

Expand `VNDailyMatrix` with additional potential VN symbols, preserve raw data for all candidates, score the universe without punishing pre-listing history, and include `VN30F1M` as an auxiliary daily series in the matrix without ranking it as equity.

### Phase 1 — Backfill, Validate, Score Universe

Scope:

- Extend `configs/symbols.vn_daily.yml` with additional candidate symbols.
- Keep `backfill_start: "2016-01-01"`.
- Make the collector merge configured `symbols` plus `candidate_symbols`.
- Keep append/deduplicate behavior by `time, symbol`.
- Accept each symbol's actual `first_valid_date`; do not mark pre-listing dates as missing.
- Build `state/vn_daily_universe_report.csv.gz` with:
  - `symbol`;
  - `asset_type`;
  - `first_valid_date`;
  - `last_valid_date`;
  - `row_count`;
  - `coverage_ratio`;
  - `max_internal_gap`;
  - `median_turnover_60d`;
  - `median_turnover_252d`;
  - `score`;
  - `tier`;
  - `reasons`.
- Score weights:
  - 40% liquidity;
  - 30% continuity/coverage from `first_valid_date`;
  - 20% history length capped at 5 years;
  - 10% recent availability.
- Keep high-liquidity new listings at least `extended`.
- Keep low-liquidity long-history symbols as `review` or `extended`, but never delete raw storage because of tier.

Phase 1 tests:

- Config parser merges `symbols` and `candidate_symbols` deterministically.
- Universe report treats missing dates before `first_valid_date` as non-missing.
- New high-liquidity short-history symbols can be `extended`.
- Long-history stale or illiquid symbols get appropriate warnings/tier.
- Collector calls report generation after raw daily run.
- No network-dependent test is required for the unit suite.

Exit criteria:

- Candidate symbols are configured and discoverable by collector.
- Report is generated with the exact schema.
- Report scoring uses only observed history from `first_valid_date`.
- No existing loader endpoint behavior is broken.

### Phase 2 — Matrix Rebuild, VN30F1M Auxiliary Series, Loader Integration

Scope:

- Extend `collectors/vn_daily_matrix.py` to read:
  - equity raw daily from `storage/vn/equity/1d`;
  - futures daily from `storage/vn/futures/1d/symbol=VN30F1M`.
- Add or reuse a daily `VN30F1M` source:
  - prefer existing daily data if available;
  - otherwise aggregate from 1m using Vietnam trading dates:
    - `open = first`;
    - `high = max`;
    - `low = min`;
    - `close = last`;
    - `volume = sum`.
- Output `VN30F1M` as a normal matrix column.
- Keep `VN30F1M` out of equity ranking universe:
  - `asset_type=future`;
  - `tier=auxiliary`.
- Update `state/vn_daily_matrix_symbols.json` with `equity_symbols` and `auxiliary_symbols`.
- Preserve `VNDailyMatrix().load(...)`, `load_features(...)`, `load_ohlcv(...)`, and `load_ohlcv_frame(...)` return behavior.

Phase 2 tests:

- Matrix builder includes `VN30F1M` column when futures daily exists.
- Matrix builder does not create all-null columns.
- Matrix index is increasing and duplicate-free.
- OHLC logic and non-negative volume checks pass.
- `VNDailyMatrix` can load the rebuilt matrix.

Exit criteria:

- `VN30F1M` is present in matrix but excluded from equity rank metadata.
- Matrix rebuild validates from `2016-01-01`.
- Permanent storage increase remains reasonable, expected under 100 MB for this upgrade.

### Implementation Logs

#### 2026-07-29 UTC — Branch Setup

- Created branch `feat/vn-daily-universe-upgrade` from `dev`.
- Kept Deribit Phase 6 branch separate to reduce conflicts with option ingestion work.
- `implementation.md` is retained as the source guide; this file is the consolidated implementation tracker.

#### 2026-07-29 UTC — Phase 1 Implementation

Status: complete

Changed:

- Added `candidate_symbols` to `configs/symbols.vn_daily.yml`.
- Added `external_symbols: [VN30F1M]` for auxiliary futures handling.
- Added `collectors/vn_daily_universe.py`:
  - deterministic config symbol merge;
  - raw Parquet/CSV fallback reader;
  - universe quality metrics;
  - liquidity/coverage/history/recent score;
  - tier assignment;
  - `state/vn_daily_universe_report.csv.gz` writer.
- Updated `collectors/vn_daily.py` to:
  - merge `symbols + candidate_symbols`;
  - generate universe report after raw daily updates;
  - include configured external symbols in the report.
- Added unit coverage in `tests/test_vn_daily_universe.py`.

Config result:

- Legacy symbols: `272`.
- New candidate symbols: `126`.
- Merged equity symbols: `398`.
- External symbols: `VN30F1M`.

Local report run on current storage:

- Output: `state/vn_daily_universe_report.csv.gz`.
- Rows: `399`.
- Tier counts:
  - `core=19`;
  - `extended=251`;
  - `review=128`;
  - `auxiliary=1`.

Validation:

- `python -m unittest tests.test_vn_daily_universe`: OK, 5 tests.
- `python -m compileall collectors/vn_daily.py collectors/vn_daily_universe.py collectors/vn_daily_matrix.py`: OK.

Notes:

- No network backfill was run in this phase implementation pass.
- Do not use `collectors.vn_daily --max-symbols` as a production smoke unless matrix overwrite behavior is explicitly controlled; existing collector rebuilds matrix from the selected symbol subset.
- Phase 2 should address matrix rebuild behavior and `VN30F1M` daily matrix inclusion.

#### 2026-07-29 UTC — Phase 2 Implementation

Status: complete

Changed:

- Extended `collectors/vn_daily_matrix.py` to:
  - default to merged `symbols + candidate_symbols`;
  - read configured `external_symbols`;
  - read futures daily from `storage/vn/futures/1d`;
  - aggregate `VN30F1M` from `storage/vn/futures/1m` if daily futures storage is missing;
  - persist aggregated daily futures to `storage/vn/futures/1d`;
  - include auxiliary futures columns in the matrix;
  - drop all-null matrix columns;
  - write `equity_symbols`, `auxiliary_symbols`, and missing-symbol lists to `state/vn_daily_matrix_symbols.json`.
- Updated README endpoint docs for `VNDailyMatrix`, `VN30F1M`, and `load_data("vn_daily_matrix", ...)`.
- Added tests for:
  - `VN30F1M` 1m -> daily aggregation;
  - matrix inclusion of auxiliary column;
  - matrix state metadata separating equity and auxiliary symbols.

Unit validation:

- `python -m unittest tests.test_vn_daily_universe`: OK, 6 tests.
- `python -m compileall collectors/vn_daily.py collectors/vn_daily_universe.py collectors/vn_daily_matrix.py data_loader.py`: OK.

Production raw-storage rebuild:

- Command: `python -m collectors.vn_daily_matrix --start-date 2016-01-01`.
- Result:
  - matrix shape: `2640 x 273`;
  - `equity_symbols=272`;
  - `auxiliary_symbols=["VN30F1M"]`;
  - `missing_symbols=126`;
  - `missing_auxiliary_symbols=[]`.
- `VN30F1M` daily futures was materialized under:
  - `storage/vn/futures/1d/symbol=VN30F1M/year=2024/part.parquet`;
  - `storage/vn/futures/1d/symbol=VN30F1M/year=2025/part.parquet`;
  - `storage/vn/futures/1d/symbol=VN30F1M/year=2026/part.parquet`.

Endpoint validation:

- `VNDailyMatrix().load("close", symbols=["FPT", "VN30F1M"], start_date="2018-01-01")`: shape `2139 x 2`.
- `VNDailyMatrix().load_ohlcv(symbols=["FPT", "VN30F1M"], start_date="2018-01-01")`: returns keys `FPT`, `VN30F1M`.
- `load_data("vn_daily_matrix", feature="close", symbols=["FPT", "VN30F1M"], start_date="2018-01-01")`: shape `2139 x 2`.

Integrity validation:

- Matrix index increasing: OK.
- Duplicate index: none.
- OHLC bounds: OK.
- Negative volume: none.
- `VN30F1M` column present: yes.
- Temporary files left under matrix/futures daily: none.
- Disk:
  - `storage/vn/equity/daily_matrix=9.3M`;
  - `storage/vn/futures/1d=56K`.

Notes:

- The 126 new candidate symbols are configured and scored, but they are not in the production matrix yet because raw daily storage for those candidates is not present. They will enter the matrix after the `vn-daily` network backfill writes raw Parquet for them.
- Phase 2 is usable for existing raw equity symbols and `VN30F1M` auxiliary immediately.
