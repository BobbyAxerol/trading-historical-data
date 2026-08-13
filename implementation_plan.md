# _get_data Implementation Plan

This file is the consolidated implementation tracker for `_get_data` jobs. Detailed job guides may live in separate markdown files, but the executable phase plan and implementation logs should be summarized here to avoid fragmented state.

## Active Cross-Cutting Operation: Primus New VPS Clean Rebuild

Detailed runbook: `PRIMUS_NEW_VPS_CLEAN_REBUILD_AND_PACKAGE_RUNBOOK.md`

Decision: migrate Git-tracked code/config/docs only; rebuild `storage/`,
`state/`, and `logs/` from upstream sources on the new VPS. The old VPS remains
an audit/reference archive. Package the reader as
`primus-historical-market-data` before broad new-VPS historical backfills, then
resume the remaining Deribit option roadmap from fresh new-VPS gates/state.

Execution boundary: old VPS performs only source freeze/push/tag and remains
live. New VPS performs Phase B0 production preflight, package implementation,
and all clean historical rebuild work. Phase B0 is a mandatory gate covering
capacity/concurrency, reproducible artifacts, production Compose mounts/ACLs,
source inventory, storage compatibility manifest, backup/restore, monitoring,
and time/environment isolation.

### 2026-08-13 UTC — Phase B0: Runtime Isolation And Reproducible-Build Start

Status: in progress; collector and backfill remain blocked.

#### Staged BTCUSDT writer scope correction (in verification)

- The first staged `crypto-1m-live` invocation used the collector's configured
  default symbol list. It was stopped immediately after the BTCUSDT tail
  completed one successful cycle (126 rows through 2026-08-13T12:30:00Z).
  ETHUSDT, SOLUSDT, BNBUSDT, and DOGEUSDT were rejected before writing because
  their new-VPS runtimes were empty.
- The replacement deployment must require the literal `--symbols BTCUSDT`
  argument in both Compose and the image entrypoint. It is not permitted to
  restart until rebuilt-image, isolated test, Compose inspection, heartbeat,
  and reader-parity evidence are refreshed.

#### 2026-08-13 UTC — owner-approved non-Deribit tail expansion

- Owner approved the seven B0-seeded live tails only: BTCUSDT futures/spot,
  BTCUSDT_260925 quarterly, BTCUSDT metrics/orderbook, FPT daily, and
  VN30F1M VNDIRECT daily. The runtime command contracts disable archive
  discovery/sync, default universe expansion, repair, broad historical work,
  and derived matrix work; Deribit remains off.
- Each service has its own protected approval variable and immutable
  entrypoint command. First cycles must be started and observed sequentially;
  a service with an error heartbeat is stopped and isolated rather than being
  retried by enabling another service.
- The resident-tail profile is bounded to seven services at 0.5 CPU, 512 MiB,
  and 128 PIDs each. `b0_seed_evidence --activate-staged-tails` records that
  profile only after the fixed B0 seed evidence passes; it does not start a
  writer and retains the prohibition on every heavy historical job.
- Every approved live tail emits a `sleeping` heartbeat every five minutes
  while it waits for the next cycle. An existing `error` heartbeat is never
  overwritten by the normal-wait status; only a later successful cycle can
  clear it.
- Runtime observation found the USD-M futures metrics ratio endpoints were
  incorrectly sent a COIN-M-style `pair` parameter, which returned HTTP 400
  / `-1121`. The collector now sends the required concrete `symbol` for all
  five USD-M metrics endpoints; the degraded service is stopped pending its
  one-image fix deployment and a bounded REST-tail recheck.

Completed evidence:

- Verified the immutable bootstrap reference
  `primus-historical-market-data-bootstrap-v0.1.0rc1` at
  `bdda4b28b302424a6c682893a9cc966cad59a17a`; active work remains on local
  `feat/option-ingestion` tracking `origin/feat/option-ingestion`.
- Created the dedicated, empty new-VPS runtime root
  `/srv/primus/historical-market-data/{storage,state,logs,releases}` as
  `bobby:bobby`, plus mode-0700 `secrets/`. No old-VPS runtime data was copied.
  Runtime inspection found only B0 JSON metadata; no Parquet or SQLite data.
- Host baseline: Ubuntu 22.04.5, kernel `5.15.0-185-generic`, Docker Engine
  `29.7.2`, Compose `v5.4.0`; ext4 runtime filesystem has 609 GiB available
  and 82,371,062 free inodes. NTP is synchronized and host timezone is UTC.
- Added hashed Python 3.12 reader and collector locks. At generation time:
  `requirements-collector.lock` SHA-256
  `66bfc0ff41ce012029385e6da66a845cc2e7334fdbbb804779a3995fb0a6f2f9`;
  `requirements-reader.lock` SHA-256
  `dbfac6a45518d1a439d5ce8e26cc8cd9e8e730e2d1c0911b018d2ecfd5ee0130`.
- Pinned the Linux/amd64 Python base manifest to
  `sha256:d657ab0ade19f404a6ccc883ab399540de667aff751748ce23c07330c5a89e64`.
  At commit `7c9d17bac30772bab0a2872414bd8969d407fdb7`, the shared image
  `sha256:deab1bf20c1d7ee6c01761f907dee72d7c11ab0cf6df3c7d13e4a69eeb4e6a79`
  and a `--no-cache` image
  `sha256:0a0e51ab12c9fadf8a89ec76fbd680b12001cf70197f1cc0ad4e8d261b7f20b8`
  both passed the same 29 focused B0/Deribit/preflight/parquet-loader tests.
  The differing image IDs are expected build timestamp/provenance metadata, not
  dependency resolution drift.
- Resolved Compose using the protected mode-0600 host deployment file. An
  inspection-only container was observed in `created` state, never started,
  with entrypoint gate, user `1000:1000`, and exactly the storage/state/logs
  runtime binds; it and its temporary network were removed afterward.
- Added `primus-market-data-readers` ACLs for storage read/traverse and default
  future-file access only. `thanhvuong` is the designated reader member;
  acceptance checks confirm canonical-manifest read succeeds while storage
  write and reads of `state`, `logs`, and `secrets` are denied. Bobby remains
  the runtime owner and root retains normal host-administrator authority.
- B0.5 bounded source probes are complete in the new environment. Binance made
  exactly eight sequential public checks and passed REST plus archive-listing
  coverage (spot archive from `2017-08`, USD-M archive from `2020-01`, and
  metrics/book-depth listings). VNStock returned eight FPT daily rows; the
  VNDIRECT hard gate passed with daily coverage from `2017-08-10` and recent
  1m coverage, while the sampled 2018 1m window was explicitly `no_data`.
- The Deribit API probe initially selected a thin two-sequence contract. The
  probe was corrected and regression-tested to skip candidates with fewer than
  three unique sequences. Its final bounded run selected
  `BTC-16AUG26-69000-C`, verified asc/desc ordering and inclusive sequence
  boundaries with zero out-of-range rows, and observed a clean 1--2 rps ramp.
  This is source evidence only: no canonical data was written and no backfill
  was started.
- Compose contract and clock evidence now pass. The created-but-never-started
  inspection container had only the three runtime binds and user `1000:1000`;
  host NTP reports synchronized with `Etc/UTC` and `LocalRTC=no`.
- B0.6 metadata writer/reader contract is implemented and tested. It atomically
  writes only a complete storage release manifest and fail-closes reader
  compatibility for an absent, draft, undeclared, malformed, or incompatible
  dataset contract. Public loader enforcement remains a Phase C package
  acceptance change per the runbook; the runtime manifest stays draft until a
  package tag/wheel and final dataset layout declarations are accepted.
- B0.2 capacity evidence was refreshed after image builds: ext4 mount options
  are `rw,relatime,discard,errors=remount-ro`, with 600.76 GiB free and
  82,193,707 free inodes. After the 35 GiB OS/state-log/Docker reserve, 565.76
  GiB remains for collectors; the known Deribit 9 GiB canonical plus 20 GiB
  staging/repair requirement fits. Capacity report SHA-256:
  `36e3beb9916c6e90c378c86f183d372a3dd8fe44dc5ccf1c19fa8f424af5142b`.
  It remains draft because the other three enabled dataset families have no
  bounded-seed measurements and no owner approval exists.
- B0.7 now has a read-only, strict operator status command for disk/inodes,
  planned heartbeat freshness, and exit/retry/validation/RSS/backup alert
  evidence. Its new-VPS baseline passes disk and inode thresholds but correctly
  blocks absent heartbeat and alert evidence; no scheduled writer was started
  merely to make the check green.
- B0.3 reproducible reader packaging is implemented and evidenced. The
  code-only `primus-historical-market-data` wheel exposes stable
  `primus.historical_market_data` objects that are the same implementations as
  legacy `data_loader`, including `check_val=True`. The wheel contains only the
  loader/namespace code, never collector runtime, storage, state, logs, tests,
  or secrets. `requirements-build.lock` is hash-pinned at
  `fba8185537cb6167f08dea0293d6716e4f1e4c8dd16caf430d93aa1b0944210c`;
  `SOURCE_DATE_EPOCH` is derived from the commit and two independent clean
  builds are byte-identical. The release wheel clean-installs with the reader
  lock in a Python 3.12 environment with no source checkout on `PYTHONPATH`.
  The runtime manifest is intentionally `draft`: B0 validates complete build
  evidence, while Phase C alone promotes it after bounded sample-data parity.

Current B0 blockers (not bypassed): capacity/concurrency requires bounded-seed
measurements plus approval; off-host backup and restore drill are deferred by
the owner but remain required by the current runbook exit criteria;
monitoring/alert activation is pending; and no approved release/data root
exists yet from which to designate a single-root consumer rollback path. The
strict preflight currently passes 7 of 11 checks; Phase C package tag and
sample-data parity begin only after these B0 blocks are resolved.

Safety result: no collector, source ingestion, `discover`, `sync-once`, pilot,
or historical backfill was started during this work.

### 2026-08-13 UTC — B0 Closure And Phase C Reader Package Acceptance

- B0 final operational state is `pass_with_accepted_waivers`. The explicitly
  deferred controls remain waived and do not permit consumer cutover or
  destructive action. The seven non-Deribit B0 live tails and read-only
  Discord monitor have fresh successful cycles.
- Phase C package acceptance passed for the bounded new-VPS sample. The
  namespaced wheel and legacy loader returned identical schema, dtypes, order,
  and values; `check_val=True` remained the default. A reader-group container
  could read canonical data but could not create, overwrite, or delete a
  Parquet file. The runtime release manifest records that bounded-sample
  acceptance; no consumer cutover is implied.

### 2026-08-13 UTC — Phase D Initial Controlled Rebuild Authorized

- Owner approved the initial Phase D source-rebuild scope: BTCUSDT Binance
  USD-M perpetual 1m only, clean new-VPS upstream data only, no Deribit.
- The reviewed one-shot service
  `phase-d-binance-usdm-perpetual-1m` is exact-command gated and runs detached
  with one CPU, 1536 MiB, and 256 PIDs. It uses completed monthly Vision
  archives, a bounded daily bridge, and REST windows capped at 10,080 minutes;
  each chunk is appended/deduplicated atomically before memory is released.
- The job writes an isolated host-visible log plus durable Phase D state and a
  streaming per-partition quality report. It must report zero duplicate/OHLC/
  negative errors, no unexplained continuity gaps, and a current tail before
  this dataset moves to Phase E acceptance. Other historical source families,
  matrix work, and Deribit remain out of scope until separately gated.

#### Phase D execution — BTCUSDT USD-M perpetual 1m

- Commit `4e5946e` introduced the exact-command service; follow-up commit
  `bae778e` added the durable standalone audit record. Both focused test runs
  passed (20 then 21 tests).
- The initial detached run exited `0` at `2026-08-13T16:15:04Z`: 79 monthly
  Vision archives wrote 3,461,760 input rows, 12 daily bridge files wrote
  17,280, and six bounded REST windows processed 50,401. The follow-up
  idempotent run skipped all 79 archives and all 35 complete bridge days,
  then refreshed only the bounded REST overlap.
- Final durable audit at
  `state/audits/crypto_binance_futures_1m_BTCUSDT_phase_d.json`: 3,480,016
  canonical rows, 80 partitions, `2020-01-01T00:00:00Z` through
  `2026-08-13T16:15:00Z`, zero duplicate/OHLC/negative rows, zero continuity
  gaps, and one-minute closed-candle tail lag. Status: `pass`.

#### Phase D execution — BTCUSDT spot 1m

- Commits `c9adf3a` and `3cf9e18` add the exact-command, one-worker service
  and ensure that both audit and futures-proxy repair operate one partition or
  one gap range at a time. The first run was stopped before the former
  whole-history proxy branch could write its repair result; partition writes
  are atomic. The resumed run reused completed archives and remained below
  229 MiB of its 1536 MiB limit. The focused 24-test collector/entrypoint/
  manifest suite passed before resumption.
- The detached job exited `0` at `2026-08-13T16:35:44Z`. Its durable audit at
  `state/audits/crypto_binance_spot_1m_BTCUSDT.json` reports 4,525,494
  canonical rows from `2018-01-01T00:00:00Z` through
  `2026-08-13T16:33:00Z`, with zero duplicate, OHLC-invalid, and negative
  rows.
- Repair rechecked each continuity gap through the configured daily Vision
  and REST sources. It filled 2,325 later gaps only where local USD-M Futures
  covered every minute, using the approved provenance
  `binance_usdm_futures_proxy_gap_fill`. The 16 remaining gaps are known
  Binance spot-provider absences in 2018--2019, before that local futures
  coverage begins; they are listed in `BINANCE_SPOT_1M.md` and the audit.
  No synthetic or forward-filled candles were written. Result status:
  `pass_with_documented_source_gaps`, not a strict all-minute continuity pass.
- No matrix, default-universe expansion, metrics/quarterly historical batch,
  VN historical batch, or Deribit work starts from this result automatically.
  The next Phase D source must receive its own exact-command gate and focused
  validation first.

#### Phase D execution — VNDIRECT `VN30F1M` daily continuous alias

- Commit `05ebef5` introduced the exact one-shot service. It fetches the raw
  VNDIRECT DChart daily continuous alias in one-year provider windows from
  `2017-08-10`, keeps matrix and contract-derived outputs disabled, and
  streams the audit across persisted partitions. A current-day daily bar is
  rejected until after the VN market-close buffer.
- The first source run wrote the complete provider result but correctly failed
  its strict 2024+ calendar assertion. The only four apparent holes were
  verified HNX exchange closures: `2024-04-29`, `2024-09-03`, `2025-05-02`,
  and `2026-01-02`. Commit `879fe81` records those exchange holidays and
  `3336bf2` ensures a retry selects the current reviewed image rather than an
  exited prior one-shot image. The focused 27-test VNDIRECT/gate/Compose/
  manifest suite passed.
- The corrected detached job exited `0` at `2026-08-13T16:56:16Z`. Durable
  audit `state/audits/vn30f1m_vndirect_dchart_1d_phase_d.json` reports 2,250
  canonical rows in 10 partitions from `2017-08-10T00:00:00` through
  `2026-08-13T00:00:00`, with zero duplicate, invalid-time/numeric, OHLC,
  negative, weekend, source-provenance, or symbol-provenance errors, and zero
  missing trading days from the supported 2024 calendar onward. Status:
  `pass`.

#### Phase D execution — BTCUSDT USD-M futures metrics 5m

- Commit `4f5a9b1` fixes a real source-normalization bug: multiple raw Vision
  observations that floor into one 5-minute bucket now coalesce each metric
  from the last non-null direct observation rather than losing a valid field
  through blind last-row dedupe. Coverage discovery/audit scan one partition
  at a time, and a partial REST response cannot overwrite a complete Vision
  row. The focused suite passed 32 tests in the reviewed image.
- The first full run downloaded the source history and correctly failed
  closed because its original strict audit interpreted all nullable metric
  fields as malformed data. Direct read-only checks of Binance Vision archives
  proved that the sparse fields are upstream availability: for example,
  `2022-01-01` has all four long/short ratio fields absent in the original
  archive. No source value was filled or invented.
- The targeted detached recheck exited `0` at `2026-08-13T17:42:04Z` after
  re-fetching days with short coverage or nullable fields. Durable audit
  `state/audits/crypto_binance_futures_metrics_5m_BTCUSDT_phase_d.json`:
  625,109 rows, `2020-09-01T00:00:00` through
  `2026-08-13T17:30:00`, zero duplicate/time/bucket/malformed-numeric/
  negative/market/contract/symbol/source-provenance errors. It records 160
  direct-source gaps, 75 short days, and 92,275 nullable metric rows
  (92,272 Vision; 3 bounded REST) rather than concealing them. Status:
  `pass_with_documented_source_gaps`.
- A network-off, read-only packaged-loader smoke with `check_val=True` read
  the canonical BTCUSDT sample with the complete schema and zero duplicate
  keys. The next source remains a separately gated concrete quarterly rebuild;
  no matrix, broad universe, or Deribit work follows automatically.

#### Phase D execution — BTCUSDT USD-M concrete quarterly 1m

- The reviewed exact-command collector was introduced in `42e64d8`; follow-up
  commits `d61ce68`, `cea612e`, and `842b118` narrowed archive discovery to the
  configured history and hardened active-tail coverage. The focused image
  suite passed 36 tests and the protected compose contract validated before
  the final rerun.
- The first archive rebuild correctly failed closed. It exposed two real
  validator assumptions rather than corrupt data: some direct Binance Vision
  contract archives contain timestamps after the date encoded in the symbol,
  and an existing partial active-day tail must not count as a complete daily
  archive. Direct provider rows are preserved and recorded as
  `after_symbol_date_rows`; they are not discarded or re-labelled.
- The final detached rerun exited `0` at `2026-08-13T18:04:16Z`. It used
  monthly/direct daily Vision data plus a bounded seven-day REST bridge only
  for active contracts, accepting a daily date only after 1,440 unique UTC
  minutes. No synthetic candles or cross-contract continuity were created.
- Durable audits under
  `state/audits/crypto_binance_usdm_quarterly_1m_*_phase_d.json` report 24/24
  contracts `pass`, 4,711,125 rows from `2021-02-03T08:20:00Z` to
  `2026-08-13T18:03:00Z`, and zero duplicate, invalid-time/numeric, OHLC,
  negative, source/symbol mismatch, ordering, or continuity-gap rows. Both
  active contracts have zero tail lag. A network-off, read-only runtime-loader
  smoke with `check_val=True` read the repaired `BTCUSDT_260925` range with
  the full schema, zero duplicate keys, and no null OHLCV values.
- This completes all five sources in the currently approved Phase D scope.
  No matrix, universe expansion, consumer cutover, destructive action, or
  Deribit work follows automatically.

## Active Job: VN30F1M VNDIRECT DChart Single-Source Upgrade

Source guide: `VN30_FUTURES_FREE_DATA_UPGRADE_PLAN_V2.md`

Status: Phase 1 complete; Phase 2 daily subphase complete; Phase 2 1m pending

Branch: `dev`

### Current Direction

The source guide was updated to a single-source design. The active implementation now uses only:

- provider: `vndirect_dchart`;
- symbol: `VN30F1M`;
- endpoint: `https://dchart-api.vndirect.com.vn/dchart/history`;
- continuous alias only, not individual contracts.

The following providers are out of scope for this task:

- KBS;
- DNSE;
- Vietstock;
- TradingView;
- XNO / `xnoapi`.

Existing KBS/DNSE/Vietstock/TradingView code may remain as historical scaffolding, but it must not be used by the active DChart service or promoted by this task.

### Phase 1 — Provider And Hard-Gated Live Probe

Goal:

- Prove early whether VNDIRECT DChart returns real usable `VN30F1M` data.
- Stop immediately if recent `1m` or daily data is not positive.
- Do not backfill, publish, compact, or update matrix in Phase 1.

Scope:

- Add `collectors/providers/vndirect_dchart_derivatives.py`.
- Add `python -m collectors.vn_derivatives probe-vndirect`.
- Add Docker one-shot `vn30f1m-vndirect-probe`.
- Write `state/vn_derivatives/vndirect_dchart_probe.json`.
- Probe exactly:
  - recent `1m`: now minus 5 calendar days to now;
  - old `1m`: `2018-08-01` to `2018-09-01`;
  - daily: `2017-08-10` to now.
- Normalize DChart UDF response to:
  - `time`;
  - `open`;
  - `high`;
  - `low`;
  - `close`;
  - `volume`;
  - `source`;
  - `source_symbol`;
  - `quality_flags`;
  - `ingested_at`.
- Validate row invariants:
  - unique/increasing `time`;
  - finite OHLCV;
  - `high >= open/close/low`;
  - `low <= open/close/high`;
  - `volume >= 0`.

Hard gate:

- recent `1m` must have `status=success` and `row_count > 100`.
- daily must have `status=success` and `row_count > 500`.
- old `1m` may be `success` or `no_data`.
- HTTP error, invalid JSON, missing fields, array length mismatch, rate limit, or network error must not be converted to `no_data`.
- CLI exits non-zero when `production_gate=FAIL`.

Phase 1 tests:

- Unit-test provider status handling:
  - `s=ok`;
  - `s=no_data`;
  - HTTP 429;
  - HTTP 500;
  - invalid JSON;
  - missing fields;
  - mismatched array lengths;
  - invalid OHLC rows.
- Unit-test probe gate PASS/FAIL without network by mocking provider results.
- Compile new provider/CLI modules.
- Docker config includes `vn30f1m-vndirect-probe`.
- Live smoke probe runs in Docker and reports real row counts.

Phase 1 exit:

- PASS: continue to Phase 2.
- FAIL: stop and report `vndirect_dchart_probe.json`; do not backfill.

### Phase 2 — Backfill, Storage, Validation, Service, Matrix

Starts only after Phase 1 PASS.

Scope:

- Add `backfill-vndirect`.
- `1m` backfill:
  - window 31 days;
  - discover earliest real bar by scanning back to `2017-08-10`;
  - split suspicious/truncated windows `31 -> 14 -> 7 days`.
- `1d` backfill:
  - yearly windows from `2017-08-10` to now.
- Storage:
  - `storage/vn/futures/continuous/1m/symbol=VN30F1M/source=vndirect_dchart/version=v1/year=YYYY/month=MM/part.parquet`;
  - `storage/vn/futures/continuous/1d/symbol=VN30F1M/source=vndirect_dchart/version=v1/year=YYYY/part.parquet`.
- Manifest resume:
  - `state/vn_derivatives/vndirect_dchart_1m.json`;
  - `state/vn_derivatives/vndirect_dchart_1d.json`.
- Atomic write per affected partition.
- Validate storage and write overlap report:
  - `state/vn_derivatives/vndirect_overlap_report.json`.
- Add active service:
  - `vn30f1m-vndirect`;
  - schedule `16:30 Asia/Ho_Chi_Minh`;
  - daily subphase syncs latest daily, validates, updates matrix;
  - `1m` remains intentionally disabled until the later 1m subphase.
- After overlap/parity pass, `VNDailyMatrix` may source `VN30F1M` from DChart daily.

### Phase 1 Implementation Log

#### 2026-07-30 UTC

Status: complete

Changed:

- Add DChart provider.
- Add hard-gated probe CLI.
- Add Docker one-shot.
- Add unit tests and live Docker smoke.

Files:

- `collectors/providers/vndirect_dchart_derivatives.py`;
- `collectors/vn_derivatives/vndirect.py`;
- `collectors/vn_derivatives/__main__.py`;
- `docker-compose.yml`;
- `tests/test_vndirect_dchart_phase1.py`;
- `README.md`.

Live hard-gate result:

- Host command:
  - `/root/bobby/pool_alpha/.venv/bin/python -m collectors.vn_derivatives probe-vndirect --json`;
  - `production_gate=PASS`;
  - recent `1m`: `status=success`, `row_count=849`, `first_bar=2026-07-27 09:00:00+07:00`, `last_bar=2026-07-30 11:05:00+07:00`;
  - old `1m`: `status=no_data`, `row_count=0`;
  - daily: `status=success`, `row_count=2240`, `first_bar=2017-08-10 07:00:00+07:00`, `last_bar=2026-07-30 07:00:00+07:00`.
- Docker command:
  - `docker compose --profile vn-derivatives run --rm --no-deps vn30f1m-vndirect-probe`;
  - `production_gate=PASS`;
  - recent `1m`: `status=success`, `row_count=850`, `first_bar=2026-07-27 09:00:00+07:00`, `last_bar=2026-07-30 11:06:00+07:00`;
  - old `1m`: `status=no_data`, `row_count=0`;
  - daily: `status=success`, `row_count=2240`, `first_bar=2017-08-10 07:00:00+07:00`, `last_bar=2026-07-30 07:00:00+07:00`.
- Report path:
  - `state/vn_derivatives/vndirect_dchart_probe.json`.

Validation:

- `/root/bobby/pool_alpha/.venv/bin/python -m unittest tests.test_vndirect_dchart_phase1`: OK, 7 tests.
- `/root/bobby/pool_alpha/.venv/bin/python -m unittest tests.test_vndirect_dchart_phase1 tests.test_vn_derivatives_phase1 tests.test_vn_derivatives_phase2 tests.test_vn_derivatives_phase3`: OK, 24 tests.
- `/root/bobby/pool_alpha/.venv/bin/python -m compileall collectors/providers/vndirect_dchart_derivatives.py collectors/vn_derivatives/vndirect.py collectors/vn_derivatives/__main__.py`: OK.
- `docker compose --profile vn-derivatives config --services`: OK, includes `vn30f1m-vndirect-probe`.
- `docker compose build vn30f1m-vndirect-probe`: OK.
- `git diff --check`: OK.

Decision:

- Phase 1 PASS. It is reasonable to proceed to Phase 2 backfill/storage work.
- The old multi-source proof remains obsolete for this active task.

### Phase 2 Daily Implementation Log

#### 2026-07-30 UTC

Status: daily subphase complete; `1m` not called.

Changed:

- Added daily VNDIRECT DChart sync under `collectors/vn_derivatives/vndirect.py`.
- Added CLI:
  - `python -m collectors.vn_derivatives sync-vndirect --resolution 1d --mode once --update-matrix --json`.
- Added Docker live service:
  - `vn30f1m-vndirect`;
  - schedule `16:30 Asia/Ho_Chi_Minh`;
  - command only supports `--resolution 1d` in this subphase.
- Added source-partitioned daily storage:
  - `storage/vn/futures/continuous/1d/symbol=VN30F1M/source=vndirect_dchart/version=v1/year=YYYY/part.parquet`.
- Added manifest:
  - `state/vn_derivatives/vndirect_dchart_1d.json`.
- Updated loader/matrix path resolution so source-partitioned continuous daily is preferred over legacy `symbol/version` path.

Live one-shot result:

- Docker command:
  - `docker compose --profile vn-derivatives run --rm --no-deps vn30f1m-vndirect python -m collectors.vn_derivatives sync-vndirect --resolution 1d --mode once --update-matrix --json`;
  - status `ok`;
  - positive windows `10`;
  - rows written `2240`;
  - first stored date `2017-08-10 00:00:00`;
  - latest stored date `2026-07-30 00:00:00`;
  - matrix auxiliary source `VN30F1M -> storage/vn/futures/continuous/1d`.

Validation:

- Source-partition files: `10`.
- Source-partition rows: `2240`.
- Duplicate `symbol,time`: `0`.
- Weekend rows: `0`.
- Source values: `vndirect_dchart`.
- `VnDerivativesContinuousDaily().load(symbols="VN30F1M")`: `2240` rows from `2017-08-10` to `2026-07-30`.
- `VNDailyMatrix().load("close", symbols=["VN30F1M"])`: first valid `2017-08-10`, last valid `2026-07-30`.
- Disk:
  - DChart daily partition: `196K`;
  - DChart manifest: `20K`;
  - VN daily matrix: `13M`.

Tests:

- `/root/bobby/pool_alpha/.venv/bin/python -m unittest tests.test_vndirect_dchart_phase1`: OK, 9 tests.
- `/root/bobby/pool_alpha/.venv/bin/python -m unittest tests.test_vndirect_dchart_phase1 tests.test_vn_derivatives_phase3 tests.test_vn_daily_universe`: OK, 19 tests.
- compileall relevant modules: OK.

Decision:

- Daily DChart source is usable now.
- Do not call or backfill `1m` yet; this remains the next subphase.

## Superseded Job: VN30 Futures Free Data Upgrade V2 Multi-Source Proof

Status: obsolete after guide update to VNDIRECT DChart single-source.

The section below is retained as historical context only. Do not use it as the active implementation scope for the current VN30F1M task.

Context:

- V1 KBS/DNSE-first implementation built useful storage/roll/service scaffolding, but live proof showed the data source assumption was not valid enough:
  - production `provider_probe_v1` has no positive bars;
  - KBS/DNSE concrete futures must be demoted to opportunistic fallback until positive proof exists;
  - no HTTP error may be interpreted as confirmed empty;
  - no provider may be promoted without real returned bars and validation.
- V2 switches to a free-first strategy:
  - public/free web sources first; after user review, `xnoapi` is not part of default Phase 1 because the quant package path requires API key;
  - Vietstock for individual daily contracts if public extraction proves full coverage;
  - TradingView only for public-accessible validation/fill;
  - KBS/DNSE only after positive bars.
- V1 guide has been moved by the user to `rebuild_derivatives_vn30f1m_v1.md`; keep it as historical context only.

### Goal

Deliver trustworthy VN30 futures data with staged promotion:

- `VN30F1M_FREE`: longest validated free continuous daily, usable as research proxy/regime/hedge input.
- `VN30F1M_CONTRACT`: daily continuous rebuilt from individual contracts, usable for contract-aware accounting after Vietstock daily proof.
- `VN30F1M_TRADE`: tradable roll policy built from contract daily volume without look-ahead.
- `VN30F1M` convenience alias is not promoted until quality gates pass.
- Continuous `1m` is best-effort and must start only at the earliest real positive bar.

### Phase 1 — Source Proof, Typed Provider Registry, Hard Gates

Scope:

- Replace KBS/DNSE-only probe assumptions with typed provider results:
  - `success`;
  - `empty_confirmed`;
  - `unsupported_symbol`;
  - `invalid_request`;
  - `auth_error`;
  - `rate_limited`;
  - `blocked`;
  - `schema_error`;
  - `unknown_error`.
- Add provider registry:
  - `collectors/providers/vietstock_derivatives.py`;
  - `collectors/providers/tradingview_derivatives.py`;
  - existing KBS/DNSE wrapped into the same result contract.
- Add V2 source-gate modules:
  - `collectors/vn_derivatives/provider_registry.py`;
  - `collectors/vn_derivatives/source_gates.py`;
  - `collectors/vn_derivatives/web_cache.py`;
  - optional parser helpers for Vietstock/TradingView.
- Add CLI:
  - `python -m collectors.vn_derivatives probe-free-sources`;
  - optional provider filters and sample-contract filters;
  - JSON summary output.
- Add Docker one-shot service:
  - `vn-derivatives-source-probe`;
  - profile/bootstrap only;
  - no canonical publish.
- Do not probe `xnoapi` by default:
  - user update: the quant package route needs API key;
  - keep V2 focused on truly public/free web extraction unless a no-key endpoint is proven later.
- Probe Vietstock:
  - sample contracts `VN30F1709`, `VN30F2003`, `VN30F2406`, `VN30F2508`, current active contract;
  - public overview/statistics/XHR discovery;
  - cache public responses;
  - no CAPTCHA/login/paywall bypass.
- Probe TradingView:
  - public-access only;
  - continuous `HNX:VN301!`;
  - daily and 1m availability samples;
  - classify blocked if login/CAPTCHA/private controls are needed.
- Probe KBS/DNSE only as fallback candidates:
  - one old expired;
  - one pre-KRX;
  - one post-KRX;
  - one active;
  - legacy and KRX symbols.
- Write state:
  - `state/vn_derivatives/source_probe_v2.parquet`;
  - `state/vn_derivatives/source_probe_v2.json`;
  - `state/vn_derivatives/source_status.json`;
  - provider-specific probe reports as needed.

Hard gates:

- `expected_request_count == actual_request_count`, otherwise fail exit.
- `positive_request_count == 0` blocks Phase 2 publish.
- HTTP 400/403/429/5xx are not `empty_confirmed`.
- `empty_confirmed` only means request succeeded, schema parsed, and bars list was actually empty.
- Provider status must be one of:
  - `UNVERIFIED`;
  - `POSITIVE_PARTIAL`;
  - `VALIDATED`;
  - `DISABLED`.

Phase 1 tests:

- Provider result status classification, especially HTTP 400 not empty.
- Public web OHLCV normalization for `1m` and `1D` style sample frames.
- Vietstock HTML/XHR parser with fixture pages and pagination fixtures.
- TradingView parser/normalizer with public-response fixtures only.
- Source-gate JSON/Parquet summary counts.
- Fail exit when request count mismatches.
- Fail gate when no provider has positive bars.
- Docker config includes `vn-derivatives-source-probe`.

Exit criteria:

- At least one provider has positive real bars before any Phase 2 publish.
- Source status tells exactly which providers are allowed for daily, continuous `1m`, validation, or disabled fallback.
- Existing V1 storage is not overwritten.

### Phase 2 — Publish Free Daily, Contract Daily, Continuous 1m, Matrix And Services

Scope:

- Publish `VN30F1M_FREE` daily if a no-key public continuous daily source passes Gate A:
  - storage: `storage/vn/futures/continuous/1d/symbol=VN30F1M_FREE/version=v2_free/...`;
  - row provenance: `source`, `source_symbol`, `quality_flags`, `ingested_at`;
  - source priority determined by Phase 1 status, not hard-coded before proof.
- Backfill individual daily contracts if Vietstock passes Gate B:
  - storage: `storage/vn/futures/contracts/1d/symbol=VN30FYYMM/year=YYYY/part.parquet`;
  - schema includes `open_interest` and `settlement_price` as nullable fields;
  - process per contract, atomic writes, resume manifest, no all-contract concat.
- Build contract-rebuilt daily continuous:
  - `VN30F1M_CONTRACT`;
  - `VN30F1M_TRADE`;
  - shared roll table at `storage/vn/futures/rolls/version=v2_free/rolls.parquet` or compatible v1 path only after migration decision;
  - no look-ahead volume.
- Publish continuous `1m` if Gate C passes:
  - storage: `storage/vn/futures/continuous/1m/symbol=VN30F1M/version=v2_free/...`;
  - no forward fill;
  - no averaged OHLC;
  - secondary sources fill exact missing timestamps only;
  - provenance per row.
- Add alias/free-series modules:
  - `collectors/vn_derivatives/alias_series.py`;
  - `collectors/vn_derivatives/parity.py`.
- Add CLI:
  - `backfill-daily-free`;
  - `backfill-alias-1m`;
  - `build-free-continuous`;
  - `validate-free-continuous`;
  - `update-matrix-v2`.
- Add Docker services:
  - `vn-derivatives-daily-backfill`;
  - `vn-derivatives-1m-backfill`;
  - updated `vn-derivatives` live flow only after source status has validated providers.
- Update loaders without breaking current endpoints:
  - add explicit loader/router aliases for `VN30F1M_FREE`, `VN30F1M_CONTRACT`, `VN30F1M_PROVIDER`;
  - keep default OHLCV projection;
  - `columns="full"` exposes provenance/roll metadata.
- Update `VNDailyMatrix` integration:
  - add futures auxiliary candidates `VN30F1M_FREE`, `VN30F1M_CONTRACT`, `VN30F1M_PROVIDER`;
  - all futures `eligible_for_equity_ranking=false`;
  - only promote convenience `VN30F1M` after Gate E.

Publish gates:

- Gate A: free continuous daily ready:
  - at least one free continuous provider positive;
  - first bar materially earlier than 2024;
  - daily validation pass.
- Gate B: individual daily ready:
  - Vietstock full extraction positive on at least 4/5 sample contracts;
  - contract rows positive;
  - OHLC/date-boundary validation pass.
- Gate C: continuous `1m` ready:
  - at least one free source returns >1000 real bars;
  - first bar earlier than existing 2024 coverage;
  - timezone/session validated.
- Gate D: contract-rebuilt continuous ready:
  - roll table complete;
  - no look-ahead;
  - parity against free/provider continuous pass where overlap exists.
- Gate E: matrix promotion:
  - quality status pass;
  - source provenance present;
  - non-null rows extend existing series.

Phase 2 tests:

- Daily free series merge priority and exact missing-date fill.
- Individual daily contract backfill resume/dedupe.
- Contract daily validation including listing/final trading date boundaries.
- Roll table calendar and tradable no-lookahead behavior.
- Continuous `1m` duplicate/session/timezone validation.
- Daily aggregation from `1m` only when completeness threshold passes.
- Parity reports against existing provider series.
- Matrix auxiliary metadata excludes futures from equity ranking.
- Loader endpoints return stable old-style OHLCV DataFrames by default.
- Docker smoke for each new service using `/tmp` storage.

Exit criteria:

- Minimum success is acceptable:
  - `VN30F1M_FREE` daily extends materially before 2024 and validates;
  - contract daily backfill is progressive and reports coverage;
  - continuous `1m` remains best-effort if no free source passes.
- No provider is promoted from V2 without positive bars.
- No private-access scraping/bypass is used.

### V2 Implementation Logs

#### 2026-07-29 UTC — V2 Planning

Status: planned

Decisions:

- Supersede the V1 KBS/DNSE-first plan with the free-first V2 plan.
- Keep V1 code as scaffolding where useful, but source proof and promotion now follow V2 hard gates.
- Split V2 into two executable phases:
  - Phase 1 proves sources and writes status gates only;
  - Phase 2 publishes validated datasets and updates matrix/services.

#### 2026-07-29 UTC — V2 Phase 1 Implementation

Status: complete

Changed:

- Added typed source proof primitives in `collectors/vn_derivatives/source_gates.py`:
  - `ProviderStatus`;
  - `ProviderFetchResult`;
  - OHLCV normalization;
  - validation metrics;
  - HTTP status classification;
  - provider quality summary.
- Added provider adapters:
  - `collectors/providers/vietstock_derivatives.py`;
  - `collectors/providers/tradingview_derivatives.py`.
- Added web cache helper:
  - `collectors/vn_derivatives/web_cache.py`;
  - public GET only;
  - local cache under `state/vn_derivatives/web_cache`;
  - project-identifying user-agent.
- Added V2 provider registry/orchestrator:
  - `collectors/vn_derivatives/provider_registry.py`;
  - source probe plan;
  - Vietstock/TradingView/KBS/DNSE dispatch;
  - hard gate summary;
  - writes `source_probe_v2.parquet`, `source_probe_v2.json`, `source_status.json`.
- Added CLI:
  - `python -m collectors.vn_derivatives probe-free-sources`.
- Added Docker one-shot:
  - `vn-derivatives-source-probe`.
- Updated README with V2 source proof command, state files, and hard-gate semantics.
- Added `tests/test_vn_derivatives_v2_phase1.py`.

Decisions:

- `probe-free-sources` defaults to Vietstock/TradingView/KBS/DNSE and fails when `positive_request_count == 0`, as required by V2.
- `--no-fail-on-no-positive` exists for diagnostics/smoke only; it still writes `status=blocked` and does not promote providers.
- `xnoapi` is not installed or included in default source proof because the user confirmed the quant package path needs API key.
- TradingView Phase 1 adapter only validates public page accessibility; it does not automate private chart sessions.
- Vietstock Phase 1 adapter resolves futures symbols through the public `/search/{query}/3` route and then checks public HTML tables conservatively.
- Vietstock public `KQGDThongKeGiaPaging` XHR is documented as reachable from public page/token flow, but Phase 1 does not promote it until a contract-level OHLC endpoint/shape returns positive rows.
- A public search hit alone means symbol discovery succeeded; it is not OHLC data and therefore remains non-positive.
- KBS/DNSE are wrapped into typed results, but HTTP 400 remains `invalid_request`, never `empty_confirmed`.

Validation:

- `/root/bobby/pool_alpha/.venv/bin/python -m unittest tests.test_vn_derivatives_v2_phase1 tests.test_vn_derivatives_phase1 tests.test_vn_derivatives_phase2 tests.test_vn_derivatives_phase3`: OK, 22 tests.
- `/root/bobby/pool_alpha/.venv/bin/python -m compileall collectors/providers collectors/vn_derivatives data_loader.py`: OK.
- `docker compose --profile vn-derivatives config --services`: OK, includes `vn-derivatives-source-probe`.
- `docker compose build vn-derivatives-source-probe`: OK.
- Container check: `xnoapi_spec None`.
- Docker smoke with `--providers vietstock,tradingview --contracts VN30F2508 --no-fail-on-no-positive --json`: OK entrypoint/state write; returned `status=blocked`, `positive_request_count=0`; `VN30F2508` is not resolved by Vietstock public search.
- Docker smoke with `--providers vietstock --contracts VN30F2509 --no-fail-on-no-positive --json`: OK; `blocked_request_count=0`, `error_request_count=0`, `status=UNVERIFIED`, `positive_request_count=0`, confirming public symbol discovery works but no OHLC rows were promoted.

Technical debt / accepted limitations:

- Live external source proof has not been promoted by this phase; Phase 1 only adds the proof mechanism and Docker entrypoint.
- Actual full-provider probe can be slow or blocked by provider/network conditions; the hard gate prevents accidental publish when this happens.

## Previous Job: VN30 Futures Historical Derivatives Upgrade V1

Source guide: `rebuild_derivatives_vn30f1m_v1.md`

Status: superseded by V2 after live proof showed no positive KBS/DNSE concrete bars

Branch: `dev`

Important adaptation:

- The source guide targets a repository-level architecture, but this `_get_data` service already uses Docker services, Parquet storage, manifest/state files, and disk-first append patterns.
- This implementation keeps Parquet + zstd as the storage format.
- The existing `vn30f1m-dnse` alias service is not removed in Phase 1. It remains the provider-alias continuity source until contract-level canonical storage and continuous rebuild pass validation.
- Phase 1 must not publish rebuilt `VN30F1M` into `VNDailyMatrix`; that happens only after Phase 2-3 data trust checks pass.

### Goal

Build a trustworthy VN30 futures derivatives pipeline:

- canonical individual contracts use stable `VN30FYYMM` identity;
- KRX and legacy provider symbols are only mappings;
- KBS/vnstock is primary, DNSE is fallback/validation;
- provider coverage must be probed before full backfill;
- continuous `VN30F1M` and optional `VN30F1M_TRADE` are built from real contracts, not from rolling alias history.

### Phase 1 — Provider Probe, Symbol Resolver, Instrument Dimension

Scope:

- Add `configs/vn_derivatives.yml` with provider priority, request windows, validation tolerances, continuous settings, and storage conventions.
- Add VN30 futures symbol utilities:
  - canonical `VN30FYYMM`;
  - stable `instrument_id`;
  - legacy symbol;
  - KRX symbol conversion;
  - monthly contract calendar from `2017-08-10` to current + 6 months;
  - theoretical expiry = third Thursday, adjusted to previous VN trading day when known holidays require it.
- Add low-level provider adapters:
  - KBS/vnstock adapter for `1m` and `1d`, with explicit provider symbol input;
  - DNSE adapter with explicit `asset_type="derivative"` so concrete contracts like `VN30F2503` are not routed as stocks.
- Update existing DNSE intraday helper to recognize concrete `VN30FYYMM` and KRX symbols as derivative instruments.
- Add provider probe CLI:
  - `python -m collectors.vn_derivatives discover`;
  - `python -m collectors.vn_derivatives probe`.
- Probe writes:
  - `storage/vn/futures/instruments/version=v1/instruments.parquet`;
  - `state/vn_derivatives/provider_probe_v1.parquet`;
  - `state/vn_derivatives/provider_probe_v1.json`.
- Add Docker profile service for probe/bootstrap only.

Phase 1 tests:

- KRX conversion examples, including `VN30F2508 -> 41I1F8000`.
- Canonical symbol parsing and derivative detection for alias, legacy, and KRX forms.
- Contract calendar includes opening contracts `VN30F1708`, `VN30F1709`, `VN30F1712`, `VN30F1803`.
- Instrument dimension writes expected schema.
- Provider probe writes Parquet and JSON with fake providers, records empty success separately from errors, and summarizes earliest coverage.
- Existing `vn_intraday_dnse.fetch_ohlc` keeps backward compatibility while allowing explicit derivative asset type.

Exit criteria:

- Probe CLI can run without publishing canonical bars.
- Probe report distinguishes request error from empty response.
- Instrument dimension exists with the planned schema.
- No full historical claim is made until live provider probe confirms real coverage.
- Existing VN daily matrix and alias `VN30F1M` behavior remains stable.

### Phase 2 — Individual Contract Backfill And Validation

Scope:

- Backfill contract-level `1m` and `1d` into:
  - `storage/vn/futures/contracts/1m/symbol=VN30FYYMM/year=YYYY/month=MM/part.parquet`;
  - `storage/vn/futures/contracts/1d/symbol=VN30FYYMM/year=YYYY/part.parquet`.
- Disk-first per contract/window; no global DataFrame accumulation.
- KBS primary, DNSE fills missing timestamps only.
- Split windows when provider response appears truncated.
- Track manifests:
  - `state/vn_derivatives/contracts_1m.json`;
  - `state/vn_derivatives/contracts_1d.json`.
- Validate OHLC, duplicate keys, expiry bounds, tick-size flags, provider parity, and unresolved gaps.

Phase 2 tests:

- Window split on truncation.
- KBS primary rows are not overwritten by DNSE.
- DNSE fills only missing timestamps.
- Daily aggregate from 1m is rejected when session coverage is insufficient.
- Manifest resume does not refetch completed windows.

### Phase 3 — Continuous Series, Matrix Integration, Services

Scope:

- Build a single roll table:
  - `storage/vn/futures/rolls/version=v1/rolls.parquet`.
- Build continuous:
  - `VN30F1M` calendar front-month;
  - `VN30F1M_TRADE` liquidity-aware no-lookahead series.
- Store continuous:
  - `storage/vn/futures/continuous/1m/symbol=VN30F1M/version=v1/...`;
  - `storage/vn/futures/continuous/1d/symbol=VN30F1M/version=v1/...`.
- Keep provider alias as `VN30F1M_PROVIDER` for overlap validation.
- Update `VNDailyMatrix` to source `VN30F1M` from continuous `1d` only after parity passes.
- Add loader endpoints for contract-level and continuous futures while keeping current endpoints stable.
- Add `vn-derivatives` daily service; deprecate or migrate `vn30f1m-dnse` to validation-only.

Phase 3 tests:

- Same roll table drives both `1m` and `1d`.
- Calendar series rolls after expiry session.
- Tradable series uses only closed prior-day volume information.
- No day mixes two contracts in continuous `1m`.
- Matrix includes rebuilt `VN30F1M`, excludes it from equity ranking, and preserves loader return types.

### Implementation Logs

#### 2026-07-29 UTC — Phase 1 Implementation

Status: complete

Changed:

- Added source guide `rebuild_derivatives_vn30f1m.md`.
- Added `configs/vn_derivatives.yml`.
- Added `collectors/providers/kbs_derivatives.py`.
- Added `collectors/providers/dnse_derivatives.py`.
- Added `collectors/vn_derivatives/` package with symbol calendar, instrument dimension, provider probe, and CLI entrypoint.
- Updated `collectors/vn_intraday_dnse.py` to support explicit `asset_type` and concrete VN30 futures/KRX detection.
- Added `tests/test_vn_derivatives_phase1.py`.

Validation:

- `python -m unittest tests.test_vn_derivatives_phase1`: OK, 6 tests.
- `python -m unittest tests.test_vn_derivatives_phase1 tests.test_vn_daily_universe`: OK, 11 tests.
- `python -m compileall collectors/providers collectors/vn_derivatives collectors/vn_intraday_dnse.py`: OK.
- `docker compose --profile vn-derivatives config --services`: OK.
- Temp-env CLI discover smoke:
  - command: `python -m collectors.vn_derivatives discover --start 2017-08-10 --end 2017-10-01 --json`;
  - result: `contracts=3`, first `VN30F1708`, last `VN30F1710`;
  - output: temp `storage/vn/futures/instruments/version=v1/instruments.parquet`.
- Production discover smoke:
  - command: `python -m collectors.vn_derivatives discover --json`;
  - wrote `storage/vn/futures/instruments/version=v1/instruments.parquet`;
  - contracts: `114`;
  - first: `VN30F1708`;
  - last: `VN30F2701`.
- Live KBS-only micro-probe:
  - command: `python -m collectors.vn_derivatives probe --contracts VN30F2508 --providers kbs --window-days 2 --json`;
  - wrote `state/vn_derivatives/provider_probe_v1.parquet`;
  - wrote `state/vn_derivatives/provider_probe_v1.json`;
  - rows: `4`;
  - status: `ok`;
  - all rows were `request_success=true`, `empty_confirmed=true`, `row_count=0`.

Notes:

- DNSE was not live-probed because `DNSE_API_KEY` and `DNSE_API_SECRET_KEY` are not present in the shell environment.
- The KBS micro-probe proves request wiring and empty-response classification, but does not establish historical coverage because the selected two-day window for `VN30F2508` returned no rows. Full provider coverage must be probed across the guide's sample contracts before Phase 2 full backfill.
- `vnstock` logs show it auto-converts derivative legacy symbol `VN30F2508` to KRX `41I1F8000`; the probe still records legacy and KRX attempts separately so this behavior is visible.
- Phase 1 provider tests are offline/fake-provider tests plus one KBS-only live micro-probe. No canonical contract bars were published.

#### 2026-07-29 UTC — Phase 2 Implementation

Status: complete

Changed:

- Added `collectors/vn_derivatives/contracts.py`:
  - contract-level backfill engine;
  - `BackfillOptions`;
  - `ProviderResult`;
  - KBS primary + DNSE fallback merge;
  - windowed request planning;
  - manifest resume through `state/vn_derivatives/contracts_{1m,1d}.json`;
  - disk-first append into contract-level Parquet partitions;
  - source-priority dedupe so DNSE cannot overwrite existing KBS rows at the same `(instrument_id, time)`;
  - optional aggregate `1m -> 1d` fallback only when enough intraday bars exist.
- Added `collectors/vn_derivatives/validate.py`:
  - required schema checks;
  - OHLC invariants;
  - duplicate key checks;
  - no rows after expiry session;
  - non-negative volume;
  - tick-size warning for off-grid prices;
  - storage-wide validation report at `state/vn_derivatives/contracts_validation_v1.json`.
- Extended CLI:
  - `python -m collectors.vn_derivatives backfill`;
  - `python -m collectors.vn_derivatives validate`.
- Added Docker bootstrap services:
  - `vn-derivatives-bootstrap`;
  - `vn-derivatives-validate`.
- Updated README with Phase 2 commands and storage boundary.
- Added `tests/test_vn_derivatives_phase2.py`.
- Updated Docker dependency from `vnstock==3.3.1` to `vnstock>=4.0.4,<5` because the old image did not support `Quote(..., source="KBS")`.
- Classified DNSE concrete-contract daily HTTP 400 across `1D/D/day` as daily-endpoint unsupported/empty for Phase 2; DNSE `1m` request errors still fail-fast.

Storage contract:

- `1m`: `storage/vn/futures/contracts/1m/symbol=VN30FYYMM/year=YYYY/month=MM/part.parquet`.
- `1d`: `storage/vn/futures/contracts/1d/symbol=VN30FYYMM/year=YYYY/part.parquet`.
- Row schema:
  - `time`;
  - `instrument_id`;
  - `open`;
  - `high`;
  - `low`;
  - `close`;
  - `volume`;
  - `source`;
  - `quality_flags`;
  - `ingested_at`.

Validation:

- `python -m unittest tests.test_vn_derivatives_phase2`: OK, 5 tests.
- `python -m unittest tests.test_vn_derivatives_phase1 tests.test_vn_derivatives_phase2`: OK, 13 tests.
- `python -m unittest tests.test_vn_derivatives_phase1 tests.test_vn_derivatives_phase2 tests.test_vn_daily_universe`: OK, 17 tests.
- Phase 2 tests cover:
  - KBS primary wins on overlap;
  - DNSE fills only missing timestamps;
  - invalid OHLC is rejected;
  - contract storage writes expected partitions;
  - storage validation returns `ok`;
  - manifest resume skips completed windows;
  - provider request errors without rows do not advance manifest;
  - empty-confirmed windows may complete without writing rows.
- Docker build smoke:
  - `docker compose build vn-derivatives-bootstrap`: OK.
  - `docker compose --profile vn-derivatives config --services`: OK.
- Docker backfill smoke with container-only `/tmp` storage:
  - command: `docker compose --profile vn-derivatives run --rm -e DATA_ROOT=/tmp/vn_deriv_smoke/storage -e STATE_ROOT=/tmp/vn_deriv_smoke/state vn-derivatives-bootstrap python -m collectors.vn_derivatives backfill --symbols VN30F2508 --resolutions 1d --max-windows 1 --json`;
  - result: `status=ok`, `windows_done=1`, `rows_written=0`, `provider_errors=0`;
  - interpretation: KBS/DNSE path works in Docker; selected daily window had no rows, and empty-confirmed logic did not publish data.
- Docker validate smoke with container-only `/tmp` storage:
  - command: `docker compose --profile vn-derivatives run --rm -e DATA_ROOT=/tmp/vn_deriv_smoke/storage -e STATE_ROOT=/tmp/vn_deriv_smoke/state vn-derivatives-validate python -m collectors.vn_derivatives validate --json`;
  - result: `status=ok`, `files=0`, `rows=0`, `duplicate_keys=0`.

Notes:

- Phase 2 still does not publish continuous `VN30F1M` or update `VNDailyMatrix`.
- Full historical backfill should run through Docker `vn-derivatives-bootstrap`, after provider probe has useful coverage and credentials are available.
- Docker smoke surfaced two real production issues and they were fixed before commit:
  - old `vnstock==3.3.1` image could not use KBS source;
  - provider request errors were initially able to mark an empty window completed, now fixed to fail-fast unless both providers are empty-confirmed.

#### 2026-07-29 UTC — Phase 3 Implementation

Status: complete

Changed:

- Added `collectors/vn_derivatives/continuous.py`:
  - shared roll table builder at `storage/vn/futures/rolls/version=v1/rolls.parquet`;
  - calendar front-month `VN30F1M`;
  - liquidity-aware `VN30F1M_TRADE`;
  - no-lookahead tradable roll using only closed prior-day volume;
  - hard roll before expiry for tradable series;
  - continuous `1m` and `1d` builders from contract-level storage;
  - continuous storage validation;
  - provider alias parity report;
  - daily `sync_once` and `live` service workflow.
- Extended `collectors.vn_derivatives` CLI:
  - `build-continuous`;
  - `validate-continuous`;
  - `compare-provider`;
  - `update-matrix`;
  - `sync-once`;
  - `live`.
- Updated `VNDailyMatrix` builder:
  - `VN30F1M` auxiliary now prefers rebuilt continuous `1d`;
  - fallback remains legacy futures `1d`, then aggregate legacy `1m`, so existing systems do not hard fail before continuous backfill;
  - metadata writes `auxiliary_sources` so downstream readers know whether `VN30F1M` came from continuous or fallback.
- Added loader endpoints:
  - `VnDerivativesContracts1m`;
  - `VnDerivativesContractsDaily`;
  - `VnDerivativesContinuous1m`;
  - `VnDerivativesContinuousDaily`;
  - router aliases `vn_derivatives_contracts_1m`, `vn_derivatives_contracts_1d`, `vn_derivatives_continuous_1m`, `vn_derivatives_continuous_1d`.
- Updated Docker Compose:
  - added production service `vn-derivatives`;
  - moved legacy `vn30f1m-dnse` under profile `legacy-vn30f1m-dnse` with `restart: "no"` to avoid double-writing rolling alias data.
- Updated README landing page with Phase 3 storage, services, CLI, loader endpoint examples, and source semantics.
- Added `tests/test_vn_derivatives_phase3.py`.

Storage contract:

- Roll table: `storage/vn/futures/rolls/version=v1/rolls.parquet`.
- Continuous `1m`: `storage/vn/futures/continuous/1m/symbol=VN30F1M/version=v1/year=YYYY/month=MM/part.parquet`.
- Continuous `1d`: `storage/vn/futures/continuous/1d/symbol=VN30F1M/version=v1/year=YYYY/part.parquet`.
- Continuous row schema:
  - `time`;
  - `symbol`;
  - `open`;
  - `high`;
  - `low`;
  - `close`;
  - `volume`;
  - `active_instrument_id`;
  - `roll_flag`;
  - `roll_gap`;
  - `roll_ratio`;
  - `source`;
  - `quality_flags`;
  - `ingested_at`.

Decisions:

- `VN30F1M` is canonical rebuilt calendar front-month after continuous validation.
- `VN30F1M_TRADE` is available for execution-oriented tests, but `VNDailyMatrix` uses `VN30F1M` only.
- `VN30F1M_PROVIDER` is a semantics label for legacy/provider alias validation. The old DNSE alias service is disabled from default compose instead of being used as canonical history.
- Daily `sync-once/live` preserves existing roll-table history outside the current lookback window; it does not overwrite full historical rolls with a 45-day partial table.
- Bootstrap/backfill remains strict on provider errors. Daily `sync-once/live` uses best-effort provider handling: errored or empty 0-row windows are recorded in manifest `last_error`, are not marked `completed_windows`, and service continues with status `warning` so the next run can retry.

Validation:

- `python -m unittest tests.test_vn_derivatives_phase3`: OK, 4 tests.
- `python -m unittest tests.test_vn_derivatives_phase1 tests.test_vn_derivatives_phase2 tests.test_vn_derivatives_phase3 tests.test_vn_daily_universe`: OK, 21 tests.
- `python -m compileall collectors/providers collectors/vn_derivatives collectors/vn_daily_matrix.py data_loader.py`: OK.
- `git diff --check`: OK.
- `docker compose build vn-derivatives`: OK.
- `docker compose config --services`: OK, default compose includes `vn-derivatives` and does not include legacy `vn30f1m-dnse`.
- `docker compose --profile vn-derivatives run --rm -e DATA_ROOT=/tmp/vn_deriv_phase3_smoke/storage -e STATE_ROOT=/tmp/vn_deriv_phase3_smoke/state vn-derivatives python -m collectors.vn_derivatives build-continuous --start 2024-01-17 --end 2024-01-19 --resolutions 1d --series VN30F1M --json`: OK, `status=ok`, `rolls=2`, `rows_written=0` because smoke storage intentionally has no contract bars.
- `docker compose --profile vn-derivatives run --rm -e DATA_ROOT=/tmp/vn_deriv_phase3_smoke/storage -e STATE_ROOT=/tmp/vn_deriv_phase3_smoke/state vn-derivatives python -m collectors.vn_derivatives validate-continuous --resolutions 1d --series VN30F1M --json`: OK, `status=ok`, `files=0`, `rows=0`.
- Live service smoke on production container exposed a provider availability case: `VN30F2606`/`VN30F2607` KBS returned empty and DNSE `1m` returned HTTP 400. Patched daily mode to continue without marking failed or empty windows complete while keeping strict bootstrap behavior.
- Phase 3 tests cover:
  - calendar roll happens after expiry session;
  - `1m` continuous does not mix two contracts in one trading day;
  - `1m` and `1d` use the same roll table;
  - liquidity-aware tradable roll uses closed prior-day volume only;
  - incremental roll-table builds preserve existing history outside the current window;
  - `VNDailyMatrix` prefers rebuilt continuous `VN30F1M` over legacy alias;
  - new loader endpoint returns the same default OHLCV DataFrame shape as existing loaders.

Technical debt / accepted limitations:

- Provider parity report is informational and does not currently block matrix rebuild automatically; manual review is still recommended after the first full historical bootstrap.
- Daily sync rebuilds continuous partitions for the configured lookback window, not the entire history. Full rebuild remains available via `build-continuous --start 2017-08-10`.
- No adjusted continuous price is materialized on disk; raw OHLC, `roll_gap`, and `roll_ratio` are stored so adjusted analytics can be computed explicitly by consumers.

## Previous Job: VN Daily Universe Upgrade

Source guide: `implementation.md`

Status: complete

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

- Confirmed Phase 2 is integrated into the existing `vn-daily` live service path:
  - `collectors.vn_daily --mode live --schedule 16:30` updates raw daily;
  - then calls `build_universe_report(...)`;
  - then calls `build_matrix(...)`.
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
- Local host commands in the validation section were smoke checks only; production operation is the existing Docker service `vn-daily`.
- To deploy this branch's VN logic into the running service, rebuild/recreate `vn-daily` with Docker Compose while on this branch.

Service deployment:

- Command: `docker compose up -d --build vn-daily`.
- Result: `get_data-vn-daily-1` recreated and started.
- Runtime flow observed in Docker logs:
  - service entered live scheduled daily update;
  - fetched recent overlap window, e.g. `BID daily 2026-07-21 -> 2026-07-28`;
  - wrote rows into raw Parquet storage.
- The service will build `vn_daily_universe_report.csv.gz` and `VNDailyMatrix` after it finishes the configured equity symbol loop.
- Docker reported an orphan `deribit-option-cycle-full` container because this VN branch does not include the Deribit compose service definitions. It was intentionally not removed.
