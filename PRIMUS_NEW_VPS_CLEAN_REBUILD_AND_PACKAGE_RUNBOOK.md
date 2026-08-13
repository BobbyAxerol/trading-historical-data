# Primus New VPS Clean Rebuild And Package Runbook

## 1. Decision Record

### Decision

Move `_get_data` to the new VPS by transferring only versioned source code,
configuration templates, tests, and documentation through Git. Rebuild all
runtime storage on the new VPS from the configured upstream sources.

Do **not** rsync, Git LFS, or otherwise migrate these runtime directories from
the old VPS:

```text
storage/
state/
logs/
```

The old VPS remains a read-only reference and rollback/audit archive until the
new VPS has passed source, storage, loader, and service acceptance checks. Do
not delete its data during this migration.

### Why Clean Rebuild Is Chosen

- The new storage is produced solely by the current collectors, normalizers,
  repair logic, Parquet writers, and schema rules.
- It avoids inheriting historical corruption, stale manifests, old SQLite
  cursor semantics, or incomplete staging files.
- It proves the current architecture can bootstrap a clean machine without
  hidden dependence on the old VPS.
- It makes source provenance, continuity reports, and validation reports belong
  to one environment and one immutable code release.

### Accepted Costs And Risks

- Initial build takes longer and consumes upstream API quota/bandwidth.
- Free providers can revise, throttle, or no longer expose some old history.
- Rows may differ legitimately from the old VPS when an upstream source has
  corrected history. Differences require audit, not automatic overwrite.
- Deribit Phase 6 work already present on the old VPS is intentionally not a
  resume checkpoint on the new VPS. The new VPS begins the Phase 6 workflow
  from fresh discovery and fresh state after its pilot gate passes again.

## 2. Scope And Freeze Boundary

### Code State

The active working branch is `feat/option-ingestion`. It already contains the
required `dev` baseline through an earlier merge, and it contains the current
Deribit option ingestion implementation.

At the time this runbook was written:

- Deribit implementation phases through Phase 5 have code, tests, and pilot
  gates implemented.
- Phase 6 Docker cycle/backfill mechanics exist, but its old-VPS historical
  output is partial and is not migrated.
- Phase 7 candidate tape and later option work remain pending.
- The Binance daily matrix volume repair commit `48ffd74` must be pushed before
  the migration checkpoint is tagged.

### Freeze Rule

Pause all new feature work after the migration checkpoint is created. This is a
code freeze, not a claim that the option plan is complete. Only migration-blocking
fixes, security fixes, or reproducibility fixes may be added before the new VPS
is accepted.

Do not merge `feat/option-ingestion` into `dev` merely to migrate. `dev` remains
the stable baseline; the new VPS checks out the frozen option branch/tag.

## 3. Names And Compatibility Contract

| Concern | Standard |
| --- | --- |
| Python distribution name | `primus-historical-market-data` |
| Python import namespace | `primus.historical_market_data` |
| Git migration tag | `primus-historical-market-data-bootstrap-v0.1.0rc1` |
| New VPS runtime root | `/srv/primus/historical-market-data` |
| Canonical data root | `/srv/primus/historical-market-data/storage` |
| Collector state root | `/srv/primus/historical-market-data/state` |
| Collector log root | `/srv/primus/historical-market-data/logs` |

The wheel contains code only. It never contains Parquet data, SQLite state,
logs, API secrets, or Docker runtime files.

The existing top-level `data_loader.py` remains a compatibility shim for a
defined deprecation period. New consumers import the namespace package:

```python
from primus.historical_market_data import CryptoBinance1m
```

The loader contract remains stable during packaging: existing loader class
names, argument defaults, `check_val=True`, return columns, timezone behavior,
and DuckDB resample behavior must not change merely because code becomes a
wheel.

## 4. Phase A: Freeze, Verify, Push, Tag

### A1. Inspect And Commit

On the old VPS, while on `feat/option-ingestion`:

1. Inspect `git status`, staged files, and recent branch history.
2. Commit every intended code, test, config, and markdown change with focused
   messages. Never stage `storage/`, `state/`, `logs/`, cache files, or secrets.
3. Run the focused regression suite for each changed collector/loader before
   committing.
4. Ensure `git diff --check` is clean.

### A2. Push Immutable Source Checkpoint

1. Push `feat/option-ingestion` to the authoritative GitHub remote.
2. Verify local `HEAD`, remote branch `HEAD`, and `git status` agree.
3. Create and push an annotated tag only after the branch is clean:

```bash
git tag -a primus-historical-market-data-bootstrap-v0.1.0rc1 \
  -m "Primus clean VPS bootstrap checkpoint"
git push origin feat/option-ingestion --tags
```

4. Record the exact commit SHA in this file's implementation log before any
   new-VPS bootstrap begins.

### A3. Freeze Evidence

The handoff must include:

- branch name and tag;
- exact commit SHA;
- `git status --short --branch` output showing no uncommitted source changes;
- focused test results;
- known pending option phases: Phase 6 fresh rebuild, Phase 7 and later.

## 5. Phase B: New VPS Foundation

### B1. Operating-System Layout

Create these owned runtime directories on the new VPS:

```text
/srv/primus/historical-market-data/
  storage/
  state/
  logs/
  releases/
```

The source checkout may live elsewhere, for example
`/srv/primus/src/trading-historical-data`. Runtime data must not be written
inside the Git checkout.

### B2. Users And ACLs

- A dedicated collector owner may write `storage`, `state`, and `logs`.
- Create group `primus-market-data-readers` for host consumers.
- Consumers receive read permission on files and traverse permission on parent
  directories in `storage` only.
- Consumers do not receive access to `state`, `logs`, `.env`, API keys, or
  write permission to storage.
- Set default ACLs so newly published Parquet partitions are readable by the
  reader group.

For Docker consumers, install the wheel into their image and bind only the data
directory as read-only. The container must use a UID/GID that is allowed by the
host ACL:

```yaml
volumes:
  - /srv/primus/historical-market-data/storage:/data:ro
environment:
  HISTORICAL_MARKET_DATA_ROOT: /data
```

A container cannot read host-local data without a filesystem mount, network
filesystem, or data service. This design removes source-code mounts, not the
necessary read-only data mount.

### B3. Secrets And Environment

- Recreate `.env` from a secure secret source; do not copy it through Git.
- Point collector containers to `/app/storage`, `/app/state`, and `/app/logs`
  through Docker mounts from the `/srv/primus/...` runtime root.
- Do not set a broad global `DATA_ROOT` for unrelated applications.
- Package readers should prefer `HISTORICAL_MARKET_DATA_ROOT`, then support
  `DATA_ROOT` as a legacy fallback during migration.

### B4. Source Checkout And Container Reproducibility

1. Clone the Git repository on the new VPS.
2. Verify the signed/annotated migration tag resolves to the expected SHA.
3. Checkout the frozen `feat/option-ingestion` commit/tag.
4. Build the shared collector image from that source state.
5. Run `docker compose config` before starting any service.

No collector is allowed to write data until the source-specific probe and
storage paths have been verified.

## 6. Phase C: Package The Reader Before Historical Backfill

### C1. Packaging Goal

Create an installable wheel named `primus-historical-market-data` so host users
can import loaders without `sys.path.append`, source checkout access, or a full
repository mount.

The initial package must export the existing reader endpoints, including:

- Binance perpetual/spot/quarterly OHLCV loaders;
- daily matrix loaders;
- VN daily and derivatives loaders already considered stable;
- Binance metrics and order-book snapshot loaders;
- Deribit loader endpoints already implemented, marked as V1/experimental in
  docs until the remaining option phases pass acceptance.

### C2. Required Package Work

1. Add package metadata/build configuration under `_get_data`; do not reuse the
   parent project's `package-mode = false` configuration.
2. Move or wrap reader code into `primus.historical_market_data` without
   changing public behavior.
3. Keep a small compatibility `data_loader.py` that re-exports existing public
   classes/functions.
4. Separate reader dependencies from optional collector dependencies where it
   materially reduces consumer environment size.
5. Exclude `storage`, `state`, `logs`, test artifacts, local secrets, and
   collector-only runtime files from wheel contents.
6. Build wheels deterministically and place release artifacts under
   `/srv/primus/historical-market-data/releases` or attach them to a private
   GitHub Release. A public PyPI upload is not required.

### C3. Package Acceptance Tests

Run these on the new VPS in a clean virtual environment with no source checkout
on `PYTHONPATH`:

1. Install only the built wheel and reader dependencies.
2. Set `HISTORICAL_MARKET_DATA_ROOT` to a small newly collected sample storage.
3. Import both interfaces:

```python
from primus.historical_market_data import CryptoBinance1m
from data_loader import CryptoBinance1m as LegacyCryptoBinance1m
```

4. Confirm `load()` and `load_resampled(..., engine="duckdb")` return the same
   schema, dtypes, sort order, and values as the pre-package interface for a
   fixed sample.
5. Confirm `check_val=True` remains the default and produces validation output
   rather than silently being disabled.
6. Confirm a reader-group user can load data but cannot create, overwrite, or
   delete a Parquet file.
7. Confirm a Docker consumer needs only the wheel and `/data:ro`, not the
   `_get_data` source directory.

Package acceptance must pass before broad historical jobs start. This limits
the chance of discovering an import/path contract break after many hours of
backfill.

## 7. Phase D: Clean Source Rebuild On The New VPS

### D1. General Rules

- Begin with empty new-VPS `storage`, `state`, and `logs` directories.
- Every source is called through its Dockerized collector/service, never a
  long raw host command.
- Use existing append, dedupe, tail/head/gap detection, retries, backoff,
  atomic write, validation, and repair logic.
- Write per-partition/chunk immediately; do not aggregate full historical
  ranges in RAM.
- Keep current-day partial candles out of daily canonical outputs.
- After a large test or backfill command, release Python/PyArrow memory and
  record any nonzero allocator retention when material.

### D2. Bootstrap Order

Bootstrap in increasing cost and dependency order:

1. Build image and run offline/unit/config tests.
2. Run bounded live source probes for every configured provider.
3. Seed small read samples required by package acceptance.
4. Start core Binance historical collectors and their validators.
5. Build Binance daily matrix only after its raw/source inputs are valid.
6. Run VN daily and VN30F1M daily services after their provider gates pass.
7. Run Binance metrics, spot, quarterly, and order-book jobs according to
   their existing source-specific plans and retention policies.
8. Start the fresh Deribit workflow only after the package and shared runtime
   foundation pass acceptance.

Do not start every expensive backfill in parallel. Respect source rate limits,
disk headroom, container RSS, and the dependency that matrix/derived outputs
must be rebuilt from validated canonical data.

### D3. Deribit Fresh Rebuild Policy

The old VPS's partial Deribit canonical data, staging parts, SQLite checkpoint,
and task cursor are not copied.

On the new VPS, execute the official plan again in this order:

```text
Phase 0/1 configuration and API probe verification
Phase 2 fresh discovery and new SQLite checkpoint
Phase 3 bounded staging/backfill capability verification
Phase 4 compact -> validate -> cleanup transaction verification
Phase 5 targeted pilot sampling and pilot_summary=status=ok
Phase 6 Docker checkpointed full backfill batches
Phase 6 compact -> validate -> repair -> compact -> validate -> cleanup
Phase 7 only after Phase 6 final acceptance
```

Phase 6 runs only through the Docker cycle service. The required operational
property is that each run logs per-task progress, writes durable staging before
advancing its fresh SQLite cursor, compacts, validates, cleans only validated
staging, then exits. Re-running the same cycle resumes from the **new VPS**
checkpoint, not from the old VPS.

The official Deribit plan remains authoritative for schemas, disk limits,
invariants, pilot gate, and Phase 7 deliverables:
`DERIBIT_BTC_OPTIONS_HISTORICAL_DATA_V1_OFFICIAL_PLAN.md`.

### D4. Runtime Service Policy

- Long-lived scheduled collectors use `restart: unless-stopped`.
- Historical one-shot/cycle jobs run detached when appropriate, produce logs,
  perform their configured validation chain, then exit normally.
- A completed one-shot container is not a failed service; re-running it starts
  the next idempotent/resumable batch on the new VPS.
- Inspect bounded log tails and manifests; do not rely on an unattended tmux
  session as the source of truth.

## 8. Phase E: Data Acceptance And Cutover

### E1. Per-Dataset Acceptance

For every source that is enabled on the new VPS, record:

- source/probe status and source date range;
- Parquet schema and partition layout;
- row count, first/last timestamp, duplicate-key count;
- continuity/gap report with listing/market-calendar exceptions documented;
- OHLCV and domain-specific invariants;
- loader endpoint smoke result using the packaged import;
- disk usage, peak RSS where material, and state/manifest status.

For data that also exists on the old VPS, compare coverage and aggregate
statistics as an audit aid. Do not demand byte-for-byte equality when upstream
source history legitimately differs.

### E2. Global Acceptance Gates

- No unresolved integrity errors in enabled canonical datasets.
- No unvalidated staging data promoted as canonical.
- Loader compatibility shim and namespaced package both pass parity tests.
- Consumer ACL permits reads and rejects writes.
- Docker collectors use the intended new runtime root and do not write inside
  the Git checkout.
- Scheduled services have successful first cycles and observable heartbeats.
- Deribit broad backfill follows its own Phase 6 acceptance gate; it is not
  required to finish before other validated datasets become usable.

### E3. Cutover

After the relevant dataset gates pass:

1. Point consumers to the new package version and new data root.
2. Keep old VPS storage read-only for an agreed retention period.
3. Disable old scheduled writers only after new service logs and heartbeats are
   verified for at least one expected schedule cycle.
4. Do not delete the old archive until an explicit separate retention decision.

## 9. Explicit Non-Goals

- Do not publish the package to public PyPI.
- Do not put Parquet or SQLite state in Git, a wheel, or GitHub source history.
- Do not use a localhost data API merely to avoid a source-code mount; direct
  local Parquet plus DuckDB remains the fastest same-host reader path.
- Do not replace stable loader defaults, especially `check_val=True`, during
  packaging.
- Do not use old-VPS checkpoints to skip new-VPS Deribit probe/pilot gates.

## 10. Implementation Log Template

Append a dated entry after each completed migration phase:

```markdown
### YYYY-MM-DD UTC - Phase <name>

Status: planned | in_progress | complete | blocked

Code/tag:
- branch:
- commit:
- tag:

Changed:
-

Commands/services:
-

Validation:
- tests:
- source/probe:
- storage/schema/continuity:
- package/ACL:

Result:
-

Decision/next gate:
-
```

## 11. Handoff Checklist For The Next Agent

- Read this runbook first.
- Read `README.md` for current service and endpoint inventory.
- Read `DERIBIT_BTC_OPTIONS_HISTORICAL_DATA_V1_OFFICIAL_PLAN.md` before any
  Deribit operation.
- Confirm the migration tag and current branch before editing.
- Treat old VPS data as reference only; do not copy it to new VPS.
- Implement package changes as compatibility-preserving changes with clean-venv
  tests before starting expensive historical jobs.
- Resume the option roadmap at fresh new-VPS Phase 0/1 verification, then
  Phase 2 through Phase 6; do not jump directly to Phase 7.
