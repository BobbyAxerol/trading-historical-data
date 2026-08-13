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
- The Binance daily matrix volume repair commit `48ffd74` and the initial
  migration runbook are already pushed on `feat/option-ingestion`.

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

## 5. Phase B0: Production Preflight And Release Controls

### B0.1. Execution Location Decision

Perform only the Git freeze, documentation, source-level tests, and migration
tag preparation on the old VPS. Do not retrofit the live old-VPS runtime paths,
ACLs, package installation, or Docker mounts during this migration.

Perform the complete B0 implementation on the new VPS after it checks out the
frozen bootstrap tag. This keeps the current live data services unchanged and
makes the new environment prove that it is self-sufficient.

Code/configuration changes created during B0 or package work on the new VPS
must still be committed and pushed to `feat/option-ingestion`. After clean-venv
package acceptance, create a second immutable package tag, for example:

```text
primus-historical-market-data-v0.1.0rc1
```

The earlier `bootstrap` tag is a migration starting point; the package tag is
the release from which broad new-VPS collectors and consumers operate.

### B0.2. Capacity, Filesystem, And Concurrency Gate

Before any collector writes, create and commit a new-VPS capacity report. It
must contain, per enabled dataset:

| Dataset | Canonical target | Peak staging/temp | State/log reserve | Source window | Max concurrent job |
| --- | ---: | ---: | ---: | --- | --- |
| Binance perpetual/spot/quarterly | measured during bounded seed | measured | measured | config-defined | one source family |
| Binance metrics/order book | measured during bounded seed | measured | measured | retention-defined | one source family |
| VN daily/derivatives | measured during bounded seed | measured | measured | provider-defined | one provider family |
| Deribit BTC options V1 | official plan target `6-9 GiB` | official plan peak requirement | SQLite/log reserve | full V1 | one cycle |

The capacity report must also record:

- filesystem type, total/free bytes, available inodes, and mount options;
- Docker image/cache budget and log retention budget;
- operating-system reserve not available to collectors;
- the largest expected compaction/repair temporary footprint;
- the selected concurrency matrix: which service families may run together and
  which are mutually exclusive.

Hard gates:

- Never start a job unless its worst-case temporary requirement plus the
  configured OS/log reserve fits in current free disk.
- Do not run two heavy historical jobs concurrently until measured RSS, I/O,
  source rate, and disk headroom prove that the capacity matrix permits it.
- Deribit must honor the free-disk requirement in its official plan and must
  stop before the configured low-water mark, not after the filesystem is full.
- Record `df -h`, `df -i`, and the capacity-report SHA in the implementation
  log before broad backfill.

### B0.3. Reproducible Build And Supply-Chain Gate

The current `python:3.12-slim` image tag and dependency ranges are not a fully
reproducible release by themselves. Before package acceptance:

1. Record Docker Engine, Docker Compose, Linux distribution, kernel, Python,
   DuckDB, PyArrow, and timezone/NTP synchronization status.
2. Resolve reader and collector dependencies in clean Python 3.12 environments
   into versioned lock artifacts. Hash-pinned install input is preferred for
   the image build and wheel test environment.
3. Pin the production Docker base image by digest, not only a mutable tag.
4. Build the shared collector image and record its image digest.
5. Build the wheel and record its filename, version, SHA256, and dependency
   lock SHA256 in a release manifest.
6. Verify a second clean build produces the same resolved versions and passes
   the same focused tests.

The release manifest must include the Git commit/tag, configuration file
hashes, image digest, wheel SHA256, dependency lock hashes, and build UTC time.

#### B0.3 Reader Wheel Procedure

The repository records the build frontend and backend in
`requirements-build.in` and the hash-pinned `requirements-build.lock`. Build
the code-only reader wheel from a committed checkout with:

```bash
tools/build_reader_wheel.sh /srv/primus/historical-market-data/releases
```

The helper uses the digest-pinned Python 3.12 builder, mounts the checkout
read-only, copies it only into the container's ephemeral `/tmp`, and writes
only the requested wheel output directory. It derives `SOURCE_DATE_EPOCH` from
the checked-out commit so two clean builds of the same commit are
byte-identical. It must not be used to run a
collector. For acceptance, independently clean-install the resulting wheel
with `requirements-reader.lock`, with no source checkout on `PYTHONPATH`, then
record the wheel filename, SHA256, lock hashes, image digest, commit, and build
UTC in the draft release manifest. A manifest becomes `pass` only after all B0
exit criteria and the sample-data parity checks in Phase C have passed.

### B0.4. Production Compose And Runtime Ownership Gate

Create a production Compose layer or equivalent deployment configuration. It
must replace the development-relative mounts in `docker-compose.yml` with a
required runtime-root variable:

```text
PRIMUS_HMD_RUNTIME_ROOT=/srv/primus/historical-market-data
```

Collector containers must mount only:

```text
${PRIMUS_HMD_RUNTIME_ROOT}/storage:/app/storage
${PRIMUS_HMD_RUNTIME_ROOT}/state:/app/state
${PRIMUS_HMD_RUNTIME_ROOT}/logs:/app/logs
```

Requirements:

- no writable runtime path inside the Git checkout;
- no source-code bind mount for collector or consumer images;
- collector UID/GID and host directory ownership are explicit and tested;
- default ACLs expose new canonical Parquet files to the reader group without
  granting write permission;
- secrets are injected from a protected host file or Docker secret, never from
  committed Compose data;
- `docker compose config` and container mount inspection prove the resolved
  paths before services start;
- long-lived services retain `restart: unless-stopped`; historical cycle jobs
  remain intentional run-to-completion jobs.

### B0.5. Source Inventory And Bootstrap Manifest Gate

Before data collection, create `state/bootstrap/source_inventory.json` on the
new VPS and commit a redacted report/template to Git. Each enabled dataset must
state:

| Field | Required value |
| --- | --- |
| dataset id and schema/layout version | canonical reader contract |
| upstream/provider and endpoint family | provenance |
| symbols/universe config hash | exact requested scope |
| requested start/end and expected first available time | historical target |
| source probe result and UTC timestamp | availability proof |
| credentials required | secret reference name only |
| rate/concurrency limit | operational bound |
| partition/repair policy | write semantics |
| expected duration/disk/RSS | capacity input |
| acceptance validator/report path | evidence location |

No broad backfill may start for a dataset whose source probe is missing,
ambiguous, rate-limited without a backoff plan, or unable to expose the desired
history. Such a dataset is marked `blocked` with evidence; it is not silently
seeded from the old VPS.

### B0.6. Storage Schema And Consumer Compatibility Gate

Add a storage release manifest under a dedicated metadata path outside data
partitions, for example:

```text
storage/_primus_metadata/release_manifest.json
```

It must record at least:

- environment id and creation UTC time;
- Git/package/image release identifiers;
- dataset IDs, canonical schema versions, partition-layout versions, and
  supported loader contract versions;
- source inventory/report references;
- schema migration policy and incompatible-version behavior.

Define and test the metadata writer/reader compatibility contract during B0.
Phase C implements the package loader check: before a dataset query it raises a
clear compatibility error when its supported reader version cannot interpret
the declared layout/schema. It must not return silently malformed data.

Each successful collector/derived-matrix publish updates the relevant dataset
manifest atomically after its own storage validation has passed.

### B0.7. Backup, Restore, Observability, And Resource Gate

Clean rebuild is not a backup strategy. Before enabling broad jobs, define and
test an off-host backup destination and retention policy for:

- canonical validated Parquet;
- release/configuration manifests;
- SQLite/checkpoint state needed to resume incomplete jobs on the new VPS;
- package wheels and release manifests.

Do not back up transient staging as a substitute for canonical data. A restore
drill must recover a small canonical partition and a checkpoint into an empty
test root, then pass its loader/validator smoke test.

Define observable alerts or at minimum an operator-visible status check for:

- free disk and inode low-water marks;
- stale heartbeat or missed scheduled cycle;
- collector/container exit with error;
- retry/rate-limit escalation;
- validation/repair failure;
- RSS above the source-specific budget;
- backup failure.

Set explicit CPU/memory constraints or a documented single-heavy-job policy.
The chosen limits must be tested against the bounded seed before full history.

### B0.8. Environment Isolation, Clock, And Cutover Gate

- Synchronize host time with NTP and record UTC/Asia-Ho-Chi-Minh timezone
  behavior before schedule tests.
- Assign a unique environment ID to the new storage manifest. Consumers may
  point to one data root per run; do not union old and new roots.
- Keep old and new scheduled writers logically isolated. Their simultaneous
  operation is allowed only because they write different hosts/roots and use
  separately budgeted upstream quotas.
- Define an explicit consumer rollback: previous wheel version plus previous
  approved data root. Do not use an ad hoc mixed-root fallback.
- Do not retire old writers until the new environment has passed the planned
  source-specific schedule and acceptance windows.

### B0.9. B0 Exit Criteria

All items below are required before Phase C package acceptance or broad data
rebuild:

- capacity report and concurrency matrix approved;
- reproducible release manifest recorded;
- production Compose resolves `/srv/primus/...` mounts and non-root ownership;
- source inventory has a passing bounded probe for every enabled source;
- storage release-manifest writer and compatibility contract are implemented
  and tested; package-side enforcement is a Phase C acceptance requirement;
- backup destination and restore drill pass;
- heartbeat/disk/error monitoring and resource policy are active;
- NTP, environment identity, and rollback path are recorded.

## 6. Phase B: New VPS Foundation

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

## 7. Phase C: Package The Reader Before Historical Backfill

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

## 8. Phase D: Clean Source Rebuild On The New VPS

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

## 9. Phase E: Data Acceptance And Cutover

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

## 10. Explicit Non-Goals

- Do not publish the package to public PyPI.
- Do not put Parquet or SQLite state in Git, a wheel, or GitHub source history.
- Do not use a localhost data API merely to avoid a source-code mount; direct
  local Parquet plus DuckDB remains the fastest same-host reader path.
- Do not replace stable loader defaults, especially `check_val=True`, during
  packaging.
- Do not use old-VPS checkpoints to skip new-VPS Deribit probe/pilot gates.

## 11. Implementation Log Template

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

## 12. Handoff Checklist For The Next Agent

- Read this runbook first.
- Read `README.md` for current service and endpoint inventory.
- Read `DERIBIT_BTC_OPTIONS_HISTORICAL_DATA_V1_OFFICIAL_PLAN.md` before any
  Deribit operation.
- Confirm the migration tag and current branch before editing.
- Treat old VPS data as reference only; do not copy it to new VPS.
- Complete and log every B0 exit criterion on the new VPS before package or
  broad historical work.
- Implement package changes as compatibility-preserving changes with clean-venv
  tests before starting expensive historical jobs.
- Resume the option roadmap at fresh new-VPS Phase 0/1 verification, then
  Phase 2 through Phase 6; do not jump directly to Phase 7.
