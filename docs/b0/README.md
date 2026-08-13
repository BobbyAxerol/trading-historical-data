# Phase B0 Evidence

Phase B0 is a release gate, not permission to start collectors. Its runtime
evidence is intentionally outside Git under:

```text
/srv/primus/historical-market-data/state/bootstrap/
/srv/primus/historical-market-data/storage/_primus_metadata/
```

The Git-tracked source inventory template is redacted. Copy it only into the
new runtime state root, fill it with probe results and resource measurements,
and never add secrets or raw data to Git.

Run the non-collector status command after the host runtime directories and
protected deployment environment file exist:

```bash
python -m collectors.production_preflight status --strict --json
```

It reports blockers for missing runtime ownership, capacity measurements,
source probes, storage manifest, backup/restore evidence, and rollback data.
`--strict` exits non-zero while a hard B0 gate is incomplete. A result of
`pass_with_accepted_waivers` remains explicit about any owner-approved,
time-bounded technical debt; it never relabels that debt as a passing control.

The protected Compose environment file must define the variables documented in
`.env.example`. `PRIMUS_HMD_PYTHON_BASE_IMAGE` must be a Docker image reference
pinned by `@sha256:<digest>`; `PRIMUS_HMD_IMAGE_REF` must not use `latest`.
`PRIMUS_HMD_SECRETS_FILE` is a separate mode-0600 host file and is never
committed.

## Build lock workflow

`requirements-collector.lock` and `requirements-reader.lock` are resolved with
Python 3.12 and contain hashes. The production Dockerfile installs the collector
lock with `--require-hashes --only-binary=:all:`; it does not resolve a mutable
range during an image build. Regenerate either lock only in a clean, pinned
Python 3.12 container, record the command/tool version in the release manifest,
and review the diff as a supply-chain change.

The pinned Linux/amd64 base manifest is
`python:3.12-slim@sha256:d657ab0ade19f404a6ccc883ab399540de667aff751748ce23c07330c5a89e64`.
Its multi-architecture index digest is
`sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36`.

Every collector image enters through `docker/entrypoint.sh`. It exits before
executing a command unless the protected host deployment file supplies
`PRIMUS_HMD_B0_APPROVED=approved`. Do not set that value while any B0 check is
blocked. This guard is defense-in-depth, not evidence that B0 has passed.
Collector services use the `collectors` profile, so a bare `docker compose up`
does not start a writer.

## Owner-approved bounded seed exception

For the 2026-08-13 new-VPS session only, policy records BobbyAxerol's approval
for a one-shot B0 seed. The exception is deliberately narrower than a normal
collector approval:

- run only `tools/run_b0_seed.sh`, with no arguments;
- its image entrypoint permits only `collectors.b0_bounded_seed` with no
  arguments, and the module owns the immutable plan;
- execute one job at a time: BTCUSDT Binance 1m windows are capped at 24
  hours, metrics at one day, order book at one current REST snapshot, and FPT
  plus VN30F1M at seven calendar days;
- skip Binance archive-wide discovery, all old-VPS imports, concurrent work,
  and every Deribit backfill;
- write secret-safe measurement/evidence to
  `state/bootstrap/b0_bounded_seed.json`, and update capacity only if all fixed
  seed checks prove a canonical Parquet publish plus a successful heartbeat.

The runner is the only permitted writer before normal B0 approval. It is not a
template for manually invoking a collector with a different start date.

The production collector Compose anchor sets `HOME`, `XDG_CONFIG_HOME`,
`XDG_CACHE_HOME`, and `MPLCONFIGDIR` below `/tmp/primus-hmd-home`. This keeps
third-party provider profile/cache files ephemeral and prevents a non-root
container from attempting to write at `/`; it must remain separate from
canonical storage and runtime state.

## Bounded source probes

`collectors.b0_source_probe` exists solely for B0. Its Binance probe makes
exactly eight sequential public GET requests; its VNStock probe makes one VCI
daily-history request. They do not call a collector, backfill, discovery, or
publish path, and record only redacted metadata beneath
`state/bootstrap/source_probes/`. Run them in a constrained container with only
the runtime `state/` mount; do not mount canonical storage.

VNDIRECT and Deribit use their existing explicitly non-publishing probe CLIs.
Their B0 invocation must likewise mount only `state/`, with low CPU/memory
limits and a fixed request/rate budget. A blocked probe is evidence to keep the
dataset blocked, never permission to fall back to old-VPS data.

When a constrained probe invokes a provider library that creates user caches,
set `HOME`, `XDG_CONFIG_HOME`, `XDG_CACHE_HOME`, and `MPLCONFIGDIR` beneath the
container's tmpfs `/tmp`. Do not relax the read-only root filesystem or mount
canonical storage just to accommodate a cache.

The Deribit probe selects only a bounded candidate that has at least three
unique `trade_seq` values. A contract with fewer sequences establishes endpoint
availability but cannot establish sorting or inclusive sequence-boundary
semantics, so it is skipped rather than producing a misleading pass.

## Storage release-manifest contract

`collectors.common.storage_manifest` defines the B0 metadata contract at
`storage/_primus_metadata/release_manifest.json`. Its writer validates a full
manifest and replaces it atomically. A complete manifest declares environment,
Git/tag and build identifiers, the source-inventory reference, each dataset's
schema/layout version, and supported loader-contract versions.

`assert_loader_compatible(storage_root, dataset_id=..., loader_contract_version=...)`
refuses an absent, malformed, draft, undeclared, or incompatible release with a
clear `StorageCompatibilityError`; it never returns possibly misinterpreted
data. The reader-wheel implementation lives in top-level `storage_manifest` so
the packaged consumer uses the same contract as collectors. Public loaders
enforce it when a consumer declares `HISTORICAL_MARKET_DATA_ROOT` (or explicitly
sets `HISTORICAL_MARKET_DATA_REQUIRE_RELEASE_MANIFEST=1`); collector-side
`DATA_ROOT` alone does not turn a writer helper into a consumer.

## Operator-visible monitoring status

Run `python -m collectors.b0_operational_status --strict --json` against the
dedicated runtime root. It is read-only: it never starts a service or modifies
state. It checks disk and inode low-water marks, expected heartbeat freshness,
and recorded operator-visible evidence for collector exits, retry/rate limits,
validation/repair, RSS, and backup failures. Missing evidence deliberately
returns `blocked`; after an approved scheduled cycle, record only factual alert
and heartbeat evidence before changing `monitoring.json` to `pass`.

Before using a production Compose command, inspect the resolved configuration:

```bash
docker compose --env-file /srv/primus/historical-market-data/deploy/compose.env config
```

Do not use `seed-existing-history` or otherwise import old-VPS `storage/`,
`state/`, or `logs/`; that migration path is deliberately absent from the
production Compose configuration.

## Discord operational delivery and deferred controls

The Discord monitor is a read-only service. Its webhook is supplied only as a
mode-0600 Docker secret file, accepts only an HTTPS Discord webhook host, and
never serializes the URL into state or output. It records delivery-provider
status, test timestamps, monitor freshness, and alert-category results only.

For this session, off-host backup/restore and an explicit consumer rollback
release/root are recorded as `approved_deferred` technical debt in the B0
policy. They may produce `waived` checks for bounded/staged collection, but
remain mandatory before Phase E consumer cutover, old-writer retirement, or a
destructive data operation. No mixed-root fallback is permitted.
