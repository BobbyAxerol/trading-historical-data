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
`--strict` exits non-zero while a B0 gate is incomplete; that is expected until
the gate has real evidence.

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

Before using a production Compose command, inspect the resolved configuration:

```bash
docker compose --env-file /srv/primus/historical-market-data/deploy/compose.env config
```

Do not use `seed-existing-history` or otherwise import old-VPS `storage/`,
`state/`, or `logs/`; that migration path is deliberately absent from the
production Compose configuration.
