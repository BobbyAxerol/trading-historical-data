# Historical Market Data — Locked Session Rules

## Scope boundary

- This session is dedicated **only** to `/srv/primus/src/historical-market-data`.
- Do not edit, reconfigure, restart, or inspect application-private files for any other project under `/srv/primus/src`, `/home/bobby`, or elsewhere on the host.
- The only permitted non-repository write location for this project is its dedicated runtime root: `/srv/primus/historical-market-data`.
- Read-only host checks that are necessary to prove a Phase B0 gate are allowed. Any write outside the repository or the dedicated runtime root requires explicit user authorization.
- Do not change Bobby's shell, SSH, sudo, Docker-wide, ACL-wide, firewall, or global system configuration while working on this repository.

## Required context and release checkpoint

Read these documents before making a technical or operational change:

1. `PRIMUS_NEW_VPS_CLEAN_REBUILD_AND_PACKAGE_RUNBOOK.md`
2. `implementation_plan.md`
3. `DERIBIT_BTC_OPTIONS_HISTORICAL_DATA_V1_OFFICIAL_PLAN.md`

The bootstrap checkpoint is:

```text
tag:    primus-historical-market-data-bootstrap-v0.1.0rc1
commit: bdda4b28b302424a6c682893a9cc966cad59a17a
```

Before edits, record `git status --short --branch` and verify the checkout. Do not reset, clean, checkout another revision, merge, retag, push, or alter remotes unless the user explicitly asks.

## Git workflow and repository identity

- Maintain one local tracking branch for each `origin/*` branch. At this checkpoint the mirrored set is `main`, `dev`, `feat/option-ingestion`, and `feat/vn-daily-universe-upgrade`.
- Work on the local tracking branch `feat/option-ingestion`, whose upstream is `origin/feat/option-ingestion`. Do not develop on a detached tag, `main`, or an unrelated branch.
- Keep `primus-historical-market-data-bootstrap-v0.1.0rc1` as the immutable bootstrap reference; verify it resolves to `bdda4b28b302424a6c682893a9cc966cad59a17a` before migration-sensitive work.
- Use this repository-local Git identity only:

```text
user.name  = BobbyAxerol
user.email = vugioan11022002@gmail.com
```

- Before any future push, inspect the branch/upstream, working-tree diff, staged diff, and commit range. Push only `HEAD` to `origin/feat/option-ingestion`, never force-push, and never push generated runtime artifacts or secrets.
- Keep scope-guard, B0, and later implementation commits focused and independently reviewable. Do not mix unrelated host/project changes into this branch.

## New-VPS data isolation

- Transfer and use Git-tracked source, configuration templates, tests, and documentation only.
- Never copy, rsync, Git-LFS migrate, or otherwise import `storage/`, `state/`, or `logs/` from the old VPS.
- Treat the old VPS as read-only audit/rollback reference; never use its Deribit checkpoint, staging files, or partial canonical data to skip new-VPS gates.
- Runtime data must never be written inside the Git checkout. Use only:

```text
/srv/primus/historical-market-data/storage
/srv/primus/historical-market-data/state
/srv/primus/historical-market-data/logs
/srv/primus/historical-market-data/releases
```

- Never commit runtime data, generated Parquet, SQLite databases, logs, caches, Docker runtime files, `.env`, credentials, or private keys.

## Current gate: Phase D controlled source rebuild

- Phase B0 is recorded as `pass_with_accepted_waivers`; the approved bounded
  seed, non-Deribit live tails, reader package acceptance, and Discord monitor
  are complete. The waiver is not a consumer-cutover or destructive-operation
  permission.
- The active work phase is **D: Clean Source Rebuild On The New VPS**. Start
  only a named, reviewed Docker service with an exact entrypoint command. A
  bare `docker compose up`, a direct collector invocation, a changed argument,
  or a generic approval environment variable is never a Phase D authorization.
- At most one heavy historical service may run at a time. B0 live tails may
  remain running, but any shared canonical dataset must use the same
  partition/manifest locking discipline and Phase D must validate its result.
- The first owner-approved Phase D job is exactly
  `phase-d-binance-usdm-perpetual-1m`, defined in
  `configs/primus_hmd_phase_d.yml` and launched only with
  `tools/run_phase_d_binance_usdm_perpetual_1m.sh`. It rebuilds BTCUSDT USD-M
  perpetual 1m data from new upstream sources, one archive or bounded REST
  window at a time.
- Do not start a default-universe expansion, derived matrix, VN historical
  batch, metrics/spot/quarterly historical batch, repair sweep, consumer
  cutover, old-writer retirement, destructive data operation, or **any
  Deribit command** under this approval. Each needs its own recorded gate.

### Owner-approved B0 exception for this session

- BobbyAxerol approved one narrow exception on 2026-08-13: the fixed new-VPS B0 seed may run only through `tools/run_b0_seed.sh` after its reviewed image has been built. It accepts no arguments and runs sequentially: BTCUSDT Binance windows capped at 24 hours, one current order-book snapshot, and seven-day FPT/VN30F1M VN windows.
- Never substitute a manual `docker compose run`, direct collector invocation, alternate environment variable, changed seed argument, archive-wide discovery, concurrent seed, old-VPS import, or Deribit backfill for that runner. The Docker entrypoint permits the pre-B0 exception only for `collectors.b0_bounded_seed` with no arguments.
- A B0 result of `pass_with_accepted_waivers` is transparent acceptance for bounded/staged collection only. It is not permission for consumer cutover, old-writer retirement, data deletion, mixed-root fallback, or unreviewed broad historical jobs.
- On 2026-08-13, BobbyAxerol additionally approved the seven B0-seeded non-Deribit live tails declared literally in `configs/primus_hmd_b0.yml`: BTCUSDT futures/spot, BTCUSDT_260925 quarterly, BTCUSDT metrics/orderbook, FPT daily, and VN30F1M VNDIRECT daily. Each protected approval is service-scoped and each entrypoint command is exact. Start/check their first cycles sequentially; each tail is capped at 0.5 CPU, 512 MiB, and 128 PIDs, and no concurrent heavy historical job is allowed. Never introduce a blanket writer flag, default-universe expansion, archive sync, repair/backfill, derived matrix job, or another collector under any of those approvals. All Deribit commands remain prohibited until separately approved.
- Off-host backup/restore and consumer rollback identity are explicitly deferred technical debt for this session. They must remain reported as `waived`, never as passed, and must be closed before Phase E consumer cutover, old-writer retirement, or any destructive data operation.
- The Discord webhook is a runtime secret only. Store it in the protected project runtime secret location with mode 0600; never read it back, print it, place it in Git, include it in evidence, or expose it in command output. The monitor records provider/status/timestamps only.

## Operational and data-safety invariants

- Keep runtime mounts rooted at `PRIMUS_HMD_RUNTIME_ROOT=/srv/primus/historical-market-data`; no writable checkout mounts and no source-code bind mount for runtime services.
- Keep internal-only services bound to loopback or Unix sockets unless the user explicitly approves another exposure.
- Preserve reader-only access boundaries: consumers may read canonical storage only; never grant them write access to storage or access to `state`, `logs`, secrets, or host administration.
- For Deribit, preserve disk-before-checkpoint, bounded queues, atomic writes, coverage-ledger validation, and cleanup-only-after-validation. Never equate API failure with confirmed empty data.
- Never delete canonical data, staging evidence, or runtime directories as part of a diagnostic or cleanup action without a validated project procedure and explicit confirmation where required.
- Do not read, print, commit, or move secrets, SSH private keys, API keys, or `.env` values.
- `docker compose up` must leave writers stopped by default; start only a named, reviewed service after registering its active heartbeat dataset and confirming the Discord monitor has a fresh healthy cycle.

## Evidence and handoff

- Keep changes narrowly scoped, explain the B0 gate they address, and verify them proportionately.
- Update the project implementation log only with factual commands, results, metrics, decisions, and blockers.
- Before handing off, report the exact B0 status and the next allowed gate. If a gate is not provably passed, mark it blocked rather than bypassing it.
