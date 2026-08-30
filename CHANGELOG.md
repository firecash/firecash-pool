# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Pre-1.0.0 releases are development snapshots and may change in
backward-incompatible ways at every minor bump.

## [Unreleased]

## [0.1.0] - 2026-08-30

First tagged release. The pool has been serving production since 2026-08-24;
this captures that tree, including work that had been deployed but never
committed.

### Added

- **Per-miner KAS payout via the stratum password field.** A merge-mining lane
  can pay its KAS to the miner instead of the pool: set the stratum password to
  your own `kaspa:` address. Anything without an explicit
  `kaspa:`/`kaspatest:`/`kaspadev:` prefix is treated as "no opinion" and the
  lane keeps paying the pool, so placeholder passwords (`x`, `*`, `(null)`,
  `d=8192`) stay silent instead of warning. Parsing is deliberately stricter
  than the ZKas address path — it never coerces a bare payload, because a
  coerced guess that happened to decode would mint real KAS to an address the
  miner cannot spend (`bridge/src/default_client.rs`).
- **KAS block statistics.** Pool-wide `kasBlocksFound`, a per-worker `kasBlocks`
  column, and per-wallet `kasBlocksFound` in the miner lookup. The data was
  already in `ks_merged_parent_submit_total` and simply never read
  (`pool-redactor.py`).

### Fixed

- **Kaspa-clearing solutions are no longer gated behind the ZKas target.** The
  Kaspa submit sat inside `if meets_network_target`, which is harmless only
  while Kaspa is the harder chain. That inverts once ZKas difficulty passes
  Kaspa's: a hash in the band `zkas_target < h <= kaspa_target` wins a real
  Kaspa block and was discarded for failing an unrelated chain's test —
  silently, with no rejection and no metric. Measured margin fell from 1.34 to
  1.136 within a single day; at 2x today's ZKas difficulty the loss would be
  ~43% of all KAS. Both targets are now evaluated independently, and a
  Kaspa-only nonce never consumes the ZKas `H_fc` claim
  (`bridge/src/share_handler.rs`).
- **Dashboard no longer reports the pool's own hashrate as the network's.** When
  the node was unreachable the network figure was filled in with
  `max(raw_sum_hs, pool_hs)` — the pool's worker sum wearing the network's
  label. On a host missing `grpcurl` this read as "100% of the network" on every
  sample and understated the real figure ~130x. Unknown is now left unknown
  (`pool-redactor.py`).

### Fixed

- **Dashboard "Block height" showed a stale/low value** (~115K while the chain
  was at DAA ~334K). The bridge reported `GetBlockDagInfo.block_count` (the
  retained-block tally on a pruned node) as the network height; it now reports
  `virtual_daa_score` — the true chain height users and the explorer see
  (`bridge/src/kaspaapi.rs`).

### Changed / verified

- **Real dual-chain merged mining tested live (2026-07-15).** Enabled via
  `ZKAS_MERGED_MINING=1` + `ZKAS_KASPA_NODE` + `ZKAS_KASPA_PAY`: the bridge pulls
  a real Kaspa parent template, embeds `ZKMM||H_fc`, and submits solved parents
  to both Kaspa (KAS) and ZKas (aux). Verified: ZKas blocks accepted by the node
  in merged mode; graceful fallback to synthetic parent (ZKas-only) while the
  Kaspa node is still in IBD. Docs updated (`help.txt` §6, §9).
- **Consensus (node) fix consumed via the rusty-kaspa fork:**
  `is_shielded_anchor_final` panicked (`.unwrap()` on a pruned anchor-source
  block's blue score), freezing the virtual processor once pruning advanced past
  a live anchor's source. Pool operators running a node MUST take the patched
  node build — the pre-fix node crash-loops and stops accepting blocks. A pruned
  source is now treated as finalized/canonical instead of panicking.

### Removed

- **Cutover one-shot tools** (`katpool-import-legacy`, `katpool-replay`,
  `scripts/legacy-importer-rehearsal.sh`,
  `scripts/replay-determinism-rehearsal.sh`, Runbooks 14 and 17).
  Mainnet cutover reconcile is green; evidence lives under
  `cutover-evidence/` and `replay-evidence/`. Replay determinism
  remains covered by `accountant::replay` and
  `cargo test -p accountant --test replay_harness_scale`.
- **Payout rehearsal one-shot tools** (`katpool-payout-rehearsal`,
  `katpool-krc20-rehearsal`, `scripts/kas-payout-rehearsal.sh`,
  `scripts/krc20-payout-rehearsal.sh`, Runbooks 18–19). Phase 4/5
  dry-run sign-off is complete and evidence lives under `payout-evidence/`;
  production dry-run remains via `KATPOOL_PAYOUT_DRY_RUN` /
  `KATPOOL_KRC20_PAYOUT_DRY_RUN`.

### Changed

- **Observability mainnet scaling guidance** (`SLO.md`). Documents the ingest
  levers that scale with miner count (Loki `ingestion_rate_mb`, Alloy trace
  `sampling_percentage`, VM retention/volume sizing) and a baseline-then-3×
  method, so retention/limits are tuned from a real mainnet baseline instead of
  guessed. No runtime values changed.
- **Origin Alloy: durable log buffering + trace sampling** (B7 hardening). The
  netcup Alloy agent now persists `loki.write`'s WAL to a host volume
  (`--storage.path` on `/etc/katpool/obs-tn10/alloy-data`, `wal { enabled = true }`),
  so a transient vmauth/Railway outage or an agent restart no longer drops logs —
  they replay when the door reopens. A `otelcol.processor.tail_sampling` block
  now fronts the Tempo exporter: error and slow (>500ms) traces are always kept,
  the rest are sampled (`sampling_percentage` 100 on tn10 — lower for mainnet API
  volume). The config is installed to a stable path decoupled from the git
  working tree. (`ops/railway/observability/origin/`.)
- **NACHO floor price now sourced from CoinGecko, not the Kasplex marketplace
  floor** (ADR-0016 amendment). The KRC-20 payout engine derives KAS-per-NACHO
  as `nacho_usd / kaspa_usd` from a single keyless CoinGecko `simple/price` call
  (`ids=nacho-the-kat,kaspa`), replacing `api.kaspa.com/api/floor-price`
  (`KaspaComFloorPrice` → `CoinGeckoFloorPrice`; `KATPOOL_KRC20_QUOTE_BASE`
  default → `https://api.coingecko.com`). CoinGecko quotes in USD but the
  conversion needs KAS/NACHO, so both legs are fetched together and the USD
  scale cancels. The division stays exact and float-free (ADR-0013): the two
  quotes are read as verbatim JSON text (serde_json `raw_value`) and divided
  with `bigdecimal`, then **floored** to 18 fractional digits — never rounded up,
  so a payout is never over-funded. A zero/negative/missing leg fails the cycle
  closed via the existing circuit breaker. The Blackbox `indexer` synthetic
  probe now targets the CoinGecko endpoint (was a dead `api.kaspa.com/info/price`
  path that 404'd), so monitoring tracks the real dependency.

### Fixed

- **Mainnet API rate-limit default would throttle the dashboard** (E2 of the
  road-to-mainnet plan). `ops/env/mainnet.env.example` shipped the bare
  `5/sec, 20 burst` per-IP default, but on mainnet the dashboard's same-origin
  Next BFF fronts the API — every viewer arrives as one source IP, so one
  overview load (≈8 concurrent `/api/v1` calls) trips the limit. The template
  now sets BFF-appropriate `100/300` with the rationale, matching the tn10
  guidance, so a fresh mainnet env does not self-throttle.

### Added

- **Phase 10 cutover tooling** (cutover gate). `ops/cutover/rollback-rehearsal.sh`
  proves the binary rollback path `scripts/deploy.sh` preserves: `--check`
  (non-disruptive) validates the latest `.bak` is a real ELF + prints the exact
  rollback command; `--execute` rolls back, verifies `/ready`, and prints the
  roll-forward command. Runbook 22 operationalizes `cutover-plan.md` Phase 10 into
  an executable checklist wired to the real tooling (shadow-run reconcile via the
  legacy importer, importer hot-run via `legacy-importer-rehearsal.sh`, DNS flip
  to the fly anycast edge, payouts dry-run→live, scripted rollback). Execution
  (72h shadow + the cutover itself) is operator/time-gated.
- **Phase 9 resilience tooling** (cutover gate). `ops/dr/dr-validate.sh` is the
  automated DR validator (ADR-0009 / Runbook 10): dump → restore into a scratch
  DB → reconcile (schema completeness + core tables non-empty + referential
  integrity), publishing `dr_validator_*` to VictoriaMetrics. New `DRValidatorMissed`
  / `DRValidatorFailed` vmalert rules (`last_over_time[14d]` for the weekly cadence)
  close the gap Runbook 10 referenced. `ops/dr/oncall-paging-test.sh` exercises the
  ntfy paging last mile, and Runbook 21 documents the full Phase 9 drill set (DR,
  chaos via `katpool-fault-injection`, custody EPERM, on-call, load, all-runbooks
  sign-off). Verified the metric-push path end-to-end against the live stack
  (push → vmauth import → read back via Grafana). Live execution (4 weekly DR
  passes, chaos/load/soak) is operator/time-gated.
- **fly.io edge bring-up script + load/failover checklist** (workstream C;
  ADR-0022). `ops/edge/flyio/bring-up.sh` idempotently orchestrates the flyctl
  bring-up (app, origin secret, dedicated anycast IPv4/v6, per-region egress IPs,
  deploy + scale) with confirmation prompts and a final egress-IP summary for the
  origin nftables allowlist. README gains a pre-mainnet **load + failover test**
  checklist. Live deploy stays operator-gated (needs a fly account + anycast IP +
  a real ASIC).
- **Canary miner tool** (B7; ADR-0004 end-to-end probe). `ops/canary/` adds a
  dependency-free Python watcher (`katpool-canary.py`) an operator runs **locally**
  (MacBook/Linux) alongside any off-the-shelf CPU miner pointed at the pool with a
  dedicated wallet. It polls the pool API for that wallet's `last_seen_at` and
  publishes `canary_last_credited_timestamp_seconds` to VictoriaMetrics via the
  vmauth import path, closing the `CanaryMinerNotPaid` loop — the ground-truth
  "are we actually crediting miners" SLI. Validated the API field against the live
  tn10 endpoint. Also de-duplicated a merge artifact in `SLO.md`.
- **Treasury key-rotation auditor** (Phase 8; backs Runbook 11). New
  `katpool treasury audit` subcommand + an hourly
  `katpool-treasury-audit-<network>.timer` (installed by `scripts/deploy.sh`)
  that continuously verifies the loaded treasury key still controls the
  configured `KATPOOL_POOL_ADDRESS`. Read-only and offline — it derives the
  key's schnorr P2PK address and compares (no funds move, nothing is signed for
  broadcast); a mismatch (botched rotation / misconfig / compromise) logs a
  structured `ERROR` and exits non-zero so the unit fails and the line ships to
  Loki. Core logic lives in `payout-kas::audit` (`treasury_address_from_secret`
  / `key_controls_address`) with unit tests.
- **Least-privilege read-only DB role for the public API** (Phase 7/8 hardening;
  ADR-0021). The embedded read-only API can now connect on a separate pool as a
  SELECT-only role, isolated from the accountant/payout writers' full-privilege
  pool. Set `KATPOOL_API_DATABASE_URL` to opt in (unset ⇒ shares the writers'
  pool, unchanged behaviour). The role is provisioned out-of-band via
  `ops/db/api-readonly-role.sql` (idempotent; kept out of `sqlx` migrations so no
  `CREATEROLE` is required at deploy and no credentials are committed). Verified
  the API issues no writes (only `SELECT 1` readiness) and the SQL applies + rolls
  back cleanly against the live schema.
- **fail2ban jails for the origin host** (Phase 7/8 edge hardening; ADR-0021,
  ADR-0008, threat-model). `ops/security/fail2ban/` adds an OS-level backstop to
  the in-process controls: a `katpool-api-4xx` jail bans clients that keep
  generating 4xx past the API's per-IP rate limiter, and a hardened `sshd` jail
  bans password/scanner bursts. Bans use nftables (already on the origin for the
  stratum firewall). `ignoreip` documented for the same-origin BFF/edge so it is
  never collateral. Stratum abuse intentionally stays app-layer (`ks_anti_abuse_*`
  + `StratumAbuseBurst`) — the pool emits no per-IP reject log to match.
- **Share-accept latency histogram** (B7). The bridge now emits
  `ks_share_accept_latency_seconds{instance}`, observed on the accepted-share
  path (`bridge/src/share_handler.rs`, submit → accept), with a
  `katpool:share_accept_latency:p99_5m` recording rule for dashboards. Closes the
  second `SLO.md` instrumentation gap; no alert until a latency objective is set
  (no guessed threshold).
- **Payout-cycle + treasury metrics** (B7, highest-value instrumentation gap in
  `SLO.md`). The KAS/KRC-20 payout engines and the consolidation engine now emit
  Prometheus series via `katpool-metrics` (previously a stub): per-tick
  `ks_payout_cycles_total{instance,engine,status}`,
  `ks_payout_last_success_timestamp_seconds{instance,engine}`, and
  `ks_treasury_balance_sompi` / `ks_treasury_spendable_utxos` from the
  consolidation snapshot. New vmalert rules `PayoutCycleFailing` (page, any
  failed/errored cycle) and `TreasuryBalanceLow` (warning, operator-tunable
  floor) close the loop with runbooks 02/05. `PayoutCycleStatus` gained
  `as_str()` / `is_success()`. (Share-accept latency histogram + canary miner
  remain as follow-up B7 items.)
- **Railway LGTM deploy artifacts** (B3 provisioning, under
  `ops/railway/observability/deploy/`). Each observability service now has a thin
  Dockerfile (upstream image pinned by digest, repo config baked in) plus a
  `railway.toml`, so the Grafana / Loki / Tempo / VictoriaMetrics (+ `vmauth`) /
  vmalert / Alertmanager / Blackbox / ntfy / ntfy-alertmanager stack deploys
  reproducibly from the repo (shared build context = `ops/railway/observability`,
  Singapore region, `RAILWAY_RUN_UID=0` on volume-backed services, secrets via
  service + reference variables) — the same provisioning is replayable for the
  mainnet environment. `vmauth` is added as the only public, basic-auth'd
  remote-write ingress so VictoriaMetrics stays private on the internal network.
  All ten images were verified to build, parse their config, and start. Also
  fixes the Grafana dashboard provider path (it pointed at
  `/var/lib/grafana/dashboards`, under the persistent volume mount, which would
  shadow the baked-in JSON — now `/etc/grafana/dashboards`) and corrects the
  `ntfy-alertmanager` image reference (Docker Hub `xenrox/ntfy-alertmanager`, not
  GHCR). See `ops/railway/observability/deploy/README.md` for the pins, settings
  matrix, secrets, and provisioning order.
- **Origin stratum firewall, operable** (C2 of the road-to-mainnet plan;
  ADR-0022). The fly.io anycast edge's PROXY-trust model requires the origin to
  accept the stratum ports only from the fly per-region egress IPs; this was
  previously just an inline placeholder in the edge README. Now a committed,
  `nft -c`-valid ruleset (`ops/edge/flyio/nftables/katpool-stratum.nft`, using
  RFC 5737/3849 documentation IPs as obvious placeholders) plus an idempotent
  `apply-origin-firewall.sh` that fills the real egress IPs (from `fly ips list`
  or args), validates, installs to `/etc/nftables.d/`, and loads them. The
  ruleset scopes to the stratum ports only (chain policy `accept`, so SSH/API/
  kaspad are untouched) and fast-paths established connections so a reload never
  severs an in-flight miner. `--check`/`--print` allow validation without root.
- **Self-hosted LGTM observability config-as-code** (B3–B7 of the road-to-mainnet
  plan; ADR-0004). New `ops/railway/observability/` with the config each service
  consumes: VictoriaMetrics scrape (`scrape.yml`) + an origin `vmagent` config
  (the pool's `/metrics` binds loopback on mainnet, so the origin remote-writes
  into the Railway stack — preserving the ADR's failure-domain split); `vmalert`
  alerting rules derived from the **actual** emitted `ks_*` / `ks_accountant_*`
  metrics and the blackbox synthetic probes, each annotated with a real
  `docs/runbooks/` URL (incl. the ADR-mandated `CanaryMinerNotPaid` page);
  SLI recording rules; Alertmanager routing to ntfy (via the ntfy-alertmanager
  bridge; secrets stay out of the repo); Blackbox HTTP/TCP probe modules; Loki
  (TSDB v13, 30-day retention) and Tempo (OTLP, 14-day) configs; Grafana
  datasource + dashboard provisioning with a pool-overview dashboard; and
  `SLO.md` (SLOs/SLIs, retention, escalation) — which also documents the
  instrumentation gaps left un-faked (payout-cycle/latency metrics, canary
  binary). Railway project provisioning and the canary miner remain
  operator/follow-up actions, per the plan's own categorization.

- **Layered YAML/TOML config file** (`katpool-config`, was a scaffold; A3 of the
  road-to-mainnet plan). An optional `KATPOOL_CONFIG` path points at a YAML or
  TOML file (format inferred from the extension) parsed + validated by the
  `katpool-config` crate. It supplies the *core* runtime keys (node/db/network,
  stratum, maturity, and the operational toggles) under a strict precedence —
  **environment variable > config file > built-in default** — so an env var
  always wins and the file only fills gaps. `deny_unknown_fields` and range
  validation make a typo'd key or out-of-range value a hard boot error; there
  are no silent fallbacks. Payout, KRC-20, consolidation, and treasury-key
  settings stay environment-only by design (secrets / money-movement policy).
  Unset/empty `KATPOOL_CONFIG` ⇒ pure-environment behavior, byte-for-byte
  unchanged. See `ops/config/katpool.example.yaml`. As part of this, a required
  value with an empty env var (previously accepted as `""`) now correctly falls
  through to the file/default layer.
- **Dedicated liveness/readiness probe port** (A4 of the road-to-mainnet plan).
  `KATPOOL_HEALTH_CHECK_PORT` was a no-op in the unified runtime — the value was
  carried into `BridgeServerConfig` but never served, so health checks required
  the full public API on `KATPOOL_API_PORT`. It now binds a minimal server
  (`api::serve_health_on`) exposing only `/health` `/ready` `/started` — no rate
  limiter, no CORS, no `/api/v1` — off the *same* readiness source as the API
  (shared `ReadinessHandle`, one DB probe, one kaspad-sync mirror; no second
  gRPC connection). Orchestrators can now probe independently of the public API.
  Both `KATPOOL_API_PORT` and `KATPOOL_HEALTH_CHECK_PORT` accept `host:port` or
  `:port` (all interfaces), matching the `KATPOOL_PROM_PORT` convention.
- **Structured telemetry wiring** (`katpool-telemetry`, was a scaffold; B1 of
  the road-to-mainnet plan). A single `init(TelemetryConfig)` installs the
  process-wide `tracing` subscriber the `katpool` binary now calls before
  config load, so even a bad config logs. It composes a `RUST_LOG` env-filter,
  a formatting layer selectable via `KATPOOL_LOG_FORMAT=text|json` (text stays
  the default for `journalctl`; json emits one structured object per event for
  Loki), and an optional OpenTelemetry OTLP/gRPC span-export layer wired only
  when `KATPOOL_OTLP_ENDPOINT` is set (Tempo, ADR-0004 — off by default until
  the LGTM stack exists). `init` returns a guard that flushes the tracer
  provider on a clean exit. `OTEL_SERVICE_NAME` overrides `service.name`
  (defaults to `KATPOOL_INSTANCE_ID`).
- **NACHO Elite tier classifier wired into the runtime** (was inert; replaces
  the hardcoded `StaticTierClassifier::standard()`). `KATPOOL_TIER_CLASSIFIER`
  selects `static` (default) or `kasplex`; the kasplex classifier carries a
  consecutive-failure circuit breaker (open → instant `Standard`, half-open
  probe after cooldown) so an indexer outage cannot stall the block-maturity
  classify loop, and degraded results are not cached so an Elite wallet
  recovers within one cycle. The indexers are mainnet-only, so `static` remains
  correct for the tn10 soak (ADR-0012).
- **Per-cycle treasury spend cap** (G1 money-safety circuit breaker).
  `KATPOOL_PAYOUT_MAX_SOMPI_PER_CYCLE` (KAS) and
  `KATPOOL_KRC20_MAX_NACHO_PER_CYCLE` (NACHO base units) refuse a cycle whose
  total non-failed outbound exceeds the cap **before any broadcast/settle** —
  the primary guard against a poisoned floor-price quote draining the treasury.
  Disabled by default; the mainnet template recommends setting both.

### Security

- **Cosign-verified deploys** (H1 of the road-to-mainnet plan).
  `scripts/deploy.sh` now treats a prebuilt artifact (`--release <tag>` to
  download from a GitHub Release, or `--binary <path>`) as untrusted until its
  keyless cosign signature is verified against the `release.yml` workflow
  identity and the Rekor transparency log — a failed or missing
  `*.sigstore-bundle.json` aborts the deploy before anything is swapped. The
  check lives in the standalone, hand-runnable `scripts/verify-release.sh`
  (overridable signer identity/issuer via `KATPOOL_RELEASE_*` env for forks).
  A locally built from-source binary is unsigned and installed as-is;
  `--no-verify` is an explicit, documented offline-only escape hatch.
- **Treasury/wallet address redaction in runtime logs and traces** (B2). The
  unified binary now emits addresses as a `prefix:…last4` tag
  (`katpool_domain::redact`, the single canonical redactor the `api` layer also
  routes through) instead of the full string, at every treasury log site
  (payout/krc20/consolidation engine startup, run-now, and the startup pool
  -address list). Treasury key material remains structurally unloggable
  (`katpool_secrets::TreasurySecret` has no `Debug`/`Display`/`Serialize`).

### Changed

- **Deploy readiness gate** (H2 of the road-to-mainnet plan). After restarting
  the service, `scripts/deploy.sh` no longer stops at "process is active": it
  polls `/ready` (DB-reachable **and** kaspad-synced) on the same probe the
  orchestrator uses — `KATPOOL_HEALTH_CHECK_PORT`, else `KATPOOL_API_PORT` — for
  up to 30 s, and fails the deploy (retaining the binary backup for rollback) if
  readiness is not reached. Runbook 09 was rewritten to match the real signed
  *binary* + cosign-bundle release flow (the prior "signed Docker image" /
  `rollback.sh` / `deploy.jsonl` text described a pipeline that does not exist).
- **Session-recording cleanup + test hardening** (A6/B1 follow-up; no runtime
  behavior change). `connection_session.worker_id` is already bound at session
  open (PR #71), so the now-dead `bind_worker` UPDATE helper — whose doc still
  described the obsolete "fill in on first `ShareCredited`" flow — was removed,
  and the stale module/audit docs were corrected. Added accountant-level
  session-handler tests (open persists a live worker-bound row; open→close
  finalizes the *same* row; close-without-open falls back to a completed row;
  a bare authorize leaves `worker_id` NULL with no phantom-worker backfill).
- **Graceful shutdown now drains the event backlog instead of aborting it**
  (A2). At SIGTERM the runtime stops the bridge producer first, then
  `EventConsumer::run_with_shutdown` persists everything already on the
  broadcast bus — bounded by an idle gap and a hard
  `KATPOOL_SHUTDOWN_DRAIN_SECS` ceiling (default 10) — before exiting, instead
  of dropping in-flight `PoolEvent`s on `task.abort()`.

- **Public read-only HTTP API** (`api` crate, embedded in the `katpool`
  runtime behind `KATPOOL_API_PORT`; ADR-0021). An env-gated `axum` task —
  spawned exactly like the prom exporter, empty port = disabled — exposing the
  unversioned `/health` `/ready` `/started` probes plus a versioned `/api/v1`
  read-only data surface for a future dashboard: pool aggregates
  (`/pool/stats`, `/pool/hashrate`, `/pool/hashrate/history`, `/pool/blocks`,
  `/pool/payouts`) and per-wallet views (`/balance/:address`,
  `/miners/:address` + `/workers`/`/hashrate/history`/`/payouts`/`/rejects`,
  `/full_rebate/:address`). It is **read-only and secret-free by
  construction** (reads PostgreSQL only; never links the payout/secret crates)
  and carries the threat-model's Phase 6 DoS controls: per-IP rate limiting
  (`tower-governor` GCRA, `KATPOOL_API_RATE_*`), a bounded `moka` TTL cache
  (`KATPOOL_API_*_CACHE_TTL_SECS`), a hard per-request timeout layered over the
  DB `statement_timeout` (503 on timeout), a bounded request body, optional
  CORS (`KATPOOL_API_CORS_ALLOW_ORIGIN`), and address redaction in every
  log/trace. Integer money amounts (sompi, NACHO base units) serialize as
  decimal **strings** so a JavaScript dashboard never loses precision above
  2^53; hashrate stays a JSON number; timestamps are RFC3339 UTC; list
  endpoints use keyset (`before_id`) pagination and time-series take a fixed
  `bucket` enum (`1m`/`5m`/`1h`/`1d`) with server-enforced span/point caps.
  Errors share one shape `{ "error": { "code", "message" } }`. `/ready` is
  DB-reachable AND kaspad-synced and `/started` latches the first observed
  sync; the kaspad-sync signal **reuses the maturity tracker's existing poll**
  (new `MaturityTracker::with_sync_observer`, no second gRPC connection).
  Covered by `insta` wire-contract snapshots, testcontainer endpoint tests
  (200/400/404/readiness), and a real-listener 429 rate-limit test; new
  read-only `katpool-db` repo functions back the new endpoints, each with its
  own testcontainer coverage.
- **Production-grade network fee policy for KAS payouts** (`FeeRate` in
  `katpool-storagemass`, ADR-0018). The fee is `feerate × effective_mass`
  floored at kaspad's minimum relay fee, where `feerate` is pulled live from
  the node's `get_fee_estimate` **priority bucket** (new
  `KaspadClient::fee_estimate_sompi_per_gram`) and `effective_mass` is the max
  over compute/storage/transient mass. A fee-estimate RPC failure is
  non-fatal — it falls back to the relay-minimum floor so payouts still go
  out. The relay-fee and dust rules are mirrored verbatim from rusty-kaspa
  `tn10-toc3` (`MIN_RELAY_TX_FEE_SOMPI_PER_KG = 100_000` sompi/kg). The planner
  reserves the fee out of treasury change and folds a dust/zero change output
  into the fee (kaspad rejects dust outputs).
- **Exact fee sized from the signed transaction** (`sign_batch_with_exact_fee`
  in `payout-kas`). The authoritative fee is computed from the *signed*
  transaction's own mass — the exact bytes kaspad validates — so it cannot
  diverge from mempool policy as mass rules evolve. Signing is in-memory with
  no external effect; the recorded txid matches the broadcast transaction.
- **`katpool payout run-now [--dry-run]` CLI subcommand** for operator
  on-demand payouts. Drives the current DAA-window cycle exactly as one daemon
  tick would (plan → broadcast → confirm → reconcile), under the shared
  `payout-kas:kas-leader` advisory lock so it is safe to run alongside a live
  daemon. `--dry-run` previews (sign + verify) without broadcasting. The
  binary otherwise runs the full daemon as before (no arguments).
- **Adaptive network fees for KRC-20 (NACHO) commit/reveal payouts, frozen
  per-transfer** (ADR-0019). The commit and reveal fees are now sized with the
  same `FeeRate` policy as KAS (`feerate × effective_mass`, floored at the
  relay minimum, with the dust/zero change output folded into the fee),
  replacing the fixed `0.0001 KAS` fee that was ~18–20× below the mempool
  minimum and would have been rejected (`RejectInsufficientFee`) on go-live.
  Because the commit change and reveal return — and therefore both txids,
  recorded *before* broadcast for crash-safety — depend on the fees, the
  resolved fees are **frozen** onto the `krc20_pending_transfer` row (new
  nullable `commit_fee_sompi` / `reveal_fee_sompi` columns) in the same
  transaction that records the commit hash. Every later reconstruction (reveal
  build, drift check, commit/reveal re-broadcast) replays the frozen fees via a
  new `Krc20FeePolicy::{Adaptive, Frozen}`, so a crash-resume re-derives
  bit-identical transactions.

### Removed

- **Fixed KRC-20 fee configuration.** `Krc20FeeConfig` and the
  `KATPOOL_KRC20_COMMIT_FEE_SOMPI` / `KATPOOL_KRC20_REVEAL_FEE_SOMPI` env knobs
  (and the matching `katpool-krc20-rehearsal` flags) are gone — KRC-20 fees are
  now adaptive and frozen per-transfer (ADR-0019), not operator-tunable.

### Changed

- **`KATPOOL_HEALTH_CHECK_PORT` documented as a no-op in the unified runtime**
  (ADR-0021 E1). The runtime still carries it into
  `BridgeServerConfig.health_check_port` for the standalone bridge binary, but
  `listen_and_serve_with_events` never served it here (the #54 `prom_port`
  class of latent bug). Liveness/readiness now come from the API's
  `/health` `/ready` `/started` on `KATPOOL_API_PORT`; the dead knob is kept
  and clearly documented rather than removed, to avoid a surprising config
  change to existing deployments.
- **Payout cadence and threshold defaults** now match the decided policy for
  both networks: cycle span `216_000` DAA (~6h at 10 BPS — tn10 and mainnet
  both run 10 BPS since Crescendo), KAS payout threshold 10 KAS
  (`KATPOOL_PAYOUT_THRESHOLD_SOMPI`), and NACHO minimum 10 KAS-worth
  (`KATPOOL_KRC20_MIN_PENDING_SOMPI`). These are now the built-in binary
  defaults (`DEFAULT_KAS_PAYOUT_THRESHOLD_SOMPI`, `DEFAULT_MIN_PENDING_SOMPI`,
  and the `cycle_span_daa` fallback), replacing the stale 1-BPS-era `86_400`
  span / 5 KAS / 1 KAS defaults, and are also set explicitly in both
  `ops/env/tn10.env` and `ops/env/mainnet.env.example`. Cadence is DAA-windowed
  (deterministic, multi-instance-safe), so the span is block-rate-specific; a
  network not running 10 BPS must recompute it. Mid-window ad-hoc top-ups are
  explicitly out of scope (ADR-0018).

- **Maturity tracker + reward allocation redesigned to a UTXO-anchored,
  two-sweep model** (corrects ADR-0014 against rusty-kaspa `tn10-toc3`
  consensus). The previous model matured a *found block* and parsed
  **that block's own coinbase** for the reward, gated maturity on a
  100-blue-score depth, and used `is_chain_block` for confirmation —
  all three wrong against consensus, and the cause of the stalled
  tracker (legitimately-blue blocks stranded in `submitted_to_node`).
  Each sweep now runs two independent, idempotent passes:
  - **Block-lifecycle telemetry** resolves every `submitted_to_node`
    block to a terminal state by **GHOSTDAG colour**
    (`get_current_block_color`): `confirmed_blue` (blue), `orphaned`
    (red), or stays pending until it ages out past coinbase-maturity
    depth. Drives no money. `block::list_by_status` is now oldest-first
    (FIFO) so a bounded sweep cannot head-of-line block.
  - **Coinbase-reward allocation** takes the **coinbase UTXO set
    credited to the pool address** (`get_utxos_by_addresses`, requires
    kaspad `--utxoindex`) as ground truth for realised reward. Each
    UTXO matured to `virtual_daa_score ≥ block_daa_score + 1000` is
    recorded in the new `coinbase_reward` table (anchored by outpoint →
    exactly-once) and allocated over the PROP window ending at its DAA
    score.
  - `KaspadClient` trait surface changed from
    `{get_virtual_blue_score, get_block}` to `{get_virtual_daa_score,
    get_block_color, get_pool_coinbase_utxos}`.
  - `AllocationEngine::allocate_matured_block(hash, …)` →
    `allocate_coinbase_reward(reward_id, …)`, gated on
    `coinbase_reward.allocated_at` (audit subject `coinbase_reward`).
  - `MaturityConfig.maturity_depth` (100) → `coinbase_maturity` (1000
    DAA); env var `KATPOOL_MATURITY_DEPTH` → `KATPOOL_COINBASE_MATURITY`.
- **DB migration** `coinbase_reward_anchor`: adds the `coinbase_reward`
  table and re-anchors `share_allocation` from `block_id` to
  `coinbase_reward_id` (clears prior allocations produced under the
  incorrect model — none are reconcilable with the new anchor).

### Fixed

- **Consolidation could strand a confirmed payout in `broadcasting` forever by
  spending its treasury change coin.** Payout confirmation infers on-chain
  acceptance from the payout tx's treasury *change* output still being present
  in the UTXO set (its `block_daa_score` gives the accepting height). The new
  consolidation engine selects from that same spendable set, so a sweep that
  ran before the 60s confirm pass could consume a just-broadcast payout's change
  coin — after which confirmation saw neither the coin nor the (already-mined)
  tx in the mempool, classified `Unknown`, and made no state change. The miner
  was paid on L1, but the payout sat at `submitted` and its cycle at
  `broadcasting` permanently. Fixed on both sides: (1) consolidation now
  excludes every treasury coin produced by a payout that is not yet terminal
  (`confirmed`/`failed`) — KAS `tx_hash` plus KRC-20 commit/reveal hashes, via
  the new `repo::payout::in_flight_spend_tx_hashes` — so an unconfirmed payout's
  change coin is held back until it settles (race-free: the shared treasury lock
  guarantees the payout row is persisted with its txid before consolidation next
  runs); and (2) `confirm_cycle` now durably records the accepting DAA score the
  first time the change coin is observed (new nullable `payout.accepted_daa_score`,
  written first-write-wins), so confirmation advances `accepted → confirmed` by
  depth even if that coin is later spent. Reproduced and verified on tn10
  (payout tx `27b64603…` accepted on L1 with its change coin swept from the
  treasury set). New `payout-kas` test asserts protected change outputs are
  excluded and released once the payout confirms; new `confirm` unit tests cover
  the recorded-score path.
- **Unified runtime ignored `KATPOOL_PROM_PORT`, so no Prometheus metrics were
  exported and the anti-abuse counters never recorded.** `BridgeServerConfig`
  carried `prom_port`, but only the standalone bridge binary spawned
  `prom::start_prom_server`; the `katpool` runtime did not. Because
  `start_prom_server` is also what calls `init_metrics()` (the
  `OnceLock`-guarded registration the `record_*` helpers check before
  incrementing), every metric in the runtime was silently a no-op. `katpool`
  now spawns the exporter when `KATPOOL_PROM_PORT` is set and logs that it is
  disabled otherwise. tn10 sets `KATPOOL_PROM_PORT=:9302` (`ops/env/tn10.env`).
  Verified live: feeding a malformed stratum frame increments
  `ks_anti_abuse_malformed_frame_total` on `GET /metrics` (Phase 1 acceptance
  rows 6 & 7).
- **KRC-20 commits in one settle sweep double-spent each other when the
  treasury held too few coins.** `get_utxos_by_addresses` returns only the
  *confirmed* UTXO set, so a commit just broadcast (still in the mempool) does
  not yet remove its spent coin nor surface its change. Planning every fresh
  commit in a sweep against that stale snapshot made the greedy, largest-first
  funder pick the **same** coin for all of them — only the first was accepted
  and the rest were rejected as double-spends, then stranded permanently on
  `CommitDrift`. The settle sweep now threads a per-sweep UTXO ledger that
  removes each commit's consumed inputs and re-injects its change output —
  keyed by the **real** signed commit txid, since the KRC-20 signer rejects
  planning-virtual inputs — so sibling transfers chain off one another instead
  of colliding (mirrors the KAS `plan_batches` chaining). Applies in dry-run
  too, so the Runbook-19 rehearsal now validates multi-recipient cycles.
  Verified live on tn10: two commit/reveal pairs accepted on L1 from a single
  dominant treasury coin (commits `eb6ddd03…`/`13abf205…`, reveals
  `8dec3c0e…`/`c847fa15…`).
- **KRC-20 per-transfer settle errors were counted but never logged**, so the
  cause of a non-zero `settle_errors` tick was invisible in the journal. The
  sweep now emits a `WARN` per failed transfer (transfer id, payout id, and the
  error) before continuing with the rest.

- **Live KAS payouts were rejected by kaspad's mempool with
  `RejectInsufficientFee` and never reached the chain** (ADR-0018). The planner
  built treasury change as `input_sum − payout_sum`, leaving an implicit
  **zero fee**; and even once a fee was reserved, the offline transaction shape
  under-estimated the signed transaction's mass (missing signed-P2PK
  signature-script length, and a per-input `sig_op_count` of 0 vs the signer's
  1 — a 1000-mass-per-input undercount for v0 transactions). Observed on the
  soak as *"transaction has 126400 fees which is under the required amount of
  203600 for compute mass 2036"*. Fixed by reserving an adaptive fee out of
  change and sizing the authoritative fee from the signed transaction's exact
  mass (see Added). First live payout confirmed on-chain: txid
  `15de9cfb663956017e42b0b83c959d8e7e855cc969ad0c169d78964ccf4d574f` (mass
  2036, accepted). Submit failures are now surfaced as `ERROR` (per batch and
  per engine tick) so a stuck cycle is never silent.

- **Maturity tracker logged a spurious `ERROR` and counted a sweep
  error for every freshly-submitted block.** `get_current_block_color`
  returns `RpcError::MergerNotFound` for a block not yet merged into the
  sink's past, which the tracker is designed to treat as
  `BlockColor::NotYetMerged` (wait, then age out). But the kaspa gRPC
  client erases typed error variants over the wire — every server error
  is rebuilt as `RpcError::General(message)` (rusty-kaspa
  `rpc/grpc/core/src/convert/error.rs` → `RpcError::from(String)`), so
  the `accountant`'s `get_block_color` match on `RpcError::MergerNotFound`
  never fired and the condition leaked through as `KaspadError::Transport`
  — `tracker per-block error; continuing sweep ... doesn't have any merger
  block` on the live tn10 soak. Reward allocation was unaffected (the
  block sweep is telemetry only), but the noise masked real errors and
  inflated the `errors` sweep counter. `accountant/src/kaspad_grpc.rs`
  now classifies the condition via `is_merger_not_found`, matching both
  the typed variant (in-process) and the `General(message)` form (over
  gRPC), with a test pinning the message fragment to kaspad's actual
  `MergerNotFound` Display so an upstream reword fails CI rather than
  silently regressing.

- **Treasury coinbase-coin maturity gate corrected from 100 → 1000 DAA**
  (`payout-kas` `COINBASE_MATURITY_DAA`). Consensus `coinbase_maturity`
  is 1000 DAA on mainnet and tn10; the old value let `is_spendable`
  select a coinbase UTXO 100–999 DAA deep, which would build a
  consensus-rejected immature-coinbase payout transaction. Same
  root-cause defect as the maturity-tracker correction above.

### Added

- Phase 5 milestone 5.6 (M5.6): KRC-20 NACHO **payout dry-run rehearsal**
  tool + runbook (closes acceptance row 6, mirroring the Phase 4 KAS
  rehearsal in M4.8). New `katpool-krc20-rehearsal` crate drives exactly one
  **dry-run** NACHO cycle through the production engine (`Krc20PayoutEngine`
  in `ExecutionMode::DryRun`): it acquires the `payout-krc20:nacho-leader`
  advisory lock, derives the DAA window, quotes the floor price
  (`BreakeredSource<KaspaComFloorPrice>`, fail-closed), plans the eligible
  rebates into commit/reveal transfers, and for every pending transfer
  mass-plans + signs + verifies the commit against the **live** treasury UTXO
  set — recording no txid, broadcasting nothing, and crediting no
  `nacho_rebate`.
  - `src/lib.rs` holds the pure, unit-tested reconcile-envelope serializer
    (`schema: katpool-krc20-rehearsal.reconcile/v1`): eligible-wallet
    snapshot, planned cycle, `krc20_pending_transfer` rows, parent `payout`
    rows, dry-run settle + (empty) credit reports, reconciled status, and the
    cycle audit trail (the `krc20_cycle.plan` entry carries the quoted floor
    price). The u128 NACHO dust gate serializes as a string to stay lossless.
  - `src/main.rs` is the one-shot binary (stdout = JSON envelope, stderr =
    tracing), with exit codes `0` clean / `2` `settle.errors` non-empty
    (underfunded/mass/sign) / `3` not leader / other hard failure.
  - `scripts/krc20-payout-rehearsal.sh` captures the JSON, tracing log, cycle
    audit trail, and a manifest (git rev + binary sha256 + exit code) into a
    timestamped `payout-evidence/` directory; `docs/runbooks/19-krc20-payout-rehearsal.md`
    is the operator procedure.
- Phase 5 milestone 5.5b (M5.5b): KRC-20 **payout engine** + `katpool`
  runtime wiring (closes acceptance row 5). `payout-krc20` gains an `engine`
  module (`Krc20PayoutEngine`) — a single-leader periodic loop mirroring the
  Phase 4 KAS engine that drives one NACHO rebate cycle per DAA window through
  **plan → settle → credit → reconcile**:
  - A Postgres session advisory lock (`katpool-idempotency`) under a distinct
    namespace (`payout-krc20:nacho-leader`) elects one leader per tick; a
    non-leader skips without work, so running multiple `katpool` replicas is
    safe and the KRC-20 and KAS engines never contend.
  - The cycle window comes from `payout_kas::cycle_window`, so ticks in one DAA
    bucket resume the same cycle (frozen amounts). Construction rejects a
    `cycle_span_daa` not exceeding `KAS_PAYOUT_CONFIRMATION_DAA` (the depth the
    executor confirms against), so a cycle settles before its window rolls.
  - **Safe-by-default**: `ExecutionMode::DryRun` settles without recording or
    broadcasting (M5.4b) and never credits; only a live tick moves funds or
    mutates `nacho_rebate.paid_sompi`.
  The `katpool` binary wires it opt-in (disabled + dry-run by default) behind
  `KATPOOL_KRC20_PAYOUT_*` env vars, sharing the treasury key/address and
  kaspad node (separate gRPC connection) with the KAS engine, and using a
  `BreakeredSource<KaspaComFloorPrice>` for fail-closed floor-price quotes.
  Verified by engine integration tests over testcontainer Postgres, an
  address-keyed mock kaspad, and a fixed floor-price source: a full multi-tick
  settlement (plan → commit → reveal → complete → credit → settled, no
  re-broadcast once confirmed, wallet drops out once paid), leader-lock mutual
  exclusion (a non-leader tick plans and broadcasts nothing), and clean
  `run_loop` shutdown.
- Phase 5 milestone 5.5a (M5.5a): KRC-20 **cycle state machine** (partial
  acceptance row 5). `payout-krc20` gains a `cycle` module that mirrors the
  Phase 4 KAS cycle (`resume_or_plan_krc20_cycle` / `reconcile_cycle_status`),
  adapted to the NACHO rebate model and reusing the shared, pure
  `payout_kas::derive_cycle_status` fold:
  - **Plan** (`plan_krc20_cycle`): quotes the floor price once via the M5.2
    `FloorPriceSource` (fail-closed circuit breaker), selects eligible wallets,
    converts each pending KAS-sompi balance to NACHO base units at that price
    (ADR-0016, **no** payout-time multiplier), applies the dust gate, and
    writes one `payout` + `krc20_pending_transfer` per payable recipient with
    the P2SH commit address bound to the treasury key and the recipient
    inscription (M5.1) — all in one idempotent transaction.
  - **Resume** (`resume_or_plan_krc20_cycle`): loads an existing cycle for a
    DAA window without recomputing, so amounts/recipients never shift under an
    in-flight commit/reveal.
  - **Credit** (`credit_completed_transfers`): turns a confirmed reveal into a
    rebate payment — confirms the payout and increments
    `nacho_rebate.paid_sompi` atomically, **exactly once** (the
    `confirm_krc20_payout_once` row transition is the latch).
  - **Refund** (`fail_krc20_transfer`): marks a stuck transfer and its payout
    terminal-failed, releasing the balance back to a future cycle.
  - **Reconcile** (`reconcile_krc20_cycle_status`): folds transfer statuses
    into the cycle status.
  `katpool-db` gains `list_krc20_eligible_wallets` (pending = accrued − paid −
  sompi in non-terminal KRC-20 payouts, so an in-flight or credited balance is
  never double-selected and a failed one is refunded), `ensure_krc20_pending`
  (idempotent one-to-one open), `list_krc20_for_cycle`, and
  `confirm_krc20_payout_once` (stamps `submitted_at`/`confirmed_at` together to
  satisfy `payout_lifecycle_order` since KRC-20 payouts skip the `submitted`
  state). Verified by DB-only tests over testcontainer Postgres with a fixed
  mock floor-price source: dust-gating, resume-freezing, exactly-once credit
  (no double-credit on re-run), in-flight un-selectability + failure refund,
  and the planned→partially-settled→settled status fold. Scope: this is the
  DB substrate; the periodic single-leader engine loop + `katpool` wiring +
  dry-run land in M5.5b.
- Phase 5 milestone 5.4b (M5.4b): restart-safe KRC-20 **executor state
  machine** (closes acceptance row 4). `payout-krc20` gains an `execute`
  module (`advance_transfer` / `settle_pending`) that drives one
  `krc20_pending_transfer` across `pending → commit_submitted →
  reveal_submitted → completed`, reusing the Phase 4 KAS scaffolding for
  everything chain-facing: the `payout_kas::KaspadClient` RPC trait, the
  maturity gate (`is_spendable`), and the confirmation policy
  (`classify_confirmation`, same `KAS_PAYOUT_CONFIRMATION_DAA` depth). Every
  broadcast is preceded by an atomic **record-before-broadcast** step — the
  deterministic txid (M5.4a) is written to the parent payout row *and* the
  transfer advanced one state in a single Postgres transaction *before* the
  tx hits the wire — so a crash anywhere after the record re-derives the
  identical txid from the same inputs on resume and re-broadcast is a no-op
  for kaspad, never a double-pay. The resume path is defensive about UTXO
  drift: a recorded commit that is neither on chain (its P2SH output) nor
  reproducible from the *current* treasury UTXO set yields a hard
  `CommitDrift` error for an operator rather than a second, distinct spend.
  `katpool-db` gains `record_krc20_commit_hash` / `record_krc20_reveal_hash`
  setters (the `payout.krc20_commit_hash` / `krc20_reveal_hash` columns
  landed in Phase 2; no migration), and `PlannedCommitReveal::reveal_only`
  reconstructs the reveal-relevant fields for resume without re-funding a
  commit. Verified by deterministic mock-kaspad orchestration over
  testcontainer Postgres: full `pending → completed` lifecycle with
  idempotent re-runs at each state, a crash-before-broadcast chaos test
  (intent recorded, broadcast fails, resume re-broadcasts the *same* commit
  txid and never a second), UTXO-drift refusal, and a dry-run that records
  and broadcasts nothing. Scope: the `krc20_pending_transfer` row is the
  source of truth here; wiring `payout.status`/cycle reconciliation and the
  end-to-end engine is M5.5. Phase 5 acceptance row 4 is now GREEN.
- Phase 5 milestone 5.4a (M5.4a): KRC-20 commit/reveal **signer** (part of
  acceptance row 4). `payout-krc20` gains a `sign` module that turns a
  mass-validated `PlannedCommitReveal` (M5.3) into the two signed,
  consensus-native transactions kaspad submits. The **commit** spends
  standard treasury P2PK inputs and is signed via the same
  `kaspa_consensus_core::sign::sign` path as the KAS engine. The **reveal**
  spends the commit's P2SH output, so it is signed manually: the Schnorr
  signature is computed over the spent output's `script_public_key` —
  exactly what the script engine recomputes in `OP_CHECKSIG`
  (`calc_schnorr_signature_hash` hashes `entry.script_public_key`), verified
  against rusty-kaspa source — then wrapped as `<OP_DATA_65 sig> <pushed
  redeem script>` via `pay_to_script_hash_signature_script`. Both signers
  re-run the **full txscript engine** over every input before returning, so
  a bad signature — or a treasury key that does not match the pubkey bound
  into the inscription — is a hard error, never a broadcast. `commit_txid` /
  `reveal_txid` expose the deterministic on-chain id (sig scripts excluded)
  for the record-before-broadcast ordering the M5.4b executor will use.
  `PlannedCommitReveal` now also carries `commit_amount_sompi`. Verified by
  deterministic, chain-free tests: commit and reveal both verify through the
  engine, the reveal signature script is `<sig><pushed redeem>`, txids are
  stable pre/post-sign, a mismatched key is rejected by verification, and
  empty / planning-virtual inputs are refused. (M5.4b adds the executor
  state machine + mock-kaspad orchestration + crash-before-broadcast test.)
- Phase 5 milestone 5.3 (M5.3): mass-aware KRC-20 commit/reveal planner
  (acceptance row 3). `payout-krc20` gains a `plan` module
  (`plan_commit_reveal`) that ties the M5.1 inscription primitives to the
  Phase 4 `katpool-storagemass` evaluator. It funds the commit (greedy
  largest-first treasury UTXO selection → P2SH commit output + change,
  folding sub-floor change into the fee), builds the minimal
  1-input/1-output reveal that spends the commit P2SH output, and asserts
  every KIP-9/KIP-13 mass fits `max_block_mass` independently. Unlike the
  KAS planner (which evaluates *unsigned* shapes because KAS payouts are
  storage-mass-dominated), this planner sizes signature scripts to their
  **signed** length first, since the reveal's `transient_storage_mass` is
  driven by the redeem-script-and-data push that only appears once the
  input is signed — a standard Schnorr push is 66 bytes (rusty-kaspa
  `wallet::tx::mass::SIGNATURE_SIZE`), and the reveal additionally carries
  the canonical push of the full redeem script. The planner also surfaces
  KIP-9 anti-dust: a commit change output that clears the economic floor
  can still exceed storage mass when funded by a much larger input, which
  is reported as a mass failure rather than emitted as an unminable tx
  (UTXO hygiene to avoid it is the execute/maintain layers' job per
  `docs/kips.md` §5.3–§5.4). Verified by deterministic, chain-free tests:
  both txs fit independently, the reveal's transient mass exceeds 4× the
  redeem-script length (proof the inscription is counted), and the
  funding/dust/sub-floor/storage-mass verdict paths. Phase 5 acceptance
  row 3 GREEN.
- Phase 5 milestone 5.2 (M5.2): NACHO eligibility, floor-price quote, and
  KAS→NACHO payout conversion (acceptance row 2). `payout-krc20` gains two
  pure/wireable modules. `rebate`: an exact fixed-point `FloorPrice`
  (`mantissa / 10^scale`, parsed from the API's decimal string — never via
  `f64`) and `nacho_base_units(pending_sompi, price)` =
  `floor(pending_sompi × 10^scale / mantissa)` in `u128`, exploiting the fact
  that KAS-sompi and NACHO base units share 8 decimals so the scales cancel;
  plus a dust gate (`is_payable`). `quote`: a `FloorPriceSource` trait, a
  strict decimal `parse_floor_price_response`, the `KaspaComFloorPrice` HTTP
  client (direct HTTPS to `api.kaspa.com/api/floor-price` — no headless
  browser, mirroring `accountant::tier_kasplex` reqwest conventions), and a
  pure, time-injected `CircuitBreaker` (`Closed→Open→HalfOpen`) wrapped by
  `BreakeredSource` that fails **closed** — a degraded price API skips the
  NACHO cycle rather than guessing an amount. Crucially, **no payout-time
  multiplier** is applied: the tier rebate (standard 33% / elite 100%) is
  already baked into `nacho_rebate_accrual.accrued_sompi` at allocation time
  (ADR-0012), so re-multiplying at payout would double-pay elite wallets;
  the legacy `×3` and architecture.md §4.4 are superseded by ADR-0016, which
  this milestone introduces (the KAS→NACHO conversion ADR-0012 deferred).
  Eligibility reuses the existing `nacho_rebate::list_pending`. Verified by
  unit + property tests (conversion exact-floor, parser accept/reject
  vectors), deterministic circuit-breaker transition tests, and wiremock
  HTTP tests (parse, non-200, malformed). Phase 5 acceptance row 2 GREEN.
- Phase 5 milestone 5.1 (M5.1): KRC-20 kasplex inscription primitives
  (acceptance row 1). `payout-krc20` gains a pure, chain-free `inscription`
  module: `Krc20Transfer` serialises to the canonical compact kasplex JSON
  payload (`{"p":"krc-20","op":"transfer","tick":..,"amt":..,"to":..}` in
  that exact field order); `build_transfer_inscription` assembles the commit
  redeem script (the `<x-only pubkey> OP_CHECKSIG OP_FALSE OP_IF "kasplex"
  OP_0 <json> OP_ENDIF` envelope) via rusty-kaspa's `ScriptBuilder`;
  `commit_script_public_key` / `commit_address` derive the P2SH output and
  address; and `reveal_signature_script` builds the `<sig><pushed redeem>`
  spend that exposes the inscription on the reveal tx. The envelope is
  byte-for-byte identical to the live production transfer
  (`katpool-payment`) the kasplex indexer is proven to credit — Schnorr
  `OP_CHECKSIG` with a 32-byte x-only key and a single `OP_0`, *not* the
  `OP_CHECKSIG_ECDSA` / `OP_1 OP_0 OP_0` layout some prose specs describe;
  the decision and its on-chain evidence are recorded in ADR-0015.
  Deterministic tests pin the exact envelope bytes (reconstructed from first
  principles), the compact JSON, testnet-10 P2SH derivation, the
  hash-binds-the-payload property, and the reveal signature script. New
  `docs/phase-5-acceptance.md` tracks the Phase 5 milestone map.
- Phase 4 milestone 4.8 (M4.8): KAS payout dry-run rehearsal tool +
  runbook (acceptance row 9). New top-level binary crate
  `katpool-payout-rehearsal` drives exactly one dry-run payout cycle through
  the production engine (`payout_kas::PayoutEngine` in
  `ExecutionMode::DryRun`): it takes the single-leader advisory lock, derives
  the DAA cycle window, plans against the live treasury UTXO set, signs and
  verifies every batch through the txscript engine, and reconciles — without
  broadcasting and without marking any `payout` row submitted. It emits one
  JSON envelope on stdout (`schema: katpool-payout-rehearsal.reconcile/v1`)
  with the eligible-wallet snapshot, planned cycle, planned payout rows, the
  dry-run broadcast/confirm reports, and the cycle's `cycle.plan` /
  `cycle.reconcile` audit trail; structured tracing goes to stderr. Exit
  codes encode go/no-go (`0` clean, `2` underfunded/sign-error, `3`
  not-leader). The reconcile-envelope builder lives in the crate's library
  and is unit-tested for the exact JSON contract (schema, dry-run invariant,
  cycle/payout fields, lowercase-hex `tx_hash`, audit actions) with no DB or
  node. `scripts/kas-payout-rehearsal.sh` wraps it (mirroring the Phase 2
  importer rehearsal): it captures `reconcile.json`, `reconcile.log`,
  `audit-log.txt` (extracted from the envelope — no `psql` dependency), and a
  `manifest.json` (git rev + binary sha256 + timestamps + exit code +
  cycle id + reconciled status + unpaid count) into a timestamped
  `payout-evidence/` directory. Runbook 18 documents the testnet-10
  operator procedure and the four sign-off artefacts.
- Phase 4 milestone 4.7 (M4.7): payout engine + `katpool` runtime wiring.
  `katpool-idempotency` gains a real, leak-safe Postgres session advisory lock
  (`AdvisoryLock`): it acquires on a connection *detached* from the pool, so the
  lock is released even on a panic (dropping the owned connection closes the
  backend session), and `advisory_key` maps a namespace string to the `bigint`
  key. `payout_kas::window::cycle_window` buckets the chain's virtual DAA score
  into a stable `[start, end)` window, so every tick inside one bucket resumes
  the *same* cycle (idempotency key `kas-{start}-{end}`) and a bucket rollover
  opens the next — a deterministic, wall-clock-free cadence. `payout_kas::engine`
  adds `PayoutEngine`: each tick takes the advisory leader lock (non-leaders skip
  cleanly), derives the window, then runs resume → broadcast → confirm →
  reconcile; construction rejects a `cycle_span_daa` that is not strictly greater
  than the confirmation depth so a cycle always confirms before its window rolls
  over. `run_loop` is the `tokio::time::interval` + `watch` shutdown pattern used
  by the maturity tracker. The `katpool` binary spawns the engine alongside the
  existing subsystems — opt-in and dry-run by default: moving funds requires both
  `KATPOOL_PAYOUT_ENABLED=true` and `KATPOOL_PAYOUT_DRY_RUN=false`. New env knobs
  cover poll interval, cycle span, threshold, and the treasury key source
  (`KATPOOL_TREASURY_KEY_PATH` raw-hex file for rehearsal, else
  `KATPOOL_TREASURY_CREDENTIAL` systemd credential). Deterministic coverage:
  advisory-lock mutual-exclusion/release test, `cycle_window` unit tests, and a
  full engine multi-tick settlement + non-leader-skip + `run_loop` shutdown over
  testcontainer Postgres and a mock kaspad.
- Phase 4 milestone 4.6 (M4.6): kaspad sign/submit/confirm adapter
  (`payout_kas::{signer, client, confirm, execute}`). `signer::sign_batch`
  assembles a native-subnetwork transaction from a `PlannedBatch`, signs every
  input with the `TreasurySecret` (Schnorr, `SIG_HASH_ALL`), and re-verifies it
  through the txscript engine before it can leave the process; `batch_txid`
  yields the deterministic txid (Kaspa hashes exclude signature scripts) so the
  executor records `payout.tx_hash` *before* broadcast. `client::KaspadClient`
  is a narrow async trait (live UTXOs, virtual DAA score, submit, mempool
  probe) with a `GrpcKaspadClient` binding; `confirm` holds the pure
  maturity/confirmation policy. `execute::broadcast_cycle` plans against live
  treasury UTXOs, signs, records intent atomically, then broadcasts (only
  `planned` rows, so a resumed cycle never re-pays); `execute::confirm_cycle`
  advances `submitted → accepted → confirmed` on a positive on-chain signal and
  never auto-fails. `ExecutionMode::DryRun` signs/verifies without recording or
  broadcasting (M4.8 rehearsal). Deterministic coverage: 5 signing tests
  (txid stability, virtual-input rejection, wrong-key + tamper detection),
  confirmation-policy units, and a full mock-kaspad lifecycle + idempotent
  re-run + dry-run over testcontainer Postgres. Adds workspace deps
  `kaspa-txscript` and `secp256k1` (pinned to rusty-kaspa v1.1.0's 0.29).
- Phase 4 milestone 4.5 (M4.5): `katpool-secrets` treasury key custody.
  `TreasurySecret` wraps `secrecy::SecretBox<[u8; 32]>` (zeroized on drop,
  no `Debug`/`Display`/`Clone`/`Serialize`) and `mlock(2)`s the backing page
  so the key never reaches swap. `load_from_systemd_credential` reads the
  decrypted key from `$CREDENTIALS_DIRECTORY/treasury-key` (the systemd
  `LoadCredentialEncrypted=` tmpfs), with `load_from_path`/`from_hex` helpers;
  all-zero and malformed keys are rejected without leaking material into
  errors. Adds the installable `ops/systemd/katpool-hardening.conf` drop-in
  (sops/age -> `systemd-creds` bridge, full OS isolation) and corrects the
  custody docs' credential-delivery description. This is the workspace's one
  sanctioned home for `unsafe` (FFI `mlock`/`munlock`), per ADR-0008.
- Phase 4 milestone 4.4 (M4.4): restart-safe KAS payout cycle state machine
  (`payout_kas::cycle`). `resume_or_plan_kas_cycle` is an idempotent entry
  point that resumes an existing cycle without recomputing eligibility;
  `CycleState::pending` exposes only `planned` recipients (never re-pays an
  in-flight or settled row); the pure `derive_cycle_status` folds recipient
  statuses into the cycle status, persisted by `reconcile_cycle_status` with
  `audit_log` hooks. Adds `repo::payout::mark_payout_accepted`. Chaos test
  proves no double-pay on crash-after-plan-before-broadcast. Idempotency
  rests on natural DB keys (`payout_cycle.idempotency_key` +
  `payout UNIQUE (cycle_id, wallet_id)`), not a side table.
- Phase 4 milestone 4.3 (M4.3): KAS payout eligibility query
  (`repo::payout::list_kas_eligible_wallets`), idempotent `ensure_payout`,
  and `payout_kas::plan_kas_cycle` (DB-only cycle planning).
- `scripts/ci-fast.sh` and `scripts/ci-local.sh` — local parity with gating CI
  jobs (`fmt`, `clippy --locked`, `doc`; full test + deny in `ci-local`).
- Phase 4 milestone 4.2 (M4.2): `katpool_storagemass::plan_batches` — greedy,
  mass-aware payout batch planner (`TreasuryUtxo`, `PayoutRecipient`,
  `PlannedBatch`); defers outputs below `MIN_PAYOUT_OUTPUT_SOMPI`; re-injects
  change as planning-only virtual UTXOs for multi-batch plans; unit +
  `proptest` invariants. Execution-layer live UTXO refresh documented in
  `docs/kips.md` §5.4 (`payout-kas`, M4.6+).
- Phase 4 milestone 4.1 (M4.1): `katpool-storagemass` mass evaluator wrapping
  `kaspa-consensus-core` (compute + KIP-9 storage + KIP-13 transient);
  `docs/phase-4-acceptance.md` acceptance matrix.
- Phase 3 milestone 4 (M4): production-log replay-determinism harness.
  - `accountant::replay` — NDJSON load, DB snapshot, dual-replay
    `verify_dual_replay`.
  - `katpool-replay` binary — replay NDJSON or legacy monitoring
    logs through the accountant (`--events`, `--legacy-log`,
    `--subsample-nth`).
  - `KATPOOL_EVENT_RECORD_PATH` on the unified `katpool` runtime
    (append-only NDJSON `PoolEvent` capture).
  - `accountant/tests/replay_harness_scale.rs` — CI dual-verify at
    ~1:50 synthetic scale.
  - `scripts/replay-determinism-rehearsal.sh` + runbook 17.

### Changed

- **Kaspa dependency migration to `tn10-toc3` (testnet-10 Toccata
  hardfork).** Bumped all `kaspa-*`/`kaspad` git tags from `v1.1.0` to
  `tn10-toc3` (`1.2.1-toc.3`, commit `1015a62`) and the Rust toolchain to
  `1.91.0` (`rust-toolchain.toml`, `Cargo.toml` `rust-version`,
  `clippy.toml` `msrv`, all six CI `toolchain:` pins, and the standalone
  `bridge/fuzz` crate). This resolves `RuleError::BadMerkleRoot` on every
  block the pool found: the `v1.1.0` crates dropped the new consensus
  transaction fields during the RPC round-trip while the live node ran
  toc3. Adapted to the Toccata consensus API across `katpool-storagemass`,
  `payout-kas`, `payout-krc20`, and `accountant`: `TransactionInput`/
  `TransactionOutput`/`RpcTransactionOutput` construction, the new
  `UtxoEntry.covenant_id` field, the per-dimension `BlockMassLimits`
  model, and the revised `TxScriptEngine::from_transaction_input`
  signature (now wired with `EngineContext` + `EngineFlags`
  `{ covenants_enabled, zk_hardening_enabled }` to mirror the post-fork
  node engine). Removed the now-unused `serde_nested_with`
  `[patch.crates-io]`. Reconciled `deny.toml` for the new subgraph (allow
  `0BSD`; allow the `workflow-perf-monitor-rs` git source; pruned 13
  advisory ignores and stale license entries no longer present).
  Added `scripts/set-kaspa-version.sh` to automate the high-fan-out crate
  pin, governed by [ADR-0017](docs/decisions/0017-kaspa-version-pinning.md)
  and [Runbook 20](docs/runbooks/20-kaspa-version-bump.md).
- CI: coverage job runs on `main` pushes only (informational; was ~23 min per PR).

### Fixed

- Stratum reject-rate regression on the tn10 soak (Goldshell + BzMiner
  v14.0.2 reported ~80% rejected): the unified `katpool` runtime was
  pinning every connection at `min_share_diff` with the bridge's
  variable-difficulty retarget loop disabled (`var_diff: false`,
  `shares_per_min: 0`), and `KATPOOL_MIN_SHARE_DIFF` defaulted to 1.
  An ASIC told to mine at difficulty 1 floods low-difficulty shares
  that go stale against the Toccata 10 BPS template rotation — visible
  in the bridge logs as a steady stream of `Timestamp is old: 80–142s`
  warnings and stale-share counts. `katpool/src/main.rs` now surfaces
  the bridge's vardiff knobs as `KATPOOL_VAR_DIFF` (default `true`)
  and `KATPOOL_SHARES_PER_MIN` (default `20`), raises the
  `KATPOOL_MIN_SHARE_DIFF` default from `1` to `4096` (ASIC-class
  floor), and threads all three through `BridgeServerConfig` so the
  bridge's existing `start_vardiff_thread` retarget loop
  (`bridge/src/share_handler.rs`) converges each miner toward 20
  shares/min instead of pinning at the floor. The repo env templates
  (`ops/env/tn10.env.example`, `ops/env/mainnet.env.example`) document
  the new vars. Verified on the live tn10 soak: stale-share +
  "Timestamp is old" warnings collapsed from ~30/min to 0, observed
  orphan rate dropped from ~50% (recent 100%) to 0% over the first
  3.5 min post-deploy (51/51 blocks confirmed BLUE, 45 coinbase rewards
  allocated end-to-end). Picking up the post-v1.1.0 upstream stratum
  fixes (e.g. rusty-kaspa #877, #1033, #1014, #1016, #1023) by
  re-vendoring `bridge/` from current upstream master is tracked as
  a separate ADR + runbook.

- Phase 3 milestone 3f: three production-grade defects uncovered by
  the Goldshell live exercise on 2026-05-27 (see
  `docs/phase-3-acceptance.md` §M3f). All three fixes ship together
  because they only become observable end-to-end once Defect 2
  unblocks DB writes:
  - **Defect 2 (`wallet_ensure` upsert failure, 9,775 occurrences):**
    `accountant::ConsumerConfig` previously hard-coded the schema
    network to `mainnet`, causing every `wallet::ensure` on a
    testnet run to fail the
    `wallet.wallet_address_format` CHECK constraint
    (`crates/katpool-db/migrations/20260526000000_bootstrap.sql`).
    `ConsumerConfig::new` now takes a validated `network: String`
    (checked against
    `accountant::consumer::VALID_NETWORKS = ["mainnet",
    "testnet-10", "testnet-11", "devnet", "simnet"]` at
    construction time); the unified `katpool` runtime derives the
    default from the pool address bech32 prefix (`kaspa:` →
    `mainnet`, `kaspatest:` → `testnet-10`) with `KATPOOL_NETWORK`
    as the operator override (required for `testnet-11`,
    `devnet`, `simnet`, which share prefixes with other targets).
    Five regression tests pin the validation surface, including a
    lock-step contract test against the migration's CHECK
    constraint list.
  - **Defect 3 (`orphan_block_accepted`, 4,853 occurrences):
    auto-resolved.** Was a downstream consequence of Defect 2 —
    when `BlockFound` failed `wallet::ensure`, the `block` row
    never inserted, so the subsequent `BlockAccepted` event had
    no prior `found` row and the accountant correctly refused
    the orphan. Fixing Defect 2 restores the lifecycle invariant
    without touching the bridge.
  - **Defect 1 (phantom `BLOCK ACCEPTED` log on kaspad reject,
    3,849 occurrences):** the bridge's
    `KaspaApi::submit_block` matched only `Ok(_)` on the gRPC
    response and ignored `SubmitBlockResponse::report`, which
    carries the actual acceptance verdict. The pre-M3f code
    logged "🎉🎉🎉 BLOCK ACCEPTED BY NODE!" and emitted
    `PoolEvent::BlockAccepted` for every `Reject(BlockInvalid)`
    / `Reject(IsInIBD)` / `Reject(RouteIsFull)` — 79% of all
    submissions during the Goldshell live exercise.
    `submit_block` now returns a typed
    `BlockSubmitOutcome { Accepted(SubmitBlockResponse) |
    RejectedByNode(SubmitBlockRejectReason) }`, so the
    share-handler can credit the miner's share (their PoW met
    the network target by construction) while suppressing the
    phantom `BlockAccepted` event. `Err` is reserved for
    genuine RPC failures plus `ErrDuplicateBlock` (mapped to
    `ShareRejectReason::Stale` as before). Operator-visible
    labels (`BlockInvalid`, `IsInIBD`, `RouteIsFull`) are
    pinned by a contract test so dashboards / runbooks can
    filter on stable strings. *The first M3f cut collapsed
    `Reject(_)` into `Err` directly; that over-correction spiked
    the miner-visible reject rate to ~68% during the Goldshell
    cut-1 verification run because the share-handler's existing
    `Err` arm classified the outcome as
    `ShareRejectReason::BadPow`. The typed-outcome refactor in
    cut 2 fixes the regression without bringing back the
    phantom accept.*

### Added

- Phase 3 milestone 3f operator hardening: `RuntimeConfig.network`
  field plus the `KATPOOL_NETWORK` env override (see Fixed → Defect
  2). Startup tracing now includes `network=<value>` so the live
  acceptance evidence captures the exact network identifier used
  for every `wallet::ensure` in the run.
- Phase 0 milestone 1: cargo workspace bootstrap with 14 crates pinning
  Rust 1.88 and edition 2024, strict workspace-wide lint configuration
  (forbid `unsafe_code`; deny `unwrap` / `expect` / `panic` / `indexing` /
  `float_arithmetic` / `print_stdout` / `print_stderr` / `todo` /
  `unimplemented` / `dbg_macro` / `integer_division`).
- Phase 0 milestone 2: `rustfmt.toml`, `clippy.toml`, and `deny.toml`
  enforcing 100-column lines, MSRV-aware clippy, strict licence allowlist
  (Apache-2.0 / MIT / ISC / BSD-2/3 / Unicode-3.0 / Zlib / CDLA-Permissive-2.0),
  `unknown-registry = deny`, `unknown-git = deny`, ban list for
  openssl/native-tls/git2/actix-web with redirects, controlled
  `skip-tree` for the sqlx, opentelemetry, rand, and config families.
- Phase 0 milestone 3: repository governance (this changelog, `SECURITY.md`,
  `README.md`, dual `LICENSE-MIT` and `LICENSE-APACHE`, root `CODEOWNERS`,
  pull-request and issue templates, `.github/branch-protection.md`
  documenting the required `main`-branch settings).
- Phase 0 milestone 4: documentation scaffold. Authoritative
  references for `architecture.md`, `threat-model.md` (STRIDE),
  `custody.md` (sops/age + OS-level isolation operational design),
  `kips.md` (KIP-9 and KIP-13 implementation reference), `capacity-plan.md`
  (measured NetCup specs, budgets, sizing triggers), `onboarding.md`,
  `cutover-plan.md`. Nine ADRs (MADR 4.0 format) cover every Phase 0
  architectural decision. Eleven runbooks cover the named incident
  classes with a uniform Symptom / Confirm / Diagnose / Remediate /
  Verify / Post-incident structure.
- Phase 0 milestone 5: CI workflows. `ci.yml` runs fmt, clippy
  (`-D warnings`), test, cargo-deny, cargo-audit, cargo doc, and
  cargo-tarpaulin coverage on every push and PR. `release.yml`
  builds a static musl binary, generates a CycloneDX SBOM via
  syft, signs both via cosign keyless (OIDC), and publishes a
  draft GitHub release. `security.yml` runs weekly cargo-audit,
  cargo-deny, and Trivy filesystem scans. Every third-party
  action is pinned by full commit SHA with a trailing comment
  naming the human-readable tag.
- Phase 1 milestone 1: vendored rusty-kaspa v1.1.0 stratum bridge
  under `bridge/`, with a documented local-divergence register
  (`bridge/UPSTREAM.md`), per-directory `rustfmt.toml` matching
  upstream style, bridge-local lint overrides, dual workspace
  build (our strict pedantic-and-nursery rules for new crates,
  upstream-tolerant for the vendored bridge), and re-vendoring
  procedure documented for future v1.x bumps.
- Phase 3 milestone 3e (custodial PROP pool coinbase override):
  the bridge's block-template path now routes every coinbase to
  the pool's address regardless of which miner authorized.
  Required to make M3d's live test representative of the
  production design — without it the bridge runs in solo /
  MM-pool mode where each miner mines to themselves and the pool
  never takes custody.
    - `KaspaApi::new` accepts a new
      `coinbase_address_override: Option<Address>` constructor
      argument. When `Some`, every `get_block_template` call
      replaces the miner-supplied `wallet_addr` with the pool's
      address before calling kaspad. When `None`, preserves
      upstream solo / MM-pool behaviour byte-for-byte. The
      override-or-fallback logic is extracted into a pure
      `resolve_coinbase_recipient` helper with 4 dedicated unit
      tests covering both branches plus malformed-address edge
      cases.
    - Bridge's own `main.rs` passes `None` (single-line
      divergence: 1 added arg). Standalone bridge binary keeps
      identical behaviour to upstream.
    - `katpool/src/main.rs` parses
      `KATPOOL_POOL_ADDRESS` (first entry; warns on multi-address
      configs since the bridge override takes only one), passes
      it to both the bridge (`KaspaApi::new` override) and the
      accountant (`KaspadGrpcClient::pool_addresses`). One env
      var, single source of truth for "what is the pool's
      address" across both subsystems.
    - Runbook 16 rewritten to drop the "addresses must match"
      workaround. Worker name on the ASIC is now the **miner's**
      address (the production pattern: miners receive their
      payout share at the address they authorize with). Pool
      address is the separate `KATPOOL_POOL_ADDRESS` env var.
      Documents that the two addresses are **expected to
      differ**.
    - `bridge/UPSTREAM.md` gains two new rows: `src/kaspaapi.rs`
      (override + helper + tests) and `src/main.rs` (the
      one-line constructor-arg addition).
    - Dry-run on the VPS validates the override fires:
      `INFO Coinbase recipient override active: every block
      template will pay kaspatest:qz...` appears at startup.
- Phase 3 milestone 3d (unified runtime): the `katpool` binary
  embeds the stratum bridge + the accountant event consumer +
  the maturity tracker into one process with a shared
  `tokio::sync::broadcast<PoolEvent>` channel. This is the
  binary the operator points a testnet ASIC at for the M3d
  acceptance test.
    - Bridge: new `listen_and_serve_with_events` public function
      (a small UPSTREAM divergence captured in
      `bridge/UPSTREAM.md`) that accepts an optional
      `broadcast::Sender<PoolEvent>` and wires it into
      `ShareHandler::with_event_bus`. The original
      `listen_and_serve` is preserved as a thin wrapper passing
      `None`, so the bridge's own `main.rs` keeps the upstream
      call shape.
    - `katpool/src/main.rs`: env-configurable Phase 7 wiring
      binary, partially active in M3d. Subsystems:
        1. stratum bridge (listener on `KATPOOL_STRATUM_PORT`,
           talks to kaspad via `KaspaApi`)
        2. accountant `EventConsumer::run` against the shared
           broadcast channel
        3. `MaturityTracker::run_loop` against
           `KaspadGrpcClient` connected to the same kaspad
      All three share a `tokio::sync::watch::Receiver<bool>`
      for graceful shutdown. Required env vars:
      `KASPAD_GRPC_URL`, `KATPOOL_DATABASE_URL`,
      `KATPOOL_POOL_ADDRESS`, `KATPOOL_STRATUM_PORT`.
    - `katpool/Cargo.toml`: replaces the scaffold-machete
      ignore with explicit per-phase ignores (Phase 4
      `payout-kas`, Phase 5 `payout-krc20`, Phase 6 `api`,
      Phase 7 telemetry/secrets/config still-unwired). Each
      future-phase PR drops its own entry as it activates.
    - `scripts/testnet10-full-pipeline-live.sh`: operator-
      facing full-pipeline live exercise. Stands up throwaway
      Docker Postgres, runs the `katpool` binary on a free
      stratum port, captures wallet/share/block/allocation
      counts into a timestamped artefact directory. Exit codes
      distinguish "shares + blocks observed" / "shares only" /
      "no activity".
    - Runbook 16 (`docs/runbooks/16-testnet10-full-pipeline-live.md`):
      preconditions, ASIC config syntax, success criteria,
      exit-code table, cleanup, and what-to-paste-into-the-
      acceptance-ticket.
    - `docs/phase-3-acceptance.md` updated with the M3d
      dry-run evidence (both runs: the pre-fix shutdown-
      ordering bug surface and the post-fix validation).
    - Dry-run on the VPS surfaced a real shutdown-ordering
      bug: consumer hung waiting for `broadcast::Sender`
      clones held by the bridge's internal kaspad-notification
      tasks. Fixed by aborting the consumer JoinHandle
      directly rather than draining it; at-most-once delivery
      is the design contract so dropping in-flight events on
      shutdown is correct. Clean exit measured at 218
      microseconds from SIGTERM.
- Phase 3 milestone 3c (accountant: real kaspad gRPC client +
  tracker-only live exercise): the real `KaspadClient` impl
  backed by `kaspa-grpc-client` so the maturity tracker (M3b)
  reaches a real `kaspad-tn10` instead of the in-memory fake.
    - `accountant::kaspad_grpc::KaspadGrpcClient`: thin
      translation layer over `kaspa-grpc-client`. Two methods:
      `get_virtual_blue_score` (via `RpcApi::get_sink_blue_score`)
      and `get_block` (via `get_block_call` with
      `include_transactions: true`).
    - `accountant::kaspad_grpc::extract_block_info`: pure
      function that turns an `RpcBlock` + configured pool
      addresses into the abstract `BlockInfo`. Sums the
      coinbase tx's outputs whose
      `verbose_data.script_public_key_address` matches a
      configured pool address. Defends against missing verbose
      data, hash mismatch between request and response,
      zero-transaction blocks, and i64-overflowing sums.
      11 unit tests against canned RpcBlock fixtures.
    - `is_block_not_found` heuristic. Matches several wordings
      observed in the wild — including the kaspad v1.1.0 /
      Toccata `"cannot find header <hash>"` response that the
      M3c dry-run caught and we patched in the same PR.
    - New binary `accountant-tracker-runner`
      (`accountant/src/bin/accountant-tracker-runner.rs`):
      env-configurable runner that wires the kaspad client +
      DB pool + StaticTierClassifier + AllocationEngine +
      MaturityTracker into a SIGTERM-aware loop. Required by
      runbook 15.
    - `scripts/testnet10-tracker-live.sh`: operator-facing
      live-exercise script. Stands up throwaway Docker
      Postgres, applies migrations, seeds a known testnet
      block, runs the binary for N seconds, captures evidence
      (manifest.json + tracker.log + db-final.txt).
    - Runbook 15 `docs/runbooks/15-testnet10-tracker-live.md`:
      preconditions, command, success criteria, exit-code
      table, cleanup, what-to-paste-into-the-acceptance-ticket.
    - `docs/phase-3-acceptance.md`: new Phase 3 acceptance
      evidence page modelled on the Phase 1 and Phase 2
      siblings. Includes the M3c dry-run evidence (both runs:
      the pre-fix bug surface and the post-fix validation) and
      cross-references the remaining gates (M3d, M4) that close
      out Phase 3.
- Phase 3 milestone 3b (accountant: block maturity tracker):
  the kaspad-watching loop that drives the M3 allocation engine
  on every matured block. Closes the loop from `submitted_to_node`
  through `confirmed_blue` to `matured` (or `orphaned` on DAG
  re-org).
    - `accountant::maturity::KaspadClient` trait — minimal
      surface (`get_virtual_blue_score`, `get_block`) over the
      kaspad gRPC surface, so the tracker's state machine has
      deterministic test coverage against an in-memory fake
      (`FakeKaspad`). Real `KaspadGrpcClient` impl deferred to
      M3c per ADR-0014 § 2.
    - `accountant::maturity::MaturityTracker::{run_once,
      run_loop}` — single polling sweep + cancellation-aware
      loop driven by `tokio::sync::watch`. Operator-tunable
      `MaturityConfig { poll_interval, maturity_depth,
      window_daa_span, batch_size }`. Defaults: 15s polling,
      100-block depth (matches Kaspa's coinbase_maturity), 600
      DAA window (~60s at 10 BPS), 200 blocks/sweep batch limit.
    - State transitions: submitted→confirmed_blue when kaspad
      reports `is_blue`; confirmed_blue→matured when
      `virtual_blue_score - block.blue_score ≥ maturity_depth`
      (calls the engine atomically); confirmed_blue→orphaned
      when kaspad has lost the block OR reports it as red. The
      `matured` path hands off to
      `AllocationEngine::allocate_matured_block` with the
      window `[block.daa_score - window_daa_span,
      block.daa_score)`.
    - Per-block error isolation: a single kaspad/DB failure
      logs + counts in `SweepStats.errors` and the sweep
      continues. Whole-sweep failures (kaspad transport down)
      bubble out as `TrackerError` but `run_loop` catches +
      retries at the next interval — the loop never dies.
    - 11 integration tests against ephemeral Postgres +
      in-memory `FakeKaspad`: confirmed-blue happy path; stays
      when kaspad doesn't know the block; stays when block is
      red; matures + triggers engine when depth reached; waits
      when depth insufficient; orphans on missing-from-DAG;
      orphans on reorg-to-red; whole-sweep fails on
      virtual-blue transport error; per-block error isolated;
      batch_size enforces per-sweep limit; run_loop exits
      cleanly on shutdown signal.
    - ADR-0014 captures the polling-vs-subscription decision,
      the kaspad-trait abstraction rationale, the window-size
      policy (independent from coinbase maturity), reward
      extraction as M3c's concern, error-isolation strategy,
      and shutdown via `watch`.
- Phase 3 milestone 3 (accountant: PROP allocation engine + HTTP
  tier classifier + audit-trail migration): the money-math
  milestone. Closes the loop from "block confirms blue" through
  "every contributing wallet has its share row written + NACHO
  rebate accrued" in one transactional engine call.
    - Migration `20260527000001_wallet_tier_audit.sql`:
      `wallet_tier` postgres enum (`standard`, `elite`) + three
      audit-trail columns on `share_allocation`
      (`applied_topline_bps`, `applied_rebate_bps`,
      `applied_tier`) with CHECK-constrained bps ranges. Every
      allocation row is now self-describing — historical
      allocations stay reproducible across future operator
      changes to `KATPOOL_FEE_TOPLINE_BPS`. Round-trip parity
      test added.
    - `repo::share_allocation::DbWalletTier`,
      `NewAllocation`/`ShareAllocation` extended with the audit
      fields; the `insert_batch` UNNEST INSERT widened to bind
      them.
    - `accountant::KasplexTierClassifier` (~340 LOC):
      `reqwest`-backed HTTP client against
      `krc721.kat.foundation` (NFT ownership) and
      `api.kasplex.org` (KRC-20 NACHO balance). OR semantics —
      either dimension qualifies the wallet as `Elite`.
      `locked` counts toward the threshold (documented in
      ADR-0012). In-process TTL cache (5-min default) keyed by
      wallet address; aggressive 2s connect-timeout so a degraded
      kasplex doesn't stall block-maturity allocation. Both
      calls run in parallel; on any error the classifier falls
      back to `Standard` (safe direction per ADR-0012's "never
      over-rebate from a transient upstream failure").
      Numeric parsing is integer-only (kasplex returns balance
      values as strings to dodge JS's safe-int limit; we parse
      to `u128`).
    - `accountant::AllocationEngine::allocate_matured_block`:
      transactional orchestrator. Closes the share window,
      reads per-wallet rollups, pro-rates the coinbase reward
      across wallets (`floor(reward × weight / total_weight)`),
      classifies each wallet's tier, runs
      `FeeConfig::compute_allocation`, batches
      `share_allocation` inserts, accrues
      `nacho_rebate` additively, advances the block to
      `matured` with the reward recorded, and appends an
      `audit_log` entry. All inside one Postgres transaction.
      Truncation residue (max N-1 sompi) awarded
      deterministically to the smallest-wallet_id contributor.
      Idempotent: re-calling on a `matured` block is a no-op
      returning `AllocationOutcome::AlreadyAllocated`. Empty
      windows produce `NoContributingWallets` with the reward
      retained by the pool and audited.
    - 20 new tests (accountant suite now 90+):
        - 10 `kasplex_classifier` against wiremock-mocked
          endpoints: NFT-only elite, KRC-20-only elite,
          locked-counts-toward-threshold, just-below-threshold
          standard, empty result, both-5xx fallback, mixed
          5xx + true, cache hit, malformed JSON fallback, clear
          cache forces refetch.
        - 10 `allocation_engine` integration tests against
          ephemeral Postgres: happy path with sum invariant
          assertion, mixed-tier elite-dominates, block status
          advances, rebate accrues additively across blocks,
          idempotent replay, empty window, unknown hash, wrong
          status, negative reward rejection, audit_log entry
          shape.
    - ADR-0012 updated with the OR semantics, the
      locked-counts-toward-threshold decision, the kasplex API
      endpoints used, and the audit-trail migration status.
- Phase 3 hardening: verification-posture pass before M3.
  Closes the gaps identified in a self-audit of "are we
  *deterministically verifying* what we ship, or cargo-culting
  good practice".
    - `FeeConfig::compute_allocation` — the per-block allocation
      math, lifted out of M3's planned scope so M2 can land it
      now with full proptest coverage. Integer-only throughout;
      truncation residues stay with the pool so the balance
      equation `gross == pool_fee + nacho_accrual + net_payout`
      holds exactly. Returns typed `AllocationError` for
      negative gross / overflow / balance-check failure.
    - `accountant/tests/allocation_properties.rs` (13 tests) —
      proptest over `(gross, topline_bps, tier)` for: balance
      equation, non-negativity, tier monotonicity, topline
      monotonicity, audit-trail faithfulness, elite-rebate-
      always-100%, standard-rebate-≈33%, boundary cases. Caught a
      genuine overflow at the i64 limit during initial run —
      surfaced as `AllocationError::Overflow { stage: "fee_share" }`
      now covered by a boundary test.
    - `crates/katpool-db/tests/enum_parity.rs` (6 tests) —
      round-trips every variant of every `sqlx::Type` enum
      (`payout_kind`, `payout_cycle_status`, `payout_status`,
      `krc20_transfer_status`, `block_status`,
      `share_reject_reason`) through a typed temporary table.
      Exhaustiveness-guard `match`es fail the build when a Rust
      variant is added without extending the round-trip loop.
    - `accountant/tests/replay_determinism.rs` (2 tests) — feeds
      an identical event stream to two independent consumers
      (separate Postgres instances) and asserts byte-equal
      content in every table the consumer wrote.
    - CI gates: added `typos` (kaspa-tuned `_typos.toml`) and
      `cargo machete` (workspace-scoped unused-dep detection).
      Both are blocking. The machete run identified pre-existing
      drift in 15 crates' Cargo.tomls; cleaned up the active
      crates (accountant, katpool-db, katpool-import-legacy)
      and added documented `[package.metadata.cargo-machete]
      ignored` blocks to scaffold crates with comments naming the
      phase that activates each dep.
    - Genuine typo fix: `unparseab` → `unparsab` in 5 files
      (function names, doc comments, runbook text).
    - ADR-0013 (`docs/decisions/0013-verification-posture.md`)
      documents the project's seven verification layers and the
      explicit out-of-scope items.
- Phase 3 milestone 2 (accountant: window aggregation + reject
  persistence + per-miner stats): the read-side primitives the
  Phase 6 HTTP API will compose, plus the pre-aggregation that
  M3's PROP allocation engine reads instead of scanning the
  live `share` table per block.
    - Migration `20260527000000_share_reject.sql` introduces the
      `share_reject_reason` postgres enum (variants byte-for-
      byte match `ShareRejectReason::as_str()`) and the
      `share_reject` table with three indexes (worker-time,
      wallet-time, reason-time) for the three canonical access
      patterns.
    - `repo::share_reject` — `insert`, `list_for_wallet`,
      `count_by_reason_for_wallet`, `count_by_reason_pool_wide`,
      plus a `TryFrom<ShareRejectReason>` mapping that
      deliberately rejects unknown upstream variants so the
      build fails until a paired migration ships (defends
      against the `#[non_exhaustive]` enum drift).
    - `repo::share_stats` — read-only aggregations:
      `accepted_for_wallet`, `accepted_pool_wide`,
      `hashrate_estimate_for_wallet` / `_pool_wide`
      (`weight * 2^32 / window_secs` convention), and
      `accepted_and_rejected_for_wallet` (one-round-trip
      summary for the `/miner/{addr}` API endpoint).
    - `accountant::WindowAggregator::close_window` — closes a
      half-open `[daa_start, daa_end)` range with a single
      transactional `INSERT ... SELECT ... GROUP BY` over
      `share`, materialising one `share_window` row per
      contributing wallet. Idempotent via the table's
      `UNIQUE (wallet_id, daa_start, daa_end)` plus
      `ON CONFLICT DO UPDATE` that refreshes
      `total_weight` / `share_count` / `ended_at` while
      preserving the original `started_at`.
    - Consumer wires `ShareRejected` → `share_reject` rows in
      addition to the existing metric tick. Unknown-reason
      events still tick the metric but skip the insert.
    - 14 new tests (5 window_aggregator + 4 share_reject + 5
      share_stats), bringing the accountant suite to 33 tests
      (11 unit + 22 integration).
- Phase 3 milestone 1 (accountant scaffold + event ingestion):
  the pool accountant's foundation — event consumer, fee model,
  wallet-tier classification framework. Subsequent Phase 3 PRs
  layer share-window aggregation (M2), PROP allocation (M3), and
  replay-determinism harness (M4) on top.
    - `accountant::EventConsumer` — `tokio::sync::broadcast::Receiver<PoolEvent>`
      consumer that writes `wallet`/`worker`/`share`/`block` rows
      via the repo layer. Handles lag (skip + metric), channel
      close (clean shutdown), per-event errors (log + metric, no
      task death), and BlockFound idempotency via the new
      `repo::block::ensure` helper (`INSERT ... ON CONFLICT (hash)
      DO UPDATE` returning a (`BlockId`, `EnsureOutcome`) pair).
    - `accountant::FeeConfig` — operator-tunable topline fee via
      `KATPOOL_FEE_TOPLINE_BPS` (basis points integer; default 75
      = 0.75%; max 1 000 bps to guard against typos). Pure
      `from_lookup` constructor takes a lookup closure, so tests
      exercise parse/validation without touching process env (the
      workspace forbids `unsafe_code` and edition-2024 `set_var`
      is now unsafe).
    - `accountant::WalletTier` — `Standard` (33% rebate of fee)
      and `Elite` (100% rebate of fee), with rebate ratios fixed
      in code (per ADR-0012). Defined as both Rust enum and
      `sqlx::Type` against a `wallet_tier` postgres enum that
      lands with M3's migration.
    - `accountant::TierClassifier` trait + `StaticTierClassifier`
      stub. HTTP-backed `KasplexTierClassifier` deferred to M3
      where the allocation engine actually needs tier resolution.
      On any classifier error the safe fallback is `Standard`.
    - Prometheus metrics: `ks_accountant_events_total`,
      `ks_accountant_events_lagged_total`,
      `ks_accountant_event_errors_total`,
      `ks_accountant_share_inserts_total`,
      `ks_accountant_block_transitions_total`. Every metric
      carries an `instance` label for primary-vs-shadow
      disambiguation during the Phase 7 shadow-run window.
    - 11 unit tests + 8 integration tests against ephemeral
      Postgres (testcontainers) covering: share path, block
      lifecycle, BlockFound idempotency, orphan BlockAccepted,
      lag tolerance, clean shutdown, share-rejected metric-only
      semantics, and weight aggregation.
    - ADR-0012 (`docs/decisions/0012-fee-model-and-tier-classification.md`)
      capturing the fee model, basis-points env knob, tier-at-
      maturity decision, deferred migration plan, and audit-trail
      column rollout strategy.
- Phase 2 milestone 4 (importer acceptance): scale + property
  tests for the legacy importer, the operator rehearsal wrapper,
  and the Phase 2 acceptance evidence page.
    - `katpool-import-legacy/tests/import_scale.rs` — two
      entry points: `scale_acceptance_ci_default` (1K blocks,
      ~7 s, runs unconditionally) and
      `scale_acceptance_local_rehearsal` (10K blocks, ~50 s,
      `#[ignore]`d for local rehearsal). Both end in a reconcile
      pass and a throughput sentinel that catches regressions
      that would blow the 30-minute cutover budget. Measured
      throughput: ~2.4 ms/block, linear in row count.
    - `katpool-import-legacy/tests/import_properties.rs` — 5
      cross-cutting invariant tests: rerun-with-new-rows
      converges; rebate `set_accrual` overwrites (not
      accumulates); partial-failure restart safety;
      reconcile-detects-legacy-mutation-after-import.
    - `scripts/legacy-importer-rehearsal.sh` — operator wrapper
      script (dry-run by default, `--no-dry-run` for cutover
      hot-run). Captures reconcile JSON, tracing log,
      audit-log snapshot, and a manifest containing git rev +
      binary sha256 into a timestamped artefact directory.
      Required by the cutover ticket.
    - `docs/runbooks/14-legacy-importer.md` updated to recommend
      the rehearsal script as the primary invocation path; the
      raw binary command is now documented as a fallback only.
    - `docs/cutover-plan.md` T-2m step rewritten to reference
      the rehearsal script + the `manifest.reconcile_all_passed`
      gate, replacing the obsolete path it inherited from the
      original plan.
    - `docs/phase-2-acceptance.md` — Phase 2 acceptance matrix
      modelled on the Phase 1 sibling: 12 acceptance rows
      cross-referenced to PRs, scale-run history with
      empirical timings, full check inventory for the
      reconciliation pass.
- Phase 2 milestone 3 (importer, part B): the four remaining
  legacy-table transforms wired into `katpool-import-legacy`,
  plus the cross-table reconciliation pass:
    - `transform::balances` — `miners_balance.nacho_rebate_kas` →
      `nacho_rebate_accrual.accrued_sompi` via the new
      `repo::nacho_rebate::set_accrual` (idempotent SET, distinct
      from the additive `accrue`).
    - `transform::payments` — `payments` rows grouped by
      `transaction_hash` → one `payout_cycle (kind=kas)` per group,
      one `payout` per recipient, idempotent on
      `UNIQUE (cycle_id, wallet_id)`. Synthetic `daa_start=0,
      daa_end=1` because legacy never tracked DAA range; cycles
      identified by `idempotency_key = 'kas-legacy-<tx_hash>'`.
      Cycle is brought to `settled` status atomically.
    - `transform::nacho_payments` — same shape as `payments` but
      with `kind=krc20_nacho` and `idempotency_key =
      'krc20-legacy-<tx_hash>'`. Stores `krc20_commit_hash` +
      `krc20_reveal_hash` (the legacy `transaction_hash` doubles
      as both since legacy didn't split commit/reveal).
    - `transform::krc20` — `pending_krc20_transfers` → singleton
      `payout_cycle` per row + `payout` + `krc20_pending_transfer`,
      with full status mapping (`PENDING`/`COMPLETED`/`FAILED` →
      `pending`/`completed`/`failed`). Failed rows carry a
      `failure_reason` for forensics.
    - `reconcile` — post-import cross-aggregate pass: row counts,
      monetary totals, per-status counts. Importer exits with code
      `2` on any mismatch so CI / runbook scripts can detect
      reconciliation failure without parsing stdout. Reconcile
      runs even in `--dry-run` mode (read-only).
    - Operator runbook ([14-legacy-importer.md](docs/runbooks/14-legacy-importer.md))
      documenting the dry-run / cutover / restart flows.
    - 16 new integration tests against ephemeral Postgres
      (testcontainers), in addition to the 6 existing
      `import_blocks` tests; full importer suite now 26 tests.
- Phase 2 milestone 3 (importer, part A): new
  `katpool-import-legacy` binary crate at the workspace top level
  that walks the previous-generation pool's `katpool_mainnet`
  database and writes into the new schema. This commit ships the
  scaffold and the `block_details` → `(wallet, worker, block)`
  transform, which is the largest single table to migrate
  (production: 539,397 rows). Subsequent commits in this series
  add the remaining transforms (`miners_balance` →
  `nacho_rebate_accrual`; `payments` + `nacho_payments` →
  `payout_cycle` + `payout`; `pending_krc20_transfers` →
  `krc20_pending_transfer`).
  Importer properties:
    - **Idempotent.** Every write goes through an `ON CONFLICT DO
      NOTHING` path or the repo layer's `ensure`-style UPSERT.
      Re-running zero-cost; classified as `skipped` in stats.
    - **Deterministic correlation ids.** UUID v5 derived from the
      block hash (DNS namespace) so audit-log forensics are
      reproducible across re-imports.
    - **Validation-first.** A pure `parse_legacy_row` produces a
      typed `Parsed` or returns a static reject reason; persistence
      is a separate function. Soft rejections (bad bech32, bad
      worker name, daa not parseable, hash not 64-char hex) bump
      the `rejected` counter; hard errors (connection lost) bubble
      up and abort.
    - **Resumable.** Source-side cursor is `(timestamp,
      mined_block_hash)` so a restart on row 200,000 resumes
      without rescanning the first 199,999.
    - **Dry-run mode.** Counts what would have been written without
      touching the target. Useful for pre-cutover sanity checks.
    - **JSON reconciliation report** on stdout when the binary
      completes; structured `tracing` events on stderr. The JSON
      contract is what the cutover runbook will pipe into the
      evidence collection.
  Six integration tests in
  `katpool-import-legacy/tests/import_blocks.rs` spin up a single
  postgres testcontainer with two databases on it (`legacy_test` +
  `target_test`), seed the legacy schema from
  `tests/fixtures/legacy_schema.sql`, and assert:
  insert+idempotent-skip, wallet/worker creation, matured block
  status with the right reward, deterministic correlation-id
  reproducibility, rejection of bad rows (5 distinct failure
  modes), dry-run-writes-nothing. Workspace test count: 133 → 139.
  `uuid` workspace dep gains the `v5` feature; `clap` workspace
  consumers can opt into the `env` feature individually (importer
  does).
- Phase 2 milestone 3 (prep): seven additional repository aggregates
  to complete the schema's query surface ahead of the legacy
  importer. New modules:
    - `repo::pool_meta` — single-row key/value store; `get` /
      idempotent `set` with `updated_at` refresh
    - `repo::connection_session` — per-stratum-TCP-session record;
      `open` / `bind_worker` / `close` / `increment_counters` /
      `list_for_worker`. Maps the postgres `INET` column to
      `String` at the Rust boundary (no `ipnetwork` dep)
    - `repo::treasury` — periodic hot-wallet snapshots; `insert`
      / `latest` / `list_recent`
    - `repo::share_window` — pre-aggregated PROP rollups for
      closed DAA windows; `insert` / `find` / `list_for_window`
      with the schema's UNIQUE-window guard
    - `repo::share_allocation` — per-wallet PROP allocation of a
      block's matured reward. `NewAllocation::is_balanced` does
      client-side rejection of unbalanced rows before the DB
      CHECK fires; `insert_batch` flattens per-wallet vectors
      via `UNNEST` in one round-trip; aggregate
      `pending_balance_for_wallet` for the accountant's
      planned-payout query
    - `repo::nacho_rebate` — running NACHO rebate balance per
      wallet; `accrue` / `mark_paid` / `list_pending` with
      `paid <= accrued` enforcement and a `pending_sompi()`
      derived getter
    - `repo::payout` — payout-cycle / payout / KRC-20 transfer
      triple. Idempotency-key composer
      (`kas-<daa_start>-<daa_end>`, `krc20-<daa_start>-<daa_end>`),
      cycle lifecycle helpers (broadcasting / partially-settled /
      settled / failed), per-recipient payout lifecycle
      (submit-with-tx-hash / confirmed / failed-with-reason),
      KRC-20 commit/reveal state machine
  Twenty-five new integration tests in
  `crates/katpool-db/tests/repo_payouts.rs` cover idempotency,
  lifecycle transitions, DB-CHECK enforcement (NACHO `paid > accrued`,
  share-allocation balance equation, payout uniqueness), the
  `NewAllocation` client-side balance guard, and the
  `(idempotency_key)` format stability. Workspace test count grows
  from 108 to 133.
- Phase 2 milestone 2: repository layer over the schema introduced
  in milestone 1. Free functions on `impl sqlx::PgExecutor<'_>`
  organised by aggregate — works with both `&PgPool` for
  single-statement contexts and `&mut Transaction` (via `&mut *tx`)
  for atomic multi-statement work. Strongly-typed ID newtypes
  (`WalletId`, `WorkerId`, `SessionId`, `ShareId`, `BlockId`,
  `AuditLogId`) prevent confusion between table identities at the
  type level. Aggregates shipped:
    - `repo::wallet` — `ensure` (upsert by address with
      `last_seen_at` refresh), `find_by_address`, `get_by_id`
    - `repo::worker` — `ensure`, `get_by_id`, `list_for_wallet`
    - `repo::share` — `insert_credited` (the hot-path call from
      the accountant's `PoolEvent::ShareCredited` handler),
      `sum_weight_for_window` / `count_for_window` /
      `total_weight_for_window` for PROP allocation reads
    - `repo::block` — `insert`, `find_by_hash`, lifecycle
      transitions (`mark_submitted`, `mark_confirmed_blue`,
      `mark_matured`, `mark_orphaned`) with idempotency, plus
      `list_by_status` for operator views
    - `repo::audit` — append-only log via the `NewEntry` builder
      with subject/correlation-id wiring, `list_for_subject`
  Validated newtypes from `katpool-domain` (`WalletAddress`,
  `WorkerName`, `BlockHash`, `DaaScore`, `ShareDifficulty`,
  `CorrelationId`) are the public API; the domain invariants flow
  into the database boundary unchanged. Seventeen new integration
  tests in `crates/katpool-db/tests/repo.rs` exercise idempotency,
  cascade behaviour, lifecycle CHECK enforcement, transaction
  rollback semantics, and the per-aggregate query contracts against
  a real Postgres testcontainer. Workspace gains
  `serde_json` as a declared dep on `katpool-db`. Two follow-up
  issues opened proactively: #8 (`gh pr edit --add-label` chokes on
  Projects-classic deprecation; REST workaround documented) and #9
  (pin `testcontainers` postgres image to match production
  `postgres:17`).
- Phase 2 milestone 1: `katpool-db` crate with the full schema for the
  rebuild — 14 tables (`wallet`, `worker`, `connection_session`,
  `share`, `share_window`, `block`, `share_allocation`, `payout_cycle`,
  `payout`, `nacho_rebate_accrual`, `krc20_pending_transfer`,
  `treasury_snapshot`, `audit_log`, `pool_meta`), 5 enum state-machines
  (`block_status`, `payout_kind`, `payout_cycle_status`,
  `payout_status`, `krc20_transfer_status`), foreign-key integrity
  throughout, CHECK constraints rejecting bad-shape data at the storage
  layer (wallet-address format per network, balance equation in
  `share_allocation`, lifecycle ordering in `block` and `payout`,
  uniqueness on `payout (cycle_id, wallet_id)` for payout idempotency).
  Connection pool builder with operator-tunable
  `KATPOOL_DB_*` env vars (mirrors the bridge's anti-abuse config
  pattern); typed `DbError` with `is_transient` / `is_not_found` /
  `sqlstate()` classification helpers; embedded `sqlx::migrate!`
  migrator that fail-closes on schema-ahead-of-binary. Twelve unit
  tests cover `PoolConfig`/`DbError`; twelve integration tests spin
  up an ephemeral postgres via `testcontainers-modules` and assert
  every documented table, enum, FK cascade, CHECK constraint, and
  idempotency invariant works end-to-end. New
  `docs/decisions/0011-db-schema-and-migrations.md` documenting the
  schema rationale and migration strategy (no down-migrations;
  rollback via pgBackRest restore from ADR-0007). New
  `docs/db-schema.md` operator reference with ER diagram and worked
  query examples per table. Workspace gains
  `testcontainers-modules` (with the `postgres` feature) and
  `kaspa-math` as declared deps.
- Phase 1 closeout: `bridge/examples/cpu_stratum_miner.rs` — a
  self-contained stratum-protocol CPU miner (~250 LOC) using the
  workspace-pinned `kaspa_pow::matrix::Matrix` + `kaspa_hashes::PowHash`
  for PoW, raw line-delimited JSON-RPC for the wire protocol, and a
  thread-striped nonce search across all available CPU cores. The
  public ecosystem has no maintained Crescendo + Toccata-aware CPU
  stratum miner (`kaspanet/cpuminer` v0.2.7 and `elichai/kaspa-miner`
  are both solo gRPC miners that bypass any stratum layer), so this
  artifact is required for end-to-end stratum smoke runs in CI.
  Companion bridge example `bridge/examples/gen_testnet_addr.rs`
  generates a valid bech32 `kaspatest:` address via
  `kaspa_addresses::Address::new` with `/dev/urandom`-seeded payload
  — used by the smoke harness's `--wallet` argument.
  `kaspa-math` added to workspace dependencies (already a transitive
  dep, now declared so the example can call `Uint256::from_le_bytes`
  directly).
  Empirical finding from running the smoke against the operator's
  Toccata-aware kaspad-tn10 at `193.26.159.181:16210`: bridge boot
  in **503 ms**, ≥ **184 mining.notify** delivered in 60 s,
  **38M PoW hashes** computed by the CPU miner, zero panics in either
  process. The Phase 1 acceptance row 12/13 volume threshold (≥ 100
  shares, ≥ 1 block in 60 s) is **mathematically out of reach for any
  CPU stratum miner at the bridge's u32 minimum pool difficulty** and
  is deferred to the Phase 7 cutover smoke with real ASIC hash. Phase
  1 acceptance now records pipeline-GREEN at CPU scale and volume-
  GREEN at ASIC scale (deferred). See
  `docs/phase-1-acceptance.md` "CPU-mining empirical limit" block.
- Phase 1 infra: dedicated Toccata-aware testnet-10 kaspad node
  co-resident with the existing dockerized mainnet kaspad on the
  pool VPS. New hardened systemd unit
  `ops/kaspad/katpool-kaspad-tn10.service` (systemd-analyze security
  exposure level **1.2 OK**), idempotent installer
  `ops/kaspad/install-kaspad-tn10.sh` that downloads the upstream
  `tn10-toc2` release zip pinned by SHA-256
  (`b1664d7336b7b536f98a7383ada6bffec71df7fc0d017f54fd4ec2434d7c5f44`),
  dedicated `kaspad-tn10` system user, data dir at
  `/var/lib/kaspad-tn10/data`, ports 16210 (gRPC) / 16211 (P2P) /
  17210 (wRPC-borsh) / 18210 (wRPC-json). The legacy mainnet
  kaspad (v1.0.1 in docker, 128 GB data dir) is left untouched per
  ADR-0010. Phase 1 acceptance row 11 (boot time) measured at
  **503 ms** against the operator's Toccata-aware external node,
  well under the 5-second budget. New ADR-0010
  (`docs/decisions/0010-multi-tenant-kaspad-on-pool-vps.md`)
  documents the multi-tenant strategy, the Toccata constraint
  (vendored `kaspa-*` v1.1.0 crates predate Toccata, so the bridge
  must run external-only against testnet-10), and the explicit
  deferral of mainnet-migration to Phase 7. New runbook 13
  (`docs/runbooks/13-kaspad-tn10-bootstrap.md`) covers install /
  upgrade / incident-recovery procedures. Capacity plan updated to
  reflect the new third-tenant footprint (~30 GB disk, ~5 GiB RAM,
  1–2 vCPU; total saturated still leaves >65% headroom). New bridge
  example `bridge/examples/gen_testnet_addr.rs` produces a valid
  bech32 `kaspatest:` address using `kaspa_addresses::Address::new`
  with cryptographic randomness from `/dev/urandom`, used by the
  acceptance smoke harness for the wallet field.
- Phase 1 milestone 4 (Phase 1 close-out): operator-tunable
  anti-abuse limits via `KATPOOL_ANTI_ABUSE_*` environment variables
  (`MAX_CONN_PER_IP`, `MAX_TRACKED_IPS`, `FRAME_RATE_PER_SEC`,
  `FRAME_BURST`). Malformed values are fail-fast at start-up so an
  operator typo never silently degrades protection. Pure
  closure-injected `AntiAbuseConfig::from_lookup` with five
  deterministic tests plus an `AntiAbuseConfig::from_env` thin
  wrapper. Hardened systemd unit at `ops/systemd/katpool-bridge.service`
  passing `systemd-analyze security` with exposure level **1.1 OK**
  (NoNewPrivileges, ProtectSystem=strict, ProtectHome,
  ProtectKernel{Tunables,Modules,Logs}, ProtectControlGroups,
  PrivateTmp/Devices/Mounts, LockPersonality,
  MemoryDenyWriteExecute, RestrictAddressFamilies, RestrictNamespaces,
  CapabilityBoundingSet emptied, SystemCallFilter `@system-service`
  minus `@privileged @resources @raw-io @reboot @swap @cpu-emulation`,
  RemoveIPC, RestrictSUIDSGID, ProtectProc=invisible, ProcSubset=pid,
  IPAddressDeny=any with explicit allow-list drop-in). Two
  `.conf.example` drop-ins for anti-abuse and network tuning,
  idempotent `install.sh`. New testnet-10 acceptance smoke harness
  at `scripts/testnet10-smoke.sh` driving a 60-second CPU-miner run
  and reporting boot time, shares accepted, and blocks mined as JSON
  against the documented Phase 1 thresholds. New
  `docs/runbooks/12-testnet10-smoke.md` runbook and
  `docs/phase-1-acceptance.md` rollup tracking the 14 Phase 1
  acceptance items.
- Phase 1 milestone 3: per-IP anti-abuse layer for the stratum
  listener. New `bridge::anti_abuse::AntiAbuseGuard` enforces a
  connection cap per source IP, a tracked-IP cap (memory safety
  under attack), and a token-bucket frame-rate limit. RAII
  `ConnTicket` releases the per-IP slot on connection drop, so the
  guard cannot leak counts. Time-injected for deterministic unit
  testing; 13 new tests cover validated config, conn-cap, ticket
  release, distinct-IP isolation, tracked-IP cap, burst behaviour,
  refill semantics, untracked-IP rejection, and the unlimited mode.
  Four new Prometheus counters
  (`ks_anti_abuse_connection_reject_total{reason}`,
  `ks_anti_abuse_frame_rate_limited_total`,
  `ks_anti_abuse_malformed_frame_total`,
  `ks_anti_abuse_bad_address_total`) surface every rejection path.
  `handle_authorize` now disconnects on bech32 failure instead of
  merely returning an error to the listener loop.
  Stratum JSON-RPC parser fuzz harness added under `bridge/fuzz/`
  as a non-workspace cargo-fuzz crate (nightly-only because
  libfuzzer-sys requires nightly); local acceptance run on
  2026-05-25 was 1,500,000 iterations in 23 s with zero panics.
- Phase 1 milestone 2: `katpool-domain` types
  (`WalletAddress`, `WorkerName`, `ShareDifficulty`, `DaaScore`,
  `BlockHash`, `CorrelationId`) — every newtype validates at
  construction, returns typed errors, and serialises transparently.
  Defines the `PoolEvent` enum (`ShareCredited`, `ShareRejected`,
  `BlockFound`, `BlockAccepted`) and the `ShareRejectReason`
  taxonomy (`stale`, `low_difficulty`, `bad_pow`, `missing_job`,
  `malformed_frame`, `duplicate_submit`, `bad_address`). The
  stratum bridge's `share_handler.rs` now emits one `PoolEvent`
  per submission outcome and per block lifecycle event on an
  optional `tokio::sync::broadcast` channel injected via
  `ShareHandler::with_event_bus`. Best-effort emission with
  shared per-submission `CorrelationId` for downstream tracing.
  Forty-eight unit tests cover types and emission (42 in
  `katpool-domain`, 6 in the bridge event-bus module).

[Unreleased]: https://github.com/Nacho-the-Kat/katpool/commits/main
