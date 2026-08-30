# zkas-pool

[![License: MIT or Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Rust 1.91+](https://img.shields.io/badge/rust-1.91%2B-orange.svg)](Cargo.toml)

Rust-first **[ZKas](https://github.com/firecash/zkas-rusty)** mining pool — a
fork of [Nacho-the-Kat/katpool](https://github.com/Nacho-the-Kat/katpool) retargeted at
the shielded-by-default ZKas chain. A single-binary deployment that owns stratum,
share validation, block submission (native **and** AuxPoW merged mining), PROP
accounting, and **shielded `$zkas` payouts** — backed by `PostgreSQL`.

Because ZKas's PoW is kHeavyHash (byte-identical to Kaspa), the stratum/mining path
is unchanged from a Kaspa pool. What is different is **payout**: ZKas has no
transparent UTXOs — the coinbase reward is minted as an Orchard shielded note — so
custodial mode requires a **shielded treasury** with automatic shielded payouts
(see below).

> The internal crate and binary names are still `katpool` / `katpool-*`, inherited from
> the upstream fork; they are not renamed to avoid churning 60+ patched `kaspa-*` deps.
> The product is ZKas; the binary you build is `katpool`.

## The chain

| | |
|---|---|
| Ticker / base unit | `$zkas` — 1 $zkas = 100,000,000 sompi |
| Consensus | rusty-kaspa fork (kaspad v2.0.1 lineage), GHOSTDAG BlockDAG |
| Proof of Work | kHeavyHash, **byte-identical to Kaspa mainnet** |
| Block rate | **1 BPS** (1000 ms target) |
| Privacy | shielded-by-default — value lives in an Orchard (Halo 2) pool; the coinbase reward is minted directly as a shielded note |
| Merged mining | with Kaspa via AuxPoW; **active from genesis** on the live network, and optional — native mining is always valid |
| Address HRP | `zkas:` (testnet `zkastest:`, devnet `zkasdev:`, simnet `zkassim:`) |
| Network id | `zkas-mainnet` |
| Default node ports | gRPC **16810**, P2P **16811** (so a ZKas node co-exists with a Kaspa node) |
| Coinbase maturity | 100 blocks (~100 s); a shielded note becomes spendable after 600 blocks (~10 min) |

**Emission.** 60 $zkas per block at launch, **halving every 3 months**. A
consensus-enforced **5% dev fee** is appended as one extra coinbase output, so the
finding miner receives **57 $zkas** of the initial 60 — the fee is skimmed from the
subsidy, never from transaction fees. Once the curve decays below the floor (around
month 10) a perpetual **tail** takes over: 6 $zkas/s until month 24, then **0.6 $zkas/s
forever** (~18.9M $zkas/year).

**Supply.** ZKas has **no fixed maximum supply** — the tail is perpetual by design, so
miner security is funded forever rather than by fees alone. The deflationary curve
itself is bounded at ~703M $zkas. Total supply on the live schedule:

| after | supply |
|---|---|
| 1 year | ~665M $zkas |
| 2 years | ~854M |
| 5 years | ~911M |
| 10 years | ~1.005B |
| 20 years | ~1.195B |

Long-run inflation is ~2.2% at tail onset (~year 2), decaying toward ~1% and below over
decades. Emission is heavily front-loaded: roughly half of all curve issuance happens in
the first quarter.

## Integrating ZKas into your own pool software

You do **not** need this codebase to run a ZKas pool. Because the PoW is byte-identical
to Kaspa, your stratum layer, share validation, vardiff and difficulty maths need no
changes at all — an unmodified Kaspa ASIC hashes ZKas headers correctly.

Exactly **three** things are mandatory, and [`POOL-INTEGRATION.md`](POOL-INTEGRATION.md) walks each one to the
exact file and symbol in the [node source](https://github.com/firecash/zkas-rusty):

1. **The payout address is a shielded (Orchard) address, version 9.** The coinbase output
   script is a raw 43-byte address and is *not* a standard script class — drop any check
   that a coinbase output "looks like P2PK/P2SH".
2. **The coinbase carries a mandatory dev-fee output.** Submit the node's template
   coinbase verbatim. If you cache templates and rewrite `outputs.last()`, you rewrite
   the *dev fee* and lose the block — this is the single most expensive integration bug
   (see POOL-INTEGRATION.md §3).
3. **The coinbase payload has an extra 32-byte field** (the shielded state root, between
   `subsidy` and the script-pubkey block). Any pool that writes its extranonce at a
   hardcoded offset corrupts the payload on ZKas.

Merged mining and custodial shielded payouts are optional; both are specified end to end
in [`POOL-INTEGRATION.md`](POOL-INTEGRATION.md) §4 and §6.

## Status

> **Live today: direct (solo-style) payout.** The stratum **bridge** connects to a ZKas
> node and mines native + AuxPoW solutions; each block template pays the coinbase to the
> **address the finding miner authorized with**, so rewards land on-chain directly —
> the pool holds nothing. The **custodial path is being built** (shielded treasury →
> PROP accounting → automatic shielded payouts), replacing the upstream
> transparent-KAS/KRC-20 payout engines, which do **not** apply to ZKas.
> Do not enable custodial mode (`coinbase_address_override`) until that path lands.

## At a glance

**Runtime** (what `cargo build --release --bin katpool` ships):

| Component | Responsibility |
|---|---|
| [`bridge/`](bridge/) | Forked `rusty-kaspa` stratum bridge. Accepts ASIC stratum, validates shares, submits native + AuxPoW blocks to a ZKas node. **Works today.** |
| [`accountant/`](accountant/) | Subscribes to share + block events, computes PROP allocations, writes per-miner balances. |
| [`payout-zkas/`](payout-zkas/) | **The ZKas payout engine.** Periodic full-balance sweeps from the shielded treasury — one Orchard tx per recipient via the `shielded-pay` CLI, single-leader, idempotent, spend-capped, with an in-flight double-pay latch. |
| [`payout-kas/`](payout-kas/) | **Upstream transparent-KAS payout — NOT used by ZKas** (no transparent UTXOs). Kept for upstream parity; superseded by `payout-zkas`. |
| [`payout-krc20/`](payout-krc20/) | **Upstream NACHO KRC-20 rebate — not applicable to ZKas** (rebate BPS are zeroed). |
| [`api/`](api/) | Read-only `axum` HTTP API. Serves **aggregate** pool stats only; per-miner stats are withheld for miner privacy. |
| [`katpool/`](katpool/) | Main wiring binary that runs the active components in one process. |
| [`crates/`](crates/) | Shared libraries: `katpool-domain`, `-db`, `-config`, `-metrics`, `-storagemass`, `-idempotency`, `-telemetry`, `-secrets`, `-fault-injection`. |

## Payout model (ZKas)

**Today (live): direct payout.** The pool runs without a coinbase override, so every
block template pays the coinbase **directly to the address the finding miner mined
with**. Rewards are minted as shielded notes to the miner's own `zkas:` address by the
chain itself — the pool is never in custody and there is nothing to claim.

**Custodial mode (in development):** the pool mines to its **own shielded (`$zkas`)
treasury address** and tracks each miner's PROP balance in Postgres, keyed by the
miner's payout shielded address. A scheduled sweep then pays each miner's **full
accrued balance** (above a dust threshold) to their `zkas:` address automatically —
**no signature, no claim, no vesting, nothing for the miner to do.** No mining
password is required.

## Connecting a miner

ZKas mining is ordinary kHeavyHash — point any Kaspa-style miner/ASIC at the pool's
stratum port with your `zkas:` payout address (legacy `firecash:` accepted) as the username:

```
stratum+tcp://<pool-host>:<port>   user=zkas:<your-address>   pass=kaspa:<your-kas-address>
```

The password field is optional and carries your **Kaspa** payout address. Send
a valid `kaspa:` address there and the Kaspa blocks your own shares solve pay
you directly; send `x`, nothing, or a worker name and that reward goes to the
pool's address instead. The address is never guessed from your username, so a
typo is treated as absent rather than rejected.

See [`POOL-INTEGRATION.md`](POOL-INTEGRATION.md) for the
full integration guide: node setup, native vs. AuxPoW merged mining, the
exact consensus files to read, and an error-message-to-cause table.

## Quick start (development)

Prerequisites: Rust 1.91+ / edition 2024 (pinned via `rust-version` in
[`Cargo.toml`](Cargo.toml)), Docker (for ephemeral test databases), and
`cargo-deny` / `cargo-audit`.

The patched `kaspa-*` / ZKas crates are pulled straight from the
[`zkas-rusty`](https://github.com/firecash/zkas-rusty) fork over git — you do
**not** need a local rusty-kaspa checkout at any particular path (Cargo fetches and pins it).

```bash
git clone https://github.com/firecash/zkas-pool.git
cd zkas-pool

# Verify your environment matches CI gates
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo deny check

# Build / run the wiring binary
cargo run --release --bin katpool
```

You also need a reachable ZKas node (`kaspad`) with `--utxoindex`. See
[`POOL-INTEGRATION.md`](POOL-INTEGRATION.md) §2 for node setup and the stratum bridge config
([`zkas-bridge.yaml`](zkas-bridge.yaml)).

## Operating principles

- **Determinism**: every reward, mass, and payout calculation is a pure function tested
  with `proptest`. No floating-point money math.
- **Zero plaintext secrets**: the treasury key only ever exists encrypted on disk;
  loaded via systemd `LoadCredentialEncrypted`, mlocked, zeroized on drop.
- **Idempotent payouts**: every outbound transaction records an idempotency key in the DB
  *before* signing. Mid-cycle restarts cannot double-pay.
- **Miner privacy**: the pool does not publish per-miner or top-miner leaderboards; only
  aggregate stats are exposed.
- **Pinned everything**: Rust toolchain, container images, and crate versions are pinned.

## Documentation

Start at [`docs/README.md`](docs/README.md) for the index and [`POOL-INTEGRATION.md`](POOL-INTEGRATION.md) for
the operator guide. Some `docs/` pages still carry upstream (Kaspa/KRC-20) specifics and
are being migrated to ZKas.

## License

Dual-licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT license](LICENSE-MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual-licensed as above, without any additional terms or conditions.
