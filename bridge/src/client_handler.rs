use crate::{
    hasher::{calculate_target, generate_iceriver_job_params, generate_job_header, generate_large_job_params, serialize_block_header},
    jsonrpc_event::JsonRpcEvent,
    mining_state::{GetMiningState, Job},
    prom::*,
    share_handler::{KaspaApiTrait, ShareHandler},
    stratum_context::StratumContext,
};
use chrono::{DateTime, Utc};
use katpool_domain::{CorrelationId, PoolEvent, WalletAddress, WorkerName};
use num_bigint::BigUint;
use num_traits::Zero;
use parking_lot::Mutex;
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};

/// Encode a stratum difficulty for `mining.set_difficulty`.
///
/// Bitmain/GodMiner (KS-series) firmware rejects a non-integer difficulty with
/// stratum error 23 ("Invalid difficulty") and then mines at its own default,
/// submitting little or nothing the pool will accept. Upstream always encoded the
/// value via `Number::from_f64`, which serialises a whole number as `8192.0` — the
/// trailing `.0` is exactly what those ASICs reject. Emit a JSON integer whenever the
/// difficulty is whole (the normal case, especially after pow2 clamping), and fall
/// back to a float only for genuinely fractional difficulties (e.g. sub-1 GPU vardiff
/// on the test port), which are never sent to ASICs.
fn difficulty_to_json(diff: f64) -> serde_json::Value {
    if diff.is_finite() && diff >= 0.0 && diff.fract() == 0.0 && diff <= u64::MAX as f64 {
        serde_json::Value::Number(serde_json::Number::from(diff as u64))
    } else {
        serde_json::Value::Number(serde_json::Number::from_f64(diff).unwrap_or_else(|| serde_json::Number::from(diff.max(0.0) as u64)))
    }
}

static BIG_JOB_REGEX: once_cell::sync::Lazy<Regex> =
    once_cell::sync::Lazy::new(|| Regex::new(r".*(BzMiner|IceRiverMiner).*").unwrap());

const BALANCE_DELAY: Duration = Duration::from_secs(60);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(20);

/// One rotating prefix counter per extranonce width (index = width in bytes).
///
/// A single shared counter is wrong as soon as two ports use different widths: a
/// 1-byte port wraps at 255 and resets the shared value, after which a 2-byte port
/// re-issues prefixes it has already handed to connected miners and puts two rigs
/// on the same nonce space. Keeping one counter per width makes each port's
/// rotation independent, so adding a narrow port cannot disturb a wide one.
static NEXT_EXTRANONCE: [AtomicI32; 4] =
    [AtomicI32::new(0), AtomicI32::new(0), AtomicI32::new(0), AtomicI32::new(0)];

/// Hand out the next nonce prefix of `width` bytes.
///
/// Returns the hex string, the numeric value, and whether the counter wrapped.
/// Split out from the assignment path so the property that matters can actually be
/// tested: each width rotates on its own counter, so a narrow port cannot reset a
/// wide one and hand two connected rigs the same nonce space.
fn next_extranonce_prefix(width: u8) -> (String, i32, bool) {
    let width = width.clamp(1, 3);
    let max_extranonce = (2_f64.powi(8 * i32::from(width)) - 1.0) as i32;
    let next = NEXT_EXTRANONCE[width as usize]
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |val| if val < max_extranonce { Some(val + 1) } else { Some(0) });
    let wrapped = next.is_err() || next.unwrap_or(0) >= max_extranonce;
    let value = next.unwrap_or(0);
    (format!("{:0hex$x}", value, hex = (width as usize) * 2), value, wrapped)
}

#[cfg(test)]
mod extranonce_width_tests {
    use super::*;

    /// The width the pool has always handed out. A 2-byte prefix is four hex
    /// characters and leaves the miner six of the eight nonce bytes.
    #[test]
    fn two_bytes_is_four_hex_chars() {
        let (hex, _, _) = next_extranonce_prefix(2);
        assert_eq!(hex.len(), 4, "2-byte prefix must be 4 hex chars, got {hex:?}");
    }

    /// What a rented proxy needs: one byte, leaving it seven to split.
    #[test]
    fn one_byte_is_two_hex_chars() {
        let (hex, _, _) = next_extranonce_prefix(1);
        assert_eq!(hex.len(), 2, "1-byte prefix must be 2 hex chars, got {hex:?}");
    }

    /// The reason the counters are per-width. Exhausting the 1-byte space wraps it
    /// back to zero; if the 2-byte port shared that counter it would then re-issue
    /// prefixes already held by connected miners, putting two rigs on one nonce
    /// space. Running a full 1-byte rotation must leave the 2-byte port advancing
    /// from where it was.
    #[test]
    fn wrapping_a_narrow_width_does_not_reset_a_wide_one() {
        let (_, before, _) = next_extranonce_prefix(2);
        for _ in 0..600 {
            let _ = next_extranonce_prefix(1);
        }
        let (_, after, _) = next_extranonce_prefix(2);
        assert!(after > before, "2-byte counter went backwards: {before} -> {after}");
    }

    /// A width-1 prefix never exceeds one byte even as the counter climbs.
    #[test]
    fn a_narrow_prefix_never_widens() {
        for _ in 0..300 {
            let (hex, value, _) = next_extranonce_prefix(1);
            assert_eq!(hex.len(), 2, "prefix widened to {hex:?}");
            assert!(value <= 0xFF, "value {value} escaped one byte");
        }
    }
}

fn parent_job_interval(remote_app: &str) -> Duration {
    let app = remote_app.to_ascii_lowercase();
    let legacy_asic = app.contains("godminer") || app.contains("bitmain") || app.contains("antminer") || app.contains("goldshell");
    let (name, default_ms) =
        if legacy_asic { ("ZKAS_LEGACY_PARENT_JOB_INTERVAL_MS", 1_000) } else { ("ZKAS_COMMON_PARENT_JOB_INTERVAL_MS", 500) };
    let milliseconds = std::env::var(name).ok().and_then(|value| value.parse::<u64>().ok()).unwrap_or(default_ms).clamp(100, 10_000);
    Duration::from_millis(milliseconds)
}

pub struct ClientHandler {
    clients: Arc<Mutex<HashMap<i32, Arc<StratumContext>>>>,
    client_counter: AtomicI32,
    /// Default starting difficulty for connections whose local port has
    /// no explicit seed in [`Self::port_seeds`].
    min_share_diff: f64,
    /// Per-port starting-difficulty seeds (ADR-0022). The connection's
    /// local (listening) port selects the *initial* difficulty only;
    /// vardiff then moves freely from there. Empty in single-port mode.
    port_seeds: HashMap<u16, f64>,
    /// Width in bytes of the nonce prefix this listener hands out (0-3).
    extranonce_size: i8,
    _max_extranonce: i32, // Kept for backward compatibility
    last_template_time: Arc<Mutex<Instant>>,
    last_balance_check: Arc<Mutex<Instant>>,
    share_handler: Arc<ShareHandler>,
    instance_id: String, // Instance identifier for logging
    kaspa_common_protocol: bool,
    /// Emit `mining.set_extranonce` with the extranonce2 size alongside the prefix.
    extranonce_with_size: bool,
}

impl ClientHandler {
    /// Disconnect an authenticated session that never receives any work.
    /// This is deliberately separate from share liveness: a slow miner may
    /// legitimately submit no shares, but an authorized miner must receive a
    /// first `mining.notify` or it cannot make progress. Closing the socket
    /// lets a miner/MRR failover policy try its next configured endpoint.
    pub fn start_first_job_watchdog(&self, client: Arc<StratumContext>) {
        const FIRST_JOB_TIMEOUT: Duration = Duration::from_secs(30);
        tokio::spawn(async move {
            tokio::time::sleep(FIRST_JOB_TIMEOUT).await;
            if !client.connected() || client.wallet_addr.lock().is_empty() {
                return;
            }
            if !client.received_job_since_authorization() {
                let summary = client.summary();
                // Stratum V1's reconnect notification is optional, so always
                // follow it with a socket close. The miner then uses its own
                // failover list if it ignores the hint. The sequence is
                // deliberately per-connection and allow-listed.
                let next = match client.local_port {
                    5577 => Some(("mining-pool.zkas.info", 5555)),
                    5555 => Some(("204.10.194.28", 5555)),
                    _ => None,
                };
                warn!(
                    "[NO_JOB_TIMEOUT] authorized worker received no mining.notify within 30s; requesting per-connection failover: wallet={} worker={} app={} port={} remote={}",
                    summary.wallet_addr, summary.worker_name, summary.remote_app, client.local_port, summary.remote_addr
                );
                if let Some((host, port)) = next {
                    let _ = client.send_reconnect_hint(host, port, 3).await;
                }
                client.disconnect();
            }
        });
    }

    pub fn new(
        share_handler: Arc<ShareHandler>,
        min_share_diff: f64,
        port_seeds: HashMap<u16, f64>,
        extranonce_size: i8,
        instance_id: String,
    ) -> Self {
        Self::new_with_protocol(share_handler, min_share_diff, port_seeds, extranonce_size, instance_id, false)
    }

    pub fn new_with_protocol(
        share_handler: Arc<ShareHandler>,
        min_share_diff: f64,
        port_seeds: HashMap<u16, f64>,
        extranonce_size: i8,
        instance_id: String,
        kaspa_common_protocol: bool,
    ) -> Self {
        let max_extranonce = if extranonce_size > 0 { (2_f64.powi(8 * extranonce_size.min(3) as i32) - 1.0) as i32 } else { 0 };

        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
            client_counter: AtomicI32::new(0),
            min_share_diff,
            port_seeds,
            extranonce_size,
            _max_extranonce: max_extranonce,
            last_template_time: Arc::new(Mutex::new(Instant::now())),
            last_balance_check: Arc::new(Mutex::new(Instant::now())),
            share_handler,
            instance_id,
            kaspa_common_protocol,
            extranonce_with_size: false,
        }
    }

    /// Starting difficulty seed for a connection's local port. Falls
    /// back to the default `min_share_diff` when the port has no
    /// explicit seed (e.g. single-port mode). A start only — vardiff
    /// owns the steady state (ADR-0022).
    #[inline]
    fn seed_for_port(&self, local_port: u16) -> f64 {
        self.port_seeds.get(&local_port).copied().unwrap_or(self.min_share_diff)
    }

    /// Accessor for the instance identifier used in Prometheus labels
    /// and structured logging. Needed by anti-abuse hooks in
    /// `default_client.rs` that record per-IP metrics.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Builder: opt this listener into the two-argument `mining.set_extranonce`.
    /// Separate from the constructor so no existing call site changes shape.
    #[must_use]
    pub const fn with_extranonce_size_param(mut self, enabled: bool) -> Self {
        self.extranonce_with_size = enabled;
        self
    }

    pub const fn extranonce_with_size(&self) -> bool {
        self.extranonce_with_size
    }

    pub fn kaspa_common_protocol(&self) -> bool {
        self.kaspa_common_protocol
    }

    /// Send the initial difficulty synchronously during the subscribe
    /// handshake. IceRiver's published Kaspa stratum sequence is:
    /// subscribe response -> set_difficulty -> set_extranonce -> authorize.
    /// Waiting for authorize before advertising these values deadlocks some
    /// rental/proxy paths, because the miner waits for the server sequence.
    pub async fn send_subscribe_difficulty(&self, client: &StratumContext) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let diff = self.seed_for_port(client.local_port);
        let diff_value = difficulty_to_json(diff);
        let event = JsonRpcEvent {
            jsonrpc: "2.0".to_string(),
            method: "mining.set_difficulty".to_string(),
            id: None,
            params: vec![diff_value],
        };

        client.send(event).await.map_err(|e| format!("failed to set subscribe difficulty: {e}").into())
    }

    /// Kaspa Common/Stratum-v1 form used by MRR-compatible public pools.
    pub async fn send_subscribe_difficulty_v1(&self, client: &StratumContext) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let diff = self.seed_for_port(client.local_port);
        // Preserve the July 23 MRR wire format on the Common-protocol port.
        // Some rental proxies distinguish 8192.0 from the integer form used by
        // direct Bitmain firmware.
        let value =
            serde_json::Value::Number(serde_json::Number::from_f64(diff).unwrap_or_else(|| serde_json::Number::from(diff as u64)));
        client
            .send_v1_notification("mining.set_difficulty", vec![value])
            .await
            .map_err(|e| format!("failed to set Kaspa Common difficulty: {e}").into())
    }

    pub fn on_connect(&self, ctx: Arc<StratumContext>) {
        let idx = self.client_counter.fetch_add(1, Ordering::Relaxed);

        // Don't assign extranonce here - will be assigned in handle_subscribe based on detected miner type
        // Leave extranonce empty initially
        *ctx.extranonce.lock() = String::new();

        ctx.set_id(idx);
        self.clients.lock().insert(idx, Arc::clone(&ctx));

        debug!(
            "{} [CONNECTION] Client {} connected (ID: {}), extranonce will be assigned after miner type detection",
            self.instance_id, ctx.remote_addr, idx
        );

        // Create stats after 5 seconds (give time for authorize)
        let share_handler = Arc::clone(&self.share_handler);
        let ctx_clone = Arc::clone(&ctx);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            if !ctx_clone.wallet_addr.lock().is_empty() {
                share_handler.get_create_stats(&ctx_clone);
            }
        });
    }

    /// Sync Prometheus session metrics for an authorized worker (hashrate/uptime labels).
    pub fn sync_worker_prom_metrics(&self, ctx: &StratumContext) {
        if ctx.wallet_addr.lock().is_empty() {
            return;
        }
        self.share_handler.activate_client_vardiff(ctx);
    }

    /// Detach this connection from its current wallet+worker vardiff entry
    /// before an in-place re-authorization changes that identity.
    pub fn prepare_worker_identity_change(&self, ctx: &StratumContext) {
        self.share_handler.deactivate_client_vardiff(ctx);
    }

    /// Assign extranonce to a client based on detected miner type
    /// Called from handle_subscribe after miner type is detected
    pub fn assign_extranonce_for_miner(&self, ctx: &StratumContext, remote_app: &str) {
        use std::sync::atomic::Ordering;

        // Detect miner type and determine required extranonce size
        // IceRiver, BzMiner, Goldshell take a 2-byte extranonce (via set_extranonce).
        // Bitmain (GodMiner) historically got size 0 — but with no extranonce every
        // connection of a GodMiner farm grinds the SAME nonce space (papa's farm produced
        // ~300 duplicate blocks/hour = provably overlapping search = wasted hashrate).
        // We now give Bitmain a 2-byte extranonce too, delivered in the subscribe
        // response ([null, extranonce, extranonce2_size]); share validation already
        // accepts both full and extranonce2-only nonce submissions.
        // Kill switch: ZKAS_BITMAIN_EXTRANONCE=0 (legacy FIRECASH_ name honored) restores the old size-0 behavior.
        let remote_app_lower = remote_app.to_lowercase();
        let is_bitmain =
            remote_app_lower.contains("godminer") || remote_app_lower.contains("bitmain") || remote_app_lower.contains("antminer");
        let bitmain_extranonce_enabled = std::env::var("ZKAS_BITMAIN_EXTRANONCE")
            .or_else(|_| std::env::var("FIRECASH_BITMAIN_EXTRANONCE"))
            .map(|v| v != "0")
            .unwrap_or(true);

        // Width comes from this listener's config. It was hardcoded to 2, which made
        // `extranonce_size` in the config file dead: a rented proxy such as NiceHash
        // needs the pool's prefix to stay narrow so it has enough of the 8-byte nonce
        // left to split across the rigs behind it, and no config value could express
        // that. The default is still 2, so every existing port keeps its behaviour.
        let configured_extranonce_size = self.extranonce_size.clamp(0, 3);
        let required_extranonce_size =
            if is_bitmain && !bitmain_extranonce_enabled { 0 } else { configured_extranonce_size };

        let extranonce = if required_extranonce_size > 0 {
            let (extranonce_str, extranonce_val, wrapped) = next_extranonce_prefix(required_extranonce_size as u8);
            if wrapped {
                warn!("wrapped extranonce! new clients may be duplicating work...");
            }
            debug!(
                "[AUTO-EXTRANONCE] Assigned extranonce '{}' (value: {}, size: {} bytes) to {} miner '{}'",
                extranonce_str,
                extranonce_val,
                required_extranonce_size,
                if is_bitmain { "Bitmain" } else { "IceRiver/BzMiner/Goldshell" },
                remote_app
            );
            extranonce_str
        } else {
            debug!("[AUTO-EXTRANONCE] Assigned empty extranonce (size: 0 bytes) to Bitmain miner '{}'", remote_app);
            String::new()
        };

        *ctx.extranonce.lock() = extranonce.clone();

        debug!(
            "[AUTO-EXTRANONCE] Client {} extranonce set to '{}' (detected miner: '{}', type: {})",
            ctx.remote_addr,
            extranonce,
            remote_app,
            if is_bitmain { "Bitmain" } else { "IceRiver/BzMiner/Goldshell" }
        );
    }

    /// Publish a lifecycle [`PoolEvent`] on the attached event bus. A
    /// no-op in standalone mode (no bus). Used by the authorize handler to
    /// emit `SessionOpened` and by `on_disconnect` for `SessionClosed`.
    pub fn emit_event(&self, event: PoolEvent) {
        self.share_handler.publish(event);
    }

    pub fn on_disconnect(&self, ctx: &StratumContext) {
        self.share_handler.deactivate_client_vardiff(ctx);
        ctx.disconnect();
        let mut clients = self.clients.lock();
        if let Some(id) = ctx.id() {
            debug!("removing client {}", id);
            clients.remove(&id);
            debug!("removed client {}", id);
        }
        let wallet_addr = ctx.wallet_addr.lock().clone();
        let worker_name = ctx.worker_name.lock().clone();
        let remote_app = ctx.remote_app.lock().clone();

        let is_unauthed = wallet_addr.is_empty() && worker_name.is_empty();
        if !is_unauthed {
            record_disconnect(&WorkerContext::from_stratum(&self.instance_id, ctx, &remote_app));
        }

        // Persist the session for per-IP forensics + the firmware
        // breakdown (ADR-0023). Only sessions that revealed something
        // useful — a worker identity or a reported user-agent — are
        // recorded, to keep port-scanner noise out of the table. A
        // no-op in standalone mode (no event bus attached).
        let remote_app_opt = (!remote_app.is_empty()).then(|| remote_app.clone());
        let worker_opt = (!worker_name.is_empty()).then(|| WorkerName::new(&worker_name).ok()).flatten();
        if remote_app_opt.is_some() || worker_opt.is_some() {
            let wallet_opt = (!wallet_addr.is_empty()).then(|| WalletAddress::new(&wallet_addr).ok()).flatten();
            let connected_at = DateTime::<Utc>::from(ctx.state.connect_time());
            self.share_handler.publish(PoolEvent::SessionClosed {
                conn_id: ctx.session_uid(),
                wallet: wallet_opt,
                worker: worker_opt,
                remote_ip: ctx.remote_addr().to_owned(),
                remote_app: remote_app_opt,
                connected_at,
                ts: Utc::now(),
                correlation_id: CorrelationId::new_v4(),
            });
        }
    }

    pub fn disconnect_all(&self) {
        let clients = {
            let guard = self.clients.lock();
            guard.values().cloned().collect::<Vec<_>>()
        };

        for client in clients {
            client.disconnect();
        }

        self.clients.lock().clear();
    }

    pub fn client_by_session_uid(&self, session_uid: u64) -> Option<Arc<StratumContext>> {
        self.clients.lock().values().find(|client| client.session_uid() == session_uid && client.connected()).cloned()
    }

    /// Send an immediate job to a specific client (for use after authorization)
    /// This ensures IceRiver and other ASICs get a job immediately, not waiting for polling
    pub async fn send_immediate_job_to_client<T: KaspaApiTrait + Send + Sync + ?Sized + 'static>(
        &self,
        client: Arc<StratumContext>,
        kaspa_api: Arc<T>,
    ) {
        // Check if client has wallet address
        let _wallet_addr_str = {
            let wallet_addr = client.wallet_addr.lock();
            if wallet_addr.is_empty() {
                debug!("send_immediate_job: client {} has no wallet address yet, skipping", client.remote_addr);
                return;
            }
            wallet_addr.clone()
        };

        if !client.connected() {
            debug!("send_immediate_job: client {} not connected, skipping", client.remote_addr);
            return;
        }

        let client_clone = Arc::clone(&client);
        let kaspa_api_clone = Arc::clone(&kaspa_api);
        let share_handler = Arc::clone(&self.share_handler);
        let min_diff = self.seed_for_port(client.local_port);
        let instance_id = self.instance_id.clone();
        let initial_diff = share_handler.register_client_vardiff(&client, min_diff);

        // Publish the restored target to this connection before detaching the
        // template fetch. The regular block loop can run concurrently with
        // this immediate-job task; without this synchronous update it could
        // send work using the subscribe-time seed during a reconnect.
        {
            use crate::hasher::KaspaDiff;
            let remote_app = client.remote_app.lock().clone();
            let mut stratum_diff = KaspaDiff::new();
            stratum_diff.set_diff_value_for_miner(initial_diff, &remote_app);
            GetMiningState(&client).set_stratum_diff(stratum_diff);
        }

        tokio::spawn(async move {
            let _job_build_guard = client_clone.lock_job_build().await;
            // Get per-client mining state from context
            let state = GetMiningState(&client_clone);

            // Get client info
            let (wallet_addr, remote_app, canxium_addr) = {
                let wallet = client_clone.wallet_addr.lock().clone();
                let app = client_clone.remote_app.lock().clone();
                let canx = client_clone.canxium_addr.lock().clone();
                (wallet, app, canx)
            };

            debug!("send_immediate_job: fetching block template for client {} (wallet: {})", client_clone.remote_addr, wallet_addr);

            // Get block template
            let generation = client_clone.next_template_generation();
            // Merged mining: this lane's own KAS payout (stratum password) and its
            // lane id, which selects the one minute an hour that pays the pool.
            let kas_payout = client_clone.kas_payout.lock().clone();
            let lane_id = client_clone.session_uid();
            let template_result = kaspa_api_clone
                .get_block_template(&wallet_addr, &remote_app, &canxium_addr, client_clone.session_uid(), generation, kas_payout, lane_id)
                .await;

            let block = match template_result {
                Ok(block) => {
                    debug!("send_immediate_job: successfully fetched block template for client {}", client_clone.remote_addr);

                    // === LOG NEW BLOCK TEMPLATE HEADER === (moved to debug level)
                    debug!("=== NEW BLOCK TEMPLATE RECEIVED ===");
                    debug!("  blue_score: {}", block.header.blue_score);
                    debug!("  bits: {} (0x{:08x})", block.header.bits, block.header.bits);
                    debug!("  timestamp: {}", block.header.timestamp);
                    debug!("  version: {}", block.header.version);
                    debug!("  daa_score: {}", block.header.daa_score);

                    // Track and log what changed from previous header
                    if let Some(old_header) = state.get_last_header() {
                        debug!("=== HEADER CHANGES ===");
                        debug!("  blue_score_changed: {}", old_header.blue_score != block.header.blue_score);
                        debug!("    old: {}, new: {}", old_header.blue_score, block.header.blue_score);
                        debug!("  bits_changed: {}", old_header.bits != block.header.bits);
                        debug!("    old: 0x{:08x}, new: 0x{:08x}", old_header.bits, block.header.bits);
                        debug!("  timestamp_changed: {}", old_header.timestamp != block.header.timestamp);
                        debug!("    delta: {} ms", block.header.timestamp - old_header.timestamp);
                        debug!("  daa_score_changed: {}", old_header.daa_score != block.header.daa_score);
                        debug!("  version_changed: {}", old_header.version != block.header.version);
                    } else {
                        debug!("=== FIRST HEADER === (no previous header to compare)");
                    }

                    // Store this header for next comparison
                    state.set_last_header((*block.header).clone());

                    block
                }
                Err(e) => {
                    if e.to_string().contains("Could not decode address") {
                        record_worker_error(&instance_id, &wallet_addr, crate::errors::ErrorShortCode::InvalidAddressFmt.as_str());
                        // Two different addresses can land here: the miner's ZKas
                        // username, and the Kaspa payout address it supplied in the
                        // stratum password. The node's error text does not say which,
                        // so decide by what the session actually carries. Dropping a
                        // miner over its *Kaspa* address would be wrong -- it is
                        // optional, and a bad one is documented to fall back to the
                        // pool. Doing so also costs the miner every ZKas reward, and
                        // it reconnects instantly, so the disconnect becomes a loop.
                        let had_kas_payout = client_clone.kas_payout.lock().take().is_some();
                        if had_kas_payout {
                            // The dashboard must stop claiming this worker is paid
                            // its own KAS the moment we stop paying it.
                            crate::prom::record_kas_payout_configured(
                                &crate::prom::worker_context(&instance_id, &client_clone, remote_app.clone()),
                                false,
                            );
                            warn!(
                                "send_immediate_job: Kaspa payout address rejected ({}); \
                                 falling back to the pool address for {} and continuing",
                                e, client_clone.remote_addr
                            );
                        } else {
                            error!("send_immediate_job: failed fetching block template, malformed address: {}", e);
                            client_clone.disconnect();
                        }
                    } else {
                        record_worker_error(&instance_id, &wallet_addr, crate::errors::ErrorShortCode::FailedBlockFetch.as_str());
                        error!("send_immediate_job: failed fetching block template: {}", e);
                    }
                    return;
                }
            };

            // Calculate target
            let big_diff = calculate_target(block.header.bits as u64);
            state.set_big_diff(big_diff);

            // Serialize header - now returns Hash type directly
            // The "Odd number of digits" error typically indicates a malformed hex string
            // in one of the hash fields. This can happen if the block data from the node
            // contains an invalid hash representation.
            let pre_pow_hash = match serialize_block_header(&block) {
                Ok(h) => h,
                Err(e) => {
                    let error_msg = e.to_string();
                    record_worker_error(&instance_id, &wallet_addr, crate::errors::ErrorShortCode::BadDataFromMiner.as_str());
                    error!("send_immediate_job: failed to serialize block header: {}", error_msg);

                    // Log block header details for debugging
                    debug!("Block header version: {}", block.header.version);
                    debug!("Block header timestamp: {}", block.header.timestamp);
                    debug!("Block header bits: {}", block.header.bits);

                    // Skip this block and continue - the next block template should work
                    return;
                }
            };

            // Create Job struct with both block and pre_pow_hash
            let job = Job { block: block.clone(), pre_pow_hash };

            // Add job
            let job_id = state.add_job(job);
            let counter_after = state.current_job_counter();
            let stored_ids = state.get_stored_job_ids();
            debug!(
                "[JOB CREATION] send_immediate_job: created job ID {} for client {} (counter: {}, stored IDs: {:?})",
                job_id, client_clone.remote_addr, counter_after, stored_ids
            );

            // Initialize state if first time
            if !state.is_initialized() {
                state.set_initialized(true);
                let use_big_job = BIG_JOB_REGEX.is_match(&remote_app);
                state.set_use_big_job(use_big_job);

                // Initialize stratum diff
                use crate::hasher::KaspaDiff;
                let mut stratum_diff = KaspaDiff::new();
                let remote_app_clone = remote_app.clone();
                stratum_diff.set_diff_value_for_miner(initial_diff, &remote_app_clone);
                state.set_stratum_diff(stratum_diff);

                update_worker_difficulty(&WorkerContext::from_stratum(&instance_id, &client_clone, &remote_app_clone), initial_diff);

                let target = state.stratum_diff().map(|d| d.target_value.clone()).unwrap_or_else(BigUint::zero);
                let target_bytes = target.to_bytes_be();
                debug!(
                    "send_immediate_job: Initialized MiningState with difficulty: {}, target: {:x} ({} bytes, {} bits)",
                    initial_diff,
                    target,
                    target_bytes.len(),
                    target_bytes.len() * 8
                );
            }

            // CRITICAL: Always send difficulty to each client (IceRiver expects this on every connection)
            // Even if state is already initialized, we need to send difficulty to this specific client
            // Use the actual current difficulty from state if available, otherwise use min_diff
            let current_diff = state.stratum_diff().map(|d| d.diff_value).unwrap_or(initial_diff);

            // Update metric to ensure displayed difficulty matches what we're sending
            // (This handles the case where state was already initialized but metric wasn't updated)
            update_worker_difficulty(&WorkerContext::from_stratum(&instance_id, &client_clone, &remote_app), current_diff);

            debug!("[DIFFICULTY] ===== SENDING DIFFICULTY TO {} =====", client_clone.remote_addr);
            debug!("[DIFFICULTY] Difficulty value: {} (from state: {})", current_diff, state.stratum_diff().is_some());
            if send_client_diff(&instance_id, &client_clone, current_diff).await.is_err() {
                return;
            }
            debug!("[DIFFICULTY] ===== DIFFICULTY SENT TO {} =====", client_clone.remote_addr);

            // Some strict ASIC/rental combinations need time to apply the new
            // target before accepting the first job. This delay existed in the
            // previously working 5577 path; removing it created a device-specific
            // regression where authorization succeeds but no shares follow.
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Build job params - check if this is an IceRiver or Bitmain miner
            let remote_app_lower = remote_app.to_lowercase();
            let is_iceriver =
                remote_app_lower.contains("iceriver") || remote_app_lower.contains("icemining") || remote_app_lower.contains("icm");
            let is_bitmain =
                remote_app_lower.contains("godminer") || remote_app_lower.contains("bitmain") || remote_app_lower.contains("antminer");

            debug!("[JOB] ===== BUILDING JOB FOR {} =====", client_clone.remote_addr);
            debug!("[JOB] Job ID: {}", job_id);
            debug!("[JOB] Remote app: '{}'", remote_app);
            debug!("[JOB] Is IceRiver: {}, Is Bitmain: {}, use_big_job: {}", is_iceriver, is_bitmain, state.use_big_job());
            debug!("[JOB] Pre-PoW hash: {}", pre_pow_hash);
            debug!("[JOB] Block timestamp: {}", block.header.timestamp);

            let mut job_params = vec![serde_json::Value::String(job_id.to_string())];
            debug!("[JOB] Job params initialized with job_id: {}", job_id);
            if state.use_big_job() && !is_iceriver {
                // BzMiner format - single hex string (big endian hash)
                // Convert Hash to bytes for BzMiner format
                debug!("[JOB] Generating BzMiner format job params");
                let header_bytes = pre_pow_hash.as_bytes();
                let large_params = generate_large_job_params(&header_bytes, block.header.timestamp);
                debug!("[JOB] BzMiner job_data length: {} (expected 80)", large_params.len());
                debug!("[JOB] BzMiner job_data (first 20 chars): {}", &large_params[..large_params.len().min(20)]);
                debug!("[JOB] BzMiner job_data (full): {}", large_params);
                job_params.push(serde_json::Value::String(large_params));
            } else if is_iceriver {
                // IceRiver format - single hex string (uses Hash::to_string() to match working stratum code)
                // This matches Ghostpool and other working implementations
                debug!("[JOB] Generating IceRiver format job params");
                let iceriver_params = generate_iceriver_job_params(&pre_pow_hash, block.header.timestamp);
                debug!("[JOB] IceRiver job_data length: {} (expected 80)", iceriver_params.len());
                debug!("[JOB] IceRiver job_data (first 20 chars): {}", &iceriver_params[..iceriver_params.len().min(20)]);
                debug!("[JOB] IceRiver job_data (full): {}", iceriver_params);
                job_params.push(serde_json::Value::String(iceriver_params));
            } else {
                // Legacy format - array + number (for Bitmain and other miners)
                let header_bytes = pre_pow_hash.as_bytes();
                let job_header = generate_job_header(&header_bytes);
                debug!("send_immediate_job: using Legacy format, array size: {}", job_header.len());
                job_params.push(serde_json::Value::Array(job_header.iter().map(|&v| serde_json::Value::Number(v.into())).collect()));
                job_params.push(serde_json::Value::Number(block.header.timestamp.into()));
            }

            debug!("[JOB] ===== SENDING MINING.NOTIFY TO {} =====", client_clone.remote_addr);
            debug!("[JOB] Method: mining.notify");
            debug!("[JOB] Params count: {}", job_params.len());

            // Also log the raw job data for verification
            if let Some(serde_json::Value::String(job_data)) = job_params.get(1) {
                debug!("[JOB] Job data string length: {} chars", job_data.len());
                if job_data.len() == 80 {
                    let hash_part = &job_data[..64];
                    let timestamp_part = &job_data[64..];
                    debug!("[JOB] Hash part (64 hex): {}", hash_part);
                    debug!("[JOB] Timestamp part (16 hex): {}", timestamp_part);
                    debug!("[JOB] Full job_data: {}", job_data);
                } else {
                    let expected_for = if is_iceriver {
                        "IceRiver"
                    } else if is_bitmain {
                        "Bitmain"
                    } else {
                        "standard"
                    };
                    warn!("[JOB] WARNING - job_data length is {} (expected 80 for {})", job_data.len(), expected_for);
                }
            }

            let format_name = if is_iceriver {
                "IceRiver"
            } else if state.use_big_job() {
                "BzMiner"
            } else {
                "Legacy"
            };
            debug!(
                "[JOB] Sending job ID {} to {} (format: {}, params: {})",
                job_id,
                client_clone.remote_addr,
                format_name,
                job_params.len()
            );

            // IceRiver expects minimal notification format (method + params only, no id or jsonrpc)
            // Send job ID in mining.notify
            let send_result = if is_iceriver {
                // IceRiver expects minimal notification format (method + params only, no id or jsonrpc)
                client_clone.send_notification("mining.notify", job_params.clone()).await
            } else {
                // For non-IceRiver, use standard JSON-RPC format with job ID
                let notify_event = JsonRpcEvent {
                    jsonrpc: "2.0".to_string(),
                    method: "mining.notify".to_string(),
                    id: Some(serde_json::Value::Number(job_id.into())),
                    params: job_params.clone(),
                };
                client_clone.send(notify_event).await
            };

            if let Err(e) = send_result {
                if e.to_string().contains("disconnected") {
                    record_worker_error(&instance_id, &wallet_addr, crate::errors::ErrorShortCode::Disconnected.as_str());
                    error!("[JOB] ERROR: Failed to send job {} - client disconnected", job_id);
                } else {
                    record_worker_error(&instance_id, &wallet_addr, crate::errors::ErrorShortCode::FailedSendWork.as_str());
                    error!("[JOB] ERROR: Failed sending work packet {}: {}", job_id, e);
                }
                debug!("[JOB] ===== JOB SEND FAILED FOR {} =====", client_clone.remote_addr);
            } else {
                client_clone.mark_job_sent();
                record_new_job(&WorkerContext::from_stratum(&instance_id, &client_clone, ""));
                debug!("[JOB] Successfully sent job ID {} to client {}", job_id, client_clone.remote_addr);
                debug!("[JOB] ===== JOB SENT SUCCESSFULLY TO {} =====", client_clone.remote_addr);
            }
        });
    }

    pub async fn new_block_available<T: KaspaApiTrait + Send + Sync + 'static>(&self, kaspa_api: Arc<T>) {
        // Rate limit templates (250ms minimum between sends)
        {
            let mut last_time = self.last_template_time.lock();
            if last_time.elapsed() < Duration::from_millis(250) {
                return;
            }
            *last_time = Instant::now();
        }

        let clients = {
            let clients_guard = self.clients.lock();
            clients_guard.values().cloned().collect::<Vec<_>>()
        };

        // Collect addresses for balance checking
        let mut addresses: Vec<String> = Vec::new();
        let mut client_count = 0;

        for client in clients {
            if !client.connected() {
                continue;
            }

            if client_count > 0 {
                tokio::time::sleep(Duration::from_micros(500)).await;
            }
            client_count += 1;

            // Collect wallet address for balance checking
            {
                let wallet_addr = client.wallet_addr.lock();
                if !wallet_addr.is_empty() {
                    addresses.push(wallet_addr.clone());
                }
            }

            let client_clone = Arc::clone(&client);
            let kaspa_api_clone = Arc::clone(&kaspa_api);
            let share_handler = Arc::clone(&self.share_handler);
            let min_diff = self.seed_for_port(client.local_port);
            let instance_id = self.instance_id.clone();

            tokio::spawn(async move {
                // Full-template notifications and the safety ticker can arrive
                // together. Coalesce them instead of queueing obsolete builds
                // behind the per-client lock.
                let Some(_job_build_guard) = client_clone.try_lock_job_build() else {
                    return;
                };
                // Get per-client mining state from context
                let state = GetMiningState(&client_clone);

                // Check if client has wallet address
                let wallet_addr_str = {
                    let wallet_addr = client_clone.wallet_addr.lock();
                    if wallet_addr.is_empty() {
                        let connect_time = state.connect_time();
                        if let Ok(elapsed) = connect_time.elapsed()
                            && elapsed > CLIENT_TIMEOUT
                        {
                            warn!("client misconfigured, no miner address specified - disconnecting");
                            let wallet_str = wallet_addr.clone();
                            record_worker_error(&instance_id, &wallet_str, crate::errors::ErrorShortCode::NoMinerAddress.as_str());
                            drop(wallet_addr); // Drop before disconnect
                            client_clone.disconnect();
                        }
                        debug!("new_block_available: client {} has no wallet address yet, skipping", client_clone.remote_addr);
                        return;
                    }
                    wallet_addr.clone()
                };

                debug!(
                    "new_block_available: fetching block template for client {} (wallet: {})",
                    client_clone.remote_addr, wallet_addr_str
                );

                // Get block template
                let (wallet_addr, remote_app, canxium_addr) = {
                    let wallet = client_clone.wallet_addr.lock().clone();
                    let app = client_clone.remote_app.lock().clone();
                    let canx = client_clone.canxium_addr.lock().clone();
                    (wallet, app, canx)
                };

                let generation = client_clone.next_template_generation();
                let kas_payout = client_clone.kas_payout.lock().clone();
                let lane_id = client_clone.session_uid();
                let template_result = kaspa_api_clone
                    .get_block_template(&wallet_addr, &remote_app, &canxium_addr, client_clone.session_uid(), generation, kas_payout, lane_id)
                    .await;

                let block = match template_result {
                    Ok(block) => {
                        debug!("new_block_available: successfully fetched block template for client {}", client_clone.remote_addr);
                        block
                    }
                    Err(e) => {
                        if e.to_string().contains("Could not decode address") {
                            record_worker_error(&instance_id, &wallet_addr, crate::errors::ErrorShortCode::InvalidAddressFmt.as_str());
                            error!("failed fetching new block template from kaspa, malformed address: {}", e);
                            client_clone.disconnect();
                        } else {
                            record_worker_error(&instance_id, &wallet_addr, crate::errors::ErrorShortCode::FailedBlockFetch.as_str());
                            error!("failed fetching new block template from kaspa: {}", e);
                        }
                        return;
                    }
                };

                // Calculate target
                let big_diff = calculate_target(block.header.bits as u64);
                state.set_big_diff(big_diff);

                // Serialize header - now returns Hash type directly
                // The "Odd number of digits" error typically indicates a malformed hex string
                // in one of the hash fields. This can happen if the block data from the node
                // contains an invalid hash representation.
                let pre_pow_hash = match serialize_block_header(&block) {
                    Ok(h) => h,
                    Err(e) => {
                        let error_msg = e.to_string();
                        record_worker_error(&instance_id, &wallet_addr, crate::errors::ErrorShortCode::BadDataFromMiner.as_str());
                        error!("failed to serialize block header: {}", error_msg);

                        // Log block header details for debugging
                        debug!("Block header version: {}", block.header.version);
                        debug!("Block header timestamp: {}", block.header.timestamp);
                        debug!("Block header bits: {}", block.header.bits);
                        debug!("Block header daa_score: {}", block.header.daa_score);
                        debug!("Block header blue_score: {}", block.header.blue_score);
                        debug!("Block header parents_by_level expanded_len: {}", block.header.parents_by_level.expanded_len());

                        // Skip this block and continue - the next block template should work
                        return;
                    }
                };

                // Create Job struct with both block and pre_pow_hash
                let job = Job { block: block.clone(), pre_pow_hash };

                // Add job
                let job_id = state.add_job(job);
                let counter_after = state.current_job_counter();
                let stored_ids = state.get_stored_job_ids();
                debug!(
                    "[JOB CREATION] new_block_available: created job ID {} for client {} (counter: {}, stored IDs: {:?})",
                    job_id, client_clone.remote_addr, counter_after, stored_ids
                );

                let initial_diff = share_handler.register_client_vardiff(&client_clone, min_diff);

                // Initialize state if first time (per-client state initialization)
                if !state.is_initialized() {
                    state.set_initialized(true);
                    let use_big_job = BIG_JOB_REGEX.is_match(&remote_app);
                    state.set_use_big_job(use_big_job);

                    // Send initial difficulty
                    use crate::hasher::KaspaDiff;
                    let mut stratum_diff = KaspaDiff::new();
                    // Use miner-specific calculation (IceRiver uses different formula)
                    let remote_app = client_clone.remote_app.lock().clone();
                    stratum_diff.set_diff_value_for_miner(initial_diff, &remote_app);
                    state.set_stratum_diff(stratum_diff);

                    update_worker_difficulty(&WorkerContext::from_stratum(&instance_id, &client_clone, &remote_app), initial_diff);

                    let target = state.stratum_diff().map(|d| d.target_value.clone()).unwrap_or_else(BigUint::zero);
                    let target_bytes = target.to_bytes_be();
                    debug!(
                        "Initialized per-client MiningState with difficulty: {}, target: {:x} ({} bytes, {} bits)",
                        initial_diff,
                        target,
                        target_bytes.len(),
                        target_bytes.len() * 8
                    );
                    if send_client_diff(&instance_id, &client_clone, initial_diff).await.is_err() {
                        return;
                    }
                } else {
                    // Check for vardiff update
                    if let Some(mut stratum_diff) = state.stratum_diff() {
                        let current_diff = stratum_diff.diff_value;
                        let mut var_diff = share_handler.get_client_vardiff(&client_clone);

                        // Recover from stale/recreated stats entries that can report 0.0 diff.
                        // Seed back to current state diff so UI/terminal does not stick at zero.
                        if var_diff <= 0.0 && current_diff > 0.0 {
                            share_handler.set_client_vardiff(&client_clone, current_diff);
                            share_handler.start_client_vardiff(&client_clone);
                            var_diff = current_diff;
                        }

                        if var_diff != current_diff {
                            debug!("changing diff from {} to {}", current_diff, var_diff);
                            // Use miner-specific calculation (IceRiver uses different formula)
                            let remote_app = client_clone.remote_app.lock().clone();
                            stratum_diff.set_diff_value_for_miner(var_diff, &remote_app);
                            state.set_stratum_diff(stratum_diff);

                            update_worker_difficulty(&WorkerContext::from_stratum(&instance_id, &client_clone, &remote_app), var_diff);

                            if send_client_diff(&instance_id, &client_clone, var_diff).await.is_err() {
                                return;
                            }
                            share_handler.start_client_vardiff(&client_clone);
                        }
                    }
                }

                // Build job params
                // Check if this is an IceRiver or Bitmain miner - they need single hex string format
                let remote_app = client_clone.remote_app.lock().clone();
                let remote_app_lower = remote_app.to_lowercase();
                let is_iceriver = remote_app_lower.contains("iceriver")
                    || remote_app_lower.contains("icemining")
                    || remote_app_lower.contains("icm");
                let is_bitmain = remote_app_lower.contains("godminer")
                    || remote_app_lower.contains("bitmain")
                    || remote_app_lower.contains("antminer");

                debug!(
                    "[JOB] new_block_available: client {}, is_iceriver: {}, is_bitmain: {}, use_big_job: {}",
                    client_clone.remote_addr,
                    is_iceriver,
                    is_bitmain,
                    state.use_big_job()
                );

                let mut job_params = vec![serde_json::Value::String(job_id.to_string())];
                if is_iceriver {
                    // IceRiver format - single hex string (uses Hash::to_string() to match working stratum code)
                    // This matches Ghostpool and other working implementations
                    debug!("[JOB] new_block_available: Generating IceRiver format job params");
                    let iceriver_params = generate_iceriver_job_params(&pre_pow_hash, block.header.timestamp);
                    debug!("[JOB] new_block_available: IceRiver job_data length: {} (expected 80)", iceriver_params.len());
                    job_params.push(serde_json::Value::String(iceriver_params));
                } else if state.use_big_job() && !is_iceriver {
                    // BzMiner format - single hex string (big endian hash)
                    // Convert Hash to bytes for BzMiner format
                    debug!("[JOB] new_block_available: Generating BzMiner format job params");
                    let header_bytes = pre_pow_hash.as_bytes();
                    let large_params = generate_large_job_params(&header_bytes, block.header.timestamp);
                    debug!("[JOB] new_block_available: BzMiner job_data length: {} (expected 80)", large_params.len());
                    job_params.push(serde_json::Value::String(large_params));
                } else {
                    // Legacy format - array + number (for Bitmain and other miners)
                    debug!("[JOB] new_block_available: Using Legacy format (array + timestamp)");
                    let header_bytes = pre_pow_hash.as_bytes();
                    let job_header = generate_job_header(&header_bytes);
                    job_params
                        .push(serde_json::Value::Array(job_header.iter().map(|&v| serde_json::Value::Number(v.into())).collect()));
                    job_params.push(serde_json::Value::Number(block.header.timestamp.into()));
                }

                // IceRiver expects minimal notification format (method + params only, no id or jsonrpc)
                // This matches StratumNotification format used by the stratum crate
                let is_iceriver_client = {
                    let remote_app = client_clone.remote_app.lock();
                    remote_app.contains("IceRiver")
                };
                let is_bitmain_client = {
                    let remote_app = client_clone.remote_app.lock();
                    let remote_app_lower = remote_app.to_lowercase();
                    remote_app_lower.contains("godminer")
                        || remote_app_lower.contains("bitmain")
                        || remote_app_lower.contains("antminer")
                };

                debug!(
                    "new_block_available: sending job ID {} to client {} (params count: {}, is_iceriver: {}, is_bitmain: {})",
                    job_id,
                    client_clone.remote_addr,
                    job_params.len(),
                    is_iceriver_client,
                    is_bitmain_client
                );

                // Send job ID in mining.notify
                // })
                let send_result = if is_iceriver_client {
                    // IceRiver expects minimal notification format (method + params only, no id or jsonrpc)
                    client_clone.send_notification("mining.notify", job_params.clone()).await
                } else {
                    // For non-IceRiver, use standard JSON-RPC format with job ID
                    let notify_event = JsonRpcEvent {
                        jsonrpc: "2.0".to_string(),
                        method: "mining.notify".to_string(),
                        id: Some(serde_json::Value::Number(job_id.into())),
                        params: job_params.clone(),
                    };
                    client_clone.send(notify_event).await
                };

                if let Err(e) = send_result {
                    if e.to_string().contains("disconnected") {
                        record_worker_error(&instance_id, &wallet_addr, crate::errors::ErrorShortCode::Disconnected.as_str());
                        warn!("new_block_available: failed to send job {} - client disconnected", job_id);
                    } else {
                        record_worker_error(&instance_id, &wallet_addr, crate::errors::ErrorShortCode::FailedSendWork.as_str());
                        error!("failed sending work packet {}: {}", job_id, e);
                        error!("new_block_available: failed to send job {} to client {}: {}", job_id, client_clone.remote_addr, e);
                    }
                } else {
                    client_clone.mark_job_sent();
                    record_new_job(&WorkerContext::from_stratum(&instance_id, &client_clone, ""));
                    debug!("new_block_available: successfully sent job ID {} to client {}", job_id, client_clone.remote_addr);
                }
            });
        }

        // Check balances periodically
        {
            let mut last_check = self.last_balance_check.lock();
            if last_check.elapsed() > BALANCE_DELAY && !addresses.is_empty() {
                *last_check = Instant::now();
                drop(last_check);

                // Fetch balances via kaspa_api
                let addresses_clone = addresses.clone();
                let kaspa_api_clone = Arc::clone(&kaspa_api);
                let instance_id = self.instance_id.clone();
                tokio::spawn(async move {
                    match kaspa_api_clone.get_balances_by_addresses(&addresses_clone).await {
                        Ok(balances) => {
                            // Record balances
                            crate::prom::record_balances(&instance_id, &balances);
                        }
                        Err(e) => {
                            warn!("failed to get balances from kaspa, prom stats will be out of date: {}", e);
                        }
                    }
                });
            }
        }
    }

    /// Refresh only the real Kaspa carrier for every unsolved ZKAS H_fc.
    /// ZKAS is 1 BPS; this path follows Kaspa's faster parent clock without
    /// changing the solo recipient or rebuilding H_fc.
    pub async fn new_parent_available<T: KaspaApiTrait + Send + Sync + 'static>(&self, kaspa_api: Arc<T>) {
        let clients = self.clients.lock().values().cloned().collect::<Vec<_>>();
        for client in clients {
            if !client.connected() {
                continue;
            }
            let remote_app = client.remote_app.lock().clone();
            let minimum_interval = parent_job_interval(&remote_app);
            let Some(parent_refresh_permit) = client.try_parent_refresh(minimum_interval) else {
                continue;
            };
            let state = GetMiningState(&client);
            let Some((_old_job_id, old_job)) = state.latest_job() else {
                continue;
            };
            let expected_h_fc = crate::merged::committed_h_fc(&old_job.block);
            let kaspa_api = Arc::clone(&kaspa_api);
            let instance_id = self.instance_id.clone();
            tokio::spawn(async move {
                let _parent_refresh_permit = parent_refresh_permit;
                // A parent-only refresh must keep paying whoever the ORIGINAL template
                // paid, or a refresh landing inside/outside the fee minute would silently
                // move the payout mid-job. `paid_pool` is the recorded truth for this lane.
                let refresh_payee = if kaspa_api.merged_lane_paid_pool(&old_job.block) { None } else { client.kas_payout.lock().clone() };
                let parent = match kaspa_api.refresh_merged_parent(&old_job.block, refresh_payee).await {
                    Ok(Some(parent)) => parent,
                    Ok(None) => return,
                    Err(e) => {
                        debug!("parent-only refresh failed for {}: {}", client.remote_addr, e);
                        return;
                    }
                };
                let Some(_job_guard) = client.try_lock_job_build() else {
                    return;
                };
                // A full ZKAS refresh may have replaced H_fc while the parent
                // RPC was in flight. Never publish a carrier for obsolete work.
                let Some((_latest_id, latest_job)) = state.latest_job() else {
                    return;
                };
                if crate::merged::committed_h_fc(&latest_job.block) != expected_h_fc {
                    return;
                }
                let pre_pow_hash = match serialize_block_header(&parent) {
                    Ok(hash) => hash,
                    Err(e) => {
                        error!("failed to serialize refreshed parent for {}: {}", client.remote_addr, e);
                        return;
                    }
                };
                let job_id = state.add_job(Job { block: parent.clone(), pre_pow_hash });
                let remote_app = client.remote_app.lock().clone();
                let remote_app_lower = remote_app.to_ascii_lowercase();
                let is_iceriver = remote_app_lower.contains("iceriver")
                    || remote_app_lower.contains("icemining")
                    || remote_app_lower.contains("icm");
                let mut params = vec![serde_json::Value::String(job_id.to_string())];
                if is_iceriver {
                    params.push(serde_json::Value::String(generate_iceriver_job_params(&pre_pow_hash, parent.header.timestamp)));
                } else if state.use_big_job() {
                    params
                        .push(serde_json::Value::String(generate_large_job_params(&pre_pow_hash.as_bytes(), parent.header.timestamp)));
                } else {
                    let header = generate_job_header(&pre_pow_hash.as_bytes());
                    params
                        .push(serde_json::Value::Array(header.iter().map(|&value| serde_json::Value::Number(value.into())).collect()));
                    params.push(serde_json::Value::Number(parent.header.timestamp.into()));
                }

                let send_result = if is_iceriver {
                    client.send_notification("mining.notify", params).await
                } else {
                    client
                        .send(JsonRpcEvent {
                            jsonrpc: "2.0".to_string(),
                            method: "mining.notify".to_string(),
                            id: Some(serde_json::Value::Number(job_id.into())),
                            params,
                        })
                        .await
                };
                if let Err(e) = send_result {
                    let wallet = client.wallet_addr.lock().clone();
                    record_worker_error(&instance_id, &wallet, crate::errors::ErrorShortCode::FailedSendWork.as_str());
                    debug!("failed sending parent-only job {} to {}: {}", job_id, client.remote_addr, e);
                } else {
                    client.mark_job_sent();
                    client.mark_parent_job_sent();
                    record_new_job(&WorkerContext::from_stratum(&instance_id, &client, &remote_app));
                }
            });
        }
    }
}

// Send difficulty update to client
async fn send_client_diff(instance_id: &str, client: &StratumContext, diff: f64) -> Result<(), ()> {
    debug!("[DIFFICULTY] Building difficulty message for {}", client.remote_addr);

    // Send diffValue directly as a number
    let diff_value = difficulty_to_json(diff);

    debug!("[DIFFICULTY] Sending mining.set_difficulty to {}", client.remote_addr);

    // Always use standard JSON-RPC format
    let diff_event =
        JsonRpcEvent { jsonrpc: "2.0".to_string(), method: "mining.set_difficulty".to_string(), id: None, params: vec![diff_value] };

    if let Err(e) = client.send(diff_event).await {
        let wallet_addr = client.wallet_addr.lock().clone();
        record_worker_error(instance_id, &wallet_addr, crate::errors::ErrorShortCode::FailedSetDiff.as_str());
        error!("[DIFFICULTY] ERROR: Failed sending difficulty: {}", e);
        return Err(());
    }
    debug!("[DIFFICULTY] Successfully sent difficulty {} to {}", diff, client.remote_addr);
    Ok(())
}

#[cfg(test)]
mod seed_tests {
    use super::{ClientHandler, send_client_diff};
    use crate::mining_state::MiningState;
    use crate::share_handler::ShareHandler;
    use crate::stratum_context::StratumContext;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;
    use tokio::sync::mpsc;

    #[test]
    fn per_port_seed_selection_falls_back_to_default() {
        let share_handler = Arc::new(ShareHandler::new("test".to_string()));
        let mut seeds = HashMap::new();
        seeds.insert(1111u16, 256.0);
        seeds.insert(7777u16, 65536.0);
        let handler = ClientHandler::new(share_handler, 4096.0, seeds, 2, "test".to_string());

        // Ports with explicit seeds use them; unknown ports use the default.
        assert_eq!(handler.seed_for_port(1111), 256.0);
        assert_eq!(handler.seed_for_port(7777), 65536.0);
        assert_eq!(handler.seed_for_port(9999), 4096.0);
    }

    #[tokio::test]
    async fn difficulty_send_finishes_after_message_is_written() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connect = tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });
        let (server, peer) = listener.accept().await.unwrap();
        let mut client = connect.await.unwrap();
        let (disconnect_tx, _disconnect_rx) = mpsc::unbounded_channel();
        let ctx =
            StratumContext::new(peer.ip().to_string(), peer.port(), addr.port(), server, Arc::new(MiningState::new()), disconnect_tx);

        send_client_diff("test", &ctx, 256.0).await.unwrap();

        let mut bytes = vec![0; 512];
        let read = tokio::time::timeout(std::time::Duration::from_secs(1), client.read(&mut bytes)).await.unwrap().unwrap();
        let message: serde_json::Value = serde_json::from_slice(&bytes[..read]).unwrap();
        assert_eq!(message["method"], "mining.set_difficulty");
        assert_eq!(message["params"][0], 256.0);
    }
}
