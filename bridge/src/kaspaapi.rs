use crate::log_colors::LogColors;
use crate::share_handler::KaspaApiTrait;
use anyhow::{Context, Result};
use kaspa_addresses::Address;
use kaspa_consensus_core::block::Block;
use kaspa_grpc_client::GrpcClient;
use kaspa_notify::{listener::ListenerId, scope::NewBlockTemplateScope};
use kaspa_rpc_core::notify::mode::NotificationMode;
use kaspa_rpc_core::{
    GetBlockDagInfoRequest, GetBlockTemplateRequest, GetConnectedPeerInfoRequest, GetCurrentBlockColorRequest, GetInfoRequest,
    GetServerInfoRequest, Notification, RpcHash, RpcRawBlock, SubmitBlockRejectReason, SubmitBlockReport, SubmitBlockRequest,
    SubmitBlockResponse, api::rpc::RpcApi,
};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// Outcome of [`KaspaApi::submit_block`].
///
/// Discriminates three operationally distinct cases that the
/// pre-M3f bridge collapsed into a single `Result<SubmitBlockResponse>`:
///
/// * [`Accepted`] — kaspad replied with
///   `SubmitBlockReport::Success`; the block is in the DAG (or
///   staged for propagation). The caller emits
///   `PoolEvent::BlockAccepted` and the accountant credits the
///   coinbase reward when it matures.
/// * [`RejectedByNode`] — the RPC transport succeeded but kaspad
///   declined to add the block (most commonly a tip-race
///   `BlockInvalid`). The miner's PoW is still valid (the
///   submission only reaches the `submit_block` call when the
///   share meets network target), so the caller MUST credit the
///   share to the miner. Only the block goes unrecorded.
/// * `Err` (separate from this enum) — true RPC / transport
///   failure or `ErrDuplicateBlock`; the share-handler maps the
///   latter to `ShareRejectReason::Stale`.
///
/// See `docs/phase-3-acceptance.md` §M3f for the live-evidence
/// trail that motivated this discriminator.
///
/// [`Accepted`]: BlockSubmitOutcome::Accepted
/// [`RejectedByNode`]: BlockSubmitOutcome::RejectedByNode
#[derive(Debug)]
pub enum BlockSubmitOutcome {
    /// kaspad added the block to the DAG (or staged it for
    /// propagation). Carries the full
    /// [`SubmitBlockResponse`] for diagnostics.
    Accepted(SubmitBlockResponse),
    /// kaspad accepted the RPC but rejected the block; the
    /// reason discriminates transient (`IsInIBD`, `RouteIsFull`)
    /// from permanent (`BlockInvalid`).
    RejectedByNode(SubmitBlockRejectReason),
}

/// Result of the independent Kaspa-parent leg of a merged-mining solution.
///
/// A parent can remain valuable to Kaspa even after another nonce has already
/// claimed the committed ZKAS block. Keep this lifecycle separate from
/// [`BlockSubmitOutcome`], which describes submission to the ZKAS node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergedParentSubmitOutcome {
    NotMerged,
    DoesNotClearKaspa,
    NoKaspaClient,
    Accepted,
    Rejected(String),
    TransportError(String),
}

impl MergedParentSubmitOutcome {
    pub const fn metric_label(&self) -> &'static str {
        match self {
            Self::NotMerged => "not_merged",
            Self::DoesNotClearKaspa => "does_not_clear",
            Self::NoKaspaClient => "no_client",
            Self::Accepted => "accepted",
            Self::Rejected(_) => "rejected",
            Self::TransportError(_) => "transport_error",
        }
    }
}

impl BlockSubmitOutcome {
    /// True iff the block is in kaspad's DAG (or about to be).
    #[must_use]
    pub const fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted(_))
    }
}

const STRATUM_COINBASE_TAG_BYTES: &[u8] = b"RK-Stratum";
const MAX_COINBASE_TAG_SUFFIX_LEN: usize = 64;

fn sanitize_coinbase_tag_suffix(suffix: &str) -> Option<String> {
    let suffix = suffix.trim().trim_start_matches('/');
    if suffix.is_empty() {
        return None;
    }

    let mut out = String::with_capacity(suffix.len().min(MAX_COINBASE_TAG_SUFFIX_LEN));
    for ch in suffix.chars() {
        if out.len() >= MAX_COINBASE_TAG_SUFFIX_LEN {
            break;
        }
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            out.push(ch);
        } else if ch.is_ascii_whitespace() {
            out.push('_');
        }
    }

    let out = out.trim_matches('_').to_string();
    if out.is_empty() { None } else { Some(out) }
}

fn build_coinbase_tag_bytes(suffix: Option<&str>) -> Vec<u8> {
    let mut tag = STRATUM_COINBASE_TAG_BYTES.to_vec();
    if let Some(suffix) = suffix.and_then(sanitize_coinbase_tag_suffix) {
        tag.push(b'/');
        tag.extend_from_slice(suffix.as_bytes());
    }
    tag
}

fn build_lane_coinbase_tag(base: &[u8], session_uid: u64, generation: u64) -> Vec<u8> {
    let mut tag = base.to_vec();
    tag.extend_from_slice(format!("/s{session_uid:016x}/g{generation:016x}").as_bytes());
    tag
}

struct BlockSubmitGuard {
    ttl: Duration,
    max_entries: usize,
    entries: HashMap<String, Instant>,
    order: VecDeque<String>,
}

impl BlockSubmitGuard {
    fn new(ttl: Duration, max_entries: usize) -> Self {
        Self { ttl, max_entries, entries: HashMap::new(), order: VecDeque::new() }
    }

    fn prune(&mut self, now: Instant) {
        while let Some(front) = self.order.front() {
            let remove = match self.entries.get(front) {
                Some(ts) => now.duration_since(*ts) > self.ttl,
                None => true,
            };
            if remove {
                if let Some(key) = self.order.pop_front() {
                    self.entries.remove(&key);
                }
            } else {
                break;
            }
        }

        while self.entries.len() > self.max_entries {
            if let Some(key) = self.order.pop_front() {
                self.entries.remove(&key);
            } else {
                break;
            }
        }
    }

    fn try_mark(&mut self, hash: &str, now: Instant) -> bool {
        self.prune(now);
        if self.entries.contains_key(hash) {
            return false;
        }
        self.entries.insert(hash.to_string(), now);
        self.order.push_back(hash.to_string());
        true
    }

    fn remove(&mut self, hash: &str, now: Instant) {
        self.prune(now);
        self.entries.remove(hash);
    }
}

static BLOCK_SUBMIT_GUARD: Lazy<Mutex<BlockSubmitGuard>> =
    Lazy::new(|| Mutex::new(BlockSubmitGuard::new(Duration::from_secs(600), 50_000)));

#[derive(Clone, Debug, Default)]
pub struct NodeStatusSnapshot {
    pub last_updated: Option<std::time::Instant>,
    pub is_connected: bool,
    pub is_synced: Option<bool>,
    pub network_id: Option<String>,
    pub server_version: Option<String>,
    pub virtual_daa_score: Option<u64>,
    pub block_count: Option<u64>,
    pub header_count: Option<u64>,
    pub difficulty: Option<f64>,
    pub tip_hash: Option<String>,
    pub peers: Option<usize>,
    pub mempool_size: Option<u64>,
}

pub static NODE_STATUS: Lazy<Mutex<NodeStatusSnapshot>> = Lazy::new(|| Mutex::new(NodeStatusSnapshot::default()));

/// Kaspa API client wrapper using RPC client
/// Both use gRPC under the hood, but through an RPC client wrapper abstraction
pub struct KaspaApi {
    client: Arc<GrpcClient>,
    zkas_node_endpoint: String,
    notification_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<Notification>>>>,
    /// Independent template stream from the real Kaspa parent node. ZKAS is
    /// 1 BPS, while Kaspa parent work changes at roughly 10 BPS.
    kaspa_notification_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<Notification>>>>,
    connected: Arc<Mutex<bool>>,
    coinbase_tag: Vec<u8>,
    /// katpool fork addition. When `Some`, every
    /// [`Self::get_block_template`] call replaces the
    /// miner-supplied `wallet_addr` with this address before
    /// calling kaspad. That's how a custodial PROP pool works:
    /// miners authorize with their own addresses (used for
    /// share-credit attribution), but every block's coinbase
    /// pays the **pool**, which then pro-rates the matured
    /// reward across miners' share weights in the accountant.
    ///
    /// When `None`, preserves upstream behaviour: each miner's
    /// coinbase pays the miner directly (solo / MM-pool model).
    coinbase_address_override: Option<Address>,

    /// Merged-mining (AuxPoW) mode, enabled by `ZKAS_MERGED_MINING=1` (legacy `FIRECASH_MERGED_MINING` honored). When on,
    /// [`Self::get_block_template`] hands the ASIC a parent block committing to the
    /// ZKas `H_fc`, and [`Self::submit_block`] turns a solved parent back into a
    /// ZKas block carrying the AuxPoW proof. See [`crate::merged`].
    merged_mining: bool,
    /// ZKas blocks awaiting a solved parent, keyed by `H_fc` (merged mode only).
    pending_fc: Arc<Mutex<crate::merged::MergedPending>>,

    /// Real dual-chain merged mining: gRPC client to the upstream **Kaspa** node that
    /// supplies the parent block template and receives Kaspa-target-clearing blocks
    /// (the KAS reward path). `None` ⇒ synthetic-parent merged mode (ZKas aux
    /// blocks only, no KAS). Set from `ZKAS_KASPA_NODE` (or legacy `FIRECASH_KASPA_NODE`).
    kaspa_client: Option<Arc<GrpcClient>>,
    /// The `kaspa:` address each real parent's coinbase pays. Set from
    /// `ZKAS_KASPA_PAY` (or legacy `FIRECASH_KASPA_PAY`).
    kaspa_pay: Option<Address>,
    /// Short-lived exact-H_fc cache for duplicate requests. Solo lanes use
    /// distinct H_fc values, so fleet-wide pressure is controlled by
    /// `kaspa_parent_rpc_gate`, not by pretending all miners share one parent.
    /// Keyed on `(H_fc, payee)` — BOTH parts are load-bearing.
    ///
    /// Keying on `H_fc` alone was safe only while every parent paid the same pool
    /// address. Now that a lane's parent pays the miner's own `kaspa:` address, an
    /// `H_fc` collision between two lanes would let one miner receive a cached
    /// template minting KAS to the OTHER miner's address. Per-lane coinbase tags
    /// should make that collision impossible, but "should" is not a good enough
    /// argument when the failure silently redirects money, so the payee is in the key.
    kaspa_parent_cache: Arc<tokio::sync::Mutex<Option<(kaspa_hashes::Hash, String, Block, Instant)>>>,
    /// Bound parent-template RPC concurrency. Unique solo lanes necessarily
    /// use distinct H_fc commitments, so a single global mutex would serialize
    /// the entire fleet; unbounded fan-out would overload one gRPC route.
    kaspa_parent_rpc_gate: Arc<tokio::sync::Semaphore>,
}

/// How long a cached Kaspa parent is reused before refetching. Kaspa makes a tip
/// ~every 100ms; 150ms keeps the parent fresh (low `BlockInvalid`) while collapsing the
/// per-worker fetch storm into ~one call per window.
const KASPA_PARENT_TTL: Duration = Duration::from_millis(150);

impl KaspaApi {
    /// Create a new Kaspa API client.
    ///
    /// `coinbase_address_override` carries the pool's address when running in
    /// custodial PROP-pool mode (see the struct-level docs). Pass `None` to keep
    /// upstream solo / MM-pool behaviour.
    pub async fn new(
        address: String,
        coinbase_tag_suffix: Option<String>,
        shutdown_rx: watch::Receiver<bool>,
        coinbase_address_override: Option<Address>,
    ) -> Result<Arc<Self>> {
        Self::new_with_merged(address, coinbase_tag_suffix, shutdown_rx, coinbase_address_override, None, None).await
    }

    /// Create a node client with optional real dual-chain merged mining.
    ///
    /// The standalone bridge supplies these values from `config.yaml`. Environment
    /// variables remain supported and take precedence for existing deployments.
    pub async fn new_with_merged(
        address: String,
        coinbase_tag_suffix: Option<String>,
        mut shutdown_rx: watch::Receiver<bool>,
        coinbase_address_override: Option<Address>,
        configured_kaspa_node: Option<String>,
        configured_kaspa_pay: Option<String>,
    ) -> Result<Arc<Self>> {
        info!("Connecting to Kaspa node at {}", address);

        // GrpcClient requires explicit "grpc://" prefix for connection
        // Always add it if not present (avoids unnecessary connection failure)
        let grpc_address = if address.starts_with("grpc://") { address.clone() } else { format!("grpc://{}", address) };

        // Log connection attempt (detailed logs moved to debug)
        debug!("{} {}", LogColors::api("[API]"), LogColors::label("Establishing RPC connection to Kaspa node:"));
        debug!("{} {} {}", LogColors::api("[API]"), LogColors::label("  - Address:"), &grpc_address);
        debug!("{} {} {}", LogColors::api("[API]"), LogColors::label("  - Protocol:"), "gRPC (via RPC client wrapper)");

        let mut attempt: u64 = 0;
        let mut backoff_ms: u64 = 250;

        let client = loop {
            attempt += 1;
            let connect_fut = GrpcClient::connect_with_args(
                NotificationMode::Direct,
                grpc_address.clone(),
                None,
                true,
                None,
                false,
                Some(500_000),
                Default::default(),
            );

            let res = tokio::select! {
                _ = shutdown_rx.wait_for(|v| *v) => {
                    return Err(anyhow::anyhow!("shutdown requested"));
                }
                res = connect_fut => res,
            };

            match res {
                Ok(client) => break Arc::new(client),
                Err(e) => {
                    let backoff = Duration::from_millis(backoff_ms);
                    warn!(
                        "failed to connect to kaspa node at {} (attempt {}): {}, retrying in {:.2}s",
                        grpc_address,
                        attempt,
                        e,
                        backoff.as_secs_f64()
                    );

                    tokio::select! {
                        _ = shutdown_rx.wait_for(|v| *v) => {
                            return Err(anyhow::anyhow!("shutdown requested"));
                        }
                        _ = sleep(backoff) => {}
                    }

                    backoff_ms = (backoff_ms.saturating_mul(2)).min(5_000);
                }
            }
        };

        // Log successful connection (detailed logs moved to debug)
        debug!("{} {}", LogColors::api("[API]"), LogColors::block("RPC Connection Established Successfully"));
        debug!("{} {} {}", LogColors::api("[API]"), LogColors::label("  - Connected to:"), &grpc_address);
        debug!("{} {} {}", LogColors::api("[API]"), LogColors::label("  - Connection Type:"), "gRPC (via RPC client wrapper)");

        // Start the client (no notify needed for Direct mode)
        client.start(None).await;

        // Subscribe to block template notifications
        // Some nodes may take time to accept notification subscriptions; retry until it succeeds.
        // This retry logic with exponential backoff handles transient failures where nodes are not
        // immediately ready to accept subscriptions after connection, preventing tight-looping and log spam.
        let mut attempt: u64 = 0;
        let mut backoff_ms: u64 = 250;
        loop {
            attempt += 1;
            let notify_fut = client.start_notify(ListenerId::default(), NewBlockTemplateScope {}.into());

            let res = tokio::select! {
                _ = shutdown_rx.wait_for(|v| *v) => {
                    return Err(anyhow::anyhow!("shutdown requested"));
                }
                res = notify_fut => res,
            };

            match res {
                Ok(_) => break,
                Err(e) => {
                    let backoff = Duration::from_millis(backoff_ms);
                    warn!(
                        "failed to subscribe to block template notifications (attempt {}): {}, retrying in {:.2}s",
                        attempt,
                        e,
                        backoff.as_secs_f64()
                    );

                    tokio::select! {
                        _ = shutdown_rx.wait_for(|v| *v) => {
                            return Err(anyhow::anyhow!("shutdown requested"));
                        }
                        _ = sleep(backoff) => {}
                    }
                    backoff_ms = (backoff_ms.saturating_mul(2)).min(5_000);
                }
            }
        }

        // Start receiving notifications
        let notification_rx = {
            let receiver = client.notification_channel_receiver();
            // Convert async_channel::Receiver to tokio::sync::mpsc::UnboundedReceiver
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let receiver_clone = receiver.clone();
            tokio::spawn(async move {
                while let Ok(notification) = receiver_clone.recv().await {
                    let _ = tx.send(notification);
                }
            });
            Arc::new(Mutex::new(Some(rx)))
        };

        let coinbase_tag = build_coinbase_tag_bytes(coinbase_tag_suffix.as_deref());
        if let Some(addr) = &coinbase_address_override {
            info!("Coinbase recipient override active: every block template will pay {}", addr);
        }
        let configured_kaspa_node = configured_kaspa_node.unwrap_or_default();
        let configured_kaspa_pay = configured_kaspa_pay.unwrap_or_default();
        let node = std::env::var("ZKAS_KASPA_NODE").or_else(|_| std::env::var("FIRECASH_KASPA_NODE")).unwrap_or(configured_kaspa_node);
        let pay = std::env::var("ZKAS_KASPA_PAY").or_else(|_| std::env::var("FIRECASH_KASPA_PAY")).unwrap_or(configured_kaspa_pay);
        let merged_mining = std::env::var("ZKAS_MERGED_MINING")
            .or_else(|_| std::env::var("FIRECASH_MERGED_MINING"))
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or_else(|_| !node.trim().is_empty() && !pay.trim().is_empty());
        if merged_mining {
            info!(
                "Merged-mining (AuxPoW) mode ENABLED: ASICs hash a parent committing to the ZKas block; solved parents are submitted as ZKas aux blocks"
            );
        }

        // Real dual-chain: connect to the upstream Kaspa node so the *same* solved
        // parent that yields a ZKas aux block also earns KAS when it clears the
        // (harder) Kaspa target. Best-effort: if unset or unreachable, merged mining
        // degrades to ZKas-aux-only rather than failing to start.
        let (kaspa_client, kaspa_pay) = if merged_mining {
            if node.is_empty() || pay.is_empty() {
                info!("Real merged mining disabled (set ZKAS_KASPA_NODE + ZKAS_KASPA_PAY to also earn KAS); running ZKas-aux-only");
                (None, None)
            } else {
                let grpc = if node.starts_with("grpc://") { node.clone() } else { format!("grpc://{node}") };
                match GrpcClient::connect_with_args(
                    NotificationMode::Direct,
                    grpc,
                    None,
                    true,
                    None,
                    false,
                    Some(500_000),
                    Default::default(),
                )
                .await
                {
                    Ok(kc) => match Address::try_from(pay.as_str()) {
                        Ok(addr) => {
                            info!("Real merged mining ENABLED: parent from Kaspa node {node}, KAS coinbase pays {pay}");
                            (Some(Arc::new(kc)), Some(addr))
                        }
                        Err(e) => {
                            warn!("ZKAS_KASPA_PAY is not a valid kaspa: address ({e}); running ZKas-aux-only");
                            (None, None)
                        }
                    },
                    Err(e) => {
                        warn!("could not connect to Kaspa node {node} ({e}); running ZKas-aux-only");
                        (None, None)
                    }
                }
            }
        } else {
            (None, None)
        };

        let kaspa_notification_rx = if let Some(kc) = kaspa_client.as_ref() {
            match kc.start_notify(ListenerId::default(), NewBlockTemplateScope {}.into()).await {
                Ok(()) => {
                    let receiver = kc.notification_channel_receiver();
                    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                    let receiver_clone = receiver.clone();
                    tokio::spawn(async move {
                        while let Ok(notification) = receiver_clone.recv().await {
                            let _ = tx.send(notification);
                        }
                    });
                    Arc::new(Mutex::new(Some(rx)))
                }
                Err(e) => {
                    warn!("failed to subscribe to Kaspa parent-template notifications ({e}); using parent ticker fallback");
                    Arc::new(Mutex::new(None))
                }
            }
        } else {
            Arc::new(Mutex::new(None))
        };

        let api = Arc::new(Self {
            client,
            zkas_node_endpoint: address,
            notification_rx,
            kaspa_notification_rx,
            connected: Arc::new(Mutex::new(true)),
            coinbase_tag,
            coinbase_address_override,
            merged_mining,
            pending_fc: Arc::new(Mutex::new(crate::merged::MergedPending::new(4096))),
            kaspa_client,
            kaspa_pay,
            kaspa_parent_cache: Arc::new(tokio::sync::Mutex::new(None)),
            kaspa_parent_rpc_gate: Arc::new(tokio::sync::Semaphore::new(4)),
        });

        // Start network stats thread
        let api_clone = Arc::clone(&api);
        tokio::spawn(async move {
            api_clone.start_stats_thread().await;
        });

        // Start node status polling thread (for console status display)
        let api_clone = Arc::clone(&api);
        tokio::spawn(async move {
            api_clone.start_node_status_thread().await;
        });

        Ok(api)
    }

    /// Start network stats thread
    /// Fetches network stats every 30 seconds and records them in Prometheus
    async fn start_stats_thread(self: Arc<Self>) {
        use crate::prom::record_network_stats;
        use kaspa_rpc_core::{EstimateNetworkHashesPerSecondRequest, GetBlockDagInfoRequest};

        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;

            // Get block DAG info
            // GetBlockDagInfoRequest is a unit struct, construct directly
            let dag_response = match self.client.get_block_dag_info_call(None, GetBlockDagInfoRequest {}).await {
                Ok(r) => r,
                Err(e) => {
                    warn!("failed to get network hashrate from kaspa, prom stats will be out of date: {}", e);
                    continue;
                }
            };

            // Get tip hash (first one)
            // tip_hashes is Vec<Hash> in the response (already parsed)
            let tip_hash = match dag_response.tip_hashes.first() {
                Some(hash) => Some(*hash), // Clone the Hash
                None => {
                    warn!("no tip hashes available for network hashrate estimation");
                    continue;
                }
            };

            // Estimate network hashes per second
            // new(window_size: u32, start_hash: Option<RpcHash>)
            // RpcHash is the same as Hash, so we can use tip_hash directly
            let hashrate_response = match self
                .client
                .estimate_network_hashes_per_second_call(None, EstimateNetworkHashesPerSecondRequest::new(1000, tip_hash))
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    warn!("failed to get network hashrate from kaspa, prom stats will be out of date: {}", e);
                    continue;
                }
            };

            // Record network stats. The dashboard labels this the chain "Block height", so
            // it must be the virtual DAA score (the height users and the explorer see), NOT
            // `block_count` — on a pruned node block_count is only the retained-block tally
            // (~115K here) and reads as a stale/wrong height while the chain is actually at
            // DAA ~334K.
            record_network_stats(hashrate_response.network_hashes_per_second, dag_response.virtual_daa_score, dag_response.difficulty);
        }
    }

    async fn start_node_status_thread(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;

            let connected = self.client.is_connected();

            let server_info_fut = self.client.get_server_info_call(None, GetServerInfoRequest {});
            let dag_info_fut = self.client.get_block_dag_info_call(None, GetBlockDagInfoRequest {});
            let peers_fut = self.client.get_connected_peer_info_call(None, GetConnectedPeerInfoRequest {});
            let info_fut = self.client.get_info_call(None, GetInfoRequest {});

            let (server_info, dag_info, peers_info, info_resp) = tokio::join!(server_info_fut, dag_info_fut, peers_fut, info_fut);

            let mut snapshot = NODE_STATUS.lock();
            snapshot.last_updated = Some(std::time::Instant::now());
            snapshot.is_connected = connected;

            if let Ok(server_info) = server_info {
                snapshot.is_synced = Some(server_info.is_synced);
                snapshot.network_id = Some(format!("{:?}", server_info.network_id));
                snapshot.server_version = Some(server_info.server_version);
                snapshot.virtual_daa_score = Some(server_info.virtual_daa_score);
            }

            if let Ok(dag) = dag_info {
                snapshot.block_count = Some(dag.block_count);
                snapshot.header_count = Some(dag.header_count);
                snapshot.difficulty = Some(dag.difficulty);
                snapshot.tip_hash = dag.tip_hashes.first().map(|h| format!("{}", h));
                if snapshot.virtual_daa_score.is_none() {
                    snapshot.virtual_daa_score = Some(dag.virtual_daa_score);
                }
                if snapshot.network_id.is_none() {
                    snapshot.network_id = Some(format!("{:?}", dag.network));
                }
            }

            if let Ok(peers) = peers_info {
                snapshot.peers = Some(peers.peer_info.len());
            }

            if let Ok(info) = info_resp {
                snapshot.mempool_size = Some(info.mempool_size);
                if snapshot.server_version.is_none() {
                    snapshot.server_version = Some(info.server_version);
                }
            }
        }
    }

    /// Fetch a real Kaspa block template from the upstream node, paying our `kaspa:`
    /// address and embedding `ZKMM || h_fc` in the coinbase `extra_data`, so a solved
    /// parent is a valid Kaspa block that both (a) can be submitted to Kaspa for KAS and
    /// (b) proves the ZKas block via AuxPoW. Errs if the Kaspa client/pay address is
    /// unset (caller falls back to a synthetic parent).
    ///
    /// `payee` overrides the pool address when a miner supplied its own `kaspa:`
    /// address in the stratum password. Passing `None` pays the pool, which is both
    /// the legacy behaviour and the pool-fee-minute behaviour.
    async fn fetch_kaspa_parent(&self, h_fc: kaspa_hashes::Hash, payee: Option<&Address>, force: bool) -> Result<Block> {
        let kc = self.kaspa_client.as_ref().ok_or_else(|| anyhow::anyhow!("no Kaspa node client"))?;
        let pay = match payee {
            Some(a) => a.clone(),
            None => self.kaspa_pay.clone().ok_or_else(|| anyhow::anyhow!("no Kaspa pay address"))?,
        };
        let pay_key = pay.to_string();

        {
            let cache = self.kaspa_parent_cache.lock().await;
            if let Some((ch, cpay, parent, at)) = cache.as_ref()
                && !force
                && *ch == h_fc
                && cpay == &pay_key
                && at.elapsed() < KASPA_PARENT_TTL
            {
                return Ok(parent.clone());
            }
        }
        let _permit = self.kaspa_parent_rpc_gate.acquire().await.context("Kaspa parent RPC gate closed")?;
        let extra_data = kaspa_consensus_core::auxpow::AuxPow::embed_commitment(&[], h_fc, &[]);
        let resp =
            kc.get_block_template_call(None, GetBlockTemplateRequest::new(pay, extra_data)).await.context("kaspa getBlockTemplate")?;
        let parent = Block::try_from(resp.block).map_err(|e| anyhow::anyhow!("kaspa block conversion: {e:?}"))?;
        *self.kaspa_parent_cache.lock().await = Some((h_fc, pay_key, parent.clone(), Instant::now()));
        Ok(parent)
    }

    pub async fn refresh_merged_parent(&self, current_parent: &Block, payee: Option<&Address>) -> Result<Option<Block>> {
        if !self.merged_mining || self.kaspa_client.is_none() {
            return Ok(None);
        }
        let h_fc =
            crate::merged::committed_h_fc(current_parent).ok_or_else(|| anyhow::anyhow!("merged parent has no ZKMM commitment"))?;
        if !self.pending_fc.lock().is_unsolved(&h_fc) {
            return Ok(None);
        }
        self.fetch_kaspa_parent(h_fc, payee, true).await.map(Some)
    }

    /// In real merged mining the parent carries the *Kaspa* target (`header.bits`), but
    /// ZKas aux blocks should be found at the ZKas (easier) cadence. This returns
    /// the ZKas target for a given parent (looked up via its committed `H_fc` → the
    /// stashed ZKas block's own `bits`), which the share handler uses as the
    /// block-found threshold in merged mode. `None` ⇒ not merged / unknown parent ⇒
    /// caller uses the parent's own `bits`.
    pub fn merged_fc_target(&self, parent_block: &Block) -> Option<num_bigint::BigUint> {
        if !self.merged_mining {
            return None;
        }
        let h_fc = crate::merged::committed_h_fc(parent_block)?;
        let fc_block = self.pending_fc.lock().get(&h_fc)?;
        Some(crate::hasher::calculate_target(fc_block.header.bits as u64))
    }

    /// The ZKas chain hash for a solved merged-mode parent: the committed
    /// `H_fc`. The AuxPoW rides outside the header hash (dual acceptance), so
    /// the block that lands on the ZKas chain keeps exactly this hash — it is
    /// the right handle for `get_current_block_color` and every block-facing
    /// stat. `None` when not merged (the job header's own hash is the chain
    /// hash) or when the parent carries no commitment.
    /// Did this lane's Kaspa parent pay the POOL (fee minute) rather than the miner?
    ///
    /// Read from what was recorded when the template was built, never re-derived from
    /// the clock — see [`crate::merged::MergedPending`].
    pub fn merged_lane_paid_pool(&self, parent_block: &Block) -> bool {
        match crate::merged::committed_h_fc(parent_block) {
            Some(h) => self.pending_fc.lock().paid_pool(&h),
            None => false,
        }
    }

    pub fn merged_chain_hash(&self, parent_block: &Block) -> Option<kaspa_hashes::Hash> {
        if !self.merged_mining {
            return None;
        }
        crate::merged::committed_h_fc(parent_block)
    }

    pub fn claim_network_solution(&self, job_block: &Block) -> bool {
        if !self.merged_mining {
            return true;
        }
        let Some(h_fc) = crate::merged::committed_h_fc(job_block) else {
            return true;
        };
        self.pending_fc.lock().claim_solution(h_fc)
    }

    /// Independently submit a solved merged-mining parent to Kaspa.
    ///
    /// This must run before ZKAS `H_fc` deduplication. A second nonce for an
    /// already-claimed ZKAS commitment cannot mint another ZKAS block, but it
    /// is still a distinct, potentially reward-bearing Kaspa block.
    pub async fn submit_merged_parent_if_solved(&self, parent: &Block) -> MergedParentSubmitOutcome {
        if !self.merged_mining {
            return MergedParentSubmitOutcome::NotMerged;
        }

        let (clears_kaspa, _) = kaspa_pow::State::new(&parent.header).check_pow(parent.header.nonce);
        if !clears_kaspa {
            return MergedParentSubmitOutcome::DoesNotClearKaspa;
        }

        let Some(kc) = &self.kaspa_client else {
            return MergedParentSubmitOutcome::NoKaspaClient;
        };

        let kaspa_hash = kaspa_consensus_core::hashing::header::hash(&parent.header).to_string();
        let rpc_parent: RpcRawBlock = parent.into();
        match kc.submit_block_call(None, SubmitBlockRequest::new(rpc_parent, false)).await {
            Ok(response) => match response.report {
                SubmitBlockReport::Success => {
                    info!("{} KASPA BLOCK FOUND & accepted! hash={}", LogColors::block("[MERGED]"), kaspa_hash);
                    MergedParentSubmitOutcome::Accepted
                }
                SubmitBlockReport::Reject(reason) => {
                    let reason = format!("{reason:?}");
                    warn!("{} Kaspa block rejected ({}) hash={}", LogColors::block("[MERGED]"), reason, kaspa_hash);
                    MergedParentSubmitOutcome::Rejected(reason)
                }
            },
            Err(error) => {
                let error = error.to_string();
                warn!("{} Kaspa submit transport error: {}", LogColors::block("[MERGED]"), error);
                MergedParentSubmitOutcome::TransportError(error)
            }
        }
    }

    /// Submit a block to kaspad.
    ///
    /// Returns:
    /// * `Ok(BlockSubmitOutcome::Accepted(response))` when kaspad
    ///   replied with `SubmitBlockReport::Success` — the block is
    ///   in the DAG (or at least staged for propagation).
    /// * `Ok(BlockSubmitOutcome::RejectedByNode(reason))` when
    ///   kaspad accepted the RPC but rejected the block itself
    ///   (`Reject(BlockInvalid)` / `Reject(IsInIBD)` /
    ///   `Reject(RouteIsFull)`). The miner's PoW is still valid
    ///   (they met the share/network difficulty by definition —
    ///   that's how we got here); the caller therefore credits the
    ///   share even though no block is recorded. This matches the
    ///   pre-M3f miner-facing behaviour while preventing the
    ///   pre-M3f phantom `BlockAccepted` accounting bug — see
    ///   `docs/phase-3-acceptance.md` §M3f for the live-evidence
    ///   trail that motivated this discriminator.
    /// * `Err(e)` only for true transport / RPC-layer failures
    ///   (including `ErrDuplicateBlock`, which the share handler
    ///   maps to `ShareRejectReason::Stale`).
    #[tracing::instrument(
        name = "kaspa.submit_block",
        skip_all,
        fields(
            block_hash = tracing::field::Empty,
            blue_score = block.header.blue_score,
            nonce = block.header.nonce,
        ),
        err,
    )]
    pub async fn submit_block(&self, block: Block) -> Result<BlockSubmitOutcome> {
        // Merged mode: what the bridge hands us here is a solved *parent* block. Rebuild
        // the ZKas block carrying the AuxPoW proof (stashed ZKas block + this
        // parent's kHeavyHash) and submit that instead. The aux rides on
        // `RpcRawHeader.aux_pow`, so the `(&block).into()` conversion below transmits it.
        let block = if self.merged_mining {
            // Assemble the ZKas block carrying the AuxPoW proof from the same parent.
            match crate::merged::committed_h_fc(&block).and_then(|h| self.pending_fc.lock().get(&h)) {
                Some(fc_block) => crate::merged::assemble_aux_block(&block, &fc_block),
                None => return Err(anyhow::anyhow!("merged: no pending ZKas block for the solved parent (stale template)")),
            }
        } else {
            block
        };
        // Use kaspa_consensus_core::hashing::header::hash() for block hash calculation
        // In Kaspa, the block hash is the header hash (transactions are represented by hash_merkle_root in header)
        use kaspa_consensus_core::hashing::header;
        let block_hash = header::hash(&block.header).to_string();
        let blue_score = block.header.blue_score;
        let timestamp = block.header.timestamp;
        let nonce = block.header.nonce;
        tracing::Span::current().record("block_hash", block_hash.as_str());

        {
            let now = Instant::now();
            let mut guard = BLOCK_SUBMIT_GUARD.lock();
            if !guard.try_mark(&block_hash, now) {
                return Err(anyhow::anyhow!("ErrDuplicateBlock: block already submitted"));
            }
        }

        debug!(
            "{} {}",
            LogColors::api("[API]"),
            LogColors::api(&format!("===== ATTEMPTING BLOCK SUBMISSION TO KASPA NODE ===== Hash: {}", block_hash))
        );
        debug!("{} {}", LogColors::api("[API]"), LogColors::label("Block Details:"));
        debug!("{} {} {}", LogColors::api("[API]"), LogColors::label("  - Hash:"), block_hash);
        debug!("{} {} {}", LogColors::api("[API]"), LogColors::label("  - Blue Score:"), blue_score);
        debug!("{} {} {}", LogColors::api("[API]"), LogColors::label("  - Timestamp:"), timestamp);
        debug!("{} {} {}", LogColors::api("[API]"), LogColors::label("  - Nonce:"), format!("{:x} ({})", nonce, nonce));
        debug!("{} {}", LogColors::api("[API]"), "Converting block to RPC format and sending to node...");

        // Convert Block to RpcRawBlock (use reference)
        let rpc_block: RpcRawBlock = (&block).into();

        // Submit block (don't allow non-DAA blocks).
        //
        // CRITICAL discrimination: the RPC `submit_block_call`
        // returns `Ok(SubmitBlockResponse)` whenever the transport
        // succeeded — *even when kaspad rejected the block*. The
        // `report` field carries the actual acceptance verdict:
        //
        //   * `SubmitBlockReport::Success`                — accepted
        //   * `SubmitBlockReport::Reject(BlockInvalid)`   — bad header / tx / coinbase / stale tip race
        //   * `SubmitBlockReport::Reject(IsInIBD)`        — node not yet synced
        //   * `SubmitBlockReport::Reject(RouteIsFull)`    — back-pressure; retry later
        //
        // The pre-M3f bridge ignored `report` and matched only on
        // `Ok(_)`, which produced the "phantom block accepted" log
        // storm uncovered during the Goldshell live exercise
        // (`docs/phase-3-acceptance.md` §M3f, 79% of submissions
        // were rejected but logged as wins). The first M3f cut
        // over-corrected: it collapsed `Reject(reason)` into an
        // `Err` so the share handler ALSO treated the outcome as
        // a miner PoW failure (`ShareRejectReason::BadPow`),
        // which spiked the miner-visible reject rate to ~68%
        // even though the miner's work was valid (Reject reasons
        // are pool-side race conditions / node state issues, not
        // miner faults — the share by definition met the network
        // target or we would not have entered this branch). We
        // now return a structured `BlockSubmitOutcome` so callers
        // can distinguish "block in DAG" from "submitted but
        // kaspad rejected" without conflating either with
        // transport failure — `Err` is reserved for genuine RPC
        // errors and `ErrDuplicateBlock`.
        debug!("{} {}", LogColors::api("[API]"), "Calling submit_block via RPC client...");
        let rpc_result =
            self.client.submit_block_call(None, SubmitBlockRequest::new(rpc_block, false)).await.context("Failed to submit block");
        let result: Result<BlockSubmitOutcome> = match rpc_result {
            Ok(response) => match response.report {
                SubmitBlockReport::Success => Ok(BlockSubmitOutcome::Accepted(response)),
                SubmitBlockReport::Reject(reason) => Ok(BlockSubmitOutcome::RejectedByNode(reason)),
            },
            Err(e) => Err(e),
        };

        // The block-submit guard only needs to be cleared on a
        // transport failure (so the submit can be retried). A
        // `RejectedByNode` outcome means kaspad already saw the
        // hash and produced a verdict — re-submitting would just
        // reproduce it — so we keep the guard set to suppress
        // duplicate noise on rapid re-races.
        if let Err(e) = &result {
            let error_str = e.to_string();
            let is_duplicate = error_str.contains("ErrDuplicateBlock") || error_str.contains("duplicate");
            if !is_duplicate {
                let now = Instant::now();
                let mut guard = BLOCK_SUBMIT_GUARD.lock();
                guard.remove(&block_hash, now);
            }
        }

        match &result {
            Ok(BlockSubmitOutcome::RejectedByNode(reason)) => {
                // Submitted but kaspad declined to add to its DAG.
                // The miner's share is still valid (they hit
                // network target); only the block goes
                // unrecorded. Log at WARN — operationally
                // significant but not actionable per-event;
                // dashboards should track rate, not absolute
                // count.
                warn!(
                    "{} {}",
                    LogColors::api("[API]"),
                    LogColors::validation(&format!(
                        "===== BLOCK REJECTED BY KASPA NODE: {} ===== Hash: {}",
                        submit_block_reject_label(*reason),
                        block_hash
                    ))
                );
                debug!(
                    "{} {} {}",
                    LogColors::api("[API]"),
                    LogColors::label("  - Blue Score:"),
                    format!("{}, Timestamp: {}, Nonce: {:x}", blue_score, timestamp, nonce)
                );
            }
            Ok(BlockSubmitOutcome::Accepted(response)) => {
                // Keep block accepted message at info (important operational event)
                info!(
                    "{} {}",
                    LogColors::api("[API]"),
                    LogColors::block(&format!("===== BLOCK ACCEPTED BY KASPA NODE ===== Hash: {}", block_hash))
                );
                // Detailed acceptance logs moved to debug
                debug!(
                    "{} {} {}",
                    LogColors::api("[API]"),
                    LogColors::label("ACCEPTANCE REASON:"),
                    "Block passed all node validation checks"
                );
                debug!("{} {} {}", LogColors::api("[API]"), LogColors::label("  - Block structure:"), "VALID");
                debug!("{} {} {}", LogColors::api("[API]"), LogColors::label("  - Block header:"), "VALID");
                debug!("{} {} {}", LogColors::api("[API]"), LogColors::label("  - Transactions:"), "VALID");
                debug!("{} {} {}", LogColors::api("[API]"), LogColors::label("  - DAA validation:"), "PASSED");
                debug!("{} {} {}", LogColors::api("[API]"), LogColors::label("  - Node Response:"), format!("{:?}", response));
                debug!(
                    "{} {} {}",
                    LogColors::api("[API]"),
                    LogColors::label("  - Blue Score:"),
                    format!("{}, Timestamp: {}, Nonce: {:x}", blue_score, timestamp, nonce)
                );

                // Optional: Check if block appears in tip hashes (verifies propagation)
                // This is informational only - block may still propagate even if not immediately in tips
                let client_clone = Arc::clone(&self.client);
                let block_hash_clone = block_hash.clone();
                let block_hash_for_check = header::hash(&block.header); // Use the actual Hash type
                tokio::spawn(async move {
                    // Wait a bit for block to be processed and potentially added to DAG
                    tokio::time::sleep(Duration::from_secs(2)).await;

                    // Check if block appears in tip hashes
                    if let Ok(dag_response) = client_clone.get_block_dag_info_call(None, GetBlockDagInfoRequest {}).await {
                        // Check if our block hash is in tip hashes
                        let in_tips = dag_response.tip_hashes.contains(&block_hash_for_check);

                        if in_tips {
                            info!(
                                "{} {} {}",
                                LogColors::api("[API]"),
                                LogColors::block("Block appears in tip hashes (good sign for propagation)"),
                                format!("Hash: {}", block_hash_clone)
                            );
                        } else {
                            // This is not necessarily bad - block may still propagate or be in a side chain
                            info!(
                                "{} {} {}",
                                LogColors::api("[API]"),
                                LogColors::label("Block not yet in tip hashes (may still propagate)"),
                                format!("Hash: {}", block_hash_clone)
                            );
                            info!(
                                "{} {} {}",
                                LogColors::api("[API]"),
                                LogColors::label("  - Note:"),
                                "Block may be in a side chain or still propagating"
                            );
                            info!(
                                "{} {} {}",
                                LogColors::api("[API]"),
                                LogColors::label("  - Tip hashes count:"),
                                dag_response.tip_hashes.len()
                            );
                        }
                    }
                });
            }
            Err(e) => {
                let error_str = e.to_string();
                if error_str.contains("ErrDuplicateBlock") || error_str.contains("duplicate") {
                    warn!(
                        "{} {}",
                        LogColors::api("[API]"),
                        LogColors::validation(&format!("===== BLOCK REJECTED BY KASPA NODE: STALE ===== Hash: {}", block_hash))
                    );
                    warn!(
                        "{} {} {}",
                        LogColors::api("[API]"),
                        LogColors::label("REJECTION REASON:"),
                        "Block already exists in the network"
                    );
                    warn!("{} {}", LogColors::api("[API]"), LogColors::label("  - Block was previously submitted and accepted"));
                    warn!("{} {}", LogColors::api("[API]"), LogColors::label("  - This is a duplicate/stale block submission"));
                    warn!("{} {} {}", LogColors::api("[API]"), LogColors::error("  - Error:"), error_str);
                    warn!(
                        "{} {} {}",
                        LogColors::api("[API]"),
                        LogColors::label("  - Blue Score:"),
                        format!("{}, Timestamp: {}, Nonce: {:x}", blue_score, timestamp, nonce)
                    );
                } else {
                    error!(
                        "{} {}",
                        LogColors::api("[API]"),
                        LogColors::error(&format!("===== BLOCK REJECTED BY KASPA NODE: INVALID ===== Hash: {}", block_hash))
                    );
                    error!("{} {} {}", LogColors::api("[API]"), LogColors::label("REJECTION REASON:"), "Block failed node validation");
                    error!("{} {}", LogColors::api("[API]"), LogColors::label("  - Possible validation failures:"));
                    error!("{} {}", LogColors::api("[API]"), "    * Invalid block structure or format");
                    error!("{} {}", LogColors::api("[API]"), "    * Block header validation failed");
                    error!("{} {}", LogColors::api("[API]"), "    * Transaction validation failed");
                    error!("{} {}", LogColors::api("[API]"), "    * DAA (Difficulty Adjustment Algorithm) validation failed");
                    error!("{} {}", LogColors::api("[API]"), "    * Block does not meet network consensus rules");
                    error!("{} {} {}", LogColors::api("[API]"), LogColors::error("  - Error from node:"), error_str);
                    error!(
                        "{} {} {}",
                        LogColors::api("[API]"),
                        LogColors::label("  - Blue Score:"),
                        format!("{}, Timestamp: {}, Nonce: {:x}", blue_score, timestamp, nonce)
                    );
                }
            }
        }

        result
    }

    /// Wait for node to sync
    async fn wait_for_sync(&self) -> Result<()> {
        loop {
            match self.client.get_sync_status().await {
                Ok(is_synced) => {
                    if is_synced {
                        break;
                    }
                }
                Err(e) => {
                    debug!("failed to get sync status: {}, retrying...", e);
                }
            }

            sleep(Duration::from_secs(10)).await;
        }

        Ok(())
    }

    pub async fn wait_for_sync_with_shutdown(&self, mut shutdown_rx: watch::Receiver<bool>) -> Result<()> {
        debug!("checking kaspad sync state");

        // ZKas: a peerless solo node reports is_synced=false forever
        // (has_sufficient_peer_connectivity needs peers) even while it mines
        // via `--enable-unsynced-mining`. Mirror that node flag here so the
        // bridge can serve jobs against a bootstrap/solo node. Opt-in via env.
        if std::env::var("BRIDGE_ALLOW_UNSYNCED").as_deref() == Ok("1") {
            warn!("BRIDGE_ALLOW_UNSYNCED=1 set — skipping node sync wait (solo/bootstrap mode)");
            return Ok(());
        }

        loop {
            let sync_fut = self.client.get_sync_status();
            let sync_res = tokio::select! {
                _ = shutdown_rx.wait_for(|v| *v) => {
                    return Err(anyhow::anyhow!("shutdown requested"));
                }
                res = sync_fut => res,
            };

            match sync_res {
                Ok(is_synced) => {
                    if is_synced {
                        debug!("kaspad synced, starting server");
                        break;
                    }
                }
                Err(e) => {
                    warn!("failed to get sync status: {}, retrying...", e);
                }
            }

            warn!(
                "Connected to ZKas RPC at {}, but GetSyncStatus returned false; waiting for the node to become mining-ready",
                self.zkas_node_endpoint
            );

            tokio::select! {
                _ = shutdown_rx.wait_for(|v| *v) => {
                    return Err(anyhow::anyhow!("shutdown requested"));
                }
                _ = sleep(Duration::from_secs(10)) => {}
            }
        }

        Ok(())
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        *self.connected.lock()
    }

    /// Get block template for a client
    /// `kas_payout` is the lane's own `kaspa:` address from the stratum password, or
    /// `None` to pay the pool. `lane_id` selects the lane's pool-fee minute; during
    /// that minute the KAS coinbase pays the pool regardless of `kas_payout`. ZKas
    /// rewards are unaffected either way — they always pay `wallet_addr`.
    pub async fn get_block_template(
        &self,
        wallet_addr: &str,
        _remote_app: &str,
        _canxium_addr: &str,
        session_uid: u64,
        generation: u64,
        kas_payout: Option<Address>,
        lane_id: u64,
    ) -> Result<Block> {
        // Retry up to 3 times if we get "Odd number of digits" error
        // This error can occur if the block template has malformed hash fields
        let max_retries = 3;
        let mut last_error = None;

        for attempt in 0..max_retries {
            // Resolve coinbase recipient. In custodial PROP-pool
            // mode (`coinbase_address_override = Some(_)`), every
            // template pays the pool regardless of which miner
            // authorized — see the struct-level docs. Falls back
            // to the miner-supplied `wallet_addr` for upstream
            // solo / MM-pool parity when no override is set.
            let address = resolve_coinbase_recipient(&self.coinbase_address_override, wallet_addr)?;

            // Request block template using RPC client wrapper
            // Zero-fee solo mining pays `address` directly. The lane suffix
            // changes only miner extra-data, producing a distinct coinbase,
            // merkle root and ZKAS `H_fc` for every connection/generation.
            // The node's canonical cached-template modifier performs all
            // consensus serialization; the bridge must not hand-build it.
            let coinbase_tag = build_lane_coinbase_tag(&self.coinbase_tag, session_uid, generation);
            let response = match self.client.get_block_template_call(None, GetBlockTemplateRequest::new(address, coinbase_tag)).await {
                Ok(r) => r,
                Err(e) => {
                    if attempt < max_retries - 1 {
                        warn!("Failed to get block template (attempt {}/{}): {}, retrying...", attempt + 1, max_retries, e);
                        sleep(Duration::from_millis(100 * (attempt + 1) as u64)).await;
                        continue;
                    }
                    return Err(anyhow::anyhow!("Failed to get block template after {} attempts: {}", max_retries, e));
                }
            };

            // Get RPC block from response
            let rpc_block = response.block;

            // Convert RpcRawBlock to Block
            // The RpcRawBlock contains the block data that we need to convert
            // The "Odd number of digits" error can occur here if hash fields have malformed hex strings
            match Block::try_from(rpc_block) {
                Ok(block) => {
                    // Validate that we can serialize the block header
                    // This catches "Odd number of digits" errors early
                    // Convert error to String immediately to avoid Send issues
                    let serialize_result = crate::hasher::serialize_block_header(&block).map_err(|e| e.to_string());

                    match serialize_result {
                        Ok(_) => {
                            if self.merged_mining {
                                // Merged mode: the ASIC hashes a *parent* block committing to this
                                // ZKas block's hash. Stash the ZKas block so a solved parent
                                // can be turned back into an aux block in `submit_block`.
                                let h_fc = block.header.hash;
                                // Pool-fee minute: this lane's KAS goes to the pool for one
                                // minute an hour. Computed ONCE here, at template-build time,
                                // and remembered with the template — see `MergedPending`'s
                                // `fee_lane` for why it must not be re-derived at submit time.
                                let minute_of_hour = (std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_secs())
                                    .unwrap_or(0)
                                    / 60)
                                    % 60;
                                let in_fee_minute = crate::merged::is_pool_fee_minute(lane_id, minute_of_hour);
                                let payee = if in_fee_minute { None } else { kas_payout.as_ref() };
                                let paid_pool = payee.is_none();
                                let parent = match self.fetch_kaspa_parent(h_fc, payee, false).await {
                                    // Real dual-chain: a genuine Kaspa block whose coinbase commits to
                                    // H_fc — clearing its (hard) target also earns KAS.
                                    Ok(p) => p,
                                    // No/failed Kaspa node: fall back to a synthetic parent so ZKas
                                    // aux blocks keep flowing (no KAS, but the chain stays live).
                                    Err(e) => {
                                        if self.kaspa_client.is_some() {
                                            warn!(
                                                "merged: Kaspa parent fetch failed ({e}); using synthetic parent this round (no KAS)"
                                            );
                                        }
                                        crate::merged::build_parent_block(&block).0
                                    }
                                };
                                self.pending_fc.lock().insert_with_payee(h_fc, block, paid_pool);
                                return Ok(parent);
                            }
                            return Ok(block);
                        }
                        Err(error_str) => {
                            if error_str.contains("Odd number of digits") {
                                last_error = Some(format!("Block has malformed hash field: {}", error_str));
                                if attempt < max_retries - 1 {
                                    warn!(
                                        "Block template has malformed hash field (attempt {}/{}), retrying...",
                                        attempt + 1,
                                        max_retries
                                    );
                                    sleep(Duration::from_millis(100 * (attempt + 1) as u64)).await;
                                    continue;
                                }
                            }
                            // If it's a different error, return it
                            return Err(anyhow::anyhow!("Failed to serialize block header: {}", error_str));
                        }
                    }
                }
                Err(e) => {
                    let error_str = format!("{:?}", e);
                    last_error = Some(error_str.clone());
                    if error_str.contains("Odd number of digits") && attempt < max_retries - 1 {
                        warn!(
                            "Block conversion failed with 'Odd number of digits' error (attempt {}/{}), retrying...",
                            attempt + 1,
                            max_retries
                        );
                        sleep(Duration::from_millis(100 * (attempt + 1) as u64)).await;
                        continue;
                    }
                    // If the error contains "Odd number of digits", provide more context
                    if error_str.contains("Odd number of digits") {
                        return Err(anyhow::anyhow!(
                            "Failed to convert RPC block to Block after {} attempts: {} - This usually indicates a malformed hash field in the block template from the Kaspa node. The block may have a hash with an odd-length hex string.",
                            max_retries,
                            error_str
                        ));
                    } else {
                        return Err(anyhow::anyhow!("Failed to convert RPC block to Block: {}", error_str));
                    }
                }
            }
        }

        // Should never reach here, but handle it just in case
        Err(anyhow::anyhow!("Failed to get valid block template after {} attempts: {:?}", max_retries, last_error))
    }

    /// Get balances by addresses (for Prometheus metrics)
    pub async fn get_balances_by_addresses(&self, addresses: &[String]) -> Result<Vec<(String, u64)>> {
        let parsed_addresses: Result<Vec<Address>, _> = addresses.iter().map(|addr| Address::try_from(addr.as_str())).collect();

        let addresses = parsed_addresses.map_err(|e| anyhow::anyhow!("Failed to parse addresses: {:?}", e))?;

        let utxos = self
            .client
            .get_utxos_by_addresses_call(None, kaspa_rpc_core::GetUtxosByAddressesRequest::new(addresses))
            .await
            .context("Failed to get UTXOs by addresses")?;

        // Calculate balances from UTXOs
        // Group entries by address
        let mut balance_map: HashMap<String, u64> = HashMap::new();
        for entry in utxos.entries {
            if let Some(address) = entry.address {
                let addr_str = address.to_string();
                let amount = entry.utxo_entry.amount;
                *balance_map.entry(addr_str).or_insert(0) += amount;
            }
        }
        let balances: Vec<(String, u64)> = balance_map.into_iter().collect();

        Ok(balances)
    }

    pub async fn get_current_block_color(&self, block_hash: &str) -> Result<bool> {
        let hash = RpcHash::from_str(block_hash).context("Failed to parse block hash")?;
        let resp = self
            .client
            .get_current_block_color_call(None, GetCurrentBlockColorRequest { hash })
            .await
            .context("Failed to query current block color")?;
        Ok(resp.blue)
    }

    /// Start listening for block template notifications
    /// Uses RegisterForNewBlockTemplateNotifications with ticker fallback
    /// This provides immediate notifications when new blocks are available, with polling as fallback
    pub async fn start_block_template_listener<F>(self: Arc<Self>, block_wait_time: Duration, mut block_cb: F) -> Result<()>
    where
        F: FnMut() + Send + 'static,
    {
        let mut rx = self.notification_rx.lock().take().ok_or_else(|| anyhow::anyhow!("Notification receiver already taken"))?;

        let api_clone = Arc::clone(&self);
        tokio::spawn(async move {
            let mut restart_channel = true;
            let mut ticker = tokio::time::interval(block_wait_time);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                // Check sync state and reconnect if needed
                if let Err(e) = api_clone.wait_for_sync().await {
                    error!("error checking kaspad sync state, attempting reconnect: {}", e);
                    // Note: gRPC client handles reconnection automatically, but we log it
                    // In Go, reconnect() is called explicitly, but Rust gRPC handles it
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    restart_channel = true;
                }

                // Re-register for notifications if needed
                if restart_channel {
                    // In Go, RegisterForNewBlockTemplateNotifications is called here when restartChannel is true
                    // In Rust, we already subscribed in new(), and the notification channel persists
                    // If the connection is lost, the gRPC client handles reconnection automatically
                    // The notification subscription should be maintained by the gRPC client
                    // If notifications stop working, we'll fall back to ticker polling
                    restart_channel = false;
                }

                // Wait for either notification or ticker timeout
                tokio::select! {
                    // Notification received
                    notification_result = rx.recv() => {
                        match notification_result {
                            Some(Notification::NewBlockTemplate(_)) => {
                                // Drain any additional notifications
                                while rx.try_recv().is_ok() {}

                                // Call callback
                                block_cb();

                                // Reset ticker
                                ticker = tokio::time::interval(block_wait_time);
                                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                            }
                            Some(_) => {
                                // Other notification types - ignore
                            }
                            None => {
                                // Channel closed - exit loop
                                warn!("Block template notification channel closed");
                                break;
                            }
                        }
                    }
                    // Ticker timeout - manually check for new blocks
                    _ = ticker.tick() => {
                        block_cb();
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn start_block_template_listener_with_shutdown<F>(
        self: Arc<Self>,
        block_wait_time: Duration,
        mut shutdown_rx: watch::Receiver<bool>,
        mut block_cb: F,
    ) -> Result<()>
    where
        F: FnMut() + Send + 'static,
    {
        let mut rx = self.notification_rx.lock().take().ok_or_else(|| anyhow::anyhow!("Notification receiver already taken"))?;

        let api_clone = Arc::clone(&self);
        tokio::spawn(async move {
            let mut restart_channel = true;
            let mut ticker = tokio::time::interval(block_wait_time);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                if *shutdown_rx.borrow() {
                    break;
                }

                if let Err(e) = api_clone.wait_for_sync().await {
                    error!("error checking kaspad sync state, attempting reconnect: {}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    restart_channel = true;
                }

                if restart_channel {
                    restart_channel = false;
                }

                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    notification_result = rx.recv() => {
                        match notification_result {
                            Some(Notification::NewBlockTemplate(_)) => {
                                while rx.try_recv().is_ok() {}
                                block_cb();
                                ticker = tokio::time::interval(block_wait_time);
                                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                            }
                            Some(_) => {}
                            None => {
                                warn!("Block template notification channel closed");
                                break;
                            }
                        }
                    }
                    _ = ticker.tick() => {
                        block_cb();
                    }
                }
            }
        });

        Ok(())
    }

    /// Drive parent-only refreshes on the real Kaspa clock without rebuilding
    /// the 1-BPS ZKAS template/H_fc.
    pub fn start_kaspa_parent_template_listener<F>(self: &Arc<Self>, mut shutdown_rx: Option<watch::Receiver<bool>>, mut parent_cb: F)
    where
        F: FnMut() + Send + 'static,
    {
        if self.kaspa_client.is_none() {
            return;
        }
        let mut rx = self.kaspa_notification_rx.lock().take();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(KASPA_PARENT_TTL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                if let Some(ref mut shutdown) = shutdown_rx {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() { break; }
                        }
                        notification = async {
                            match rx.as_mut() {
                                Some(receiver) => receiver.recv().await,
                                None => std::future::pending().await,
                            }
                        } => {
                            if matches!(notification, Some(Notification::NewBlockTemplate(_))) {
                                if let Some(receiver) = rx.as_mut() {
                                    while receiver.try_recv().is_ok() {}
                                }
                                parent_cb();
                            } else if notification.is_none() {
                                rx = None;
                            }
                        }
                        _ = ticker.tick() => parent_cb(),
                    }
                } else {
                    tokio::select! {
                        notification = async {
                            match rx.as_mut() {
                                Some(receiver) => receiver.recv().await,
                                None => std::future::pending().await,
                            }
                        } => {
                            if matches!(notification, Some(Notification::NewBlockTemplate(_))) {
                                if let Some(receiver) = rx.as_mut() {
                                    while receiver.try_recv().is_ok() {}
                                }
                                parent_cb();
                            } else if notification.is_none() {
                                rx = None;
                            }
                        }
                        _ = ticker.tick() => parent_cb(),
                    }
                }
            }
        });
    }
}

#[async_trait::async_trait]
impl KaspaApiTrait for KaspaApi {
    async fn submit_merged_parent_if_solved(&self, parent: &Block) -> crate::kaspaapi::MergedParentSubmitOutcome {
        KaspaApi::submit_merged_parent_if_solved(self, parent).await
    }

    fn merged_fc_target(&self, parent_block: &Block) -> Option<num_bigint::BigUint> {
        KaspaApi::merged_fc_target(self, parent_block)
    }

    fn merged_chain_hash(&self, parent_block: &Block) -> Option<kaspa_hashes::Hash> {
        KaspaApi::merged_chain_hash(self, parent_block)
    }

    fn claim_network_solution(&self, job_block: &Block) -> bool {
        KaspaApi::claim_network_solution(self, job_block)
    }

    async fn refresh_merged_parent(
        &self,
        current_parent: &Block,
        payee: Option<Address>,
    ) -> Result<Option<Block>, Box<dyn std::error::Error + Send + Sync>> {
        KaspaApi::refresh_merged_parent(self, current_parent, payee.as_ref())
            .await
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error + Send + Sync>)
    }

    async fn get_block_template(
        &self,
        wallet_addr: &str,
        _remote_app: &str,
        _canxium_addr: &str,
        session_uid: u64,
        generation: u64,
        kas_payout: Option<Address>,
        lane_id: u64,
    ) -> Result<Block, Box<dyn std::error::Error + Send + Sync>> {
        KaspaApi::get_block_template(self, wallet_addr, "", "", session_uid, generation, kas_payout, lane_id).await.map_err(|e| {
            let error_msg = e.to_string();
            Box::new(std::io::Error::other(error_msg)) as Box<dyn std::error::Error + Send + Sync>
        })
    }

    async fn submit_block(&self, block: Block) -> Result<BlockSubmitOutcome, Box<dyn std::error::Error + Send + Sync>> {
        KaspaApi::submit_block(self, block)
            .await
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error + Send + Sync>)
    }

    async fn get_balances_by_addresses(
        &self,
        addresses: &[String],
    ) -> Result<Vec<(String, u64)>, Box<dyn std::error::Error + Send + Sync>> {
        KaspaApi::get_balances_by_addresses(self, addresses)
            .await
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error + Send + Sync>)
    }

    async fn get_current_block_color(&self, block_hash: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        KaspaApi::get_current_block_color(self, block_hash)
            .await
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error + Send + Sync>)
    }
}

/// Pure helper that resolves which `Address` to use as the
/// coinbase recipient for one block-template request.
///
/// Extracted so the override behaviour has a deterministic unit
/// test independent of the gRPC layer. See
/// [`KaspaApi::coinbase_address_override`] for the
/// pool-custody rationale.
fn resolve_coinbase_recipient(override_addr: &Option<Address>, wallet_addr: &str) -> Result<Address> {
    if let Some(a) = override_addr {
        return Ok(a.clone());
    }
    Address::try_from(wallet_addr).map_err(|e| anyhow::anyhow!("Could not decode address {}: {}", wallet_addr, e))
}

/// Stable, operator-friendly label for a kaspad submit-block
/// rejection reason. Used in the WARN log emitted from the
/// `Ok(BlockSubmitOutcome::RejectedByNode(_))` arm of
/// [`KaspaApi::submit_block`] so operators / dashboards / runbooks
/// can discriminate "node not synced" (`IsInIBD`, transient),
/// "back-pressure" (`RouteIsFull`, transient), and
/// "kaspad refused" (`BlockInvalid`, persistent — usually a tip
/// race or coinbase / DAA mismatch).
const fn submit_block_reject_label(reason: SubmitBlockRejectReason) -> &'static str {
    match reason {
        SubmitBlockRejectReason::BlockInvalid => "BlockInvalid",
        SubmitBlockRejectReason::IsInIBD => "IsInIBD",
        SubmitBlockRejectReason::RouteIsFull => "RouteIsFull",
    }
}

#[cfg(test)]
mod coinbase_recipient_tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
    use super::*;

    // Canonical ZKAS fixtures from kaspa-addresses. Keep them distinct so the
    // override test cannot pass accidentally when override == miner.
    const MINER_ADDR: &str = "zkas:qp0l70zd5x85ttwd6jv7g3s3a8llzj96d8dncn4zmhv4tlzx5k2jygc4lzm58";
    const POOL_ADDR: &str = "zkas:qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq7fwd2pgv";

    #[test]
    fn override_replaces_miner_address_when_set() {
        let pool = Address::try_from(POOL_ADDR).expect("valid pool address");
        let resolved = resolve_coinbase_recipient(&Some(pool.clone()), MINER_ADDR).expect("resolves");
        assert_eq!(resolved, pool, "override must replace the miner-supplied address");
    }

    #[test]
    fn no_override_falls_through_to_miner_address() {
        let resolved = resolve_coinbase_recipient(&None, MINER_ADDR).expect("resolves");
        let expected = Address::try_from(MINER_ADDR).expect("valid miner address");
        assert_eq!(resolved, expected, "with no override the miner-supplied address is used (upstream behaviour)");
    }

    #[test]
    fn no_override_propagates_malformed_miner_address() {
        let err = resolve_coinbase_recipient(&None, "not-a-kaspa-address").expect_err("invalid address must error");
        let msg = format!("{err}");
        assert!(msg.contains("Could not decode address"), "unexpected error message: {msg}");
    }

    #[test]
    fn override_ignores_malformed_miner_address() {
        // Important: a configured pool override must short-circuit
        // the wallet_addr parse, otherwise a single misbehaving
        // miner sending garbage on `mining.authorize` would crash
        // every block-template fetch.
        let pool = Address::try_from(POOL_ADDR).expect("valid pool address");
        let resolved = resolve_coinbase_recipient(&Some(pool.clone()), "not-a-kaspa-address").expect("resolves");
        assert_eq!(resolved, pool);
    }
}

#[cfg(test)]
mod solo_lane_tests {
    use super::build_lane_coinbase_tag;

    #[test]
    fn lane_tag_is_stable_and_separates_sessions_and_generations() {
        let base = b"RK-Stratum/zkas";
        let a1 = build_lane_coinbase_tag(base, 7, 1);
        let a2 = build_lane_coinbase_tag(base, 7, 2);
        let b1 = build_lane_coinbase_tag(base, 8, 1);

        assert_eq!(a1, build_lane_coinbase_tag(base, 7, 1));
        assert_ne!(a1, a2);
        assert_ne!(a1, b1);
        assert!(a1.starts_with(base));
        assert!(a1.len() < 64, "lane tag should leave ample room in the 204-byte coinbase payload");
    }
}

#[cfg(test)]
mod submit_block_report_tests {
    //! Regression guards for the "phantom block accepted" bug
    //! uncovered during the Goldshell M3d live exercise: the
    //! pre-M3f bridge treated every `Ok(SubmitBlockResponse)` as a
    //! win even when `report = Reject(BlockInvalid)`, producing
    //! 4,853 false-positive "BLOCK ACCEPTED" log lines against
    //! 1,004 actual acceptances. These tests pin the
    //! discriminator's behaviour so a future refactor cannot
    //! silently regress.
    #![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
    use super::*;

    /// Mirrors the production discriminator inserted in
    /// [`KaspaApi::submit_block`]. Kept here as a pure function
    /// so the test exercises identical logic without standing up
    /// a gRPC client.
    fn classify(response: SubmitBlockResponse) -> BlockSubmitOutcome {
        match response.report {
            SubmitBlockReport::Success => BlockSubmitOutcome::Accepted(response),
            SubmitBlockReport::Reject(reason) => BlockSubmitOutcome::RejectedByNode(reason),
        }
    }

    #[test]
    fn success_report_resolves_to_accepted() {
        let resp = SubmitBlockResponse { report: SubmitBlockReport::Success, reject_detail: String::new() };
        let out = classify(resp);
        assert!(out.is_accepted(), "Success must produce BlockSubmitOutcome::Accepted");
        match out {
            BlockSubmitOutcome::Accepted(r) => assert!(matches!(r.report, SubmitBlockReport::Success)),
            BlockSubmitOutcome::RejectedByNode(r) => panic!("unexpected RejectedByNode({r:?})"),
        }
    }

    #[test]
    fn reject_block_invalid_resolves_to_rejected_not_err() {
        // Critical regression guard: the first M3f cut collapsed
        // this into Err, which the share-handler then mapped to
        // ShareRejectReason::BadPow — penalising the miner for a
        // pool-side race condition (Goldshell 68% reject UI
        // regression). Reject(*) must stay Ok(RejectedByNode(_))
        // so the share-handler credits the share and only the
        // BlockAccepted event is suppressed.
        let resp = SubmitBlockResponse {
            report: SubmitBlockReport::Reject(SubmitBlockRejectReason::BlockInvalid),
            reject_detail: String::new(),
        };
        let out = classify(resp);
        assert!(!out.is_accepted(), "Reject(BlockInvalid) must NOT be Accepted");
        match out {
            BlockSubmitOutcome::RejectedByNode(SubmitBlockRejectReason::BlockInvalid) => {}
            other => panic!("expected RejectedByNode(BlockInvalid), got {other:?}"),
        }
    }

    #[test]
    fn reject_is_in_ibd_resolves_to_rejected() {
        let resp =
            SubmitBlockResponse { report: SubmitBlockReport::Reject(SubmitBlockRejectReason::IsInIBD), reject_detail: String::new() };
        match classify(resp) {
            BlockSubmitOutcome::RejectedByNode(SubmitBlockRejectReason::IsInIBD) => {}
            other => panic!("expected RejectedByNode(IsInIBD), got {other:?}"),
        }
    }

    #[test]
    fn reject_route_is_full_resolves_to_rejected() {
        let resp = SubmitBlockResponse {
            report: SubmitBlockReport::Reject(SubmitBlockRejectReason::RouteIsFull),
            reject_detail: String::new(),
        };
        match classify(resp) {
            BlockSubmitOutcome::RejectedByNode(SubmitBlockRejectReason::RouteIsFull) => {}
            other => panic!("expected RejectedByNode(RouteIsFull), got {other:?}"),
        }
    }

    #[test]
    fn label_is_stable_for_all_known_reasons() {
        // Pin the operator-visible labels — dashboards / alerts /
        // runbooks may filter on these exact strings; treat the
        // mapping as a public contract.
        assert_eq!(submit_block_reject_label(SubmitBlockRejectReason::BlockInvalid), "BlockInvalid");
        assert_eq!(submit_block_reject_label(SubmitBlockRejectReason::IsInIBD), "IsInIBD");
        assert_eq!(submit_block_reject_label(SubmitBlockRejectReason::RouteIsFull), "RouteIsFull");
    }
}
