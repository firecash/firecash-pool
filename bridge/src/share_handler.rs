use crate::{
    errors::*,
    jsonrpc_event::{JsonRpcEvent, JsonRpcResponse},
    kaspaapi::NODE_STATUS,
    log_colors::LogColors,
    mining_state::GetMiningState,
    prom::*,
    stratum_context::StratumContext,
};

#[cfg(feature = "rkstratum_cpu_miner")]
use crate::rkstratum_cpu_miner::InternalMinerMetrics;
use kaspa_consensus_core::block::Block;
// kaspa_pow used inline for PoW validation
use katpool_domain::{
    BlockHash as DomainBlockHash, CorrelationId, DaaScore, PoolEvent, ShareDifficulty, ShareRejectReason, WalletAddress, WorkerName,
};
use num_bigint::BigUint;
use num_traits::{ToPrimitive, Zero};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde_json::Value;
use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, watch};
use tracing::{debug, error, info, warn};

/// Capacity (in events) of the [`ShareHandler::with_event_bus`] broadcast
/// channel suggested for callers. A capacity of 4096 covers ~3 minutes
/// of sustained 20 shares/sec submission before a slow consumer would
/// see `RecvError::Lagged`; in practice consumers are expected to drain
/// at line-rate.
pub const POOL_EVENT_CHANNEL_CAPACITY: usize = 4096;

#[allow(dead_code)]
const VAR_DIFF_THREAD_SLEEP: u64 = 10;
#[allow(dead_code)]
const WORK_WINDOW: u64 = 80;
const STATS_PRUNE_INTERVAL: Duration = Duration::from_secs(60);
const INACTIVE_VARDIFF_TTL: Duration = Duration::from_secs(60 * 60);
const STATS_PRINT_INTERVAL: Duration = Duration::from_secs(10);
const BLOCK_CONFIRM_RETRY_DELAY: Duration = Duration::from_secs(2);
const BLOCK_CONFIRM_MAX_ATTEMPTS: usize = 30;
/// Narrow compatibility window for ASIC firmware that reports a nearby job
/// number. Never walk the full 300-slot history: each attempt performs a full
/// kHeavyHash calculation.
const MAX_COMPAT_JOB_ATTEMPTS: usize = 8;

// VarDiff tunables
const VARDIFF_MIN_ELAPSED_SECS: f64 = 30.0;
// Bootstrap unknown miners from the ASIC-safe 8192 seed toward the slowest
// supported IceRiver class without making a KS0 wait many minutes for its
// first measurable share. Seven 15-second halvings reach diff 64 in ~105s.
const VARDIFF_MAX_ELAPSED_SECS_NO_SHARES: f64 = 15.0;
const VARDIFF_MAX_ELAPSED_SECS_SPARSE_SHARES: f64 = 90.0;
const VARDIFF_MIN_DIFF: f64 = 64.0;
const VARDIFF_MIN_SHARES: f64 = 3.0;
const VARDIFF_LOWER_RATIO: f64 = 0.75; // below this => decrease diff
const VARDIFF_UPPER_RATIO: f64 = 1.25; // above this => increase diff
// Up-steps are allowed to be large so a grossly under-difficultied miner (e.g. a
// multi-TH/s ASIC that connected at a low seed) converges to its correct share
// difficulty in a few ticks instead of ~40 doublings. The step itself is the
// self-damping sqrt(observed/expected), so a big cap does not overshoot — it just
// stops var-diff from being uselessly slow (which, at a low seed, let the miner
// flood shares and trip the anti-abuse frame limiter before it could adjust).
const VARDIFF_MAX_STEP_UP: f64 = 128.0; // up to 128x per adjustment tick (sqrt-damped)
const VARDIFF_MAX_STEP_DOWN: f64 = 0.5; // max -50% per adjustment tick

fn vardiff_pow2_clamp_towards(current: f64, next: f64) -> f64 {
    if !next.is_finite() || next <= 0.0 {
        return 1.0;
    }

    let exp = if next >= current { next.log2().ceil() } else { next.log2().floor() };
    let clamped = 2_f64.powi(exp as i32);
    if clamped < 1.0 { 1.0 } else { clamped }
}

fn vardiff_compute_next_diff(current: f64, shares: f64, elapsed_secs: f64, expected_spm: f64, clamp_pow2: bool) -> Option<f64> {
    if !current.is_finite() || current <= 0.0 {
        return None;
    }
    if !elapsed_secs.is_finite() || elapsed_secs <= 0.0 {
        return None;
    }

    if shares == 0.0 && elapsed_secs >= VARDIFF_MAX_ELAPSED_SECS_NO_SHARES {
        let mut next = current * VARDIFF_MAX_STEP_DOWN;
        if next < VARDIFF_MIN_DIFF {
            next = VARDIFF_MIN_DIFF;
        }
        if clamp_pow2 {
            next = vardiff_pow2_clamp_towards(current, next);
        }
        return if (next - current).abs() > f64::EPSILON { Some(next) } else { None };
    }

    // Prefer at least three samples, but do not strand a slow miner forever
    // after it happens to find only one or two shares at the bootstrap diff.
    if elapsed_secs < VARDIFF_MIN_ELAPSED_SECS
        || (shares < VARDIFF_MIN_SHARES && elapsed_secs < VARDIFF_MAX_ELAPSED_SECS_SPARSE_SHARES)
    {
        return None;
    }

    let observed_spm = (shares / elapsed_secs) * 60.0;
    let ratio = observed_spm / expected_spm.max(1.0);
    if !ratio.is_finite() || ratio <= 0.0 {
        return None;
    }
    if ratio > VARDIFF_LOWER_RATIO && ratio < VARDIFF_UPPER_RATIO {
        return None;
    }

    let step = ratio.sqrt().clamp(VARDIFF_MAX_STEP_DOWN, VARDIFF_MAX_STEP_UP);
    let mut next = current * step;
    if next < VARDIFF_MIN_DIFF {
        next = VARDIFF_MIN_DIFF;
    }
    if clamp_pow2 {
        next = vardiff_pow2_clamp_towards(current, next);
    }

    let rel_change = (next - current).abs() / current.max(1.0);
    if rel_change < 0.10 {
        return None;
    }
    if (next - current).abs() > f64::EPSILON { Some(next) } else { None }
}

pub fn average_worker_spm(sum_spm: f64, worker_count: usize) -> f64 {
    if worker_count == 0 { 0.0 } else { sum_spm / worker_count as f64 }
}

struct StatsPrinterEntry {
    instance_id: String,
    inst_short: String,
    target_spm: f64,
    start: Instant,
    stats: Arc<Mutex<HashMap<String, WorkStats>>>,
    overall: Arc<WorkStats>,
}

static STATS_PRINTER_REGISTRY: Lazy<Mutex<Vec<StatsPrinterEntry>>> = Lazy::new(|| Mutex::new(Vec::new()));
pub static STATS_PRINTER_STARTED: AtomicBool = AtomicBool::new(false);

#[cfg(feature = "rkstratum_cpu_miner")]
pub static RKSTRATUM_CPU_MINER_METRICS: Lazy<parking_lot::Mutex<Option<Arc<InternalMinerMetrics>>>> =
    Lazy::new(|| parking_lot::Mutex::new(None));

#[cfg(feature = "rkstratum_cpu_miner")]
pub fn set_rkstratum_cpu_miner_metrics(metrics: Arc<InternalMinerMetrics>) {
    *RKSTRATUM_CPU_MINER_METRICS.lock() = Some(metrics);
}

#[derive(Clone)]
pub struct WorkStats {
    pub blocks_found: Arc<Mutex<i64>>,
    pub shares_found: Arc<Mutex<i64>>,
    pub shares_diff: Arc<Mutex<f64>>,
    pub stale_shares: Arc<Mutex<i64>>,
    pub invalid_shares: Arc<Mutex<i64>>,
    pub worker_name: Arc<Mutex<String>>,
    pub start_time: Instant,
    pub last_share: Arc<Mutex<Instant>>,
    pub var_diff_start_time: Arc<Mutex<Option<Instant>>>,
    pub var_diff_shares_found: Arc<Mutex<i64>>,
    pub var_diff_window: Arc<Mutex<usize>>,
    pub min_diff: Arc<Mutex<f64>>,
    pub active_connections: Arc<Mutex<u32>>,
    pub last_session_activity: Arc<Mutex<Instant>>,
}

impl WorkStats {
    pub fn new(worker_name: String) -> Self {
        Self {
            blocks_found: Arc::new(Mutex::new(0)),
            shares_found: Arc::new(Mutex::new(0)),
            shares_diff: Arc::new(Mutex::new(0.0)),
            stale_shares: Arc::new(Mutex::new(0)),
            invalid_shares: Arc::new(Mutex::new(0)),
            worker_name: Arc::new(Mutex::new(worker_name)),
            start_time: Instant::now(),
            last_share: Arc::new(Mutex::new(Instant::now())),
            var_diff_start_time: Arc::new(Mutex::new(None)),
            var_diff_shares_found: Arc::new(Mutex::new(0)),
            var_diff_window: Arc::new(Mutex::new(0)),
            min_diff: Arc::new(Mutex::new(0.0)),
            active_connections: Arc::new(Mutex::new(0)),
            last_session_activity: Arc::new(Mutex::new(Instant::now())),
        }
    }
}

fn should_retain_vardiff_stats(stats: &WorkStats, now: Instant) -> bool {
    *stats.active_connections.lock() > 0 || now.duration_since(*stats.last_session_activity.lock()) < INACTIVE_VARDIFF_TTL
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DuplicateSubmitOutcome {
    InFlight,
    Accepted,
    Stale,
    LowDiff,
    Bad,
}

/// Preserve the independent Kaspa reward before deduplicating the committed
/// ZKAS block. The ordering is consensus-significant for merged-mining
/// economics: a late parent may be useless to ZKAS but still valid on Kaspa.
async fn submit_parent_then_claim_zkas<T: KaspaApiTrait + ?Sized>(
    kaspa_api: &T,
    solved_parent: &Block,
    job_parent: &Block,
    clears_zkas: bool,
) -> (crate::kaspaapi::MergedParentSubmitOutcome, bool) {
    let parent_outcome = kaspa_api.submit_merged_parent_if_solved(solved_parent).await;
    let claimed_zkas = clears_zkas && kaspa_api.claim_network_solution(job_parent);
    (parent_outcome, claimed_zkas)
}

struct DuplicateSubmitEntry {
    ts: Instant,
    outcome: DuplicateSubmitOutcome,
}

struct DuplicateSubmitGuard {
    ttl: Duration,
    max_entries: usize,
    entries: HashMap<String, DuplicateSubmitEntry>,
    order: VecDeque<String>,
}

impl DuplicateSubmitGuard {
    fn new(ttl: Duration, max_entries: usize) -> Self {
        Self { ttl, max_entries, entries: HashMap::new(), order: VecDeque::new() }
    }

    fn prune(&mut self, now: Instant) {
        while let Some(front) = self.order.front() {
            let remove = match self.entries.get(front) {
                Some(e) => now.duration_since(e.ts) > self.ttl,
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

    fn get(&mut self, key: &str, now: Instant) -> Option<DuplicateSubmitOutcome> {
        self.prune(now);
        self.entries.get(key).map(|e| e.outcome)
    }

    fn insert_inflight(&mut self, key: String, now: Instant) {
        self.prune(now);
        if self.entries.contains_key(&key) {
            return;
        }
        self.entries.insert(key.clone(), DuplicateSubmitEntry { ts: now, outcome: DuplicateSubmitOutcome::InFlight });
        self.order.push_back(key);
    }

    fn set_outcome(&mut self, key: &str, now: Instant, outcome: DuplicateSubmitOutcome) {
        self.prune(now);
        if let Some(e) = self.entries.get_mut(key) {
            e.ts = now;
            e.outcome = outcome;
        }
    }
}

pub struct ShareHandler {
    #[allow(dead_code)]
    tip_blue_score: Arc<Mutex<u64>>,
    stats: Arc<Mutex<HashMap<String, WorkStats>>>,
    overall: Arc<WorkStats>,
    instance_id: String, // Instance identifier for logging
    duplicate_submit_guard: Arc<Mutex<DuplicateSubmitGuard>>,
    /// Optional broadcast sender for [`PoolEvent`]s. When `None`, the
    /// bridge runs in legacy standalone mode (parity with upstream).
    /// When `Some`, every accepted share, every rejected share, every
    /// block candidate, and every kaspad-accept produces one event.
    event_tx: Option<broadcast::Sender<PoolEvent>>,
    /// Requests immediate replacement work for a connection after its current
    /// network-target job has been solved.
    job_refresh_tx: broadcast::Sender<u64>,
}

impl ShareHandler {
    pub fn new(instance_id: String) -> Self {
        let (job_refresh_tx, _) = broadcast::channel(1024);
        Self {
            tip_blue_score: Arc::new(Mutex::new(0)),
            stats: Arc::new(Mutex::new(HashMap::new())),
            overall: Arc::new(WorkStats::new("overall".to_string())),
            instance_id,
            duplicate_submit_guard: Arc::new(Mutex::new(DuplicateSubmitGuard::new(Duration::from_secs(180), 50_000))),
            event_tx: None,
            job_refresh_tx,
        }
    }

    pub fn subscribe_job_refreshes(&self) -> broadcast::Receiver<u64> {
        self.job_refresh_tx.subscribe()
    }

    /// Attach a broadcast sender that receives one [`PoolEvent`] per
    /// share submission outcome and per block lifecycle event. Designed
    /// to be called once during pool start-up, before the handler is
    /// shared across worker threads.
    #[must_use]
    pub fn with_event_bus(mut self, event_tx: broadcast::Sender<PoolEvent>) -> Self {
        self.event_tx = Some(event_tx);
        self
    }

    /// Publish a [`PoolEvent`] on the attached bus, if any. Public
    /// entry point for lifecycle events (e.g. session close) emitted
    /// from outside the share-submission path; a no-op in standalone
    /// mode with no bus attached.
    pub fn publish(&self, event: PoolEvent) {
        self.emit(event);
    }

    /// Best-effort event emission. Drops the event silently if no bus is
    /// attached or if all receivers have been dropped — both are valid
    /// runtime states (legacy mode, accountant restart) and must not
    /// stall share processing.
    fn emit(&self, event: PoolEvent) {
        if let Some(tx) = &self.event_tx {
            // broadcast::Sender::send returns Err only when there are no
            // active receivers; we don't care, we just drop the event.
            let _ = tx.send(event);
        }
    }

    /// Build a `ShareRejected` event, returning `None` if the wallet or
    /// worker stored on the context fail domain validation. This is a
    /// defence-in-depth check: by the time `handle_submit` runs the
    /// values should already be sane, but we never want to fabricate
    /// events with bad data, and we never want to panic from a value
    /// flowing in from the network.
    fn build_share_rejected(
        wallet_raw: &str,
        worker_raw: &str,
        reason: ShareRejectReason,
        correlation_id: CorrelationId,
    ) -> Option<PoolEvent> {
        let wallet = WalletAddress::new(wallet_raw).ok()?;
        let worker = WorkerName::new(worker_raw).ok()?;
        Some(PoolEvent::ShareRejected { wallet, worker, reason, ts: chrono::Utc::now(), correlation_id })
    }

    fn log_prefix(&self) -> String {
        format!("[{}]", self.instance_id)
    }

    fn worker_prom_context(&self, ctx: &StratumContext, miner: &str) -> crate::prom::WorkerContext {
        crate::prom::WorkerContext::from_stratum(&self.instance_id, ctx, miner)
    }

    fn workstats_session_start_unix(stats: &WorkStats) -> f64 {
        let now_unix = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs_f64();
        now_unix - stats.start_time.elapsed().as_secs_f64()
    }

    fn sync_worker_prom_session(&self, ctx: &StratumContext, stats: &WorkStats) {
        if ctx.wallet_addr.lock().is_empty() {
            return;
        }
        let worker = self.worker_prom_context(ctx, "");
        ensure_worker_session_metrics(&worker, Self::workstats_session_start_unix(stats));
    }

    fn stats_key(ctx: &StratumContext) -> String {
        let wallet = ctx.wallet_addr.lock().trim().to_ascii_lowercase();
        format!("{wallet}\0{}", ctx.vardiff_worker_identity())
    }

    /// Return in-memory stats for a wallet+worker when already registered.
    fn get_stats_if_exists(&self, ctx: &StratumContext) -> Option<WorkStats> {
        self.stats.lock().get(&Self::stats_key(ctx)).cloned()
    }

    fn current_stratum_diff(ctx: &StratumContext) -> f64 {
        GetMiningState(ctx).stratum_diff().map(|d| d.diff_value).unwrap_or(0.0)
    }

    pub fn get_create_stats(&self, ctx: &StratumContext) -> WorkStats {
        let worker_id = ctx.effective_worker_name();
        let stats_key = Self::stats_key(ctx);

        let stats = {
            let mut stats_map = self.stats.lock();

            if let Some(stats) = stats_map.get(&stats_key) {
                stats.clone()
            } else {
                let stats = WorkStats::new(worker_id.clone());
                // Seed per-worker displayed diff from current mining state so recreated
                // entries do not start at 0.0 and get stuck in terminal/UI.
                let seeded_diff = GetMiningState(ctx).stratum_diff().map(|d| d.diff_value).unwrap_or(0.0);
                if seeded_diff > 0.0 {
                    *stats.min_diff.lock() = seeded_diff;
                }
                stats_map.insert(stats_key, stats.clone());
                stats
            }
        };

        self.sync_worker_prom_session(ctx, &stats);
        stats
    }

    pub fn activate_client_vardiff(&self, ctx: &StratumContext) {
        if !ctx.claim_vardiff_registration() {
            return;
        }
        let stats = self.get_create_stats(ctx);
        let mut active = stats.active_connections.lock();
        *active = active.saturating_add(1);
        drop(active);
        // Offline time is not evidence that the assigned difficulty was too
        // high. Start a fresh observation window once per connection (this
        // method is registration-guarded), not from the per-job path.
        *stats.var_diff_start_time.lock() = Some(Instant::now());
        *stats.var_diff_shares_found.lock() = 0;
        *stats.var_diff_window.lock() = 0;
        *stats.last_session_activity.lock() = Instant::now();
    }

    pub fn deactivate_client_vardiff(&self, ctx: &StratumContext) {
        if !ctx.release_vardiff_registration() {
            return;
        }
        if let Some(stats) = self.get_stats_if_exists(ctx) {
            let mut active = stats.active_connections.lock();
            *active = active.saturating_sub(1);
            drop(active);
            *stats.last_session_activity.lock() = Instant::now();
        }
    }

    /// Initialize a connection from a safe cached difficulty, or from its
    /// configured seed when this wallet+worker has no prior vardiff state.
    pub fn register_client_vardiff(&self, ctx: &StratumContext, seed: f64) -> f64 {
        let stats = self.get_create_stats(ctx);
        let mut current = stats.min_diff.lock();
        if !current.is_finite() || *current < VARDIFF_MIN_DIFF {
            *current = seed.max(VARDIFF_MIN_DIFF);
        }
        let restored = *current;
        drop(current);
        if stats.var_diff_start_time.lock().is_none() {
            *stats.var_diff_start_time.lock() = Some(Instant::now());
        }
        *stats.last_session_activity.lock() = Instant::now();
        restored
    }

    pub async fn handle_submit(
        &self,
        ctx: Arc<StratumContext>,
        event: JsonRpcEvent,
        kaspa_api: Arc<dyn KaspaApiTrait + Send + Sync>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // One correlation id per submission, reused across every PoolEvent
        // emitted during this call so downstream consumers can pair the
        // ShareCredited / BlockFound / BlockAccepted lifecycle.
        let correlation_id = CorrelationId::new_v4();

        // Start of the submit→accept latency window (observed on the accept path).
        let submit_started = Instant::now();

        let prefix = self.log_prefix();
        debug!("{} [SUBMIT] ===== SHARE SUBMISSION FROM {} =====", prefix, ctx.remote_addr);
        debug!("{} [SUBMIT] Event ID: {:?}", prefix, event.id);
        debug!("{} [SUBMIT] Params count: {}", prefix, event.params.len());
        debug!("{} [SUBMIT] Full params: {:?}", prefix, event.params);

        // Get per-client mining state from context
        let state = GetMiningState(&ctx);
        let _max_jobs = state.max_jobs() as u64;
        let current_counter = state.current_job_counter();
        let stored_ids = state.get_stored_job_ids();
        debug!("{} [SUBMIT] Retrieved MiningState - counter: {}, stored IDs: {:?}", prefix, current_counter, stored_ids);

        // Validate submit
        // Different miners use different parameter layouts:
        // - ASIC-style (3 params): [address.name, job_id, nonce]
        // - EthereumStratum-style (5 params, e.g. lolMiner): [address.name, job_id, extranonce2, ntime, nonce]
        // We get the address from authorize, but we can optionally validate params[0] if present.
        if event.params.len() < 3 {
            error!("{} [SUBMIT] ERROR: Expected at least 3 params, got {}", prefix, event.params.len());
            let wallet_addr = ctx.wallet_addr.lock().clone();
            let worker_name = ctx.worker_name.lock().clone();
            record_worker_error(&self.instance_id, &wallet_addr, ErrorShortCode::BadDataFromMiner.as_str());
            if let Some(ev) = Self::build_share_rejected(&wallet_addr, &worker_name, ShareRejectReason::MalformedFrame, correlation_id)
            {
                self.emit(ev);
            }
            return Err("malformed event, expected at least 3 params".into());
        }

        let prefix = self.log_prefix();
        debug!("{} [SUBMIT] Params[0] (address/identity): {:?}", prefix, event.params.first());
        debug!("{} [SUBMIT] Params[1] (job_id): {:?}", prefix, event.params.get(1));
        debug!("{} [SUBMIT] Params[2] (nonce-ish): {:?}", prefix, event.params.get(2));

        // Optionally validate params[0] (address.name) if present
        // Some miners send it, others don't - we get address from authorize anyway
        if let Some(Value::String(submitted_identity)) = event.params.first() {
            let wallet_addr = ctx.wallet_addr.lock().clone();
            let _worker_name = ctx.worker_name.lock().clone();

            // Extract address from submitted identity (format: "address.worker")
            let parts: Vec<&str> = submitted_identity.split('.').collect();
            let submitted_address = parts[0];

            // Check if submitted address matches authorized address (case-insensitive, ignore prefix)
            let submitted_clean = submitted_address.trim_start_matches("kaspa:").trim_start_matches("kaspatest:");
            let authorized_clean = wallet_addr.trim_start_matches("kaspa:").trim_start_matches("kaspatest:");

            if submitted_clean.to_lowercase() != authorized_clean.to_lowercase() {
                debug!(
                    "Submit params[0] address mismatch: submitted '{}' vs authorized '{}' (using authorized)",
                    submitted_identity, wallet_addr
                );
            } else {
                debug!("Submit params[0] matches authorized address: {}", submitted_identity);
            }
        }

        // Parse job ID - can be either string or number
        let mut job_id = match &event.params[1] {
            serde_json::Value::String(s) => {
                debug!("[SUBMIT] Job ID is string: '{}'", s);
                s.parse::<u64>().map_err(|e| format!("job id is not parsable as a number: {}", e))?
            }
            serde_json::Value::Number(n) => {
                debug!("[SUBMIT] Job ID is number: {}", n);
                n.as_u64().ok_or("job id number is out of range")?
            }
            _ => {
                error!("[SUBMIT] ERROR: Job ID must be string or number, got: {:?}", event.params[1]);
                return Err("job id must be a string or number".into());
            }
        };

        debug!("[SUBMIT] Parsed job_id: {}", job_id);

        // Get current job counter for debugging
        let current_job_counter = state.current_job_counter();
        debug!(
            "[SUBMIT] Current job counter: {}, submitted job_id: {} (diff: {})",
            current_job_counter,
            job_id,
            if job_id > current_job_counter {
                format!("+{}", job_id - current_job_counter)
            } else {
                format!("-{}", current_job_counter - job_id)
            }
        );

        // Fail immediately if job doesn't exist
        //          if !exists { return nil, fmt.Errorf("job does not exist. stale?") }
        // GetJob returns job at slot (id % maxJobs) without verifying ID matches
        let job = state.get_job(job_id);
        let current_counter = state.current_job_counter();
        let prefix = self.log_prefix();
        let job = match job {
            Some(j) => {
                debug!("{} [SUBMIT] Found job ID {} (current counter: {})", prefix, job_id, current_counter);
                j
            }
            None => {
                // Job doesn't exist at slot. IceRivers routinely submit off-by-N job IDs
                // (own numbering after reconnects / self-incremented IDs), so before dropping
                // the share, try the client's newest stored job — the header the rig is most
                // plausibly mining. The PoW check downstream is header-specific, so a wrong
                // guess just fails validation; a right guess recovers the share (and any KAS
                // block riding on it). The walk-back loop below then tries older jobs too.
                let stored_job_ids = state.get_stored_job_ids();
                let fallback = stored_job_ids.iter().max().copied().and_then(|latest| state.get_job(latest).map(|j| (latest, j)));
                match fallback {
                    Some((latest, j)) => {
                        warn!(
                            "[SUBMIT] Job ID {} not found (counter: {}) — retrying against latest stored job {}",
                            job_id, current_counter, latest
                        );
                        job_id = latest;
                        j
                    }
                    None => {
                        warn!(
                            "[SUBMIT] Job ID {} not found at slot {} (current counter: {}, stored IDs: {:?})",
                            job_id,
                            job_id % 300,
                            current_counter,
                            stored_job_ids
                        );
                        let wallet_addr = ctx.wallet_addr.lock().clone();
                        let worker_name = ctx.worker_name.lock().clone();
                        record_worker_error(&self.instance_id, &wallet_addr, ErrorShortCode::MissingJob.as_str());
                        if let Some(ev) =
                            Self::build_share_rejected(&wallet_addr, &worker_name, ShareRejectReason::MissingJob, correlation_id)
                        {
                            self.emit(ev);
                        }
                        return Err("job does not exist. stale?".into());
                    }
                }
            }
        };

        // Choose nonce param index based on miner param layout.
        // - 3 params: nonce is params[2]
        // - 5+ params: nonce is params[4] for EthereumStratum-style miners (and generally last param)
        let nonce_param_idx = if event.params.len() >= 5 { 4 } else { 2 };
        let nonce_str = event.params[nonce_param_idx].as_str().ok_or("nonce must be a string")?;
        debug!("[SUBMIT] Raw nonce string: '{}'", nonce_str);

        let nonce_str = nonce_str.replace("0x", "");
        debug!("[SUBMIT] Nonce after removing 0x: '{}' (length: {} hex chars)", nonce_str, nonce_str.len());

        // Add extranonce if enabled
        let mut final_nonce_str = nonce_str.clone();
        {
            let extranonce = ctx.extranonce.lock();
            if !extranonce.is_empty() {
                let extranonce_val = extranonce.clone();
                let extranonce2_len = 16 - extranonce_val.len();

                // Only prepend extranonce if nonce is shorter than expected
                if nonce_str.len() <= extranonce2_len {
                    // Format with zero-padding on the right
                    final_nonce_str = format!("{}{:0>width$}", extranonce_val, nonce_str, width = extranonce2_len);
                    debug!(
                        "[SUBMIT] Extranonce prepended: '{}' = '{}' + '{:0>width$}'",
                        final_nonce_str,
                        extranonce_val,
                        nonce_str,
                        width = extranonce2_len
                    );
                }
            }
        } // extranonce guard is dropped here

        debug!("[SUBMIT] Final nonce string: '{}'", final_nonce_str);
        let nonce_val = {
            let prefix = self.log_prefix();
            u64::from_str_radix(&final_nonce_str, 16).map_err(|e| {
                error!("{} [SUBMIT] ERROR: Failed to parse nonce '{}' as hex: {}", prefix, final_nonce_str, e);
                format!("failed parsing noncestr: {}", e)
            })?
        };

        debug!("[SUBMIT] Parsed nonce value (u64): {}", nonce_val);
        debug!("[SUBMIT] Nonce hex: {:016x}", nonce_val);

        let worker_id = ctx.effective_worker_name();
        let submit_key = format!("{}|{}|{}", worker_id, job_id, final_nonce_str);

        let duplicate_outcome = {
            let now = Instant::now();
            let mut guard = self.duplicate_submit_guard.lock();
            if let Some(outcome) = guard.get(&submit_key, now) {
                Some(outcome)
            } else {
                guard.insert_inflight(submit_key.clone(), now);
                None
            }
        };

        if let Some(outcome) = duplicate_outcome {
            match outcome {
                DuplicateSubmitOutcome::Accepted | DuplicateSubmitOutcome::InFlight => {
                    ctx.reply(JsonRpcResponse { id: event.id.clone(), result: Some(serde_json::Value::Bool(true)), error: None })
                        .await?;
                    return Ok(());
                }
                DuplicateSubmitOutcome::Stale => {
                    ctx.reply_stale_share(event.id.clone()).await?;
                    return Ok(());
                }
                DuplicateSubmitOutcome::LowDiff => {
                    if let Some(id) = &event.id {
                        let _ = ctx.reply_low_diff_share(id).await;
                    }
                    return Ok(());
                }
                DuplicateSubmitOutcome::Bad => {
                    ctx.reply_bad_share(event.id.clone()).await?;
                    return Ok(());
                }
            }
        }

        // PoW validation with job ID workaround
        // Go validates the submitted job first, then tries previous jobs if share doesn't meet pool difficulty
        // This workaround handles IceRiver/Bitmain ASICs that submit jobs with incorrect IDs
        let mut current_job_id = job_id;
        let mut current_job = job;
        let mut invalid_share = false;
        let mut pow_passed;
        let mut pow_value;
        let max_jobs = state.max_jobs() as u64;
        let mut compat_job_attempts = 0usize;

        debug!("[SUBMIT] Starting PoW validation for job_id: {} (max_jobs: {})", current_job_id, max_jobs);

        loop {
            // DIAGNOSTIC: Run full diagnostic on first share
            static DIAGNOSTIC_RUN: std::sync::Once = std::sync::Once::new();
            let header = &current_job.block.header;
            let mut header_clone = (**header).clone();

            DIAGNOSTIC_RUN.call_once(|| {
                debug!("{}", LogColors::block("===== RUNNING POW DIAGNOSTIC ====="));
                crate::pow_diagnostic::diagnose_pow_issue(&header_clone, nonce_val);
                debug!("{}", LogColors::block("===== DIAGNOSTIC COMPLETE ====="));
            });

            // DEBUG: Compare what we sent to ASIC vs what we're validating (moved to debug level)
            debug!("{} {}", LogColors::validation("[DEBUG]"), LogColors::label("===== VALIDATION DEBUG ====="));
            debug!(
                "{} {} {}",
                LogColors::validation("[DEBUG]"),
                LogColors::label("Job we sent to ASIC:"),
                format!("job_id={}, timestamp={}", current_job_id, current_job.block.header.timestamp)
            );
            debug!(
                "{} {} {}",
                LogColors::validation("[DEBUG]"),
                LogColors::label("ASIC submitted:"),
                format!("job_id={}, nonce=0x{:x}", current_job_id, nonce_val)
            );
            debug!(
                "{} {} {}",
                LogColors::validation("[DEBUG]"),
                LogColors::label("Header we're validating:"),
                format!("timestamp={}, nonce={}, bits=0x{:08x}", header_clone.timestamp, header_clone.nonce, header_clone.bits)
            );

            // Set the nonce in the header
            header_clone.nonce = nonce_val;

            debug!(
                "{} {} {}",
                LogColors::validation("[DEBUG]"),
                LogColors::label("After setting nonce:"),
                format!("timestamp={}, nonce=0x{:x}, bits=0x{:08x}", header_clone.timestamp, header_clone.nonce, header_clone.bits)
            );

            // Use kaspa_pow::State for PoW validation against the header's compact bits target.
            use kaspa_pow::State as PowState;
            let pow_state = PowState::new(&header_clone);
            let (check_passed, pow_value_uint256) = pow_state.check_pow(nonce_val);

            // Convert Uint256 to BigUint for comparison
            pow_value = num_bigint::BigUint::from_bytes_be(&pow_value_uint256.to_be_bytes());

            debug!(
                "{} {} {}",
                LogColors::validation("[DEBUG]"),
                LogColors::label("PowState result:"),
                format!("check_passed={}, pow_value={:x}", check_passed, pow_value)
            );

            // Calculate the block-found target. In real merged mining the parent carries
            // the (hard) Kaspa target in header.bits, but ZKas aux blocks must be found
            // at the ZKas (easier) cadence — so use the ZKas target when merged,
            // falling back to the parent's own bits otherwise.
            use crate::hasher::calculate_target;
            let merged_fc = kaspa_api.merged_fc_target(&current_job.block);
            let merged_mode = merged_fc.is_some();
            let network_target = merged_fc.unwrap_or_else(|| calculate_target(header_clone.bits as u64));

            // Check if pow_value meets network target (lower hash is better)
            let meets_network_target = pow_value <= network_target;

            // Merged-mining visibility: full KAS clears are rare (minutes apart fleet-wide),
            // so shares within 1024x of the Kaspa target are logged as near-misses — at the
            // current fleet hashrate several per minute are expected, which makes a silent
            // failure in the share→Kaspa pipeline observable long before a real find.
            if merged_mode {
                let kaspa_target = calculate_target(header_clone.bits as u64);
                if pow_value <= (kaspa_target.clone() << 10u32) {
                    info!(
                        "{} kaspa near-miss (within 1024x of KAS target): full_clear={} worker={} job={}",
                        LogColors::block("[MERGED]"),
                        pow_value <= kaspa_target,
                        worker_id,
                        current_job_id
                    );
                }
            }
            // IMPORTANT: Use kaspa_pow's own compact-target handling as the source of truth.
            // This avoids any potential mismatch in our BigUint conversion/comparison path.
            pow_passed = check_passed;

            let pow_value_bytes = pow_value.to_bytes_be();
            let network_target_bytes = network_target.to_bytes_be();

            debug!("[SUBMIT] Target comparison:");
            debug!("[SUBMIT]   - pow_value: {:x} ({} bytes)", pow_value, pow_value_bytes.len());
            debug!("[SUBMIT]   - network_target: {:x} ({} bytes)", network_target, network_target_bytes.len());
            debug!("[SUBMIT]   - meets_network_target(BigUint): {}", meets_network_target);
            debug!("[SUBMIT]   - check_passed(kaspa_pow): {}", check_passed);

            debug!(
                "[SUBMIT] PoW check result: passed={}, pow_value={:x}, network_target={:x}, header.bits={}",
                pow_passed, pow_value, network_target, header_clone.bits
            );

            // Log detailed validation information with colors (moved to debug level)
            debug!(
                "{} {} {}",
                LogColors::validation("[VALIDATION]"),
                LogColors::label("PoW Validation -"),
                format!(
                    "Nonce: {:x}, Pow Value: {:x} ({} bytes), Network Target: {:x} ({} bytes)",
                    nonce_val,
                    pow_value,
                    pow_value_bytes.len(),
                    network_target,
                    network_target_bytes.len()
                )
            );
            debug!(
                "{} {} {}",
                LogColors::validation("[VALIDATION]"),
                LogColors::label("Comparison:"),
                format!("pow_value <= network_target = {} (lower hash is better)", meets_network_target)
            );
            debug!(
                "{} {} {}",
                LogColors::validation("[VALIDATION]"),
                LogColors::label("PowState.check_pow() result:"),
                format!("passed={}, Header bits: {}", pow_passed, header_clone.bits)
            );

            // On devnet, network difficulty is very low, so we should see blocks being found
            // Log at debug level (detailed validation logs moved to debug)
            if pow_passed {
                debug!(
                    "{} {} {}",
                    LogColors::validation("[VALIDATION]"),
                    LogColors::block("*** NETWORK TARGET PASSED ***"),
                    format!("pow_value={:x} <= network_target={:x}", pow_value, network_target)
                );
            } else if !network_target.is_zero() {
                let ratio = if !pow_value.is_zero() {
                    let target_f64 = network_target.to_f64().unwrap_or(0.0);
                    let pow_f64 = pow_value.to_f64().unwrap_or(1.0);
                    if pow_f64 > 0.0 { (target_f64 / pow_f64) * 100.0 } else { 0.0 }
                } else {
                    0.0
                };
                debug!(
                    "{} {} {}",
                    LogColors::validation("[VALIDATION]"),
                    LogColors::label("Network target NOT met -"),
                    format!("pow_value={:x} > network_target={:x} ({}% of target)", pow_value, network_target, ratio)
                );
            } else {
                warn!("{} {}", LogColors::validation("[VALIDATION]"), LogColors::error("Network target is ZERO - cannot validate!"));
            }

            // Check network target (block)
            // Use meets_network_target (not pow_passed) for network target validation
            // Go code compares: powValue.Cmp(&powState.Target) <= 0 where Target is network target from header.bits
            // We calculate network_target directly from current job's header.bits (not from stored state)
            // This ensures we use the correct target for each job, as different jobs may have different header.bits
            if meets_network_target || (merged_mode && check_passed) {
                let wallet_addr = ctx.wallet_addr.lock().clone();
                let worker_name = ctx.effective_worker_name();
                let prefix = self.log_prefix();

                // Materialize the solved parent before either settlement leg.
                // Kaspa-parent submission is deliberately independent of the
                // ZKAS H_fc claim: a later parent nonce can still earn KAS even
                // when an earlier nonce already minted the committed ZKAS block.
                let header_bits = header_clone.bits;
                let header_version = header_clone.version;
                let original_timestamp = header_clone.timestamp;
                header_clone.nonce = nonce_val;
                let transactions_vec = current_job.block.transactions.iter().cloned().collect();
                let block = Block::from_arcs(Arc::new(header_clone), Arc::new(transactions_vec));

                let (parent_outcome, claimed_zkas) = submit_parent_then_claim_zkas(
                    kaspa_api.as_ref(),
                    &block,
                    &current_job.block,
                    meets_network_target,
                )
                .await;
                crate::prom::record_merged_parent_submit(&self.worker_prom_context(&ctx, ""), &parent_outcome, claimed_zkas);

                // The two chain targets are independent. If this nonce clears
                // only the Kaspa parent target, its KAS block has already been
                // submitted and recorded above. Credit the Stratum share but do
                // not claim or submit a ZKAS block it did not solve.
                if !meets_network_target {
                    invalid_share = false;
                    break;
                }

                // In merged mode many distinct parent nonces can prove the same
                // fixed ZKAS `H_fc`, but that ZKAS block can pay only once.
                // Claim only the ZKAS leg, after preserving any Kaspa reward.
                if !claimed_zkas {
                    debug!(
                        "{} duplicate ZKAS network-target solution for already-solved job {} \
                         (Kaspa parent handled independently; share credited)",
                        prefix, current_job_id
                    );
                    invalid_share = false;
                    break;
                }
                let _ = self.job_refresh_tx.send(ctx.session_uid());

                info!(
                    "{} {} {}",
                    prefix,
                    LogColors::block("===== BLOCK FOUND! ====="),
                    format!("Worker: {}, Wallet: {}, Nonce: {:x}", worker_name, wallet_addr, nonce_val)
                );
                debug!(
                    "{} {} {} {}",
                    prefix,
                    LogColors::block("[BLOCK]"),
                    LogColors::label("ACCEPTANCE REASON:"),
                    format!("pow_value ({:x}) <= network_target ({:x})", pow_value, network_target)
                );
                debug!("{} {} {} {}", prefix, LogColors::block("[BLOCK]"), LogColors::label("Pow Value:"), format!("{:x}", pow_value));

                // Verify timestamp is still valid (not too old)
                // Kaspa typically accepts blocks with timestamps within a reasonable window
                // Block templates are fetched frequently, so the timestamp should be recent
                let current_time_ms =
                    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
                let timestamp_age_ms = current_time_ms.saturating_sub(original_timestamp);
                let timestamp_age_sec = timestamp_age_ms / 1000;

                // Log header verification to confirm we're using real headers (moved to debug level)
                debug!(
                    "{} {} {}",
                    LogColors::block("[BLOCK]"),
                    LogColors::label("Header Verification:"),
                    "Using REAL header from Kaspa node block template"
                );
                debug!("{} {} {}", LogColors::block("[BLOCK]"), LogColors::label("  - Header Version:"), header_version);
                debug!(
                    "{} {} {}",
                    LogColors::block("[BLOCK]"),
                    LogColors::label("  - Header Bits:"),
                    format!("{} (0x{:x})", header_bits, header_bits)
                );
                debug!(
                    "{} {} {}",
                    LogColors::block("[BLOCK]"),
                    LogColors::label("  - Timestamp:"),
                    format!("{} (age: {}s, preserved from template)", original_timestamp, timestamp_age_sec)
                );
                debug!(
                    "{} {} {}",
                    LogColors::block("[BLOCK]"),
                    LogColors::label("  - Nonce:"),
                    format!("{:x} (set from ASIC submission)", nonce_val)
                );

                // A share on a template older than 10s means the rig is grinding a dead job —
                // at Kaspa's 10 BPS a 10s-old parent is already ~100 blocks deep, so any KAS
                // find on it is red/unpaid; healthy rigs submit on jobs ≤2s old.
                // it fell out of the job-broadcast path (seen after mass reconnects: per-client
                // job counter frozen at 1-2 while healthy rigs advance). Every KAS find made on
                // such a parent lands red/unpaid on Kaspa (10 BPS), so the rig's hashrate is
                // 100% wasted. Kick it: on reconnect it re-registers and gets fresh templates.
                if timestamp_age_sec > 10 {
                    warn!(
                        "{} {} {}",
                        LogColors::block("[BLOCK]"),
                        LogColors::error("STALE RIG KICKED:"),
                        format!(
                            "worker={} submitted work for a {}s-old template (stuck job) — disconnecting to force a clean re-handshake",
                            worker_name, timestamp_age_sec
                        )
                    );
                    ctx.disconnect();
                }

                let blue_score = block.header.blue_score;

                // Calculate block hash immediately after block creation
                // Use kaspa_consensus_core::hashing::header::hash() for block hash calculation
                // In Kaspa, the block hash is the header hash (transactions are represented by hash_merkle_root in header)
                //
                // MERGED MODE: the solved block is the *parent* (Kaspa-shaped)
                // carrier whose coinbase commits to the ZKas block hash H_fc.
                // The block that actually lands on the ZKas chain keeps H_fc
                // (the AuxPoW rides outside the header hash), so every
                // block-facing consumer — the blue-confirm poll, BlockFound /
                // BlockAccepted events, the dashboard blocks list — must use
                // H_fc, not the parent header hash (which never exists on the
                // ZKas chain; using it left the pool at "0 blocks confirmed"
                // for a full day of live mining).
                use kaspa_consensus_core::hashing::header;
                let block_hash = kaspa_api
                    .merged_chain_hash(&current_job.block)
                    .map_or_else(|| header::hash(&block.header).to_string(), |h| h.to_string());
                let block_daa_score = block.header.daa_score;

                // Emit BlockFound *before* submitting to kaspad so consumers
                // record the candidate even if our submit_block call hangs
                // or kaspad responds slowly. Pairs with BlockAccepted by
                // correlation_id when submission succeeds.
                if let (Ok(wallet_d), Ok(worker_d), Ok(hash_d)) = (
                    WalletAddress::new(wallet_addr.clone()),
                    WorkerName::new(worker_name.clone()),
                    DomainBlockHash::from_hex(&block_hash),
                ) {
                    self.emit(PoolEvent::BlockFound {
                        wallet: wallet_d,
                        worker: worker_d,
                        hash: hash_d,
                        daa_score: DaaScore::new(block_daa_score),
                        ts: chrono::Utc::now(),
                        correlation_id,
                    });
                }

                // Log prominent "Block Found" message with hash
                info!("{} {} {}", prefix, LogColors::block("BLOCK FOUND!"), format!("Hash: {}", block_hash));
                debug!("{} {} {} {}", prefix, LogColors::block("[BLOCK]"), LogColors::label("Worker:"), worker_name);
                debug!("{} {} {} {}", prefix, LogColors::block("[BLOCK]"), LogColors::label("Wallet:"), wallet_addr);
                debug!("{} {} {} {}", prefix, LogColors::block("[BLOCK]"), LogColors::label("Nonce:"), format!("{:x}", nonce_val));

                // Log block submission details before submission (moved to debug level)
                debug!("{} {}", LogColors::block("[BLOCK]"), LogColors::block("=== SUBMITTING BLOCK TO NODE ==="));
                debug!("{} {} {}", LogColors::block("[BLOCK]"), LogColors::label("Worker:"), worker_name);
                debug!(
                    "{} {} {}",
                    LogColors::block("[BLOCK]"),
                    LogColors::label("Nonce:"),
                    format!("{:x} (0x{:016x})", nonce_val, nonce_val)
                );
                debug!(
                    "{} {} {}",
                    LogColors::block("[BLOCK]"),
                    LogColors::label("Bits:"),
                    format!("{} (0x{:08x})", header_bits, header_bits)
                );
                debug!("{} {} {}", LogColors::block("[BLOCK]"), LogColors::label("Timestamp:"), format!("{}", original_timestamp));
                debug!("{} {} {}", LogColors::block("[BLOCK]"), LogColors::label("Blue Score:"), blue_score);
                debug!("{} {} {}", LogColors::block("[BLOCK]"), LogColors::label("Pow Value:"), format!("{:x}", pow_value));
                debug!("{} {} {}", LogColors::block("[BLOCK]"), LogColors::label("Network Target:"), format!("{:x}", network_target));
                debug!("{} {} {}", LogColors::block("[BLOCK]"), LogColors::label("Job ID:"), current_job_id);
                debug!("{} {} {}", LogColors::block("[BLOCK]"), LogColors::label("Wallet:"), wallet_addr);
                debug!(
                    "{} {} {}",
                    LogColors::block("[BLOCK]"),
                    LogColors::label("Client:"),
                    format!("{}:{}", ctx.remote_addr(), ctx.remote_port())
                );
                debug!("{} {} {}", LogColors::block("[BLOCK]"), LogColors::label("Block Hash:"), block_hash);
                debug!("{} {}", LogColors::block("[BLOCK]"), "Calling kaspa_api.submit_block()...");

                // Submit block to node
                let block_submit_result = kaspa_api.submit_block(block.clone()).await;

                match block_submit_result {
                    // kaspad accepted the RPC but declined the block (most
                    // commonly a tip-race `Reject(BlockInvalid)` against
                    // testnet-10's 10 BPS). The miner's PoW is still
                    // valid by construction — we only entered this branch
                    // because the share met the network target — so we
                    // credit the share but SKIP `PoolEvent::BlockAccepted`
                    // and the BLOCK_CONFIRM_MAX_ATTEMPTS polling job.
                    // This restores the pre-M3f miner-facing accept rate
                    // without bringing back the pre-M3f phantom
                    // `BlockAccepted` accounting bug. See
                    // `docs/phase-3-acceptance.md` §M3f for the live
                    // evidence (Goldshell 68% reject regression).
                    Ok(crate::kaspaapi::BlockSubmitOutcome::RejectedByNode(_reason)) => {
                        // `kaspaapi::submit_block` already emitted the
                        // operator-visible WARN with the reject reason
                        // and block hash; no second log here keeps the
                        // share-handler log volume bounded at high
                        // submission rates.
                        let prom_worker = crate::prom::WorkerContext {
                            instance_id: self.instance_id.clone(),
                            worker_name: worker_name.clone(),
                            miner: String::new(),
                            wallet: wallet_addr.clone(),
                            ip: format!("{}:{}", ctx.remote_addr(), ctx.remote_port()),
                        };
                        record_block_not_confirmed_blue(&prom_worker);
                        invalid_share = false;
                        break;
                    }
                    Ok(crate::kaspaapi::BlockSubmitOutcome::Accepted(_response)) => {
                        let prefix = self.log_prefix();
                        // Block accepted - log after submit to get it submitted faster
                        info!(
                            "{} {} {}",
                            prefix,
                            LogColors::block("[BLOCK]"),
                            LogColors::block(&format!("Block submitted successfully! Hash: {}", block_hash))
                        );
                        info!(
                            "{} {} {}",
                            prefix,
                            LogColors::block("[BLOCK]"),
                            LogColors::block(&format!("BLOCK ACCEPTED BY NODE! Hash: {}", block_hash))
                        );
                        info!("{} {} {} {}", prefix, LogColors::block("[BLOCK]"), LogColors::label("  - Worker:"), worker_name);
                        info!(
                            "{} {} {} {}",
                            prefix,
                            LogColors::block("[BLOCK]"),
                            LogColors::label("  - Nonce:"),
                            format!("{:x}", nonce_val)
                        );

                        let stats = self.get_create_stats(&ctx);
                        let overall = self.overall.clone();
                        let instance_id = self.instance_id.clone();
                        let prom_worker = self.worker_prom_context(&ctx, "");

                        record_block_accepted_by_node(&prom_worker);

                        // Emit BlockAccepted now that kaspad has acknowledged
                        // our submitted block. This is *not* the coinbase-
                        // maturity signal; the accountant emits its own
                        // event when it observes the matured coinbase.
                        if let Ok(hash_d) = DomainBlockHash::from_hex(&block_hash) {
                            self.emit(PoolEvent::BlockAccepted { hash: hash_d, ts: chrono::Utc::now(), correlation_id });
                        }

                        let kaspa_api = Arc::clone(&kaspa_api);
                        let block_hash_for_confirm = block_hash.clone();

                        tokio::spawn(async move {
                            for _ in 0..BLOCK_CONFIRM_MAX_ATTEMPTS {
                                match kaspa_api.get_current_block_color(&block_hash_for_confirm).await {
                                    Ok(true) => {
                                        *stats.blocks_found.lock() += 1;
                                        *overall.blocks_found.lock() += 1;
                                        record_block_found(&prom_worker, nonce_val, blue_score, block_hash_for_confirm.clone());
                                        info!(
                                            "[{}] {} {}",
                                            instance_id,
                                            LogColors::block("[BLOCK]"),
                                            LogColors::block(&format!(
                                                "Block confirmed BLUE in DAG! Hash: {}",
                                                block_hash_for_confirm
                                            ))
                                        );
                                        return;
                                    }
                                    Ok(false) => {
                                        tokio::time::sleep(BLOCK_CONFIRM_RETRY_DELAY).await;
                                    }
                                    Err(_) => {
                                        tokio::time::sleep(BLOCK_CONFIRM_RETRY_DELAY).await;
                                    }
                                }
                            }

                            record_block_not_confirmed_blue(&prom_worker);
                            info!(
                                "[{}] {} {}",
                                instance_id,
                                LogColors::block("[BLOCK]"),
                                LogColors::label(&format!(
                                    "Block not confirmed blue after {} attempts (not counted as Blocks). Hash: {}",
                                    BLOCK_CONFIRM_MAX_ATTEMPTS, block_hash_for_confirm
                                ))
                            );
                        });

                        // Return allows HandleSubmit to record share (blocks are shares too!)
                        // After successful block submission, continue to record the share
                        // Don't return early - let the code continue to record the share
                        invalid_share = false;
                        break;
                    }
                    Err(e) => {
                        let prefix = self.log_prefix();
                        // Only check for "ErrDuplicateBlock" (not "duplicate" or "stale")
                        // Block submission failed
                        let error_str = e.to_string();
                        error!("{} {} {}", prefix, LogColors::block("[BLOCK]"), LogColors::error("Block submission FAILED"));
                        error!("{} {} {} {}", prefix, LogColors::block("[BLOCK]"), LogColors::label("Worker:"), worker_name);
                        error!("{} {} {} {}", prefix, LogColors::block("[BLOCK]"), LogColors::label("Blockhash:"), block_hash);
                        error!("{} {} {} {}", prefix, LogColors::block("[BLOCK]"), LogColors::error("Error:"), error_str);

                        if error_str.contains("ErrDuplicateBlock") {
                            // Block rejected, stale
                            warn!("{} {} {}", prefix, LogColors::block("[BLOCK]"), LogColors::error("block rejected, stale"));
                            warn!(
                                "{} {} {} {}",
                                prefix,
                                LogColors::block("[BLOCK]"),
                                LogColors::label("REJECTION REASON:"),
                                "Block was already submitted to the network (stale/duplicate)"
                            );

                            {
                                let now = Instant::now();
                                let mut guard = self.duplicate_submit_guard.lock();
                                guard.set_outcome(&submit_key, now, DuplicateSubmitOutcome::Stale);
                            }

                            let stats = self.get_create_stats(&ctx);
                            *stats.stale_shares.lock() += 1;
                            *self.overall.stale_shares.lock() += 1;

                            record_stale_share(&self.worker_prom_context(&ctx, ""));
                            if let Some(ev) =
                                Self::build_share_rejected(&wallet_addr, &worker_name, ShareRejectReason::Stale, correlation_id)
                            {
                                self.emit(ev);
                            }
                            ctx.reply_stale_share(event.id.clone()).await?;
                            return Ok(());
                        } else {
                            // Block rejected, unknown issue (probably bad pow)
                            warn!(
                                "{} {} {}",
                                prefix,
                                LogColors::block("[BLOCK]"),
                                LogColors::error("block rejected, unknown issue (probably bad pow)")
                            );
                            error!(
                                "{} {} {} {}",
                                prefix,
                                LogColors::block("[BLOCK]"),
                                LogColors::label("REJECTION REASON:"),
                                "Block failed node validation (probably bad pow)"
                            );
                            error!("{} {} {} {}", prefix, LogColors::block("[BLOCK]"), LogColors::error("Error:"), error_str);

                            let stats = self.get_create_stats(&ctx);
                            *stats.invalid_shares.lock() += 1;
                            *self.overall.invalid_shares.lock() += 1;

                            record_invalid_share(&self.worker_prom_context(&ctx, ""));
                            if let Some(ev) =
                                Self::build_share_rejected(&wallet_addr, &worker_name, ShareRejectReason::BadPow, correlation_id)
                            {
                                self.emit(ev);
                            }

                            {
                                let now = Instant::now();
                                let mut guard = self.duplicate_submit_guard.lock();
                                guard.set_outcome(&submit_key, now, DuplicateSubmitOutcome::Bad);
                            }
                            ctx.reply_bad_share(event.id.clone()).await?;
                            return Ok(());
                        }
                    }
                }
            }

            // Check pool difficulty
            let job_diff = state.get_job_diff(current_job_id).or_else(|| state.stratum_diff());
            let pool_target = job_diff.as_ref().map(|d| d.target_value.clone()).unwrap_or_else(BigUint::zero);

            // Compare FULL pow_value against pool_target (not just lower bits)
            // Compare full 256-bit values
            let pow_bytes = pow_value.to_bytes_be();
            let target_bytes = pool_target.to_bytes_be();

            // Log difficulty check for debugging
            if pool_target.is_zero() {
                warn!("stratum_diff target is zero! pow_value: {:x}, pool_target: {:x}", pow_value, pool_target);
            } else {
                let pow_len = pow_bytes.len();
                let target_len = target_bytes.len();

                debug!(
                    "difficulty check: nonce: {:x} ({}), pow_value (full): {:x} ({} bytes), pool_target: {:x} ({} bytes), diff_value: {:?}, pow_value <= pool_target = {}",
                    nonce_val,
                    nonce_val,
                    pow_value,
                    pow_len,
                    pool_target,
                    target_len,
                    job_diff.as_ref().map(|d| d.diff_value),
                    pow_value <= pool_target
                );
                debug!(
                    "Full comparison - pow_value: {:x} ({} bytes), pool_target: {:x} ({} bytes)",
                    pow_value, pow_len, pool_target, target_len
                );
            }

            // Check pool difficulty (stratum target)
            // If pow_value >= pool_target, share doesn't meet pool difficulty
            // Higher hash value means worse share
            if pow_value > pool_target {
                // Share doesn't meet pool difficulty - might be wrong job ID (moved to debug to keep terminal clean)
                let worker_name = ctx.worker_name.lock().clone();
                debug!(
                    "{} {} {}",
                    LogColors::validation("INVALID SHARE (too high)"),
                    LogColors::label("worker:"),
                    format!(
                        "{}, nonce: {:x}, pow_value: {:x}, pool_target: {:x}, pow_ge_pool_target: true",
                        worker_name, nonce_val, pow_value, pool_target
                    )
                );

                if current_job_id == job_id {
                    debug!("low diff share... checking for bad job ID ({})", current_job_id);
                    invalid_share = true;
                }

                // Job ID workaround for Bitmain/IceRiver ASICs - try previous jobs
                // Validate job ID: jobId == 1 || jobId%maxJobs == submitInfo.jobId%maxJobs+1
                if current_job_id == 1 || (current_job_id % max_jobs == ((job_id % max_jobs) + 1) % max_jobs) {
                    // Exhausted all previous blocks (wrapped around or reached job 1)
                    debug!("Job ID loop exhausted: current_job_id={}, job_id={}, max_jobs={}", current_job_id, job_id, max_jobs);
                    break;
                } else {
                    compat_job_attempts += 1;
                    if compat_job_attempts >= MAX_COMPAT_JOB_ATTEMPTS {
                        debug!("Job compatibility window exhausted after {} attempts (submitted job {})", compat_job_attempts, job_id);
                        break;
                    }
                    // Try previous job ID
                    let prev_job_id = current_job_id - 1;
                    if let Some(prev_job) = state.get_job(prev_job_id) {
                        current_job_id = prev_job_id;
                        current_job = prev_job;
                        debug!("Trying previous job ID: {} (submitted as {})", current_job_id, job_id);
                        // Continue loop to validate with previous job
                        continue;
                    } else {
                        // Job doesn't exist, exit loop - bad share will be recorded
                        debug!("Previous job ID {} doesn't exist, exiting loop", prev_job_id);
                        break;
                    }
                }
            } else {
                // Valid share (pow_value < pool_target) - moved to debug to keep terminal clean
                let worker_name = ctx.worker_name.lock().clone();
                debug!(
                    "{} {} {}",
                    LogColors::validation("VALID SHARE"),
                    LogColors::label("worker:"),
                    format!(
                        "{}, nonce: {:x}, pow_value: {:x}, pool_target: {:x}, pow_lt_pool_target: true",
                        worker_name, nonce_val, pow_value, pool_target
                    )
                );

                if invalid_share {
                    debug!("found correct job ID: {} (submitted as {})", current_job_id, job_id);
                }
                invalid_share = false;
                break;
            }
        }

        let stats = self.get_create_stats(&ctx);

        if invalid_share {
            debug!("low diff share confirmed");
            *stats.invalid_shares.lock() += 1;
            *self.overall.invalid_shares.lock() += 1;

            let wallet_addr = ctx.wallet_addr.lock().clone();
            let worker_name = ctx.worker_name.lock().clone();
            record_weak_share(&self.worker_prom_context(&ctx, ""));
            if let Some(ev) = Self::build_share_rejected(&wallet_addr, &worker_name, ShareRejectReason::LowDifficulty, correlation_id)
            {
                self.emit(ev);
            }

            if let Some(id) = &event.id {
                let _ = ctx.reply_low_diff_share(id).await;
            }

            {
                let now = Instant::now();
                let mut guard = self.duplicate_submit_guard.lock();
                guard.set_outcome(&submit_key, now, DuplicateSubmitOutcome::LowDiff);
            }
            return Ok(());
        }

        // Record valid share
        //   stats.SharesFound.Add(1)
        //   stats.VarDiffSharesFound.Add(1)
        //   stats.SharesDiff.Add(state.stratumDiff.hashValue)  // Accumulates hashValue, not diffValue!
        //   stats.LastShare = time.Now()
        //   sh.overall.SharesFound.Add(1)
        //   RecordShareFound(ctx, state.stratumDiff.hashValue)
        let stats = self.get_create_stats(&ctx);
        *stats.shares_found.lock() += 1;
        *stats.var_diff_shares_found.lock() += 1;

        // Get hashValue from stratum_diff
        let credited_job_diff = state.get_job_diff(current_job_id).or_else(|| state.stratum_diff());
        let hash_value = credited_job_diff.as_ref().map_or(0.0, |d| d.hash_value);

        // Accumulate hashValue for hashrate calculation
        *stats.shares_diff.lock() += hash_value;
        *stats.last_share.lock() = Instant::now();
        *self.overall.shares_found.lock() += 1;

        let wallet_addr = ctx.wallet_addr.lock().clone();
        let worker_name = ctx.worker_name.lock().clone();
        record_share_found(&self.worker_prom_context(&ctx, ""), hash_value);
        crate::prom::observe_share_accept_latency(&self.instance_id, submit_started.elapsed().as_secs_f64());

        // Emit ShareCredited. We use the worker's *assigned* pool
        // difficulty here (the value the stratum layer set on the
        // job), not the share's hash_value — accounting downstream
        // multiplies by difficulty to get PROP weight.
        let assigned_diff = credited_job_diff.as_ref().map_or(hash_value, |d| d.diff_value);
        let job_daa_score = current_job.block.header.daa_score;
        if let (Ok(wallet_d), Ok(worker_d), Ok(difficulty_d)) =
            (WalletAddress::new(wallet_addr.clone()), WorkerName::new(worker_name.clone()), ShareDifficulty::new(assigned_diff))
        {
            self.emit(PoolEvent::ShareCredited {
                wallet: wallet_d,
                worker: worker_d,
                difficulty: difficulty_d,
                daa_score: DaaScore::new(job_daa_score),
                ts: chrono::Utc::now(),
                correlation_id,
            });
        }
        {
            let now = Instant::now();
            let mut guard = self.duplicate_submit_guard.lock();
            guard.set_outcome(&submit_key, now, DuplicateSubmitOutcome::Accepted);
        }

        ctx.reply(JsonRpcResponse { id: event.id.clone(), result: Some(serde_json::Value::Bool(true)), error: None })
            .await
            .map_err(|e| format!("failed to reply: {}", e))?;
        Ok(())
    }

    #[allow(dead_code)]
    async fn submit_block(
        &self,
        _ctx: &StratumContext,
        _block: Block,
        _nonce: u64,
        _event_id: &serde_json::Value,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Block submission is handled at the HandleSubmit level
        // This method is kept for compatibility but actual submission
        // happens when PoW passes network target in handle_submit
        Ok(())
    }

    pub fn set_client_vardiff(&self, ctx: &StratumContext, min_diff: f64) -> f64 {
        let Some(stats) = self.get_stats_if_exists(ctx) else {
            // Job/difficulty paths must not resurrect pruned 0-share workers in the terminal table.
            return Self::current_stratum_diff(ctx);
        };
        let previous = *stats.min_diff.lock();
        *stats.min_diff.lock() = min_diff;
        *stats.var_diff_start_time.lock() = Some(Instant::now());
        *stats.var_diff_shares_found.lock() = 0;
        *stats.var_diff_window.lock() = 0;
        previous
    }

    pub fn get_client_vardiff(&self, ctx: &StratumContext) -> f64 {
        if let Some(stats) = self.get_stats_if_exists(ctx) {
            return *stats.min_diff.lock();
        }
        Self::current_stratum_diff(ctx)
    }

    pub fn start_client_vardiff(&self, ctx: &StratumContext) {
        let Some(stats) = self.get_stats_if_exists(ctx) else {
            return;
        };
        if stats.var_diff_start_time.lock().is_none() {
            *stats.var_diff_start_time.lock() = Some(Instant::now());
            *stats.var_diff_shares_found.lock() = 0;
        }
    }

    pub fn start_prune_stats_thread(&self) {
        self.start_prune_stats_thread_impl(None);
    }

    pub fn start_prune_stats_thread_with_shutdown(&self, shutdown_rx: watch::Receiver<bool>) {
        self.start_prune_stats_thread_impl(Some(shutdown_rx));
    }

    fn start_prune_stats_thread_impl(&self, mut shutdown_rx: Option<watch::Receiver<bool>>) {
        let stats = Arc::clone(&self.stats);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(STATS_PRUNE_INTERVAL);
            loop {
                if let Some(ref mut rx) = shutdown_rx {
                    tokio::select! {
                        _ = rx.changed() => {
                            if *rx.borrow() {
                                break;
                            }
                        }
                        _ = interval.tick() => {
                            let mut stats_map = stats.lock();
                            let now = Instant::now();
                            stats_map.retain(|_, v| should_retain_vardiff_stats(v, now));
                        }
                    }
                } else {
                    interval.tick().await;
                    let mut stats_map = stats.lock();
                    let now = Instant::now();
                    stats_map.retain(|_, v| should_retain_vardiff_stats(v, now));
                }
            }
        });
    }

    pub fn start_print_stats_thread(&self, target_spm: u32) {
        self.start_print_stats_thread_impl(target_spm, None);
    }

    pub fn start_print_stats_thread_with_shutdown(&self, target_spm: u32, shutdown_rx: watch::Receiver<bool>) {
        self.start_print_stats_thread_impl(target_spm, Some(shutdown_rx));
    }

    fn start_print_stats_thread_impl(&self, target_spm: u32, shutdown_rx: Option<watch::Receiver<bool>>) {
        let target_spm = if target_spm == 0 { 20.0 } else { target_spm as f64 };
        let instance_id = self.instance_id.clone();
        let inst_short = {
            let digits: String = instance_id.chars().filter(|c| c.is_ascii_digit()).collect();
            if let Ok(n) = digits.parse::<u32>() { format!("Ins{:02}", n) } else { "Ins??".to_string() }
        };

        {
            let mut registry = STATS_PRINTER_REGISTRY.lock();
            if !registry.iter().any(|e| e.instance_id == instance_id) {
                registry.push(StatsPrinterEntry {
                    instance_id,
                    inst_short,
                    target_spm,
                    start: Instant::now(),
                    stats: Arc::clone(&self.stats),
                    overall: Arc::clone(&self.overall),
                });
            }
        }

        if STATS_PRINTER_STARTED.swap(true, Ordering::AcqRel) {
            return;
        }

        let mut shutdown_rx = shutdown_rx;
        tokio::spawn(async move {
            fn trunc<'a>(s: &'a str, max: usize) -> Cow<'a, str> {
                if s.len() <= max { Cow::Borrowed(s) } else { Cow::Owned(s.chars().take(max).collect()) }
            }

            fn format_uptime(d: Duration) -> String {
                let total_secs = d.as_secs();
                let days = total_secs / 86_400;
                let hours = (total_secs % 86_400) / 3_600;
                let mins = (total_secs % 3_600) / 60;
                let secs = total_secs % 60;
                format!("{:02}:{:02}:{:02}:{:02}", days, hours, mins, secs)
            }

            const WORKER_W: usize = 16;
            const INST_W: usize = 5;
            const HASH_W: usize = 11;
            const DIFF_W: usize = 6;
            const SPM_W: usize = 11;
            const TRND_W: usize = 4;
            const ACC_W: usize = 12;
            const BLK_W: usize = 6;
            const TBLK_W: usize = 6;
            const TIME_W: usize = 11;

            fn border() -> String {
                format!(
                    "+-{}-+-{}-+-{}-+-{}-+-{}-+-{}-+-{}-+-{}-+-{}-+-{}-+",
                    "-".repeat(WORKER_W),
                    "-".repeat(INST_W),
                    "-".repeat(HASH_W),
                    "-".repeat(DIFF_W),
                    "-".repeat(SPM_W),
                    "-".repeat(TRND_W),
                    "-".repeat(ACC_W),
                    "-".repeat(BLK_W),
                    "-".repeat(TBLK_W),
                    "-".repeat(TIME_W)
                )
            }

            fn header() -> String {
                format!(
                    "| {:<WORKER_W$} | {:<INST_W$} | {:>HASH_W$} | {:>DIFF_W$} | {:>SPM_W$} | {:<TRND_W$} | {:>ACC_W$} | {:>BLK_W$} | {:>TBLK_W$} | {:>TIME_W$} |",
                    "Worker", "Inst", "Hash", "Diff", "SPM|TGT", "Trnd", "Acc|Stl|Inv", "Blocks", "Total", "D|HR|M|S",
                )
            }

            let mut interval = tokio::time::interval(STATS_PRINT_INTERVAL);
            // Internal miner hashrate is based on hashes/sec (not Stratum shares), so we keep a
            // last-sample snapshot to compute a stable, accurate rate (matching the dashboard).
            #[cfg(feature = "rkstratum_cpu_miner")]
            let mut last_internal_hashes: Option<u64> = None;
            #[cfg(feature = "rkstratum_cpu_miner")]
            let mut last_internal_sample = Instant::now();
            loop {
                if let Some(ref mut rx) = shutdown_rx {
                    tokio::select! {
                        _ = rx.changed() => {
                            if *rx.borrow() {
                                break;
                            }
                        }
                        _ = interval.tick() => {}
                    }
                } else {
                    interval.tick().await;
                }

                let node_status = {
                    let s = NODE_STATUS.lock();
                    s.clone()
                };

                let entries = {
                    let registry = STATS_PRINTER_REGISTRY.lock();
                    registry
                        .iter()
                        .map(|e| (e.inst_short.clone(), e.target_spm, e.start, Arc::clone(&e.stats), Arc::clone(&e.overall)))
                        .collect::<Vec<_>>()
                };

                if entries.is_empty() {
                    continue;
                }

                let mut rows: Vec<(String, String)> = Vec::new();
                let mut total_rate = 0.0;
                let mut total_worker_spm = 0.0;
                let mut total_worker_count: usize = 0;
                let mut total_shares: i64 = 0;
                let mut total_stales: i64 = 0;
                let mut total_invalids: i64 = 0;
                let mut total_blocks: i64 = 0;
                let mut total_blocks_all_time: i64 = 0;

                let now = Instant::now();
                let start = entries.iter().map(|(_, _, start, _, _)| *start).max_by_key(|t| t.elapsed()).unwrap_or_else(Instant::now);
                let mut total_target: Option<f64> = Some(entries[0].1);
                for (inst_short, target_spm, _, stats, overall) in entries.iter() {
                    if let Some(t) = total_target
                        && (t - *target_spm).abs() > 0.0001
                    {
                        total_target = None;
                    }

                    total_shares += *overall.shares_found.lock();
                    total_stales += *overall.stale_shares.lock();
                    total_invalids += *overall.invalid_shares.lock();
                    // overall.blocks_found includes blocks from all workers (even pruned ones)
                    // Accumulate for the "Total" column (all-time blocks)
                    total_blocks_all_time += *overall.blocks_found.lock();

                    let stats_map = stats.lock();
                    for (_, v) in stats_map.iter() {
                        let elapsed = v.start_time.elapsed().as_secs_f64();
                        let rate = if elapsed > 0.0 {
                            let total_hash_value = *v.shares_diff.lock();
                            total_hash_value / elapsed
                        } else {
                            0.0
                        };
                        total_rate += rate;

                        let shares = *v.shares_found.lock();
                        let stales = *v.stale_shares.lock();
                        let invalids = *v.invalid_shares.lock();
                        let blocks = *v.blocks_found.lock();
                        let min_diff = *v.min_diff.lock();

                        // Sum blocks from individual workers for "Blocks" column (online workers only)
                        total_blocks += blocks;

                        let spm = if elapsed > 0.0 { (shares as f64) / (elapsed / 60.0) } else { 0.0 };
                        total_worker_spm += spm;
                        total_worker_count += 1;
                        let trend = if spm > *target_spm * 1.2 {
                            "up"
                        } else if spm < *target_spm * 0.8 {
                            "down"
                        } else {
                            "flat"
                        };

                        let worker = v.worker_name.lock().clone();

                        let spm_tgt = format!("{:>4.1}/{:<4.1}", spm, *target_spm);

                        // For individual workers, "Blocks" and "Total" are the same (they're currently online)
                        let line = format!(
                            "| {:<WORKER_W$} | {:<INST_W$} | {:>HASH_W$} | {:>DIFF_W$} | {:>SPM_W$} | {:<TRND_W$} | {:>ACC_W$} | {:>BLK_W$} | {:>TBLK_W$} | {:>TIME_W$} |",
                            trunc(&worker, WORKER_W),
                            inst_short,
                            format_hashrate(rate),
                            min_diff.round() as u64,
                            spm_tgt,
                            trend,
                            format!("{}/{}/{}", shares, stales, invalids),
                            blocks,
                            blocks, // Total blocks (same as Blocks for active workers)
                            format_uptime(v.start_time.elapsed())
                        );
                        let sort_key = format!("{}:{}", inst_short, worker);
                        rows.push((sort_key, line));
                    }
                }

                rows.sort_by(|a, b| a.0.cmp(&b.0));

                let top = border();
                let sep = border();
                let hdr = header();

                let mut out = Vec::new();

                let sync_str = match node_status.is_synced {
                    Some(true) => "synced".to_string(),
                    Some(false) => "syncing".to_string(),
                    None => "unknown".to_string(),
                };
                let conn_str = if node_status.is_connected { "connected" } else { "disconnected" };

                let net = node_status.network_id.as_deref().unwrap_or("-");
                let ver = node_status.server_version.as_deref().unwrap_or("-");
                let peers = node_status.peers.map(|p| p.to_string()).unwrap_or_else(|| "-".to_string());
                let vdaa = node_status.virtual_daa_score.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string());
                let blocks = node_status.block_count.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string());
                let headers = node_status.header_count.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string());
                let diff = node_status.difficulty.map(|d| format!("{:.2e}", d)).unwrap_or_else(|| "-".to_string());
                let tip = node_status.tip_hash.as_deref().unwrap_or("-");
                let mempool = node_status.mempool_size.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string());

                let tip_short = if tip.len() > 28 { format!("{}...{}", &tip[..16], &tip[tip.len() - 8..]) } else { tip.to_string() };

                let net_short = {
                    let mut network_type = None;
                    let mut suffix = None;
                    if let Some(pos) = net.find("network_type:") {
                        let s = &net[pos + "network_type:".len()..];
                        let s = s.trim_start();
                        network_type = s.split(&[',', '}'][..]).next().map(|v| v.trim());
                    }
                    if let Some(pos) = net.find("suffix:") {
                        let s = &net[pos + "suffix:".len()..];
                        let s = s.trim_start();
                        let raw = s.split(&[',', '}'][..]).next().map(|v| v.trim());
                        if raw != Some("None") {
                            suffix = raw;
                        }
                    }
                    match (network_type, suffix) {
                        (Some(nt), Some(suf)) => format!("{}-{}", nt, suf),
                        (Some(nt), None) => nt.to_string(),
                        _ => net.to_string(),
                    }
                };

                out.push(format!(
                    "[NODE] {}|{} | n={} | v={} | p={} | vd={} | blk={}/{} | d={} | mp={} | tip={}",
                    conn_str, sync_str, net_short, ver, peers, vdaa, blocks, headers, diff, mempool, tip_short
                ));

                out.push(top.clone());
                out.push(hdr);
                out.push(sep.clone());

                for (_, line) in rows.iter() {
                    out.push(line.clone());
                }

                out.push(sep.clone());

                // If present, we also fold the feature-gated internal miner into the TOTAL row.
                // Note: Internal CPU mining doesn't produce Stratum shares; we treat accepted/submitted blocks
                // as the closest analogue for the Acc/Stl columns (same as the InternalCPU row does).
                let internal_totals: Option<(f64, i64, i64, i64, i64)> = {
                    // Feature-gated internal miner row
                    #[cfg(feature = "rkstratum_cpu_miner")]
                    {
                        let mut internal_totals: Option<(f64, i64, i64, i64, i64)> = None; // (ghs, acc, stl, inv, blocks)
                        if let Some(metrics) = RKSTRATUM_CPU_MINER_METRICS.lock().as_ref() {
                            let hashes = metrics.hashes_tried.load(Ordering::Relaxed);
                            let submitted = metrics.blocks_submitted.load(Ordering::Relaxed);
                            let accepted = metrics.blocks_accepted.load(Ordering::Relaxed);

                            // Calculate hashrate based on hash delta
                            let hashrate_ghs = if let Some(last_hashes) = last_internal_hashes {
                                let dt = now.duration_since(last_internal_sample).as_secs_f64().max(0.000_001);
                                let dh = hashes.saturating_sub(last_hashes);
                                // Hashrate as GH/s (format_hashrate expects GH/s)
                                (dh as f64 / dt) / 1e9
                            } else {
                                // First iteration: initialize but show 0 hashrate
                                0.0
                            };

                            // Update tracking variables for next iteration
                            last_internal_hashes = Some(hashes);
                            last_internal_sample = now;
                            internal_totals =
                                Some((hashrate_ghs, accepted as i64, submitted.saturating_sub(accepted) as i64, 0, accepted as i64));
                            let internal_line = format!(
                                "| {:<WORKER_W$} | {:<INST_W$} | {:>HASH_W$} | {:>DIFF_W$} | {:>SPM_W$} | {:<TRND_W$} | {:>ACC_W$} | {:>BLK_W$} | {:>TBLK_W$} | {:>TIME_W$} |",
                                "InternalCPU",
                                "-",
                                format_hashrate(hashrate_ghs),
                                "-",
                                "-",
                                "-",
                                format!("{}/{}/{}", accepted, submitted.saturating_sub(accepted), 0),
                                accepted,
                                accepted, // Total blocks (same as Blocks for InternalCPU)
                                format_uptime(now.duration_since(start))
                            );
                            out.push(internal_line);
                            out.push(sep.clone());
                        }
                        internal_totals
                    }
                    #[cfg(not(feature = "rkstratum_cpu_miner"))]
                    {
                        None
                    }
                };

                if let Some((ghs, acc, stl, inv, blocks)) = internal_totals {
                    total_rate += ghs;
                    total_shares += acc;
                    total_stales += stl;
                    total_invalids += inv;
                    total_blocks += blocks;
                    total_blocks_all_time += blocks; // Also add to all-time total for the "Total" column
                }

                let overall_spm = average_worker_spm(total_worker_spm, total_worker_count);
                let total_spm_tgt = match total_target {
                    Some(t) => format!("{:>4.1}/{:<4.1}", overall_spm, t),
                    None => format!("{:>4.1}/-", overall_spm),
                };

                out.push(format!(
                    "| {:<WORKER_W$} | {:<INST_W$} | {:>HASH_W$} | {:>DIFF_W$} | {:>SPM_W$} | {:<TRND_W$} | {:>ACC_W$} | {:>BLK_W$} | {:>TBLK_W$} | {:>TIME_W$} |",
                    "TOTAL",
                    "ALL",
                    format_hashrate(total_rate),
                    "-",
                    total_spm_tgt,
                    "-",
                    format!("{}/{}/{}", total_shares, total_stales, total_invalids),
                    total_blocks,        // Blocks from online workers only
                    total_blocks_all_time, // Total blocks from all workers (including offline)
                    format_uptime(now.duration_since(start))
                ));

                out.push(top);
                info!("{}", out.join("\n"));
            }
        });
    }

    pub fn start_vardiff_thread(&self, _expected_share_rate: u32, _log_stats: bool, _clamp: bool) {
        self.start_vardiff_thread_impl(_expected_share_rate, _log_stats, _clamp, None);
    }

    pub fn start_vardiff_thread_with_shutdown(
        &self,
        expected_share_rate: u32,
        log_stats: bool,
        clamp: bool,
        shutdown_rx: watch::Receiver<bool>,
    ) {
        self.start_vardiff_thread_impl(expected_share_rate, log_stats, clamp, Some(shutdown_rx));
    }

    fn start_vardiff_thread_impl(
        &self,
        expected_share_rate: u32,
        log_stats: bool,
        clamp: bool,
        mut shutdown_rx: Option<watch::Receiver<bool>>,
    ) {
        let stats = Arc::clone(&self.stats);
        let prefix = self.log_prefix();

        tokio::spawn(async move {
            let expected_spm = expected_share_rate.max(1) as f64;
            let mut interval = tokio::time::interval(Duration::from_secs(VAR_DIFF_THREAD_SLEEP));

            if log_stats {
                info!(
                    "{} VarDiff enabled (target={} shares/min, tick={}s, pow2_clamp={})",
                    prefix, expected_spm, VAR_DIFF_THREAD_SLEEP, clamp
                );
            } else {
                debug!(
                    "{} VarDiff thread started (target={} shares/min, tick={}s, pow2_clamp={})",
                    prefix, expected_spm, VAR_DIFF_THREAD_SLEEP, clamp
                );
            }

            loop {
                if let Some(ref mut rx) = shutdown_rx {
                    tokio::select! {
                        _ = rx.changed() => {
                            if *rx.borrow() {
                                break;
                            }
                        }
                        _ = interval.tick() => {}
                    }
                } else {
                    interval.tick().await;
                }

                let mut stats_map = stats.lock();
                let now = Instant::now();

                for (_worker_id, v) in stats_map.iter_mut() {
                    // Never lower cached difficulty while the worker is
                    // disconnected; only live sessions can provide evidence.
                    if *v.active_connections.lock() == 0 {
                        continue;
                    }
                    let start_opt = *v.var_diff_start_time.lock();
                    let Some(start) = start_opt else { continue };

                    let elapsed = now.duration_since(start).as_secs_f64().max(0.0);
                    let shares = *v.var_diff_shares_found.lock() as f64;
                    let current = *v.min_diff.lock();
                    let next_opt = vardiff_compute_next_diff(current, shares, elapsed, expected_spm, clamp);
                    let Some(next) = next_opt else { continue };

                    *v.min_diff.lock() = next;
                    *v.var_diff_start_time.lock() = Some(now);
                    *v.var_diff_shares_found.lock() = 0;
                    *v.var_diff_window.lock() = 0;

                    if log_stats {
                        let observed_spm = if elapsed > 0.0 { (shares / elapsed) * 60.0 } else { 0.0 };
                        info!(
                            "{} VarDiff: {:.1} spm (target {:.1}), shares={}, window={:.0}s, diff {:.0} -> {:.0}",
                            prefix, observed_spm, expected_spm, shares as i64, elapsed, current, next
                        );
                    }
                }
            }
        });
    }
}

fn format_hashrate(ghs: f64) -> String {
    if ghs < 1.0 {
        format!("{:.2}MH/s", ghs * 1000.0)
    } else if ghs < 1000.0 {
        format!("{:.2}GH/s", ghs)
    } else {
        format!("{:.2}TH/s", ghs / 1000.0)
    }
}

// Trait for kaspa API operations
#[async_trait::async_trait]
pub trait KaspaApiTrait: Send + Sync {
    async fn get_block_template(
        &self,
        wallet_addr: &str,
        remote_app: &str,
        canxium_addr: &str,
        session_uid: u64,
        generation: u64,
        // `kas_payout`: the lane's own `kaspa:` payout from the stratum password;
        // `None` pays the pool. `lane_id`: selects the lane's pool-fee minute.
        kas_payout: Option<kaspa_addresses::Address>,
        lane_id: u64,
    ) -> Result<Block, Box<dyn std::error::Error + Send + Sync>>;

    async fn submit_block(
        &self,
        block: Block,
    ) -> Result<crate::kaspaapi::BlockSubmitOutcome, Box<dyn std::error::Error + Send + Sync>>;

    /// Submit the independent Kaspa-parent leg before attempting to claim the
    /// committed ZKAS block. The default keeps non-merged mocks and adapters
    /// source-compatible.
    async fn submit_merged_parent_if_solved(&self, _parent: &Block) -> crate::kaspaapi::MergedParentSubmitOutcome {
        crate::kaspaapi::MergedParentSubmitOutcome::NotMerged
    }

    /// Get balances by addresses (for Prometheus metrics)
    /// Get balances for addresses
    async fn get_balances_by_addresses(
        &self,
        addresses: &[String],
    ) -> Result<Vec<(String, u64)>, Box<dyn std::error::Error + Send + Sync>>;

    async fn get_current_block_color(&self, block_hash: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>;

    /// Real merged mining: the ZKas (easier) block-found target for a given parent,
    /// or `None` when not merged / the parent is unknown (caller then uses the parent's
    /// own `header.bits`). Default `None` keeps non-KaspaApi impls (mocks) unaffected.
    /// Whether this lane's Kaspa parent paid the pool (fee minute). Mocks say no.
    fn merged_lane_paid_pool(&self, _parent_block: &Block) -> bool {
        false
    }

    fn merged_fc_target(&self, _parent_block: &Block) -> Option<num_bigint::BigUint> {
        None
    }

    /// The hash of the block that actually lands on the **ZKas chain** for a
    /// solved job. In merged mode the ASIC grinds a *parent* (Kaspa-shaped)
    /// block whose coinbase commits to the ZKas block hash `H_fc`; the parent
    /// header's own hash never exists on the ZKas chain, so confirming /
    /// displaying it always fails (the live "0 blocks / never confirmed blue"
    /// bug). `None` ⇒ not merged: the job's own header hash IS the chain hash.
    fn merged_chain_hash(&self, _parent_block: &Block) -> Option<kaspa_hashes::Hash> {
        None
    }

    /// Atomically claim a network-target solution. In merged mode this is
    /// keyed by `H_fc`; the first caller returns true and later AuxPoW proofs
    /// for the same ZKAS block return false.
    fn claim_network_solution(&self, _job_block: &Block) -> bool {
        true
    }

    async fn refresh_merged_parent(
        &self,
        _current_parent: &Block,
        _payee: Option<kaspa_addresses::Address>,
    ) -> Result<Option<Block>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(None)
    }
}

#[cfg(test)]
mod merged_settlement_order_tests {
    use super::{KaspaApiTrait, submit_parent_then_claim_zkas};
    use crate::kaspaapi::{BlockSubmitOutcome, MergedParentSubmitOutcome};
    use kaspa_consensus_core::{block::Block, header::Header};
    use kaspa_hashes::Hash;
    use parking_lot::Mutex;

    struct AlreadyClaimedApi {
        calls: Mutex<Vec<&'static str>>,
    }

    #[async_trait::async_trait]
    impl KaspaApiTrait for AlreadyClaimedApi {
        async fn get_block_template(
            &self,
            _wallet_addr: &str,
            _remote_app: &str,
            _canxium_addr: &str,
            _session_uid: u64,
            _generation: u64,
            _kas_payout: Option<kaspa_addresses::Address>,
            _lane_id: u64,
        ) -> Result<Block, Box<dyn std::error::Error + Send + Sync>> {
            unreachable!("template fetch is outside this regression")
        }

        async fn submit_block(&self, _block: Block) -> Result<BlockSubmitOutcome, Box<dyn std::error::Error + Send + Sync>> {
            unreachable!("ZKAS submission must not run after a failed claim")
        }

        async fn submit_merged_parent_if_solved(&self, _parent: &Block) -> MergedParentSubmitOutcome {
            self.calls.lock().push("parent");
            MergedParentSubmitOutcome::Accepted
        }

        fn claim_network_solution(&self, _job_block: &Block) -> bool {
            self.calls.lock().push("claim");
            false
        }

        async fn get_balances_by_addresses(
            &self,
            _addresses: &[String],
        ) -> Result<Vec<(String, u64)>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(vec![])
        }

        async fn get_current_block_color(&self, _block_hash: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
            Ok(false)
        }
    }

    #[tokio::test]
    async fn late_kaspa_parent_is_submitted_before_failed_zkas_claim() {
        let parent = Block::new(Header::from_precomputed_hash(Hash::from_bytes([7; 32]), vec![]), vec![]);
        let api = AlreadyClaimedApi { calls: Mutex::new(vec![]) };

        let (outcome, claimed_zkas) = submit_parent_then_claim_zkas(&api, &parent, &parent, true).await;

        assert_eq!(outcome, MergedParentSubmitOutcome::Accepted);
        assert!(!claimed_zkas, "fixture represents an H_fc already claimed by an earlier nonce");
        assert_eq!(api.calls.lock().as_slice(), ["parent", "claim"], "Kaspa submission must never be gated behind ZKAS deduplication");
    }

    /// The crossover regression: once ZKas difficulty passes Kaspa's, a nonce can
    /// clear the Kaspa parent while failing the ZKas target. Its KAS block must
    /// still be submitted, and it must NOT consume the ZKas H_fc claim — burning
    /// that would stop a later, genuinely ZKas-clearing nonce from ever minting it.
    #[tokio::test]
    async fn kas_only_solution_never_claims_zkas() {
        let parent = Block::new(Header::from_precomputed_hash(Hash::from_bytes([8; 32]), vec![]), vec![]);
        let api = AlreadyClaimedApi { calls: Mutex::new(vec![]) };

        let (outcome, claimed_zkas) = submit_parent_then_claim_zkas(&api, &parent, &parent, false).await;

        assert_eq!(outcome, MergedParentSubmitOutcome::Accepted);
        assert!(!claimed_zkas);
        assert_eq!(api.calls.lock().as_slice(), ["parent"], "KAS-only work must not consume the ZKAS H_fc claim");
    }
}

#[cfg(test)]
mod retention_tests {
    use super::*;
    use crate::mining_state::MiningState;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn test_ctx() -> Arc<StratumContext> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let accept_handle = tokio::spawn(async move { listener.accept().await });
            let _stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let (accepted_stream, _) = accept_handle.await.unwrap().unwrap();
            let state = Arc::new(MiningState::new());
            let (tx, _rx) = mpsc::unbounded_channel();
            StratumContext::new("127.0.0.1".to_string(), 12345, 0, accepted_stream, state, tx)
        })
    }

    fn identify(ctx: &StratumContext, wallet: &str, worker: &str) {
        *ctx.wallet_addr.lock() = wallet.to_string();
        ctx.set_authorized_worker_name(worker.to_string());
    }

    #[test]
    fn set_client_vardiff_does_not_recreate_pruned_stats() {
        let handler = ShareHandler::new("test-instance".to_string());
        let ctx = test_ctx();
        identify(&ctx, "kaspatest:ghost", "ghost");

        handler.get_create_stats(&ctx);
        assert_eq!(handler.stats.lock().len(), 1);

        handler.stats.lock().clear();
        assert!(handler.stats.lock().is_empty());

        let previous = handler.set_client_vardiff(&ctx, 512.0);
        assert_eq!(previous, 0.0, "vardiff should fall back to mining-state diff when stats were pruned");
        assert!(handler.stats.lock().is_empty(), "job/vardiff paths must not recreate pruned stats");

        handler.get_create_stats(&ctx);
        assert_eq!(handler.stats.lock().len(), 1, "authorize/submit lifecycle may recreate stats");
    }

    #[test]
    fn reconnect_restores_wallet_worker_difficulty() {
        let handler = ShareHandler::new("test-instance".to_string());
        let first = test_ctx();
        identify(&first, "kaspatest:wallet-a", "rig");
        handler.activate_client_vardiff(&first);
        assert_eq!(handler.register_client_vardiff(&first, 8192.0), 8192.0);
        handler.set_client_vardiff(&first, 256.0);
        handler.deactivate_client_vardiff(&first);

        let reconnect = test_ctx();
        identify(&reconnect, "kaspatest:wallet-a", "rig");
        handler.activate_client_vardiff(&reconnect);
        assert_eq!(handler.register_client_vardiff(&reconnect, 8192.0), 256.0);
    }

    #[test]
    fn same_worker_name_on_another_wallet_has_independent_vardiff() {
        let handler = ShareHandler::new("test-instance".to_string());
        let wallet_a = test_ctx();
        identify(&wallet_a, "kaspatest:wallet-a", "rig");
        handler.activate_client_vardiff(&wallet_a);
        handler.register_client_vardiff(&wallet_a, 8192.0);
        handler.set_client_vardiff(&wallet_a, 256.0);

        let wallet_b = test_ctx();
        identify(&wallet_b, "kaspatest:wallet-b", "rig");
        handler.activate_client_vardiff(&wallet_b);
        assert_eq!(handler.register_client_vardiff(&wallet_b, 8192.0), 8192.0);
        assert_eq!(handler.stats.lock().len(), 2);
    }

    #[test]
    fn generated_display_worker_does_not_break_reconnect_restore() {
        let handler = ShareHandler::new("test-instance".to_string());
        let first = test_ctx();
        first.set_id(1);
        identify(&first, "kaspatest:wallet-a", "");
        handler.activate_client_vardiff(&first);
        handler.register_client_vardiff(&first, 8192.0);
        handler.set_client_vardiff(&first, 128.0);
        handler.deactivate_client_vardiff(&first);

        let reconnect = test_ctx();
        reconnect.set_id(2);
        identify(&reconnect, "kaspatest:wallet-a", "");
        assert_ne!(first.effective_worker_name(), reconnect.effective_worker_name());
        handler.activate_client_vardiff(&reconnect);
        assert_eq!(handler.register_client_vardiff(&reconnect, 8192.0), 128.0);
    }

    #[test]
    fn active_stats_survive_ttl_and_inactive_stats_expire() {
        let stats = WorkStats::new("rig".to_string());
        let now = Instant::now();
        *stats.last_session_activity.lock() = now - INACTIVE_VARDIFF_TTL - Duration::from_secs(1);

        *stats.active_connections.lock() = 1;
        assert!(should_retain_vardiff_stats(&stats, now));

        *stats.active_connections.lock() = 0;
        assert!(!should_retain_vardiff_stats(&stats, now));
    }

    #[test]
    fn zero_share_bootstrap_descends_quickly_but_stops_at_ks0_floor() {
        assert_eq!(vardiff_compute_next_diff(8192.0, 0.0, 15.0, 20.0, true), Some(4096.0));
        assert_eq!(vardiff_compute_next_diff(128.0, 0.0, 15.0, 20.0, true), Some(64.0));
        assert_eq!(vardiff_compute_next_diff(64.0, 0.0, 15.0, 20.0, true), None);
        assert_eq!(vardiff_compute_next_diff(8192.0, 1.0, 89.0, 20.0, true), None);
        assert_eq!(vardiff_compute_next_diff(8192.0, 1.0, 90.0, 20.0, true), Some(4096.0));
    }
}

#[cfg(test)]
mod event_bus_tests {
    //! Tests for the [`ShareHandler::with_event_bus`] event-emission
    //! path. We deliberately exercise the lowest-level surface — the
    //! `emit` method via `build_share_rejected` — rather than the whole
    //! `handle_submit` flow, which requires a live stratum context and
    //! a kaspad mock. End-to-end coverage lands when we run against
    //! testnet-10 in the Phase 1 close-out milestone.

    use super::*;
    use tokio::sync::broadcast;

    const SAMPLE_HASH: &str = "06acc7179752e80fa4ef421f3dd7ff5b5bda006e3fc76c14f33f324079a3a9e2";
    const SAMPLE_WALLET: &str = "kaspa:qz4j8mu269z8llgcczmfukm9fan2fq822kzxu4cfukd5fq";

    fn handler_with_bus() -> (ShareHandler, broadcast::Receiver<PoolEvent>) {
        let (tx, rx) = broadcast::channel::<PoolEvent>(POOL_EVENT_CHANNEL_CAPACITY);
        let h = ShareHandler::new("test-0".to_string()).with_event_bus(tx);
        (h, rx)
    }

    fn sample_block_accepted() -> PoolEvent {
        PoolEvent::BlockAccepted {
            hash: DomainBlockHash::from_hex(SAMPLE_HASH).expect("valid hex"),
            ts: chrono::Utc::now(),
            correlation_id: CorrelationId::new_v4(),
        }
    }

    #[tokio::test]
    async fn emit_is_noop_when_no_bus_attached() {
        let h = ShareHandler::new("test-1".to_string());
        h.emit(sample_block_accepted());
    }

    #[tokio::test]
    async fn emit_delivers_to_attached_bus() {
        let (h, mut rx) = handler_with_bus();
        let cid = CorrelationId::new_v4();
        let hash = DomainBlockHash::from_hex(SAMPLE_HASH).expect("valid hex");
        h.emit(PoolEvent::BlockAccepted { hash, ts: chrono::Utc::now(), correlation_id: cid });
        let got = rx.recv().await.expect("event received");
        match got {
            PoolEvent::BlockAccepted { hash: got_hash, correlation_id: got_cid, .. } => {
                assert_eq!(got_hash, hash);
                assert_eq!(got_cid, cid);
            }
            other => panic!("unexpected event variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn emit_drops_event_when_no_receivers() {
        let (tx, rx) = broadcast::channel::<PoolEvent>(4);
        let h = ShareHandler::new("test-2".to_string()).with_event_bus(tx);
        drop(rx);
        h.emit(sample_block_accepted());
    }

    #[test]
    fn build_share_rejected_validates_wallet_and_worker() {
        assert!(
            ShareHandler::build_share_rejected("not-a-wallet", "rig-01", ShareRejectReason::Stale, CorrelationId::new_v4()).is_none()
        );
        assert!(ShareHandler::build_share_rejected(SAMPLE_WALLET, "", ShareRejectReason::Stale, CorrelationId::new_v4()).is_none());
    }

    #[test]
    fn build_share_rejected_builds_event_when_valid() {
        let cid = CorrelationId::new_v4();
        let ev =
            ShareHandler::build_share_rejected(SAMPLE_WALLET, "rig-01", ShareRejectReason::LowDifficulty, cid).expect("event built");
        match ev {
            PoolEvent::ShareRejected { reason, correlation_id, .. } => {
                assert_eq!(reason, ShareRejectReason::LowDifficulty);
                assert_eq!(correlation_id, cid);
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn slow_receiver_observes_lagged_not_panic() {
        let (tx, mut rx) = broadcast::channel::<PoolEvent>(2);
        let h = ShareHandler::new("test-3".to_string()).with_event_bus(tx);
        let hash = DomainBlockHash::from_hex(SAMPLE_HASH).expect("valid hex");
        for _ in 0..6 {
            h.emit(PoolEvent::BlockAccepted { hash, ts: chrono::Utc::now(), correlation_id: CorrelationId::new_v4() });
        }
        match rx.recv().await {
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                assert!(skipped > 0);
            }
            other => panic!("expected Lagged, got {other:?}"),
        }
    }
}
