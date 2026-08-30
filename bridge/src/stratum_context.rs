use crate::jsonrpc_event::{JsonRpcEvent, JsonRpcResponse};
use crate::log_colors::LogColors;
use kaspa_addresses::Address;
use hex;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// Monotonic source of process-unique connection ids. Decoupled from the
/// mutable, reusable stratum client `id` (which defaults to 0 and keys the
/// in-memory client map); this is a stable correlation key for the
/// session-lifecycle events (`SessionOpened`/`SessionClosed`).
static NEXT_SESSION_UID: AtomicU64 = AtomicU64::new(1);

/// Error for disconnected clients
#[derive(Debug, thiserror::Error)]
#[error("disconnecting")]
pub struct ErrorDisconnected;

/// Context summary for logging
#[derive(Debug, Clone)]
pub struct ContextSummary {
    pub remote_addr: String,
    pub remote_port: u16,
    pub wallet_addr: String,
    pub worker_name: String,
    pub remote_app: String,
}

/// Stratum client context
pub struct StratumContext {
    pub remote_addr: String,
    pub remote_port: u16,
    /// Local (listening) port the connection arrived on. Used to select
    /// the per-port starting-difficulty seed (ADR-0022). With the fly
    /// edge in front, HAProxy maps each public port 1:1 to the same
    /// origin port, so this is the port the miner dialed.
    pub local_port: u16,
    pub wallet_addr: Arc<Mutex<String>>,
    pub worker_name: Arc<Mutex<String>>,
    worker_name_supplied: Arc<AtomicBool>,
    pub canxium_addr: Arc<Mutex<String>>,
    /// The `kaspa:` address this connection's merged-mining KAS rewards pay to,
    /// taken from the stratum PASSWORD field at authorize (`params[1]`).
    ///
    /// `None` means "pay the pool" — either the miner supplied nothing, or what it
    /// supplied did not parse. Parsed exactly once at authorize and stored as an
    /// `Address`, never re-parsed per job: a job is built roughly once a second per
    /// connection and bech32 decoding on that path would be pure waste.
    ///
    /// This does NOT affect ZKas rewards, which always pay `wallet_addr`.
    pub kas_payout: Arc<Mutex<Option<Address>>>,
    pub remote_app: Arc<Mutex<String>>,
    pub id: Arc<Mutex<i32>>,
    pub extranonce: Arc<Mutex<String>>,
    pub state: Arc<crate::mining_state::MiningState>,
    /// Process-unique connection id correlating this connection's
    /// `SessionOpened`/`SessionClosed` lifecycle events.
    session_uid: u64,
    /// Monotonic ZKAS-template generation for this connection. Together with
    /// `session_uid`, this is embedded in coinbase extra-data so two solo
    /// miners paying the same wallet never share an `H_fc`.
    template_generation: Arc<AtomicU64>,
    /// Serialize full-template and parent-only refreshes for this client.
    job_build_lock: Arc<tokio::sync::Mutex<()>>,
    parent_refresh_gate: Arc<tokio::sync::Semaphore>,
    /// Last parent-only job actually published to this connection. Kaspa can
    /// produce parent templates faster than some ASIC firmware can switch
    /// work, so parent notifications are coalesced per connection.
    last_parent_job_at: Arc<Mutex<Option<Instant>>>,
    /// Authorization time and last successfully published job. These are used
    /// only by the first-job watchdog; a lack of shares after authorization is
    /// not itself considered failure because low-hashrate miners may be idle.
    authorized_at: Arc<Mutex<Option<Instant>>>,
    last_job_sent_at: Arc<Mutex<Option<Instant>>>,
    /// One-shot guard so `SessionOpened` is emitted at most once per
    /// connection even if the miner re-authorizes.
    session_opened: AtomicBool,
    vardiff_registered: Arc<AtomicBool>,
    disconnecting: Arc<AtomicBool>,
    write_lock: Arc<AtomicBool>,
    read_half: Arc<Mutex<Option<tokio::io::ReadHalf<TcpStream>>>>,
    write_half: Arc<Mutex<Option<tokio::io::WriteHalf<TcpStream>>>>,
    on_disconnect: mpsc::UnboundedSender<Arc<StratumContext>>,
}

impl StratumContext {
    pub fn new(
        remote_addr: String,
        remote_port: u16,
        local_port: u16,
        stream: TcpStream,
        state: Arc<crate::mining_state::MiningState>,
        on_disconnect: mpsc::UnboundedSender<Arc<StratumContext>>,
    ) -> Arc<Self> {
        let (read_half, write_half) = tokio::io::split(stream);
        Arc::new(Self {
            remote_addr,
            remote_port,
            local_port,
            wallet_addr: Arc::new(Mutex::new(String::new())),
            worker_name: Arc::new(Mutex::new(String::new())),
            worker_name_supplied: Arc::new(AtomicBool::new(false)),
            canxium_addr: Arc::new(Mutex::new(String::new())),
            kas_payout: Arc::new(Mutex::new(None)),
            remote_app: Arc::new(Mutex::new(String::new())),
            id: Arc::new(Mutex::new(0)),
            extranonce: Arc::new(Mutex::new(String::new())),
            state,
            session_uid: NEXT_SESSION_UID.fetch_add(1, Ordering::Relaxed),
            template_generation: Arc::new(AtomicU64::new(0)),
            job_build_lock: Arc::new(tokio::sync::Mutex::new(())),
            parent_refresh_gate: Arc::new(tokio::sync::Semaphore::new(1)),
            last_parent_job_at: Arc::new(Mutex::new(None)),
            authorized_at: Arc::new(Mutex::new(None)),
            last_job_sent_at: Arc::new(Mutex::new(None)),
            session_opened: AtomicBool::new(false),
            vardiff_registered: Arc::new(AtomicBool::new(false)),
            disconnecting: Arc::new(AtomicBool::new(false)),
            write_lock: Arc::new(AtomicBool::new(false)),
            read_half: Arc::new(Mutex::new(Some(read_half))),
            write_half: Arc::new(Mutex::new(Some(write_half))),
            on_disconnect,
        })
    }

    /// Check if client is connected
    pub fn connected(&self) -> bool {
        !self.disconnecting.load(Ordering::Acquire)
    }

    /// Ask compatible Stratum V1 clients to reconnect to an approved failover
    /// endpoint. This is only a hint; clients that do not implement
    /// `client.reconnect` will simply be disconnected by the caller.
    pub async fn send_reconnect_hint(&self, host: &str, port: u16, wait_secs: u64) -> Result<(), ErrorDisconnected> {
        let event = JsonRpcEvent {
            jsonrpc: "2.0".to_string(),
            method: "client.reconnect".to_string(),
            id: None,
            params: vec![
                serde_json::Value::String(host.to_string()),
                serde_json::Value::Number(port.into()),
                serde_json::Value::Number(wait_secs.into()),
            ],
        };
        self.send(event).await.map_err(|_| ErrorDisconnected)
    }

    /// Get client ID
    pub fn id(&self) -> Option<i32> {
        let id = *self.id.lock();
        if id > 0 { Some(id) } else { None }
    }

    /// Set client ID
    pub fn set_id(&self, id: i32) {
        *self.id.lock() = id;
    }

    /// Process-unique connection id for session-lifecycle correlation.
    pub fn session_uid(&self) -> u64 {
        self.session_uid
    }

    /// Allocate the next ZKAS-template generation for this connection.
    pub fn next_template_generation(&self) -> u64 {
        self.template_generation.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Serialize job construction and publication for this connection.
    pub async fn lock_job_build(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.job_build_lock).lock_owned().await
    }

    pub fn try_lock_job_build(&self) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        Arc::clone(&self.job_build_lock).try_lock_owned().ok()
    }

    pub fn try_parent_refresh(&self, minimum_interval: Duration) -> Option<tokio::sync::OwnedSemaphorePermit> {
        if self.last_parent_job_at.lock().as_ref().is_some_and(|at| at.elapsed() < minimum_interval) {
            return None;
        }
        let permit = Arc::clone(&self.parent_refresh_gate).try_acquire_owned().ok()?;
        // Recheck after owning the gate so two callbacks that raced before
        // acquisition cannot both publish within the interval.
        if self.last_parent_job_at.lock().as_ref().is_some_and(|at| at.elapsed() < minimum_interval) {
            return None;
        }
        Some(permit)
    }

    pub fn mark_parent_job_sent(&self) {
        *self.last_parent_job_at.lock() = Some(Instant::now());
    }

    /// Start a new authorization epoch. Re-authorization must require a fresh
    /// job, otherwise a stale job from an earlier identity could satisfy the
    /// watchdog.
    pub fn mark_authorized(&self) {
        *self.authorized_at.lock() = Some(Instant::now());
        *self.last_job_sent_at.lock() = None;
    }

    /// Record a successful `mining.notify` publication.
    pub fn mark_job_sent(&self) {
        *self.last_job_sent_at.lock() = Some(Instant::now());
    }

    pub fn received_job_since_authorization(&self) -> bool {
        let authorized_at = *self.authorized_at.lock();
        let job_at = *self.last_job_sent_at.lock();
        matches!((authorized_at, job_at), (Some(a), Some(j)) if j >= a)
    }

    /// Claim the one-shot "session opened" flag. Returns `true` exactly
    /// once per connection (the caller that should emit `SessionOpened`);
    /// subsequent calls return `false`.
    pub fn claim_session_open(&self) -> bool {
        !self.session_opened.swap(true, Ordering::SeqCst)
    }

    pub fn claim_vardiff_registration(&self) -> bool {
        !self.vardiff_registered.swap(true, Ordering::SeqCst)
    }

    pub fn release_vardiff_registration(&self) -> bool {
        self.vardiff_registered.swap(false, Ordering::SeqCst)
    }

    /// Get context summary
    pub fn summary(&self) -> ContextSummary {
        ContextSummary {
            remote_addr: self.remote_addr.clone(),
            remote_port: self.remote_port,
            wallet_addr: self.wallet_addr.lock().clone(),
            worker_name: self.worker_name.lock().clone(),
            remote_app: self.remote_app.lock().clone(),
        }
    }

    /// Get remote address string
    pub fn remote_addr(&self) -> &str {
        &self.remote_addr
    }

    /// Get remote port
    pub fn remote_port(&self) -> u16 {
        self.remote_port
    }

    /// Assign a stable display label when the miner omits `wallet.worker` in authorize.
    ///
    /// Uses connection id (`asic-3`), not IP, so Prometheus/dashboard metrics stay non-empty and
    /// do not leak addresses.
    pub fn ensure_default_worker_name(&self) {
        let mut name = self.worker_name.lock();
        if !name.trim().is_empty() {
            return;
        }
        *name = if let Some(id) = self.id() { format!("asic-{}", id) } else { format!("asic-{}", self.remote_port) };
    }

    /// Store the worker parsed from an authorize request while remembering
    /// whether it was miner-supplied or merely a per-connection display name.
    pub fn set_authorized_worker_name(&self, worker_name: String) {
        self.worker_name_supplied.store(!worker_name.trim().is_empty(), Ordering::SeqCst);
        *self.worker_name.lock() = worker_name;
        self.ensure_default_worker_name();
    }

    /// Stable worker component for reconnect state. Generated `asic-{id}`
    /// labels are intentionally excluded because the id changes on reconnect.
    pub fn vardiff_worker_identity(&self) -> String {
        if self.worker_name_supplied.load(Ordering::SeqCst) { self.worker_name.lock().clone() } else { String::new() }
    }

    /// Worker label for stats, Prometheus, and dashboard (never empty after authorize).
    pub fn effective_worker_name(&self) -> String {
        self.ensure_default_worker_name();
        self.worker_name.lock().clone()
    }

    /// Send a JSON-RPC response
    pub async fn reply(&self, response: JsonRpcResponse) -> Result<(), ErrorDisconnected> {
        if self.disconnecting.load(Ordering::Acquire) {
            return Err(ErrorDisconnected);
        }

        let json = serde_json::to_string(&response).map_err(|_| ErrorDisconnected)?;
        let data = format!("{}\n", json);

        // Get client context for detailed logging
        let wallet_addr = self.wallet_addr.lock().clone();
        let worker_name = self.worker_name.lock().clone();
        let remote_app = self.remote_app.lock().clone();

        // Log outgoing response at DEBUG level (detailed logs moved to debug)
        tracing::debug!("{}", LogColors::bridge_to_asic("========================================"));
        tracing::debug!("{}", LogColors::bridge_to_asic("===== SENDING RESPONSE TO ASIC ===== "));
        tracing::debug!("{}", LogColors::bridge_to_asic("========================================"));
        tracing::debug!("{} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("Client Information:"));
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - IP Address:"),
            format!("{}:{}", self.remote_addr, self.remote_port)
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Wallet Address:"),
            format!("'{}'", wallet_addr)
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Worker Name:"),
            format!("'{}'", worker_name)
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Miner Application:"),
            format!("'{}'", remote_app)
        );
        tracing::debug!("{} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("Response Details:"));
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Response ID:"),
            format!("{:?}", response.id)
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Response Type:"),
            "JSON-RPC Response"
        );
        if let Some(ref result) = response.result {
            let result_str = serde_json::to_string(result).unwrap_or_else(|_| "N/A".to_string());
            tracing::debug!("{} {} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("  - Result:"), result_str);
            tracing::debug!(
                "{} {} {}",
                LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
                LogColors::label("  - Result Length:"),
                format!("{} characters", result_str.len())
            );
        }
        if let Some(ref error) = response.error {
            let error_str = serde_json::to_string(error).unwrap_or_else(|_| "N/A".to_string());
            tracing::debug!("{} {} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::error("  - Error:"), error_str);
            tracing::debug!(
                "{} {} {}",
                LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
                LogColors::label("  - Error Length:"),
                format!("{} characters", error_str.len())
            );
        }
        tracing::debug!("{} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("Message Data:"));
        tracing::debug!("{} {} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("  - Raw JSON:"), json);
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - JSON Length:"),
            format!("{} characters", json.len())
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Total Bytes (with newline):"),
            format!("{} bytes", data.len())
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Raw Bytes (hex):"),
            hex::encode(data.as_bytes())
        );
        tracing::debug!("{}", LogColors::bridge_to_asic("========================================"));

        self.write_data(data.as_bytes()).await?;
        Ok(())
    }

    /// Send a JSON-RPC event
    pub async fn send(&self, event: JsonRpcEvent) -> Result<(), ErrorDisconnected> {
        if self.disconnecting.load(Ordering::Acquire) {
            return Err(ErrorDisconnected);
        }

        let json = serde_json::to_string(&event).map_err(|_| ErrorDisconnected)?;
        let data = format!("{}\n", json);

        // Get client context for detailed logging
        let wallet_addr = self.wallet_addr.lock().clone();
        let worker_name = self.worker_name.lock().clone();
        let remote_app = self.remote_app.lock().clone();
        let params_str = serde_json::to_string(&event.params).unwrap_or_else(|_| "[]".to_string());

        // Log outgoing event at DEBUG level (detailed logs moved to debug)
        tracing::debug!("{}", LogColors::bridge_to_asic("========================================"));
        tracing::debug!("{}", LogColors::bridge_to_asic("===== SENDING EVENT TO ASIC ===== "));
        tracing::debug!("{}", LogColors::bridge_to_asic("========================================"));
        tracing::debug!("{} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("Client Information:"));
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - IP Address:"),
            format!("{}:{}", self.remote_addr, self.remote_port)
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Wallet Address:"),
            format!("'{}'", wallet_addr)
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Worker Name:"),
            format!("'{}'", worker_name)
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Miner Application:"),
            format!("'{}'", remote_app)
        );
        tracing::debug!("{} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("Event Details:"));
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Method:"),
            format!("'{}'", event.method)
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Event ID:"),
            format!("{:?}", event.id)
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - JSON-RPC Version:"),
            format!("'{}'", event.jsonrpc)
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Format:"),
            "Standard JSON-RPC (with jsonrpc field)"
        );
        tracing::debug!("{} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("Parameters:"));
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Params Count:"),
            event.params.len()
        );
        tracing::debug!("{} {} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("  - Params JSON:"), params_str);
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Params Length:"),
            format!("{} characters", params_str.len())
        );
        // Log each param individually
        for (idx, param) in event.params.iter().enumerate() {
            let param_str = serde_json::to_string(param).unwrap_or_else(|_| "N/A".to_string());
            let param_type = if param.is_string() {
                "String".to_string()
            } else if param.is_number() {
                "Number".to_string()
            } else if param.is_array() {
                "Array".to_string()
            } else if param.is_object() {
                "Object".to_string()
            } else if param.is_boolean() {
                "Boolean".to_string()
            } else {
                "Null".to_string()
            };
            tracing::debug!(
                "{} {} {}",
                LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
                LogColors::label(&format!("  - Param[{}]:", idx)),
                format!("{} (type: {})", param_str, param_type)
            );
        }
        tracing::debug!("{} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("Message Data:"));
        tracing::debug!("{} {} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("  - Raw JSON:"), json);
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - JSON Length:"),
            format!("{} characters", json.len())
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Total Bytes (with newline):"),
            format!("{} bytes", data.len())
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Raw Bytes (hex):"),
            hex::encode(data.as_bytes())
        );
        tracing::debug!("{}", LogColors::bridge_to_asic("========================================"));

        self.write_data(data.as_bytes()).await?;
        Ok(())
    }

    /// Send a minimal Stratum notification (method + params only, no id or jsonrpc)
    /// This matches the format used by the stratum crate and expected by IceRiver ASICs
    pub async fn send_notification(&self, method: &str, params: Vec<serde_json::Value>) -> Result<(), ErrorDisconnected> {
        if self.disconnecting.load(Ordering::Acquire) {
            return Err(ErrorDisconnected);
        }

        // Manually construct JSON without id or jsonrpc fields (matches StratumNotification format)
        let notification = serde_json::json!({
            "method": method,
            "params": params
        });

        let json = serde_json::to_string(&notification).map_err(|_| ErrorDisconnected)?;
        let data = format!("{}\n", json);

        // Get client context for detailed logging
        let wallet_addr = self.wallet_addr.lock().clone();
        let worker_name = self.worker_name.lock().clone();
        let remote_app = self.remote_app.lock().clone();
        let params_str = serde_json::to_string(&params).unwrap_or_else(|_| "[]".to_string());

        // Log outgoing notification at DEBUG level (detailed logs moved to debug)
        tracing::debug!("{}", LogColors::bridge_to_asic("========================================"));
        tracing::debug!("{}", LogColors::bridge_to_asic("===== SENDING NOTIFICATION TO ASIC ===== "));
        tracing::debug!("{}", LogColors::bridge_to_asic("========================================"));
        tracing::debug!("{} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("Client Information:"));
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - IP Address:"),
            format!("{}:{}", self.remote_addr, self.remote_port)
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Wallet Address:"),
            format!("'{}'", wallet_addr)
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Worker Name:"),
            format!("'{}'", worker_name)
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Miner Application:"),
            format!("'{}'", remote_app)
        );
        tracing::debug!("{} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("Notification Details:"));
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Method:"),
            format!("'{}'", method)
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Format:"),
            "Minimal Stratum (no id/jsonrpc fields)"
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Target:"),
            "IceRiver/BzMiner compatible"
        );
        tracing::debug!("{} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("Parameters:"));
        tracing::debug!("{} {} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("  - Params Count:"), params.len());
        tracing::debug!("{} {} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("  - Params JSON:"), params_str);
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Params Length:"),
            format!("{} characters", params_str.len())
        );
        // Log each param individually
        for (idx, param) in params.iter().enumerate() {
            let param_str = serde_json::to_string(param).unwrap_or_else(|_| "N/A".to_string());
            let param_type = if param.is_string() {
                format!("String (length: {})", param.as_str().map(|s| s.len()).unwrap_or(0))
            } else if param.is_number() {
                "Number".to_string()
            } else if param.is_array() {
                format!("Array (length: {})", param.as_array().map(|a| a.len()).unwrap_or(0))
            } else if param.is_object() {
                "Object".to_string()
            } else if param.is_boolean() {
                "Boolean".to_string()
            } else {
                "Null".to_string()
            };
            tracing::debug!(
                "{} {} {}",
                LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
                LogColors::label(&format!("  - Param[{}]:", idx)),
                format!("{} (type: {})", param_str, param_type)
            );
        }
        tracing::debug!("{} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("Message Data:"));
        tracing::debug!("{} {} {}", LogColors::bridge_to_asic("[BRIDGE->ASIC]"), LogColors::label("  - Raw JSON:"), json);
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - JSON Length:"),
            format!("{} characters", json.len())
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Total Bytes (with newline):"),
            format!("{} bytes", data.len())
        );
        tracing::debug!(
            "{} {} {}",
            LogColors::bridge_to_asic("[BRIDGE->ASIC]"),
            LogColors::label("  - Raw Bytes (hex):"),
            hex::encode(data.as_bytes())
        );
        tracing::debug!("{}", LogColors::bridge_to_asic("========================================"));

        self.write_data(data.as_bytes()).await?;
        Ok(())
    }

    /// Send a Stratum v1 server notification exactly as used by the major
    /// Kaspa pools: explicit null id, no `jsonrpc` member.
    pub async fn send_v1_notification(&self, method: &str, params: Vec<serde_json::Value>) -> Result<(), ErrorDisconnected> {
        if self.disconnecting.load(Ordering::Acquire) {
            return Err(ErrorDisconnected);
        }
        let notification = serde_json::json!({
            "id": serde_json::Value::Null,
            "method": method,
            "params": params,
        });
        let data = format!("{}\n", serde_json::to_string(&notification).map_err(|_| ErrorDisconnected)?);
        self.write_data(data.as_bytes()).await
    }

    /// Write data to the connection with backoff
    async fn write_data(&self, data: &[u8]) -> Result<(), ErrorDisconnected> {
        // Check if already disconnected
        if self.disconnecting.load(Ordering::Acquire) {
            return Err(ErrorDisconnected);
        }

        for attempt in 0..3 {
            if self.write_lock.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                // Extract write half (drop guard before await)
                let write_half_opt = {
                    let mut write_guard = self.write_half.lock();
                    write_guard.take()
                };

                let result = if let Some(mut write_half) = write_half_opt {
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

                    // Try to write directly (no need to wait for writable)
                    let write_result = tokio::time::timeout_at(deadline, write_half.write_all(data)).await;

                    // Put write half back regardless of result
                    {
                        let mut write_guard = self.write_half.lock();
                        *write_guard = Some(write_half);
                    }

                    write_result
                } else {
                    self.write_lock.store(false, Ordering::Release);
                    return Err(ErrorDisconnected);
                };

                self.write_lock.store(false, Ordering::Release);

                match result {
                    Ok(Ok(_)) => return Ok(()),
                    Ok(Err(e)) => {
                        tracing::warn!("Write error: {}", e);
                        self.check_disconnect();
                        return Err(ErrorDisconnected);
                    }
                    Err(_) => {
                        // Timeout on write - try again if we have attempts left
                        if attempt < 2 {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            continue;
                        } else {
                            self.check_disconnect();
                            return Err(ErrorDisconnected);
                        }
                    }
                }
            } else {
                // Write blocked - wait and retry
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        Err(ErrorDisconnected)
    }

    /// Reply with stale share error
    pub async fn reply_stale_share(&self, id: Option<Value>) -> Result<(), ErrorDisconnected> {
        tracing::debug!("[BRIDGE->ASIC] Preparing STALE SHARE response (Error Code: 21, Job not found)");
        self.reply(JsonRpcResponse::error(id, 21, "Job not found", None)).await
    }

    /// Reply with duplicate share error
    pub async fn reply_dupe_share(&self, id: Option<Value>) -> Result<(), ErrorDisconnected> {
        tracing::debug!("[BRIDGE->ASIC] Preparing DUPLICATE SHARE response (Error Code: 22, Duplicate share submitted)");
        self.reply(JsonRpcResponse::error(id, 22, "Duplicate share submitted", None)).await
    }

    /// Reply with bad share error
    pub async fn reply_bad_share(&self, id: Option<Value>) -> Result<(), ErrorDisconnected> {
        tracing::debug!("[BRIDGE->ASIC] Preparing BAD SHARE response (Error Code: 20, Unknown problem)");
        self.reply(JsonRpcResponse::error(id, 20, "Unknown problem", None)).await
    }

    /// Reply with low difficulty share error
    pub async fn reply_low_diff_share(&self, id: &serde_json::Value) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::debug!("[BRIDGE->ASIC] Preparing LOW DIFFICULTY SHARE response (Error Code: 23, Invalid difficulty)");
        self.reply(JsonRpcResponse::error(Some(id.clone()), 23, "Invalid difficulty", None))
            .await
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error + Send + Sync>)
    }

    /// Send a response (async)
    #[allow(dead_code)]
    async fn send_response(&self, response: JsonRpcResponse) -> Result<(), ErrorDisconnected> {
        let json = serde_json::to_string(&response).map_err(|_| ErrorDisconnected)?;
        let data = format!("{}\n", json);
        self.write_data(data.as_bytes()).await
    }

    /// Disconnect the client
    pub fn disconnect(&self) {
        if !self.disconnecting.swap(true, Ordering::Release) {
            let worker_name = self.worker_name.lock().clone();
            let remote_app = self.remote_app.lock().clone();
            let wallet_addr = self.wallet_addr.lock().clone();
            let is_pre_handshake = worker_name.is_empty() && remote_app.is_empty() && wallet_addr.is_empty();
            if is_pre_handshake {
                tracing::debug!(
                    "disconnecting client {}:{} worker='{}' app='{}'",
                    self.remote_addr,
                    self.remote_port,
                    worker_name,
                    remote_app
                );
            } else {
                tracing::info!(
                    "disconnecting client {}:{} worker='{}' app='{}'",
                    self.remote_addr,
                    self.remote_port,
                    worker_name,
                    remote_app
                );
            }

            // Close the write half
            let write_half_opt = {
                let mut write_guard = self.write_half.lock();
                write_guard.take()
            };

            if let Some(mut write_half) = write_half_opt {
                // Try to shutdown gracefully in async context
                tokio::spawn(async move {
                    let _ = write_half.shutdown().await;
                });
            }

            // Close the read half
            let _ = {
                let mut read_guard = self.read_half.lock();
                read_guard.take()
            };
        }
    }

    /// Route this connection's teardown through the listener's disconnect
    /// handler so [`ClientHandler::on_disconnect`] runs — Prometheus
    /// disconnect accounting plus the `SessionClosed` lifecycle event that
    /// feeds the firmware breakdown (ADR-0023). The read loop calls this
    /// exactly once per task at teardown; the handler performs the actual
    /// (idempotent) [`Self::disconnect`]. Best-effort: a dropped receiver
    /// (listener shutting down) silently no-ops, matching the socket
    /// teardown's own semantics.
    pub fn notify_disconnect(self: &Arc<Self>) {
        let _ = self.on_disconnect.send(Arc::clone(self));
    }

    fn check_disconnect(&self) {
        if !self.disconnecting.load(Ordering::Acquire) {
            // Spawn async disconnect
            let ctx = self.clone();
            tokio::spawn(async move {
                ctx.disconnect();
            });
        }
    }

    /// Get a reference to the read half (for reading)
    pub fn get_read_half(&self) -> parking_lot::MutexGuard<'_, Option<tokio::io::ReadHalf<TcpStream>>> {
        self.read_half.lock()
    }
}

impl Clone for StratumContext {
    fn clone(&self) -> Self {
        Self {
            remote_addr: self.remote_addr.clone(),
            remote_port: self.remote_port,
            local_port: self.local_port,
            wallet_addr: self.wallet_addr.clone(),
            worker_name: self.worker_name.clone(),
            worker_name_supplied: self.worker_name_supplied.clone(),
            canxium_addr: self.canxium_addr.clone(),
            kas_payout: self.kas_payout.clone(),
            remote_app: self.remote_app.clone(),
            id: self.id.clone(),
            extranonce: self.extranonce.clone(),
            state: self.state.clone(),
            // A clone represents the same connection: keep its id and
            // preserve the one-shot open flag so it can't re-emit.
            session_uid: self.session_uid,
            template_generation: self.template_generation.clone(),
            job_build_lock: self.job_build_lock.clone(),
            parent_refresh_gate: self.parent_refresh_gate.clone(),
            last_parent_job_at: self.last_parent_job_at.clone(),
            authorized_at: self.authorized_at.clone(),
            last_job_sent_at: self.last_job_sent_at.clone(),
            session_opened: AtomicBool::new(self.session_opened.load(Ordering::SeqCst)),
            vardiff_registered: self.vardiff_registered.clone(),
            disconnecting: self.disconnecting.clone(),
            write_lock: self.write_lock.clone(),
            read_half: self.read_half.clone(),
            write_half: self.write_half.clone(),
            on_disconnect: self.on_disconnect.clone(),
        }
    }
}

use serde_json::Value;
