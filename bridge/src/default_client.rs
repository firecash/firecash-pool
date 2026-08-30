use crate::jsonrpc_event::{JsonRpcEvent, JsonRpcResponse};
use crate::stratum_context::StratumContext;
use chrono::{DateTime, Utc};
use kaspa_addresses::Address;
use katpool_domain::{CorrelationId, PoolEvent, WalletAddress, WorkerName};
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

/// Regex for matching miners that use big job format
/// Matches: BzMiner, IceRiverMiner (from client_handler.go bigJobRegex)
static BIG_JOB_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r".*(BzMiner|IceRiverMiner).*").unwrap());

/// Regex for matching wallet addresses
static WALLET_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"kaspa(test|dev)?:([a-z0-9]{61}|[a-z0-9]{63})").unwrap());

/// Default logger configuration
pub fn default_logger() {
    // Logger is configured via tracing-subscriber in main
    // This function is kept for API compatibility
}

/// Default handler map
pub fn default_handlers() -> HashMap<String, crate::stratum_listener::EventHandler> {
    let mut handlers = HashMap::new();

    handlers.insert(
        "mining.subscribe".to_string(),
        Arc::new(|ctx: Arc<StratumContext>, event: JsonRpcEvent| {
            let ctx = ctx.clone();
            let event = event.clone();
            Box::pin(async move { handle_subscribe(ctx, event, None).await })
                as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>>
        }) as crate::stratum_listener::EventHandler,
    );

    handlers.insert(
        "mining.extranonce.subscribe".to_string(),
        Arc::new(|ctx: Arc<StratumContext>, event: JsonRpcEvent| {
            let ctx = ctx.clone();
            let event = event.clone();
            Box::pin(async move { handle_extranonce_subscribe(ctx, event).await })
                as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>>
        }) as crate::stratum_listener::EventHandler,
    );

    handlers.insert(
        "mining.authorize".to_string(),
        Arc::new(|ctx: Arc<StratumContext>, event: JsonRpcEvent| {
            let ctx = ctx.clone();
            let event = event.clone();
            Box::pin(async move {
                // Default handler - no client_handler/kaspa_api (will use polling fallback)
                handle_authorize(ctx, event, None, None).await
            })
                as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>>
        }) as crate::stratum_listener::EventHandler,
    );

    handlers.insert(
        "mining.submit".to_string(),
        Arc::new(|ctx: Arc<StratumContext>, event: JsonRpcEvent| {
            let ctx = ctx.clone();
            let event = event.clone();
            Box::pin(async move { handle_submit(ctx, event).await })
                as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>>
        }) as crate::stratum_listener::EventHandler,
    );

    handlers
}

/// Handle subscribe request
pub async fn handle_subscribe(
    ctx: Arc<StratumContext>,
    event: JsonRpcEvent,
    client_handler: Option<Arc<crate::client_handler::ClientHandler>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::debug!("[SUBSCRIBE] ===== SUBSCRIBE REQUEST FROM {} =====", ctx.remote_addr);
    tracing::debug!("[SUBSCRIBE] Event ID: {:?}", event.id);
    tracing::debug!("[SUBSCRIBE] Params count: {}", event.params.len());

    tracing::info!(
        "[HANDSHAKE] subscribe from {}:{} params_count={} (before app parse)",
        ctx.remote_addr,
        ctx.remote_port,
        event.params.len()
    );

    // Extract remote app from params if present
    if let Some(Value::String(app)) = event.params.first() {
        *ctx.remote_app.lock() = app.clone();
        tracing::debug!("[SUBSCRIBE] Extracted app from params[0]: '{}'", app);
    } else {
        tracing::warn!("[SUBSCRIBE] No app string in params[0], params: {:?}", event.params);
    }

    let remote_app = ctx.remote_app.lock().clone();

    tracing::info!("[HANDSHAKE] subscribe parsed app='{}' from {}:{}", remote_app, ctx.remote_addr, ctx.remote_port);

    // Auto-detect miner type and assign appropriate extranonce
    if let Some(ref handler) = client_handler {
        handler.assign_extranonce_for_miner(&ctx, &remote_app);
    }

    let extranonce = ctx.extranonce.lock().clone();

    tracing::debug!("[SUBSCRIBE] Client info - app: '{}', extranonce: '{}', addr: {}", remote_app, extranonce, ctx.remote_addr);

    // Select the handshake profile once from the listener and miner identity.
    //
    // Live probes of 2Miners and K1Pool with `GodMiner/2.0.0` both use the
    // legacy Bitmain subscription result `[null, prefix, remaining_nonce_size]`
    // and legacy array+timestamp jobs. A Common subscribe response followed by
    // a legacy job is a hybrid that some firmware silently ignores.
    let remote_app_lower = remote_app.to_lowercase();
    let kaspa_common_protocol = client_handler.as_ref().is_some_and(|handler| handler.kaspa_common_protocol());
    let is_bitmain =
        remote_app_lower.contains("godminer") || remote_app_lower.contains("bitmain") || remote_app_lower.contains("antminer");
    tracing::debug!("[SUBSCRIBE] Detected miner type - Remote app: '{}', Is Bitmain: {}", remote_app, is_bitmain);

    if is_bitmain {
        tracing::info!("[SUBSCRIBE] Bitmain/GodMiner detected ({}) — using embedded-extranonce legacy handshake", ctx.remote_addr);
    }

    let response = if is_bitmain {
        let extranonce2_size = 8 - (extranonce.len() / 2);
        JsonRpcResponse::new(
            &event,
            Some(Value::Array(vec![Value::Null, Value::String(extranonce.clone()), Value::Number(extranonce2_size.into())])),
            None,
        )
    } else {
        // Standard format (for IceRiver, BzMiner, and other miners)
        // Extranonce will be sent via mining.set_extranonce after authorize
        if BIG_JOB_REGEX.is_match(&remote_app) {
            tracing::debug!("[SUBSCRIBE] Using standard subscribe format for IceRiver/BzMiner {}", ctx.remote_addr);
        } else {
            tracing::debug!("[SUBSCRIBE] Using standard subscribe format for {}", ctx.remote_addr);
        }
        tracing::debug!("[SUBSCRIBE] Standard response: [true, 'EthereumStratum/1.0.0']");
        JsonRpcResponse::new(
            &event,
            Some(Value::Array(vec![Value::Bool(true), Value::String("EthereumStratum/1.0.0".to_string())])),
            None,
        )
    };

    let response_json = serde_json::to_string(&response).unwrap_or_else(|_| "failed".to_string());
    tracing::debug!("[SUBSCRIBE] Sending subscribe response to {}: {}", ctx.remote_addr, response_json);

    ctx.reply(response).await.map_err(|e| format!("failed to send response to subscribe: {}", e))?;

    if kaspa_common_protocol {
        let handler = client_handler.as_ref().ok_or("Kaspa Common protocol requires a client handler")?;
        let extranonce2_size = 8usize.saturating_sub(extranonce.len() / 2);
        tracing::info!(
            "[HANDSHAKE] sending July-23 Kaspa Common extranonce/difficulty before authorize to {}:{}",
            ctx.remote_addr,
            ctx.remote_port
        );
        ctx.send_v1_notification("set_extranonce", vec![Value::String(extranonce.clone()), Value::Number(extranonce2_size.into())])
            .await
            .map_err(|e| format!("failed to set Kaspa Common extranonce: {e}"))?;
        handler.send_subscribe_difficulty_v1(&ctx).await?;
    }

    // The working 2Miners and Kaspa-pool IceRiver paths send the nonce prefix
    // after subscribe and the difficulty only after authorize. Do not send the
    // same values again from both phases.
    if !kaspa_common_protocol && remote_app_lower.contains("iceriver") && client_handler.is_some() {
        tracing::info!("[HANDSHAKE] sending pre-authorize extranonce to IceRiver {}:{}", ctx.remote_addr, ctx.remote_port);
        if !extranonce.is_empty() {
            send_extranonce(ctx.clone()).await?;
        }
    }

    tracing::debug!("[SUBSCRIBE] ===== SUBSCRIBE COMPLETE FOR {} =====", ctx.remote_addr);
    Ok(())
}

/// Handle extranonce subscribe request
async fn handle_extranonce_subscribe(
    ctx: Arc<StratumContext>,
    event: JsonRpcEvent,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::debug!("[EXTRANONCE_SUBSCRIBE] ===== EXTRANONCE SUBSCRIBE FROM {} =====", ctx.remote_addr);
    tracing::debug!("[EXTRANONCE_SUBSCRIBE] Event ID: {:?}", event.id);

    let response = JsonRpcResponse::new(&event, Some(Value::Bool(true)), None);
    let response_json = serde_json::to_string(&response).unwrap_or_else(|_| "failed".to_string());
    tracing::debug!("[EXTRANONCE_SUBSCRIBE] Sending response to {}: {}", ctx.remote_addr, response_json);

    ctx.reply(response).await.map_err(|e| format!("failed to send response to extranonce subscribe: {}", e))?;

    tracing::debug!("[EXTRANONCE_SUBSCRIBE] ===== EXTRANONCE SUBSCRIBE COMPLETE FOR {} =====", ctx.remote_addr);
    Ok(())
}

/// Handle authorize request
/// If client_handler and kaspa_api are provided, sends immediate job after authorization
pub async fn handle_authorize(
    ctx: Arc<StratumContext>,
    event: JsonRpcEvent,
    client_handler: Option<Arc<crate::client_handler::ClientHandler>>,
    kaspa_api: Option<Arc<dyn crate::share_handler::KaspaApiTrait + Send + Sync>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::debug!("[AUTHORIZE] ===== AUTHORIZE REQUEST FROM {} =====", ctx.remote_addr);
    tracing::debug!("[AUTHORIZE] Event ID: {:?}", event.id);
    tracing::debug!("[AUTHORIZE] Params count: {}", event.params.len());
    tracing::debug!("[AUTHORIZE] Full params: {:?}", event.params);

    tracing::info!("[HANDSHAKE] authorize from {}:{} params_count={}", ctx.remote_addr, ctx.remote_port, event.params.len());

    if event.params.is_empty() {
        tracing::error!("[AUTHORIZE] ERROR: Empty params from {}", ctx.remote_addr);
        return Err("malformed event from miner, expected param[0] to be address".into());
    }

    let address_value = event.params.first().ok_or("missing address parameter")?;

    let address_str = address_value.as_str().ok_or("expected param[0] to be address string")?;

    tracing::debug!("[AUTHORIZE] Address string from params[0]: '{}'", address_str);

    let parts: Vec<&str> = address_str.split('.').collect();
    tracing::debug!("[AUTHORIZE] Split address into {} parts: {:?}", parts.len(), parts);

    let mut address = parts[0].to_string();
    let mut worker_name = String::new();
    let mut canxium_address = String::new();

    if parts.len() >= 2 {
        worker_name = parts[1].to_string();
        tracing::debug!("[AUTHORIZE] Extracted worker name: '{}'", worker_name);
        if parts.len() >= 3 {
            canxium_address = process_canxium_address(parts[2]);
            tracing::debug!("[AUTHORIZE] Extracted canxium address: '{}'", canxium_address);
        }
    }

    // Clean and validate wallet address.
    // Anti-abuse: a failed bech32 check disconnects the client and
    // bumps `ks_anti_abuse_bad_address_total{ip}`. This stops scanners
    // and misconfigured miners from indefinitely tying up handler
    // threads with retry storms.
    tracing::debug!("[AUTHORIZE] Cleaning wallet address: '{}'", address);
    address = match clean_wallet(&address) {
        Ok(a) => a,
        Err(e) => {
            // ZKas policy: never drop a miner for a malformed address. Accept
            // it and mine the coinbase to the POOL's own address instead — a miner
            // that supplies a bad address forfeits its rewards to the pool rather
            // than being disconnected.
            let fallback = pool_fallback_address();
            let instance_id = client_handler.as_ref().map_or("", |h| h.instance_id());
            crate::prom::record_bad_address(instance_id, &ctx.remote_addr);
            tracing::warn!(
                "[AUTHORIZE] invalid address '{}' from {}:{} ({e}); accepting miner, coinbase -> pool ({})",
                address,
                ctx.remote_addr,
                ctx.remote_port,
                fallback
            );
            fallback
        }
    };
    tracing::debug!("[AUTHORIZE] Cleaned address: '{}'", address);

    tracing::debug!("[AUTHORIZE] Final parsed - address: '{}', worker: '{}', canxium: '{}'", address, worker_name, canxium_address);

    if let Some(ref client_handler) = client_handler {
        client_handler.prepare_worker_identity_change(&ctx);
    }
    *ctx.wallet_addr.lock() = address.clone();
    ctx.set_authorized_worker_name(worker_name);
    let worker_name = ctx.effective_worker_name();

    if let Some(ref client_handler) = client_handler {
        client_handler.sync_worker_prom_metrics(&ctx);
    }

    let remote_app = ctx.remote_app.lock().clone();
    tracing::info!("[HANDSHAKE] authorized {}:{} worker='{}' app='{}'", ctx.remote_addr, ctx.remote_port, worker_name, remote_app);

    if !canxium_address.is_empty() {
        *ctx.canxium_addr.lock() = canxium_address.clone();
    }

    // Merged mining: the stratum PASSWORD (`params[1]`) carries the miner's own
    // `kaspa:` address, so a KAS block found on its lane pays the miner instead of
    // the pool. Historically this field was ignored entirely.
    //
    // Parsed once, here, and stored as an `Address`. Jobs are rebuilt about once a
    // second per connection, so bech32-decoding this on the job path would be pure
    // repeated waste for a value that cannot change without a re-authorize.
    //
    // Same policy the ZKas address above uses: a miner is NEVER dropped for a bad
    // value. Anything unparseable leaves `kas_payout = None`, which means "pay the
    // pool" — the miner keeps mining and keeps earning ZKas normally. That is the
    // forgiving choice, but it is also a silent one: a typo donates this miner's KAS
    // to the pool indefinitely, so it is logged at WARN with the offending value.
    let kas_payout = parse_kas_payout(event.params.get(1).and_then(|v| v.as_str()));
    match &kas_payout {
        Some(addr) => {
            tracing::info!(
                "[AUTHORIZE] {}:{} merged-mining KAS rewards -> {} (from password field)",
                ctx.remote_addr,
                ctx.remote_port,
                addr
            );
        }
        None => {
            if let Some(raw) = event.params.get(1).and_then(|v| v.as_str())
                && !is_placeholder_password(raw)
            {
                tracing::warn!(
                    "[AUTHORIZE] {}:{} password '{}' is not a valid kaspa: address; KAS from this lane pays the POOL.                      Set the password to your kaspa: address to be paid directly.",
                    ctx.remote_addr,
                    ctx.remote_port,
                    raw
                );
            }
        }
    }
    *ctx.kas_payout.lock() = kas_payout;

    // Open a live `connection_session` row now that the connection has
    // authenticated (B1): the session becomes visible while still
    // connected, with its worker bound from the start, and `connected_at`
    // carries the real TCP-accept time. Emitted at most once per
    // connection; a no-op in standalone mode (no event bus attached).
    if let Some(handler) = client_handler.as_ref()
        && ctx.claim_session_open()
    {
        let connected_at = DateTime::<Utc>::from(ctx.state.connect_time());
        let wallet_opt = WalletAddress::new(&address).ok();
        let worker_opt = (!worker_name.is_empty()).then(|| WorkerName::new(&worker_name).ok()).flatten();
        let remote_app_opt = (!remote_app.is_empty()).then(|| remote_app.clone());
        handler.emit_event(PoolEvent::SessionOpened {
            conn_id: ctx.session_uid(),
            wallet: wallet_opt,
            worker: worker_opt,
            remote_ip: ctx.remote_addr().to_owned(),
            remote_app: remote_app_opt,
            connected_at,
            correlation_id: CorrelationId::new_v4(),
        });
    }

    let response = JsonRpcResponse::new(&event, Some(Value::Bool(true)), None);
    let response_json = serde_json::to_string(&response).unwrap_or_else(|_| "failed".to_string());
    tracing::debug!("[AUTHORIZE] Sending authorize response to {}: {}", ctx.remote_addr, response_json);

    ctx.reply(response).await.map_err(|e| format!("failed to send response to authorize: {}", e))?;

    tracing::debug!("[AUTHORIZE] Authorize response sent successfully");

    // Begin a fresh job-delivery epoch. The watchdog only disconnects if no
    // work is published at all; it never treats a low share rate as failure.
    ctx.mark_authorized();

    // CRITICAL: Message order for IceRiver must be:
    // 1. authorize response (done above)
    // 2. extranonce (if enabled) - MUST complete before difficulty/job
    // 3. difficulty
    // 4. job

    let extranonce = ctx.extranonce.lock().clone();
    // Bitmain/GodMiner receives its extranonce inside the subscribe response
    // ([null, extranonce, extranonce2_size]); an additional set_extranonce
    // notification is outside its protocol and could confuse the firmware.
    let remote_app_lower = ctx.remote_app.lock().to_lowercase();
    let kaspa_common_protocol = client_handler.as_ref().is_some_and(|handler| handler.kaspa_common_protocol());
    let is_bitmain =
        remote_app_lower.contains("godminer") || remote_app_lower.contains("bitmain") || remote_app_lower.contains("antminer");
    let is_iceriver =
        remote_app_lower.contains("iceriver") || remote_app_lower.contains("icemining") || remote_app_lower.contains("icm");
    if !extranonce.is_empty() && !is_bitmain && !is_iceriver && !kaspa_common_protocol {
        tracing::debug!("[AUTHORIZE] Step 2: Sending extranonce to client {} before difficulty/job", ctx.remote_addr);
        tracing::debug!("[AUTHORIZE] Extranonce value: '{}'", extranonce);
        send_extranonce(ctx.clone()).await?;
        tracing::debug!("[AUTHORIZE] Extranonce sent successfully to client {}", ctx.remote_addr);
    } else {
        tracing::debug!("[AUTHORIZE] No extranonce step (empty or bitmain; bitmain gets it via subscribe response)");
    }

    let wallet_addr = ctx.wallet_addr.lock().clone();
    let mut log_message = format!("[AUTHORIZE] Client authorized - address: {}", wallet_addr);
    if !canxium_address.is_empty() {
        log_message.push_str(&format!(", canxium address: {}", canxium_address));
    }
    tracing::debug!("{}", log_message);

    // CRITICAL: Send immediate job after authorization (IceRiver KS2L expects this)
    // Don't wait for polling loop - send job immediately
    // Difficulty will be sent inside send_immediate_job_to_client
    if let (Some(client_handler), Some(kaspa_api)) = (client_handler, kaspa_api) {
        client_handler.start_first_job_watchdog(ctx.clone());
        tracing::debug!(
            "[AUTHORIZE] Step 3-4: Triggering immediate job send for client {} (extranonce already sent)",
            ctx.remote_addr
        );
        client_handler.send_immediate_job_to_client(ctx.clone(), kaspa_api).await;
    } else {
        // Fallback: let polling loop handle it (may cause disconnects for IceRiver)
        tracing::warn!(
            "[AUTHORIZE] WARNING: No client_handler/kaspa_api available - job will be sent by polling loop (may cause IceRiver disconnect)"
        );
    }

    tracing::debug!("[AUTHORIZE] ===== AUTHORIZE COMPLETE FOR {} =====", ctx.remote_addr);
    Ok(())
}

/// Handle submit request (stub - actual implementation in share_handler)
async fn handle_submit(ctx: Arc<StratumContext>, event: JsonRpcEvent) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::debug!("[SUBMIT] ===== SUBMIT REQUEST FROM {} =====", ctx.remote_addr);
    tracing::debug!("[SUBMIT] Event ID: {:?}", event.id);
    tracing::debug!("[SUBMIT] Params count: {}", event.params.len());
    tracing::debug!("[SUBMIT] Full params: {:?}", event.params);
    tracing::debug!("[SUBMIT] Note: Actual processing happens in share_handler");
    Ok(())
}

/// Process Canxium address
fn process_canxium_address(address: &str) -> String {
    let mut addr = address.to_string();

    // Remove 0x prefix if present
    if addr.starts_with("0x") {
        addr = addr[2..].to_string();
    } else if addr.to_lowercase().starts_with("canxiuminer:0x") {
        // If it has both prefixes, remove the 0x part
        let prefix = &addr[.."canxiuminer:".len()];
        let address_part = &addr["canxiuminer:0x".len()..];
        addr = format!("{}{}", prefix, address_part);
    }

    // Make sure the address has the canxiuminer: prefix
    if !addr.to_lowercase().starts_with("canxiuminer:") {
        addr = format!("canxiuminer:{}", addr);
    }

    addr
}

/// Clean and validate wallet address
/// The pool's own address, used as the coinbase target when a miner supplies an
/// unparsable address (see the authorize handler). Configurable via the
/// POOL_FALLBACK_ADDRESS env var; defaults to the ZKas pool wallet.
fn pool_fallback_address() -> String {
    std::env::var("POOL_FALLBACK_ADDRESS")
        .unwrap_or_else(|_| "zkas:py82h42m9qjff0knpcmllzq3c7qhurje5auh4tq2ceagf69wjpf23djwwmqr26zhsua8rrglrwdltsh".to_string())
}

/// Passwords miners send when they mean "I have no password".
///
/// Every one of these is a convention, not an address, and treating them as a failed
/// parse would spam WARN for correctly-configured miners who simply are not opting
/// into direct KAS payout.
fn is_placeholder_password(raw: &str) -> bool {
    !looks_like_payout_attempt(raw)
}

/// Did this miner plausibly TRY to give us a payout address?
///
/// The rule is deliberately inverted: stay silent by default and warn only on a
/// genuine attempt. Passwords in the wild are overwhelmingly conventions rather than
/// addresses — measured on this pool within minutes of shipping the feature: `x`,
/// `*`, `(null)`, `d=8192`, `m=solo`. Warning on "not a valid address" caught every
/// one of those and buried the warning that actually matters — a real address with a
/// typo, which silently donates that miner's KAS to the pool forever.
///
/// So: an attempt is something carrying an explicit `kaspa`-family prefix. Anything
/// else is treated as "no opinion", pays the pool exactly as before this feature
/// existed, and says nothing.
fn looks_like_payout_attempt(raw: &str) -> bool {
    let t = raw.trim().to_ascii_lowercase();
    t.starts_with("kaspa:") || t.starts_with("kaspatest:") || t.starts_with("kaspadev:")
}

/// Read the stratum password field as the miner's KAS payout address.
///
/// Deliberately STRICTER than [`clean_wallet`]: that function coerces a bare payload
/// into `kaspa:...` and falls back to a regex scrape, which is right for the ZKas
/// address (where a wrong guess costs the miner nothing — the pool still pays them via
/// the pool address) and wrong here. This value decides where real KAS is minted. A
/// coerced guess that happens to decode would silently mint to an address the miner
/// never chose and cannot spend, so anything that is not already a well-formed,
/// explicitly-prefixed address is refused and the lane falls back to the pool.
///
/// Some miners send `address:worker` or `address.worker` in the password; the address
/// is taken as the leading token so those still work.
fn parse_kas_payout(raw: Option<&str>) -> Option<Address> {
    let raw = raw?.trim();
    if is_placeholder_password(raw) {
        return None;
    }
    // Tolerate a trailing worker/difficulty suffix, but never invent a prefix.
    let candidate = raw.split([',', ' ']).next().unwrap_or(raw).trim();
    if !(candidate.starts_with("kaspa:") || candidate.starts_with("kaspatest:") || candidate.starts_with("kaspadev:")) {
        return None;
    }
    Address::try_from(candidate).ok()
}

fn clean_wallet(input: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // Try to decode as Kaspa address (supports kaspa:, kaspatest:, kaspadev:)
    if Address::try_from(input).is_ok() {
        return Ok(input.to_string());
    }

    // Try with kaspa: prefix if no recognized prefix
    if !input.starts_with("kaspa:") && !input.starts_with("kaspatest:") && !input.starts_with("kaspadev:") {
        return clean_wallet(&format!("kaspa:{}", input));
    }

    // Try regex match
    if let Some(captures) = WALLET_REGEX.find(input) {
        return Ok(captures.as_str().to_string());
    }

    Err("unable to coerce wallet to valid kaspa address".into())
}

/// Send extranonce to client
async fn send_extranonce(ctx: Arc<StratumContext>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::debug!("[EXTRANONCE] ===== SENDING EXTRANONCE TO {} =====", ctx.remote_addr);

    let remote_app = ctx.remote_app.lock().clone();
    let extranonce = ctx.extranonce.lock().clone();

    tracing::debug!("[EXTRANONCE] Remote app: '{}', Extranonce: '{}'", remote_app, extranonce);

    // Bitmain requires extranonce2_size parameter - use same detection logic as assign_extranonce_for_miner
    // (case-insensitive matching for consistency)
    let remote_app_lower = remote_app.to_lowercase();
    let is_bitmain =
        remote_app_lower.contains("godminer") || remote_app_lower.contains("bitmain") || remote_app_lower.contains("antminer");
    tracing::debug!("[EXTRANONCE] Detected miner type - Remote app: '{}', Is Bitmain: {}", remote_app, is_bitmain);

    let params = if is_bitmain {
        let extranonce2_size = 8 - (extranonce.len() / 2);
        tracing::debug!("[EXTRANONCE] ===== USING BITMAIN EXTRANONCE FORMAT FOR {} =====", ctx.remote_addr);
        tracing::debug!(
            "[EXTRANONCE] Bitmain extranonce: '{}' ({} bytes), extranonce2_size: {} (calculated: 8 - {} / 2)",
            extranonce,
            extranonce.len() / 2,
            extranonce2_size,
            extranonce.len()
        );
        tracing::debug!("[EXTRANONCE] Bitmain params: ['{}', {}]", extranonce, extranonce2_size);
        vec![Value::String(extranonce.clone()), Value::Number(extranonce2_size.into())]
    } else {
        tracing::debug!("[EXTRANONCE] Using standard format (IceRiver/BzMiner)");
        vec![Value::String(extranonce.clone())]
    };

    // IceRiver expects minimal notification format (method + params only, no id or jsonrpc)
    let is_iceriver = remote_app.contains("IceRiver");

    if is_iceriver {
        tracing::debug!("[EXTRANONCE] Using minimal format for IceRiver (no id/jsonrpc)");
        ctx.send_notification("mining.set_extranonce", params.clone())
            .await
            .map_err(|e| format!("failed to set extranonce: {}", e))?;
    } else {
        // For non-IceRiver, use standard JSON-RPC format with jsonrpc field
        let event = JsonRpcEvent::new(None, "mining.set_extranonce", params.clone());
        let event_json = serde_json::to_string(&event).unwrap_or_else(|_| "failed".to_string());
        tracing::debug!("[EXTRANONCE] Sending mining.set_extranonce to {}: {}", ctx.remote_addr, event_json);
        ctx.send(event).await.map_err(|e| format!("failed to set extranonce: {}", e))?;
    }

    tracing::debug!("[EXTRANONCE] ===== EXTRANONCE SENT TO {} =====", ctx.remote_addr);
    Ok(())
}

#[cfg(test)]
mod protocol_wire_tests {
    use super::*;
    use crate::{client_handler::ClientHandler, mining_state::MiningState, share_handler::ShareHandler};
    use serde_json::json;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::sync::mpsc;
    use tokio::time::{Duration, timeout};

    async fn context_with_peer(local_port: u16) -> (Arc<StratumContext>, BufReader<tokio::net::TcpStream>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::net::TcpStream::connect(address).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (disconnect_tx, _disconnect_rx) = mpsc::unbounded_channel();
        let context =
            StratumContext::new("127.0.0.1".to_string(), 12345, local_port, server, Arc::new(MiningState::new()), disconnect_tx);
        (context, BufReader::new(peer))
    }

    async fn read_json_line(peer: &mut BufReader<tokio::net::TcpStream>) -> Value {
        let mut line = String::new();
        timeout(Duration::from_secs(1), peer.read_line(&mut line)).await.unwrap().unwrap();
        serde_json::from_str(line.trim()).unwrap()
    }

    #[tokio::test]
    async fn godminer_uses_one_embedded_extranonce_subscribe_response() {
        let share_handler = Arc::new(ShareHandler::new("wire-test".to_string()));
        let handler = Arc::new(ClientHandler::new(share_handler, 8192.0, HashMap::new(), 2, "wire-test".to_string()));
        let (context, mut peer) = context_with_peer(5555).await;
        let request = JsonRpcEvent::new(
            Some("1".to_string()),
            "mining.subscribe",
            vec![json!("GodMiner/2.0.0"), json!("EthereumStratum/1.0.0")],
        );

        handle_subscribe(context, request, Some(handler)).await.unwrap();

        let response = read_json_line(&mut peer).await;
        let result = response["result"].as_array().unwrap();
        assert!(result[0].is_null());
        assert!(result[1].is_string());
        assert_eq!(result[2].as_u64().unwrap(), 8 - result[1].as_str().unwrap().len() as u64 / 2);

        let mut unexpected = String::new();
        let read = timeout(Duration::from_millis(50), peer.read_line(&mut unexpected)).await;
        assert!(
            matches!(read, Err(_) | Ok(Ok(0))),
            "GodMiner subscribe must not emit a second extranonce/difficulty message: {unexpected}"
        );
    }

    #[tokio::test]
    async fn strict_common_subscribe_preserves_july23_mrr_sequence() {
        let share_handler = Arc::new(ShareHandler::new("wire-test".to_string()));
        let handler =
            Arc::new(ClientHandler::new_with_protocol(share_handler, 8192.0, HashMap::new(), 2, "wire-test".to_string(), true));
        let (context, mut peer) = context_with_peer(5577).await;
        let request = JsonRpcEvent::new(
            Some("1".to_string()),
            "mining.subscribe",
            vec![json!("IceRiverMiner-v1.1"), json!("EthereumStratum/1.0.0")],
        );

        handle_subscribe(context, request, Some(handler)).await.unwrap();

        let response = read_json_line(&mut peer).await;
        assert_eq!(response["result"], json!([true, "EthereumStratum/1.0.0"]));
        let extranonce = read_json_line(&mut peer).await;
        assert_eq!(extranonce["id"], Value::Null);
        assert_eq!(extranonce["method"], "set_extranonce");
        assert_eq!(extranonce["params"].as_array().unwrap().len(), 2);
        let difficulty = read_json_line(&mut peer).await;
        assert_eq!(difficulty["id"], Value::Null);
        assert_eq!(difficulty["method"], "mining.set_difficulty");
        assert_eq!(difficulty["params"], json!([8192.0]));
    }

    #[tokio::test]
    async fn parent_refresh_gate_coalesces_jobs_within_profile_interval() {
        let (context, _peer) = context_with_peer(5555).await;
        let permit = context.try_parent_refresh(Duration::from_millis(80)).expect("first parent refresh");
        context.mark_parent_job_sent();
        drop(permit);

        assert!(context.try_parent_refresh(Duration::from_millis(80)).is_none());
        tokio::time::sleep(Duration::from_millis(90)).await;
        assert!(context.try_parent_refresh(Duration::from_millis(80)).is_some());
    }
}
