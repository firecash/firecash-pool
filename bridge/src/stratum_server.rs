use crate::{
    client_handler::ClientHandler,
    default_client::{default_handlers, handle_authorize, handle_subscribe},
    jsonrpc_event::JsonRpcEvent,
    kaspaapi::KaspaApi,
    share_handler::{KaspaApiTrait, ShareHandler},
    stratum_context::StratumContext,
    stratum_listener::{StratumListener, StratumListenerConfig},
};
use katpool_domain::PoolEvent;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tracing::{debug, info, warn};

pub struct BridgeConfig {
    pub instance_id: String, // Instance identifier for logging (e.g., "Instance 1", "Instance 2")
    pub stratum_port: String,
    /// Multi-port stratum binding with per-port starting-difficulty
    /// seeds (ADR-0022): `(port, seed)`. When non-empty the bridge binds
    /// every listed port over one shared pipeline and selects each
    /// connection's *initial* difficulty by its local port; vardiff then
    /// owns the steady state. When empty, behaviour is identical to the
    /// single [`Self::stratum_port`] / [`Self::min_share_diff`] path, so
    /// the standalone binary's instance model is unchanged.
    pub stratum_ports: Vec<(String, u32)>,
    pub kaspad_address: String,
    pub prom_port: String,
    pub print_stats: bool,
    pub log_to_file: bool,
    pub health_check_port: String,
    pub block_wait_time: Duration,
    pub min_share_diff: f64,
    pub var_diff: bool,
    pub shares_per_min: u32,
    pub var_diff_stats: bool,
    pub extranonce_size: u8,
    pub pow2_clamp: bool,
    pub coinbase_tag_suffix: Option<String>,
    /// Require + parse a PROXY protocol v2 header on every accepted
    /// connection, recovering the real client IP behind the fly.io edge
    /// (ADR-0022). Default `false` (raw TCP peer). Enable only when the
    /// listener sits behind the trusted forwarder.
    pub proxy_protocol: bool,
    /// Emit the proxy-compatible Kaspa Common Stratum v1 handshake.
    pub kaspa_common_protocol: bool,
    pub extranonce_with_size: bool,
}

/// Start block template listener with concrete KaspaApi
/// This should be called from main.rs where we have concrete type
pub async fn start_block_template_listener_with_api(
    kaspa_api: Arc<KaspaApi>,
    block_wait_time: Duration,
    client_handler: Arc<ClientHandler>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client_handler_cb = Arc::clone(&client_handler);
    let kaspa_api_cb = Arc::clone(&kaspa_api);

    let block_cb = move || {
        let client_handler = Arc::clone(&client_handler_cb);
        let kaspa_api = Arc::clone(&kaspa_api_cb);
        tokio::spawn(async move {
            client_handler.new_block_available(kaspa_api).await;
        });
    };

    kaspa_api
        .start_block_template_listener(block_wait_time, block_cb)
        .await
        .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error + Send + Sync>)
}

pub async fn listen_and_serve<T: KaspaApiTrait + Send + Sync + 'static>(
    config: BridgeConfig,
    kaspa_api: Arc<T>,
    // Optional: if concrete KaspaApi is provided, use notification-based listener
    concrete_kaspa_api: Option<Arc<KaspaApi>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    listen_and_serve_impl(config, kaspa_api, concrete_kaspa_api, None, None).await
}

/// `listen_and_serve` plus an optional broadcast sender for `PoolEvent`s.
///
/// katpool fork addition. When `event_tx` is provided, the internal
/// `ShareHandler` is wired via [`ShareHandler::with_event_bus`] and every
/// share / block lifecycle event the handler emits goes into the channel —
/// the seam the unified `katpool` runtime uses to connect the bridge to the
/// accountant in-process. Pass `None` for the upstream call shape. Logged
/// divergence per `bridge/UPSTREAM.md`.
pub async fn listen_and_serve_with_events<T: KaspaApiTrait + Send + Sync + 'static>(
    config: BridgeConfig,
    kaspa_api: Arc<T>,
    concrete_kaspa_api: Option<Arc<KaspaApi>>,
    event_tx: Option<broadcast::Sender<PoolEvent>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    listen_and_serve_impl(config, kaspa_api, concrete_kaspa_api, event_tx, None).await
}

/// `listen_and_serve` plus a graceful-shutdown watch channel (upstream v2.0.0).
pub async fn listen_and_serve_with_shutdown<T: KaspaApiTrait + Send + Sync + 'static>(
    config: BridgeConfig,
    kaspa_api: Arc<T>,
    concrete_kaspa_api: Option<Arc<KaspaApi>>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    listen_and_serve_impl(config, kaspa_api, concrete_kaspa_api, None, Some(shutdown_rx)).await
}

async fn listen_and_serve_impl<T: KaspaApiTrait + Send + Sync + 'static>(
    config: BridgeConfig,
    kaspa_api: Arc<T>,
    concrete_kaspa_api: Option<Arc<KaspaApi>>,
    event_tx: Option<broadcast::Sender<PoolEvent>>,
    shutdown_rx: Option<watch::Receiver<bool>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Calculate min diff with pow2 clamp if needed. This is the default
    // seed used for any port without an explicit per-port seed.
    let clamp_seed = |raw: f64| -> f64 {
        let mut d = raw;
        if config.pow2_clamp && d > 0.0 {
            d = 2_f64.powi((d.log2().floor()) as i32);
        }
        if d == 0.0 { 4.0 } else { d }
    };
    let min_diff = clamp_seed(config.min_share_diff);

    // Resolve the ports to bind and their per-port starting-difficulty
    // seeds (ADR-0022). Empty `stratum_ports` => single-port mode using
    // `stratum_port` + `min_share_diff` (standalone-binary behaviour).
    let listen_ports: Vec<String> = if config.stratum_ports.is_empty() {
        vec![config.stratum_port.clone()]
    } else {
        config.stratum_ports.iter().map(|(p, _)| p.clone()).collect()
    };
    let port_seeds: std::collections::HashMap<u16, f64> =
        config.stratum_ports.iter().filter_map(|(p, seed)| parse_port_number(p).map(|n| (n, clamp_seed(*seed as f64)))).collect();

    // Extranonce size is now auto-detected per client based on miner type
    // We still need to pass a value to ClientHandler::new() for backward compatibility,
    // but it will be ignored as extranonce is assigned per-client in handle_subscribe
    // Default to 2 (for IceRiver/BzMiner/Goldshell) as that's the most common case
    let extranonce_size = if config.extranonce_size > 0 {
        config.extranonce_size.min(3) as i8
    } else {
        2 // Default to 2, will be auto-detected per client anyway
    };

    // Create share handler with instance identifier. When the
    // caller supplied an event bus sender, wire it in so every
    // share + block lifecycle event flows to the downstream
    // accountant consumer.
    let instance_id = config.instance_id.clone();
    let share_handler = {
        let mut handler = ShareHandler::new(instance_id.clone());
        if let Some(tx) = event_tx {
            handler = handler.with_event_bus(tx);
        }
        Arc::new(handler)
    };

    // Create client handler. `extranonce_size` is this listener's configured nonce
    // prefix width and is honoured per client in handle_subscribe.
    let client_handler = Arc::new(
        ClientHandler::new_with_protocol(
            Arc::clone(&share_handler),
            min_diff,
            port_seeds,
            extranonce_size,
            instance_id.clone(),
            config.kaspa_common_protocol,
        )
        .with_extranonce_size_param(config.extranonce_with_size),
    );

    // A network-target clear retires that connection's current ZKAS H_fc.
    // Publish replacement work immediately rather than waiting for the
    // periodic 1-BPS safety ticker or node confirmation.
    {
        let mut refresh_rx = share_handler.subscribe_job_refreshes();
        let client_handler = Arc::clone(&client_handler);
        let kaspa_api = Arc::clone(&kaspa_api);
        tokio::spawn(async move {
            loop {
                match refresh_rx.recv().await {
                    Ok(session_uid) => {
                        if let Some(client) = client_handler.client_by_session_uid(session_uid) {
                            client_handler.send_immediate_job_to_client(client, Arc::clone(&kaspa_api)).await;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!("job refresh coordinator lagged by {skipped} requests");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    let shutdown_rx_for_bg = shutdown_rx.clone();

    // Setup default handlers
    let mut handlers = default_handlers();

    // Override subscribe handler to enable automatic extranonce detection
    let subscribe_handler = {
        let client_handler = Arc::clone(&client_handler);
        Arc::new(move |ctx: Arc<StratumContext>, event: JsonRpcEvent| {
            let client_handler = Arc::clone(&client_handler);
            let ctx_clone = Arc::clone(&ctx);
            let event_clone = event.clone();
            Box::pin(async move {
                handle_subscribe(ctx_clone, event_clone, Some(client_handler))
                    .await
                    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error + Send + Sync>)
            })
                as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>>
        }) as crate::stratum_listener::EventHandler
    };
    handlers.insert("mining.subscribe".to_string(), subscribe_handler);

    // Override authorize handler to send immediate job (critical for IceRiver KS2L)
    let authorize_handler = {
        let client_handler = Arc::clone(&client_handler);
        let kaspa_api = Arc::clone(&kaspa_api);
        Arc::new(move |ctx: Arc<StratumContext>, event: JsonRpcEvent| {
            let client_handler = Arc::clone(&client_handler);
            let kaspa_api = Arc::clone(&kaspa_api);
            let ctx_clone = Arc::clone(&ctx);
            let event_clone = event.clone();
            Box::pin(async move {
                handle_authorize(ctx_clone, event_clone, Some(client_handler), Some(kaspa_api))
                    .await
                    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error + Send + Sync>)
            })
                as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>>
        }) as crate::stratum_listener::EventHandler
    };
    handlers.insert("mining.authorize".to_string(), authorize_handler);

    // Override submit handler
    let submit_handler = {
        let share_handler = Arc::clone(&share_handler);
        let kaspa_api = Arc::clone(&kaspa_api);
        Arc::new(move |ctx: Arc<StratumContext>, event: JsonRpcEvent| {
            let share_handler = Arc::clone(&share_handler);
            let kaspa_api = Arc::clone(&kaspa_api);
            let ctx_clone = Arc::clone(&ctx);
            Box::pin(async move {
                share_handler
                    .handle_submit(ctx_clone, event, kaspa_api)
                    .await
                    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error + Send + Sync>)
            })
                as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>>
        }) as crate::stratum_listener::EventHandler
    };
    handlers.insert("mining.submit".to_string(), submit_handler);

    // Setup listener config
    // Each client will get its own MiningState (created in stratum_listener)
    // Each client gets its own isolated state
    // Per-IP anti-abuse guard. Defaults are production-grade (256 conns
    // per IP, 100 frames/sec sustained, 200 burst). Operators tune
    // these via `AntiAbuseConfig` injected at start-up; the Phase 1
    // close-out milestone surfaces them through a CLI/env layer.
    // Per-IP anti-abuse guard. Defaults are production-grade (256
    // conns per IP, 100 frames/sec sustained, 200 burst). Operators
    // override individual limits via the `KATPOOL_ANTI_ABUSE_*`
    // environment variables (see `AntiAbuseConfig::from_lookup` docs
    // and `ops/systemd/katpool-bridge.conf.d/anti-abuse.conf.example`).
    // Malformed env values fail-fast at start-up rather than silently
    // falling back to defaults, so an operator typo never ships into
    // production unnoticed.
    let anti_abuse_config = crate::anti_abuse::AntiAbuseConfig::from_env()
        .map_err(|e| Box::new(std::io::Error::other(format!("anti-abuse config: {e}"))) as Box<dyn std::error::Error + Send + Sync>)?;
    tracing::info!(
        "[{}] anti-abuse: max_conn_per_ip={}, max_tracked_ips={}, frame_rate_per_sec={}, frame_burst={}",
        config.instance_id,
        anti_abuse_config.max_conn_per_ip,
        anti_abuse_config.max_tracked_ips,
        anti_abuse_config.frame_rate_per_sec,
        anti_abuse_config.frame_burst
    );
    let anti_abuse = std::sync::Arc::new(crate::anti_abuse::AntiAbuseGuard::new(anti_abuse_config));

    // Shared across every per-port listener (ADR-0022): one handler map,
    // one connect/disconnect callback set, and one anti-abuse guard so
    // per-IP caps and attribution stay global regardless of which port a
    // miner lands on.
    let handler_map = Arc::new(handlers);
    let on_connect: Arc<dyn Fn(Arc<StratumContext>) + Send + Sync> = Arc::new({
        let client_handler = Arc::clone(&client_handler);
        move |ctx: Arc<StratumContext>| {
            client_handler.on_connect(ctx);
        }
    });
    let on_disconnect: Arc<dyn Fn(Arc<StratumContext>) + Send + Sync> = Arc::new({
        let client_handler = Arc::clone(&client_handler);
        move |ctx: Arc<StratumContext>| {
            client_handler.on_disconnect(&ctx);
        }
    });

    // Start vardiff thread if enabled
    if config.var_diff {
        let shares_per_min = if config.shares_per_min > 0 { config.shares_per_min } else { 20 };
        if let Some(rx) = shutdown_rx_for_bg.as_ref().cloned() {
            share_handler.start_vardiff_thread_with_shutdown(shares_per_min, config.var_diff_stats, config.pow2_clamp, rx);
        } else {
            share_handler.start_vardiff_thread(shares_per_min, config.var_diff_stats, config.pow2_clamp);
        }
    }

    // Start stats printing thread if enabled
    if config.print_stats {
        let shares_per_min = if config.shares_per_min > 0 { config.shares_per_min } else { 20 };
        if let Some(rx) = shutdown_rx_for_bg.as_ref().cloned() {
            share_handler.start_print_stats_thread_with_shutdown(shares_per_min, rx);
        } else {
            share_handler.start_print_stats_thread(shares_per_min);
        }
    }

    // Start stats pruning thread
    if let Some(rx) = shutdown_rx_for_bg.as_ref().cloned() {
        share_handler.start_prune_stats_thread_with_shutdown(rx);
    } else {
        share_handler.start_prune_stats_thread();
    }

    // Start block template listener with notifications + ticker fallback
    // This provides immediate notifications when new blocks are available, with polling as fallback

    // If concrete KaspaApi is provided, use notification-based listener
    // Otherwise, use polling only (fallback for trait objects)
    if concrete_kaspa_api.is_none() {
        // Standalone multi-instance mode gives the notification receiver to
        // the first port only. Other ports still need fresh real-Kaspa
        // carriers, so drive their parent-only path from the 150 ms fallback.
        let parent_client_handler = Arc::clone(&client_handler);
        let parent_kaspa_api = Arc::clone(&kaspa_api);
        let mut parent_shutdown = shutdown_rx_for_bg.as_ref().cloned();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(150));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                if let Some(ref mut shutdown) = parent_shutdown {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() { break; }
                        }
                        _ = interval.tick() => {
                            parent_client_handler.new_parent_available(Arc::clone(&parent_kaspa_api)).await;
                        }
                    }
                } else {
                    interval.tick().await;
                    parent_client_handler.new_parent_available(Arc::clone(&parent_kaspa_api)).await;
                }
            }
        });
    }

    if let Some(concrete_api) = concrete_kaspa_api {
        let parent_client_handler = Arc::clone(&client_handler);
        let parent_kaspa_api = Arc::clone(&kaspa_api);
        concrete_api.start_kaspa_parent_template_listener(shutdown_rx_for_bg.as_ref().cloned(), move || {
            let client_handler = Arc::clone(&parent_client_handler);
            let kaspa_api = Arc::clone(&parent_kaspa_api);
            tokio::spawn(async move {
                client_handler.new_parent_available(kaspa_api).await;
            });
        });

        // We have concrete KaspaApi - use notification-based listener
        let client_handler_cb = Arc::clone(&client_handler);
        let kaspa_api_cb = Arc::clone(&kaspa_api);

        let block_cb = move || {
            let client_handler = Arc::clone(&client_handler_cb);
            let kaspa_api = Arc::clone(&kaspa_api_cb);
            tokio::spawn(async move {
                client_handler.new_block_available(kaspa_api).await;
            });
        };

        // Start notification-based listener with ticker fallback
        // Method signature: start_block_template_listener(self: Arc<Self>, ...)
        // Call the method directly on Arc<KaspaApi> (it's an instance method taking Arc<Self>)
        let listener_result = if let Some(rx) = shutdown_rx_for_bg.as_ref().cloned() {
            Arc::clone(&concrete_api).start_block_template_listener_with_shutdown(config.block_wait_time, rx, block_cb).await
        } else {
            Arc::clone(&concrete_api).start_block_template_listener(config.block_wait_time, block_cb).await
        };

        if let Err(e) = listener_result {
            warn!("Failed to start notification-based block template listener: {}, falling back to polling", e);
            // Fall through to polling approach
        } else {
            // Successfully started notification-based listener
            debug!("Started notification-based block template listener");
        }
    } else {
        // No concrete KaspaApi provided - use polling only
        warn!("Using polling-based block template listener (concrete KaspaApi not provided, notifications not available)");

        let client_handler_poll = Arc::clone(&client_handler);
        let kaspa_api_poll = Arc::clone(&kaspa_api);
        let mut shutdown_rx_poll = shutdown_rx_for_bg;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.block_wait_time);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                if let Some(ref mut rx) = shutdown_rx_poll {
                    tokio::select! {
                        _ = rx.changed() => {
                            if *rx.borrow() {
                                break;
                            }
                        }
                        _ = interval.tick() => {
                            client_handler_poll.new_block_available(Arc::clone(&kaspa_api_poll)).await;
                        }
                    }
                } else {
                    interval.tick().await;
                    client_handler_poll.new_block_available(Arc::clone(&kaspa_api_poll)).await;
                }
            }
        });
    }

    // Start one listener per bound port over the shared pipeline. In
    // single-port mode `listen_ports` has one entry, so this is
    // behaviourally identical to the original single-listener path.
    let mut listeners = tokio::task::JoinSet::new();
    for port in listen_ports {
        let listener = StratumListener::new(StratumListenerConfig {
            port: port.clone(),
            handler_map: Arc::clone(&handler_map),
            on_connect: Arc::clone(&on_connect),
            on_disconnect: Arc::clone(&on_disconnect),
            anti_abuse: Arc::clone(&anti_abuse),
            instance_id: config.instance_id.clone(),
            proxy_protocol: config.proxy_protocol,
        });
        info!("{} Starting stratum listener on {}", instance_id, port);
        // Honor graceful shutdown per listener (upstream v2.0.0): clone the
        // watch receiver so every bound port observes the same signal.
        let shutdown_rx = shutdown_rx.clone();
        listeners.spawn(async move {
            match shutdown_rx {
                Some(rx) => listener.listen_with_shutdown(rx).await,
                None => listener.listen().await,
            }
        });
    }

    // Each listener runs until shutdown or a bind/accept error. Return on
    // the first that finishes: a startup bind failure surfaces immediately
    // (fail fast); on shutdown the runtime aborts this task anyway.
    let listen_result = match listeners.join_next().await {
        Some(Ok(result)) => result,
        Some(Err(join_err)) => Err(Box::new(std::io::Error::other(format!("stratum listener task panicked: {join_err}")))
            as Box<dyn std::error::Error + Send + Sync>),
        None => Ok(()),
    };

    // Ensure all clients are disconnected when the listeners stop (shutdown
    // or error) so `connection_session` rows close (upstream v2.0.0).
    client_handler.disconnect_all();

    listen_result
}

/// Parse the numeric port from a listener address string such as
/// `"5555"`, `":5555"`, or `"0.0.0.0:5555"`. Returns `None` if no port
/// number can be extracted.
fn parse_port_number(addr: &str) -> Option<u16> {
    let tail = addr.rsplit(':').next().unwrap_or(addr);
    tail.trim().parse::<u16>().ok()
}

#[cfg(test)]
mod port_parse_tests {
    use super::parse_port_number;

    #[test]
    fn parses_supported_forms() {
        assert_eq!(parse_port_number("1111"), Some(1111));
        assert_eq!(parse_port_number(":5555"), Some(5555));
        assert_eq!(parse_port_number("0.0.0.0:7777"), Some(7777));
        assert_eq!(parse_port_number("[::]:8888"), Some(8888));
        assert_eq!(parse_port_number("not-a-port"), None);
        assert_eq!(parse_port_number(""), None);
    }
}
