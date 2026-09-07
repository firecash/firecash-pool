use std::collections::HashSet;
use std::time::Duration;

use crate::net_utils::normalize_port;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Instance-specific configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InstanceConfig {
    #[serde(deserialize_with = "deserialize_port")]
    pub stratum_port: String,
    // f64 so vardiff can drop the share difficulty below 1 on low-difficulty
    // chains (ZKas's block target is far easier than a stratum diff of 1).
    pub min_share_diff: f64,
    #[serde(default, deserialize_with = "deserialize_optional_port")]
    pub prom_port: Option<String>, // Optional per-instance prom port
    pub log_to_file: Option<bool>, // Optional per-instance logging
    #[serde(default, deserialize_with = "deserialize_optional_duration_ms", serialize_with = "serialize_optional_duration_ms")]
    pub block_wait_time: Option<Duration>,
    pub extranonce_size: Option<u8>,
    // Instance-specific settings that can override global defaults
    pub var_diff: Option<bool>,
    pub shares_per_min: Option<u32>,
    pub var_diff_stats: Option<bool>,
    pub pow2_clamp: Option<bool>,
    /// Use the Kaspa Common Stratum handshake used by K1Pool, 2Miners and
    /// Kryptex. Intended for strict Stratum proxies such as MRR.
    #[serde(default)]
    pub kaspa_common_protocol: bool,
    /// Send `mining.set_extranonce` as `[prefix, extranonce2_size]` instead of
    /// `[prefix]`. The one-argument form is what IceRiver and BzMiner have always
    /// been given here and they infer the size, but a client that reads the second
    /// argument gets nothing and drops the connection. Off by default so no
    /// existing listener changes shape.
    #[serde(default)]
    pub extranonce_with_size: bool,
}

/// Global configuration (shared across all instances)
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct GlobalConfig {
    pub kaspad_address: String,
    /// Merged mining (real dual-chain): gRPC address of the upstream **Kaspa** node
    /// that supplies the parent block template and receives Kaspa-target-clearing
    /// blocks. Empty disables real merged mining. Overridable via
    /// `ZKAS_KASPA_NODE`.
    #[serde(default)]
    pub merged_kaspa_address: String,
    /// The `kaspa:` address every merged parent's coinbase pays (the KAS reward
    /// recipient). Overridable via `ZKAS_KASPA_PAY`.
    #[serde(default)]
    pub merged_kaspa_pay_address: String,
    #[serde(deserialize_with = "deserialize_duration_ms", serialize_with = "serialize_duration_ms")]
    pub block_wait_time: Duration,
    pub print_stats: bool,
    pub log_to_file: bool, // Default for instances that don't specify
    pub health_check_port: String,
    #[serde(deserialize_with = "deserialize_port")]
    pub web_dashboard_port: String,
    pub var_diff: bool,
    pub shares_per_min: u32,
    pub var_diff_stats: bool,
    pub extranonce_size: u8,
    pub pow2_clamp: bool,
    #[serde(deserialize_with = "deserialize_coinbase_tag_suffix")]
    pub coinbase_tag_suffix: Option<String>,
}

/// Bridge configuration (supports both single and multi-instance modes)
#[derive(Debug, Serialize)]
pub struct BridgeConfig {
    pub global: GlobalConfig,
    pub instances: Vec<InstanceConfig>,
}

#[derive(Serialize)]
struct BridgeConfigYaml<'a> {
    #[serde(flatten)]
    global: &'a GlobalConfig,
    instances: &'a [InstanceConfig],
}

// Custom deserializers

/// Deserialize a port string and normalize it
fn deserialize_port<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(normalize_port(&s))
}

/// Deserialize an optional port string and normalize it
fn deserialize_optional_port<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.and_then(|s| {
        let normalized = normalize_port(&s);
        if normalized.is_empty() { None } else { Some(normalized) }
    }))
}

/// Deserialize a duration from milliseconds (supports both int and float)
fn deserialize_duration_ms<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    let value: serde_yaml::Value = Deserialize::deserialize(deserializer)?;

    let ms = match value {
        serde_yaml::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i as u64
            } else if let Some(f) = n.as_f64() {
                f as u64
            } else {
                return Err(D::Error::custom("duration must be a number"));
            }
        }
        _ => return Err(D::Error::custom("duration must be a number")),
    };

    Ok(Duration::from_millis(ms))
}

/// Deserialize an optional duration from milliseconds
fn deserialize_optional_duration_ms<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    let value: Option<serde_yaml::Value> = Option::deserialize(deserializer)?;

    if let Some(v) = value {
        let ms = match v {
            serde_yaml::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    i as u64
                } else if let Some(f) = n.as_f64() {
                    f as u64
                } else {
                    return Err(D::Error::custom("duration must be a number"));
                }
            }
            _ => return Err(D::Error::custom("duration must be a number")),
        };
        Ok(Some(Duration::from_millis(ms)))
    } else {
        Ok(None)
    }
}

/// Deserialize coinbase_tag_suffix, converting empty strings to None
fn deserialize_coinbase_tag_suffix<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = Option::<String>::deserialize(deserializer)?;
    Ok(s.and_then(|s| {
        let trimmed = s.trim();
        if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
    }))
}

// Custom serializers

/// Serialize a duration as milliseconds (u64)
fn serialize_duration_ms<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_u64(duration.as_millis() as u64)
}

/// Serialize an optional duration as milliseconds (u64)
fn serialize_optional_duration_ms<S>(duration: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match duration {
        Some(d) => serializer.serialize_some(&(d.as_millis() as u64)),
        None => serializer.serialize_none(),
    }
}

/// Raw config structure for deserialization (handles both single and multi-instance modes)
#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct BridgeConfigRaw {
    #[serde(flatten)]
    global: GlobalConfig,

    // Multi-instance mode
    #[serde(default)]
    instances: Option<Vec<InstanceConfig>>,

    // Single-instance mode (for backward compatibility)
    #[serde(default, deserialize_with = "deserialize_optional_port")]
    stratum_port: Option<String>,
    #[serde(default)]
    min_share_diff: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_optional_port")]
    prom_port: Option<String>,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            kaspad_address: "localhost:16110".to_string(),
            merged_kaspa_address: String::new(),
            merged_kaspa_pay_address: String::new(),
            block_wait_time: Duration::from_millis(1000),
            print_stats: true,
            log_to_file: true,
            health_check_port: String::new(),
            web_dashboard_port: String::new(),
            var_diff: true,
            shares_per_min: 20,
            var_diff_stats: false,
            // 2 bytes, matching what the assignment path hardcoded before it was
            // made configurable. Defaulting to 0 here would silently drop every
            // deployment that omits the key from a 2-byte prefix to none.
            extranonce_size: 2,
            pow2_clamp: false,
            coinbase_tag_suffix: None,
        }
    }
}

impl Default for InstanceConfig {
    fn default() -> Self {
        Self {
            stratum_port: ":5555".to_string(),
            min_share_diff: 8192.0,
            prom_port: None,
            log_to_file: None,
            block_wait_time: None,
            extranonce_size: None,
            var_diff: None,
            shares_per_min: None,
            var_diff_stats: None,
            pow2_clamp: None,
            kaspa_common_protocol: false,
            extranonce_with_size: false,
        }
    }
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self { global: GlobalConfig::default(), instances: vec![InstanceConfig::default()] }
    }
}

impl BridgeConfig {
    pub fn from_yaml(content: &str) -> Result<Self, anyhow::Error> {
        // Deserialize using serde_yaml
        let raw: BridgeConfigRaw = serde_yaml::from_str(content)?;

        // Post-process: Handle single-instance mode
        let instances = if let Some(instances) = raw.instances {
            // Multi-instance mode

            // Validate: instances cannot be empty
            if instances.is_empty() {
                return Err(anyhow::anyhow!("instances array cannot be empty"));
            }

            // Validate: required fields are present (serde will error if missing, but we check anyway)
            for (idx, instance) in instances.iter().enumerate() {
                if instance.stratum_port.is_empty() {
                    return Err(anyhow::anyhow!("Instance {} missing required 'stratum_port'", idx));
                }
                if instance.min_share_diff == 0.0 {
                    // Note: 0 is technically valid but unlikely, we'll allow it
                }
            }

            instances
        } else {
            // Single-instance mode (backward compatible)
            let mut instance = InstanceConfig { prom_port: raw.prom_port, ..InstanceConfig::default() };
            if let Some(stratum_port) = raw.stratum_port {
                instance.stratum_port = stratum_port;
            }
            if let Some(min_share_diff) = raw.min_share_diff {
                instance.min_share_diff = min_share_diff;
            }

            vec![instance]
        };

        // Validate: duplicate ports
        let mut ports = HashSet::new();
        for instance in &instances {
            if !ports.insert(&instance.stratum_port) {
                return Err(anyhow::anyhow!("Duplicate stratum_port: {}", instance.stratum_port));
            }
        }

        Ok(BridgeConfig { global: raw.global, instances })
    }

    pub(crate) fn to_yaml(&self) -> Result<String, serde_yaml::Error> {
        let yaml = BridgeConfigYaml { global: &self.global, instances: &self.instances };
        serde_yaml::to_string(&yaml)
    }
}
