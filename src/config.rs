//! Runtime configuration, from code or from `KRAKEN_*` environment variables (the names the
//! compose files use).

use std::collections::HashMap;
use std::time::Duration;

use crate::error::MessagingError;
use crate::retry::RetryPolicy;

#[derive(Clone, Debug)]
pub struct Config {
    /// Hub `Register.service_name`, Kafka consumer group and event `source`.
    pub service_name: String,
    /// `static://host:port` (prefix optional).
    pub hub_address: String,
    /// Request timeout sent as `timeout_ms` when a call sets none (`SendOptions.timeout` wins).
    /// `None` (default): a protocol v2 hub decides (its per-type override or its default, 5 s);
    /// a legacy hub gets 5 s as before.
    pub hub_request_timeout: Option<Duration>,
    /// Heartbeat period when the hub does not announce one (hubs older than protocol v2; a v2 hub
    /// announces its own, 3 s by default, in `RegisterAck`).
    pub heartbeat_interval: Duration,
    /// Reconnect backoff floor; also where the backoff restarts once a connect reached the handshake.
    pub reconnect_backoff_min: Duration,
    /// Reconnect backoff ceiling (each wait is jittered by ±20 % and never exceeds it).
    pub reconnect_backoff_max: Duration,
    /// Handlers run concurrently; also the permits granted to a hub with flow control
    /// (`Register.initial_permits`), which then queues further requests for this instance.
    pub max_concurrent_handlers: usize,
    /// `Register.load_factor`: this instance's relative capacity (100 = normal). Weights its share
    /// of keyed commands and its load score for unkeyed requests.
    pub hub_load_factor: u32,
    /// `Register.tags`: unkeyed requests from a sender prefer instances whose tags match more of
    /// the sender's tags (for example `zone=eu-1`).
    pub hub_tags: HashMap<String, String>,
    /// Default `Request.priority` per outgoing type (higher is served first when the target's hub
    /// queue is not empty); `SendOptions.priority` wins. Unlisted types send 0.
    pub hub_priorities: HashMap<String, i32>,
    /// Caller-side retry for transient failures (off by default); `SendOptions.retry` wins.
    pub hub_retry: RetryPolicy,
    pub kafka_bootstrap: String,
    pub kafka_topic: String,
    pub consumer_enabled: bool,
    pub consumer_max_attempts: u32,
    /// Must name a database: `mongodb://host:27017/<service>?replicaSet=rs0`.
    pub mongodb_uri: String,
    pub outbox_enabled: bool,
    pub outbox_poll_interval: Duration,
    pub outbox_batch_size: usize,
    pub outbox_max_attempts: u32,
    pub outbox_retention: Duration,
    pub idempotency_retention: Duration,
    /// When set (with `issuer`), forwarded tokens are verified; otherwise read unverified.
    pub jwks_url: Option<String>,
    pub issuer: Option<String>,
    /// Forward the current handler's bearer token on outgoing requests.
    pub forward_jwt: bool,
}

impl Config {
    pub fn new(
        service_name: &str,
        hub_address: &str,
        kafka_bootstrap: &str,
        mongodb_uri: &str,
    ) -> Config {
        Config {
            service_name: service_name.to_string(),
            hub_address: hub_address.to_string(),
            hub_request_timeout: None,
            heartbeat_interval: Duration::from_secs(10),
            reconnect_backoff_min: Duration::from_millis(500),
            reconnect_backoff_max: Duration::from_secs(5),
            max_concurrent_handlers: 256,
            hub_load_factor: 100,
            hub_tags: HashMap::new(),
            hub_priorities: HashMap::new(),
            hub_retry: RetryPolicy::default(),
            kafka_bootstrap: kafka_bootstrap.to_string(),
            kafka_topic: crate::kafka::EVENTS_TOPIC.to_string(),
            consumer_enabled: true,
            consumer_max_attempts: 5,
            mongodb_uri: mongodb_uri.to_string(),
            outbox_enabled: true,
            outbox_poll_interval: Duration::from_millis(200),
            outbox_batch_size: 100,
            outbox_max_attempts: 10,
            outbox_retention: Duration::from_secs(7 * 86_400),
            idempotency_retention: Duration::from_secs(30 * 86_400),
            jwks_url: None,
            issuer: None,
            forward_jwt: true,
        }
    }

    /// `KRAKEN_SERVICE_NAME` and `KRAKEN_MONGODB_URI` are required; `KRAKEN_HUB_ADDRESS` defaults to
    /// `static://kraken-hub:7000`, `KRAKEN_KAFKA_BOOTSTRAP` to `kafka:19092`. Optional:
    /// `KRAKEN_KAFKA_TOPIC`, `KRAKEN_JWKS_URL`, `KRAKEN_ISSUER`, `KRAKEN_HUB_REQUEST_TIMEOUT_MS`,
    /// `KRAKEN_HEARTBEAT_INTERVAL_MS`, `KRAKEN_RECONNECT_BACKOFF_MIN_MS`, `KRAKEN_RECONNECT_BACKOFF_MAX_MS`,
    /// `KRAKEN_MAX_CONCURRENT_HANDLERS`, `KRAKEN_CONSUMER_ENABLED`, `KRAKEN_CONSUMER_MAX_ATTEMPTS`,
    /// `KRAKEN_OUTBOX_ENABLED`, `KRAKEN_OUTBOX_POLL_INTERVAL_MS`, `KRAKEN_OUTBOX_BATCH_SIZE`,
    /// `KRAKEN_OUTBOX_MAX_ATTEMPTS`, `KRAKEN_FORWARD_JWT`, `KRAKEN_HUB_LOAD_FACTOR`,
    /// `KRAKEN_HUB_TAGS` (`k=v,k=v`), `KRAKEN_HUB_PRIORITIES` (`type=n,type=n`),
    /// `KRAKEN_HUB_RETRY_ENABLED`, `KRAKEN_HUB_RETRY_MAX_ATTEMPTS`,
    /// `KRAKEN_HUB_RETRY_INITIAL_BACKOFF_MS`, `KRAKEN_HUB_RETRY_MULTIPLIER`,
    /// `KRAKEN_HUB_RETRY_MAX_BACKOFF_MS`.
    pub fn from_env() -> Result<Config, MessagingError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// [`Config::from_env`] over an arbitrary variable lookup (hermetic in tests).
    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Config, MessagingError> {
        let required = |name: &str| {
            lookup(name)
                .filter(|v| !v.trim().is_empty())
                .ok_or_else(|| MessagingError::InvalidMessage(format!("{name} is required")))
        };
        let optional = |name: &str| lookup(name).filter(|v| !v.trim().is_empty());
        let mut c = Config::new(
            &required("KRAKEN_SERVICE_NAME")?,
            &optional("KRAKEN_HUB_ADDRESS").unwrap_or_else(|| "static://kraken-hub:7000".into()),
            &optional("KRAKEN_KAFKA_BOOTSTRAP").unwrap_or_else(|| "kafka:19092".into()),
            &required("KRAKEN_MONGODB_URI")?,
        );
        if let Some(t) = optional("KRAKEN_KAFKA_TOPIC") {
            c.kafka_topic = t;
        }
        c.jwks_url = optional("KRAKEN_JWKS_URL");
        c.issuer = optional("KRAKEN_ISSUER");
        if let Some(v) = optional("KRAKEN_HUB_REQUEST_TIMEOUT_MS") {
            // 0 means "not chosen", like leaving it unset (the hub decides).
            c.hub_request_timeout =
                Some(millis("KRAKEN_HUB_REQUEST_TIMEOUT_MS", &v)?).filter(|d| !d.is_zero());
        }
        if let Some(v) = optional("KRAKEN_HEARTBEAT_INTERVAL_MS") {
            c.heartbeat_interval = millis("KRAKEN_HEARTBEAT_INTERVAL_MS", &v)?;
        }
        if let Some(v) = optional("KRAKEN_RECONNECT_BACKOFF_MIN_MS") {
            c.reconnect_backoff_min = millis("KRAKEN_RECONNECT_BACKOFF_MIN_MS", &v)?;
        }
        if let Some(v) = optional("KRAKEN_RECONNECT_BACKOFF_MAX_MS") {
            c.reconnect_backoff_max = millis("KRAKEN_RECONNECT_BACKOFF_MAX_MS", &v)?;
        }
        if let Some(v) = optional("KRAKEN_MAX_CONCURRENT_HANDLERS") {
            c.max_concurrent_handlers = number("KRAKEN_MAX_CONCURRENT_HANDLERS", &v)? as usize;
        }
        if let Some(v) = optional("KRAKEN_HUB_LOAD_FACTOR") {
            c.hub_load_factor = number("KRAKEN_HUB_LOAD_FACTOR", &v)?.min(u32::MAX as u64) as u32;
        }
        if let Some(v) = optional("KRAKEN_HUB_TAGS") {
            c.hub_tags = pairs("KRAKEN_HUB_TAGS", &v, |s| Some(s.to_string()))?;
        }
        if let Some(v) = optional("KRAKEN_HUB_PRIORITIES") {
            c.hub_priorities = pairs("KRAKEN_HUB_PRIORITIES", &v, |s| s.parse::<i32>().ok())?;
        }
        if let Some(v) = optional("KRAKEN_HUB_RETRY_ENABLED") {
            c.hub_retry.enabled = flag("KRAKEN_HUB_RETRY_ENABLED", &v)?;
        }
        if let Some(v) = optional("KRAKEN_HUB_RETRY_MAX_ATTEMPTS") {
            c.hub_retry.max_attempts =
                number("KRAKEN_HUB_RETRY_MAX_ATTEMPTS", &v)?.min(u32::MAX as u64) as u32;
        }
        if let Some(v) = optional("KRAKEN_HUB_RETRY_INITIAL_BACKOFF_MS") {
            c.hub_retry.initial_backoff = millis("KRAKEN_HUB_RETRY_INITIAL_BACKOFF_MS", &v)?;
        }
        if let Some(v) = optional("KRAKEN_HUB_RETRY_MULTIPLIER") {
            c.hub_retry.multiplier = v
                .trim()
                .parse::<f64>()
                .ok()
                .filter(|m| m.is_finite() && *m >= 1.0)
                .ok_or_else(|| {
                    MessagingError::InvalidMessage(format!(
                        "KRAKEN_HUB_RETRY_MULTIPLIER must be a number >= 1, got '{v}'"
                    ))
                })?;
        }
        if let Some(v) = optional("KRAKEN_HUB_RETRY_MAX_BACKOFF_MS") {
            c.hub_retry.max_backoff = millis("KRAKEN_HUB_RETRY_MAX_BACKOFF_MS", &v)?;
        }
        if let Some(v) = optional("KRAKEN_CONSUMER_ENABLED") {
            c.consumer_enabled = flag("KRAKEN_CONSUMER_ENABLED", &v)?;
        }
        if let Some(v) = optional("KRAKEN_CONSUMER_MAX_ATTEMPTS") {
            c.consumer_max_attempts = number("KRAKEN_CONSUMER_MAX_ATTEMPTS", &v)? as u32;
        }
        if let Some(v) = optional("KRAKEN_OUTBOX_ENABLED") {
            c.outbox_enabled = flag("KRAKEN_OUTBOX_ENABLED", &v)?;
        }
        if let Some(v) = optional("KRAKEN_OUTBOX_POLL_INTERVAL_MS") {
            c.outbox_poll_interval = millis("KRAKEN_OUTBOX_POLL_INTERVAL_MS", &v)?;
        }
        if let Some(v) = optional("KRAKEN_OUTBOX_BATCH_SIZE") {
            c.outbox_batch_size = number("KRAKEN_OUTBOX_BATCH_SIZE", &v)? as usize;
        }
        if let Some(v) = optional("KRAKEN_OUTBOX_MAX_ATTEMPTS") {
            c.outbox_max_attempts = number("KRAKEN_OUTBOX_MAX_ATTEMPTS", &v)? as u32;
        }
        if let Some(v) = optional("KRAKEN_FORWARD_JWT") {
            c.forward_jwt = flag("KRAKEN_FORWARD_JWT", &v)?;
        }
        c.validate()?;
        Ok(c)
    }

    pub(crate) fn validate(&self) -> Result<(), MessagingError> {
        if self.service_name.trim().is_empty() {
            return Err(MessagingError::InvalidMessage(
                "service_name must not be empty".into(),
            ));
        }
        if self.jwks_url.is_some() && self.issuer.is_none() {
            return Err(MessagingError::InvalidMessage(
                "issuer is required when jwks_url is set".into(),
            ));
        }
        Ok(())
    }
}

fn number(name: &str, v: &str) -> Result<u64, MessagingError> {
    v.trim().parse::<u64>().map_err(|_| {
        MessagingError::InvalidMessage(format!("{name} must be a non-negative integer, got '{v}'"))
    })
}

fn millis(name: &str, v: &str) -> Result<Duration, MessagingError> {
    number(name, v).map(Duration::from_millis)
}

/// `k=v,k=v` (blanks around entries and a trailing comma ignored); `value` parses each value.
fn pairs<T>(
    name: &str,
    v: &str,
    value: impl Fn(&str) -> Option<T>,
) -> Result<HashMap<String, T>, MessagingError> {
    let bad = |entry: &str| {
        MessagingError::InvalidMessage(format!("{name}: entry '{entry}' is not key=value"))
    };
    let mut out = HashMap::new();
    for entry in v.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (k, val) = entry.split_once('=').ok_or_else(|| bad(entry))?;
        let (k, val) = (k.trim(), val.trim());
        if k.is_empty() {
            return Err(bad(entry));
        }
        out.insert(k.to_string(), value(val).ok_or_else(|| bad(entry))?);
    }
    Ok(out)
}

fn flag(name: &str, v: &str) -> Result<bool, MessagingError> {
    match v.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(MessagingError::InvalidMessage(format!(
            "{name} must be true or false, got '{v}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn from(vars: &[(&str, &str)]) -> Config {
        let vars: Vec<(String, String)> = [
            ("KRAKEN_SERVICE_NAME", "svc"),
            ("KRAKEN_MONGODB_URI", "mongodb://m/db"),
        ]
        .iter()
        .chain(vars)
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        Config::from_lookup(|n| {
            vars.iter()
                .rev()
                .find(|(k, _)| k == n)
                .map(|(_, v)| v.clone())
        })
        .unwrap()
    }

    #[test]
    fn zero_request_timeout_means_unset() {
        assert_eq!(from(&[]).hub_request_timeout, None);
        assert_eq!(
            from(&[("KRAKEN_HUB_REQUEST_TIMEOUT_MS", "0")]).hub_request_timeout,
            None
        );
        assert_eq!(
            from(&[("KRAKEN_HUB_REQUEST_TIMEOUT_MS", "2500")]).hub_request_timeout,
            Some(Duration::from_millis(2500))
        );
    }

    #[test]
    fn flow_control_routing_and_retry_defaults() {
        let c = Config::new("svc", "static://hub:7000", "kafka:19092", "mongodb://m/db");
        assert_eq!(c.hub_load_factor, 100);
        assert!(c.hub_tags.is_empty());
        assert!(c.hub_priorities.is_empty());
        assert_eq!(c.hub_retry, crate::retry::RetryPolicy::default());
        assert!(!c.hub_retry.enabled);
    }

    #[test]
    fn flow_control_routing_and_retry_from_env() {
        let c = from(&[
            ("KRAKEN_HUB_LOAD_FACTOR", "250"),
            ("KRAKEN_HUB_TAGS", " zone=eu-1 , tier = gold,"),
            ("KRAKEN_HUB_PRIORITIES", "orders.Cancel=10,reports.Build=-5"),
            ("KRAKEN_HUB_RETRY_ENABLED", "true"),
            ("KRAKEN_HUB_RETRY_MAX_ATTEMPTS", "5"),
            ("KRAKEN_HUB_RETRY_INITIAL_BACKOFF_MS", "50"),
            ("KRAKEN_HUB_RETRY_MULTIPLIER", "1.5"),
            ("KRAKEN_HUB_RETRY_MAX_BACKOFF_MS", "800"),
        ]);
        assert_eq!(c.hub_load_factor, 250);
        assert_eq!(
            c.hub_tags,
            HashMap::from([
                ("zone".to_string(), "eu-1".to_string()),
                ("tier".to_string(), "gold".to_string())
            ])
        );
        assert_eq!(
            c.hub_priorities,
            HashMap::from([
                ("orders.Cancel".to_string(), 10),
                ("reports.Build".to_string(), -5)
            ])
        );
        assert_eq!(
            c.hub_retry,
            crate::retry::RetryPolicy {
                enabled: true,
                max_attempts: 5,
                initial_backoff: Duration::from_millis(50),
                multiplier: 1.5,
                max_backoff: Duration::from_millis(800),
            }
        );
    }

    #[test]
    fn malformed_maps_are_rejected() {
        for (name, value) in [
            ("KRAKEN_HUB_TAGS", "zone"),
            ("KRAKEN_HUB_TAGS", "=x"),
            ("KRAKEN_HUB_PRIORITIES", "t.X=high"),
            ("KRAKEN_HUB_RETRY_MULTIPLIER", "fast"),
        ] {
            let vars = [
                ("KRAKEN_SERVICE_NAME".to_string(), "svc".to_string()),
                (
                    "KRAKEN_MONGODB_URI".to_string(),
                    "mongodb://m/db".to_string(),
                ),
                (name.to_string(), value.to_string()),
            ];
            let result =
                Config::from_lookup(|n| vars.iter().find(|(k, _)| k == n).map(|(_, v)| v.clone()));
            assert!(result.is_err(), "{name}={value} accepted");
        }
    }

    #[test]
    fn hub_defaults_follow_protocol_v2() {
        let c = Config::new("svc", "static://hub:7000", "kafka:19092", "mongodb://m/db");
        // No chosen timeout: the hub decides (per-type override or its default).
        assert_eq!(c.hub_request_timeout, None);
        // F2: the backoff is capped at 5 s.
        assert_eq!(c.reconnect_backoff_min, Duration::from_millis(500));
        assert_eq!(c.reconnect_backoff_max, Duration::from_secs(5));
        // Fallback for hubs that announce no interval (legacy hubs evict after 30 s).
        assert_eq!(c.heartbeat_interval, Duration::from_secs(10));
    }
}
