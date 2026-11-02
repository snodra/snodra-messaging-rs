//! Runtime configuration, from code or from `KRAKEN_*` environment variables (the names the
//! compose files use).

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use crate::error::MessagingError;
use crate::hub::HubTargets;
use crate::retry::RetryPolicy;
use crate::security::{HubTls, SecretString};

/// `Config.hub_stream_credit_timeout` when nothing sets it: the hub's default maximum stream
/// duration (`KRAKEN_HUB_MAX_STREAM_DURATION`, 10 minutes), after which the hub ends any stream
/// itself.
pub const DEFAULT_STREAM_CREDIT_TIMEOUT: Duration = Duration::from_secs(10 * 60);

#[derive(Clone, Debug)]
pub struct Config {
    /// Hub `Register.service_name`, Kafka consumer group and event `source`.
    pub service_name: String,
    /// The hubs to connect to (ADR 0016): `host:port` (`static://` prefix optional), a
    /// comma-separated list of them, or one `dns:///name:port` whose addresses are re-resolved
    /// every 30 s. One address is the single-hub setup.
    pub hub_address: String,
    /// How often an `InstanceStatus` goes to every hub that shares the `instance-status`
    /// capability (default 5 s, at least 1 ms).
    pub hub_status_interval: Duration,
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
    /// Handlers run concurrently, at least 1; also the permits granted to a hub with flow control
    /// (`Register.initial_permits`), which then queues further requests for this instance.
    pub max_concurrent_handlers: usize,
    /// `Register.load_factor`: this instance's relative capacity (100 = normal; 0 is sent as is and
    /// read as 100 by the hub). Weights its share of keyed commands and its load score for unkeyed
    /// requests.
    pub hub_load_factor: u32,
    /// `Register.tags`: unkeyed requests from a sender prefer instances whose tags match more of
    /// the sender's tags (for example `zone=eu-1`).
    pub hub_tags: HashMap<String, String>,
    /// Default `Request.priority` per outgoing type (higher is served first when the target's hub
    /// queue is not empty); `SendOptions.priority` wins. Unlisted types send 0.
    pub hub_priorities: HashMap<String, i32>,
    /// Caller-side retry for transient failures: `None` (the default) is no retry, a policy is in
    /// force as written; `SendOptions.retry` wins per call. Its `max_attempts` must be at least 1
    /// and its multiplier a finite number >= 1 ([`Config::validate`]).
    pub hub_retry: Option<RetryPolicy>,
    /// How long a streaming query handler waits for credit before ending its stream with
    /// `TIMEOUT` `no credit within <d>` and releasing its permits (a hub that forgot the stream
    /// would otherwise leave the handler task waiting for ever). Default 10 minutes, the hub's
    /// default `KRAKEN_HUB_MAX_STREAM_DURATION`; at least 1 ms ([`Config::validate`]).
    pub hub_stream_credit_timeout: Duration,
    /// The most subscription queries (ADR 0017, P4c) this instance serves at once: a
    /// subscription request beyond it is refused with `ReplyEnd{error = RESOURCE_EXHAUSTED
    /// "subscription limit (N) reached"}`. Subscriptions hold no handler permit, so this is
    /// their only bound here (the hub has its own per-instance limit,
    /// `KRAKEN_HUB_MAX_SUBSCRIPTIONS_PER_INSTANCE`, 1024). Default 1024; at least 1
    /// ([`Config::validate`]).
    pub hub_max_subscriptions: usize,
    /// The static token sent in `Register.credentials.token` on every hub stream (ADR 0018;
    /// `KRAKEN_HUB_TOKEN`). Needed when the hub runs with `KRAKEN_HUB_AUTH_MODE=enforce`; ignored
    /// by a hub in `off` mode and by hubs older than P5a. Never logged (`Debug` prints `"***"`).
    /// An empty token is the same as none. Overridden by `hub_token_file` when that is set.
    pub hub_token: Option<SecretString>,
    /// A file holding the token (`KRAKEN_HUB_TOKEN_FILE`), for container secrets. Read at
    /// startup ([`Config::validate`]) and again on every reconnect, so a rotated secret is picked
    /// up without a restart. **It wins over `hub_token` when both are set.** Surrounding
    /// whitespace is trimmed; an empty file is no token.
    pub hub_token_file: Option<PathBuf>,
    /// TLS to the hubs (`KRAKEN_HUB_TLS=true`, with `KRAKEN_HUB_TLS_CA` and
    /// `KRAKEN_HUB_TLS_SERVER_NAME`). `None` (the default): plain text, unless an address starts
    /// with `https://` (then TLS with the platform roots). Needed when the hub sets
    /// `KRAKEN_HUB_TLS_CERT`.
    pub hub_tls: Option<HubTls>,
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
            hub_status_interval: Duration::from_secs(5),
            hub_request_timeout: None,
            heartbeat_interval: Duration::from_secs(10),
            reconnect_backoff_min: Duration::from_millis(500),
            reconnect_backoff_max: Duration::from_secs(5),
            max_concurrent_handlers: 256,
            hub_load_factor: 100,
            hub_tags: HashMap::new(),
            hub_priorities: HashMap::new(),
            hub_retry: None,
            hub_stream_credit_timeout: DEFAULT_STREAM_CREDIT_TIMEOUT,
            hub_max_subscriptions: crate::hub::DEFAULT_MAX_SUBSCRIPTIONS,
            hub_token: None,
            hub_token_file: None,
            hub_tls: None,
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
    /// `KRAKEN_HUB_STATUS_INTERVAL_MS`, `KRAKEN_HEARTBEAT_INTERVAL_MS`, `KRAKEN_RECONNECT_BACKOFF_MIN_MS`, `KRAKEN_RECONNECT_BACKOFF_MAX_MS`,
    /// `KRAKEN_MAX_CONCURRENT_HANDLERS`, `KRAKEN_CONSUMER_ENABLED`, `KRAKEN_CONSUMER_MAX_ATTEMPTS`,
    /// `KRAKEN_OUTBOX_ENABLED`, `KRAKEN_OUTBOX_POLL_INTERVAL_MS`, `KRAKEN_OUTBOX_BATCH_SIZE`,
    /// `KRAKEN_OUTBOX_MAX_ATTEMPTS`, `KRAKEN_FORWARD_JWT`, `KRAKEN_HUB_LOAD_FACTOR`,
    /// `KRAKEN_HUB_TAGS` (`k=v,k=v`), `KRAKEN_HUB_PRIORITIES` (`type=n,type=n`),
    /// `KRAKEN_HUB_RETRY_ENABLED`, `KRAKEN_HUB_RETRY_MAX_ATTEMPTS`,
    /// `KRAKEN_HUB_RETRY_INITIAL_BACKOFF_MS`, `KRAKEN_HUB_RETRY_MULTIPLIER`,
    /// `KRAKEN_HUB_RETRY_MAX_BACKOFF_MS`, `KRAKEN_HUB_STREAM_CREDIT_TIMEOUT_MS`,
    /// `KRAKEN_HUB_TOKEN`, `KRAKEN_HUB_TOKEN_FILE`, `KRAKEN_HUB_TLS` (`true` turns TLS on),
    /// `KRAKEN_HUB_TLS_CA`, `KRAKEN_HUB_TLS_SERVER_NAME` (both need `KRAKEN_HUB_TLS=true`),
    /// `KRAKEN_HUB_MAX_SUBSCRIPTIONS`.
    /// `KRAKEN_HUB_RETRY_ENABLED=true` sets `hub_retry` to the
    /// policy the other `KRAKEN_HUB_RETRY_*` variables describe (defaults: 3 attempts, 100 ms
    /// doubling to 2 s); those are validated whether or not retry is enabled.
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
        if let Some(v) = optional("KRAKEN_HUB_STATUS_INTERVAL_MS") {
            c.hub_status_interval = millis("KRAKEN_HUB_STATUS_INTERVAL_MS", &v)?;
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
        let retry_enabled = match optional("KRAKEN_HUB_RETRY_ENABLED") {
            Some(v) => flag("KRAKEN_HUB_RETRY_ENABLED", &v)?,
            None => false,
        };
        let mut retry = RetryPolicy::default();
        if let Some(v) = optional("KRAKEN_HUB_RETRY_MAX_ATTEMPTS") {
            retry.max_attempts = Some(number("KRAKEN_HUB_RETRY_MAX_ATTEMPTS", &v)?)
                .filter(|&n| n >= 1)
                .ok_or_else(|| {
                    MessagingError::InvalidMessage(format!(
                        "KRAKEN_HUB_RETRY_MAX_ATTEMPTS must be at least 1 (1 = no retry), got '{v}'"
                    ))
                })?
                .min(u32::MAX as u64) as u32;
        }
        if let Some(v) = optional("KRAKEN_HUB_RETRY_INITIAL_BACKOFF_MS") {
            retry.initial_backoff = millis("KRAKEN_HUB_RETRY_INITIAL_BACKOFF_MS", &v)?;
        }
        if let Some(v) = optional("KRAKEN_HUB_RETRY_MULTIPLIER") {
            retry.multiplier = v
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
            retry.max_backoff = millis("KRAKEN_HUB_RETRY_MAX_BACKOFF_MS", &v)?;
        }
        retry.validate()?;
        c.hub_retry = retry_enabled.then_some(retry);
        if let Some(v) = optional("KRAKEN_HUB_STREAM_CREDIT_TIMEOUT_MS") {
            c.hub_stream_credit_timeout = millis("KRAKEN_HUB_STREAM_CREDIT_TIMEOUT_MS", &v)?;
        }
        if let Some(v) = optional("KRAKEN_HUB_MAX_SUBSCRIPTIONS") {
            c.hub_max_subscriptions = number("KRAKEN_HUB_MAX_SUBSCRIPTIONS", &v)? as usize;
        }
        c.hub_token = optional("KRAKEN_HUB_TOKEN").map(|v| SecretString::new(v.trim()));
        c.hub_token_file = optional("KRAKEN_HUB_TOKEN_FILE").map(|v| PathBuf::from(v.trim()));
        let tls_on = match optional("KRAKEN_HUB_TLS") {
            Some(v) => flag("KRAKEN_HUB_TLS", &v)?,
            None => false,
        };
        let tls_ca = optional("KRAKEN_HUB_TLS_CA").map(|v| PathBuf::from(v.trim()));
        let tls_name = optional("KRAKEN_HUB_TLS_SERVER_NAME").map(|v| v.trim().to_string());
        if tls_on {
            c.hub_tls = Some(HubTls {
                ca_file: tls_ca,
                server_name: tls_name,
            });
        } else if tls_ca.is_some() || tls_name.is_some() {
            return Err(MessagingError::InvalidMessage(
                "KRAKEN_HUB_TLS_CA and KRAKEN_HUB_TLS_SERVER_NAME need KRAKEN_HUB_TLS=true".into(),
            ));
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

    /// Rejects what cannot work: an empty service name, a `hub_address` that names no hub (or a
    /// malformed `dns:///` target), a zero `reconnect_backoff_max`, a zero `reconnect_backoff_min` or one above the max, a zero `hub_status_interval`, `jwks_url` without `issuer`,
    /// `max_concurrent_handlers` 0, an invalid `hub_retry` policy, a zero
    /// `hub_stream_credit_timeout`, a zero `hub_max_subscriptions`, a `hub_token_file` that cannot
    /// be read, a `hub_tls` CA file that cannot be read or holds no certificate, an empty
    /// `hub_tls` server name. Called by `from_env` and by `KrakenMessaging::builder`; error
    /// messages never carry the token.
    pub fn validate(&self) -> Result<(), MessagingError> {
        if self.service_name.trim().is_empty() {
            return Err(MessagingError::InvalidMessage(
                "service_name must not be empty".into(),
            ));
        }
        HubTargets::parse(&self.hub_address)?;
        if self.reconnect_backoff_max.is_zero() {
            return Err(MessagingError::InvalidMessage(
                "reconnect_backoff_max (KRAKEN_RECONNECT_BACKOFF_MAX_MS) must be at least 1 ms"
                    .into(),
            ));
        }
        if self.reconnect_backoff_min.is_zero() {
            return Err(MessagingError::InvalidMessage(
                "reconnect_backoff_min (KRAKEN_RECONNECT_BACKOFF_MIN_MS) must be at least 1 ms (0 doubles to 0 forever)"
                    .into(),
            ));
        }
        if self.reconnect_backoff_min > self.reconnect_backoff_max {
            return Err(MessagingError::InvalidMessage(format!(
                "reconnect_backoff_min (KRAKEN_RECONNECT_BACKOFF_MIN_MS, {:?}) must not exceed reconnect_backoff_max (KRAKEN_RECONNECT_BACKOFF_MAX_MS, {:?})",
                self.reconnect_backoff_min, self.reconnect_backoff_max
            )));
        }
        if self.hub_status_interval.is_zero() {
            return Err(MessagingError::InvalidMessage(
                "hub_status_interval (KRAKEN_HUB_STATUS_INTERVAL_MS) must be at least 1 ms".into(),
            ));
        }
        if self.jwks_url.is_some() && self.issuer.is_none() {
            return Err(MessagingError::InvalidMessage(
                "issuer is required when jwks_url is set".into(),
            ));
        }
        if self.max_concurrent_handlers == 0 {
            return Err(MessagingError::InvalidMessage(
                "max_concurrent_handlers (KRAKEN_MAX_CONCURRENT_HANDLERS) must be at least 1"
                    .into(),
            ));
        }
        if let Some(retry) = &self.hub_retry {
            retry.validate()?;
        }
        if self.hub_stream_credit_timeout.is_zero() {
            return Err(MessagingError::InvalidMessage(
                "hub_stream_credit_timeout (KRAKEN_HUB_STREAM_CREDIT_TIMEOUT_MS) must be at least 1 ms"
                    .into(),
            ));
        }
        if self.hub_max_subscriptions == 0 {
            return Err(MessagingError::InvalidMessage(
                "hub_max_subscriptions (KRAKEN_HUB_MAX_SUBSCRIPTIONS) must be at least 1".into(),
            ));
        }
        if let Some(path) = &self.hub_token_file {
            crate::security::read_token_file(path).map_err(|e| {
                MessagingError::InvalidMessage(format!(
                    "hub_token_file (KRAKEN_HUB_TOKEN_FILE) '{}' cannot be read: {e}",
                    path.display()
                ))
            })?;
        }
        if let Some(tls) = &self.hub_tls {
            if tls
                .server_name
                .as_deref()
                .is_some_and(|n| n.trim().is_empty())
            {
                return Err(MessagingError::InvalidMessage(
                    "hub_tls.server_name (KRAKEN_HUB_TLS_SERVER_NAME) must not be empty".into(),
                ));
            }
            if let Some(ca) = &tls.ca_file {
                crate::hub::read_ca_file(ca).map_err(|e| {
                    MessagingError::InvalidMessage(format!(
                        "hub_tls.ca_file (KRAKEN_HUB_TLS_CA) '{}': {e}",
                        ca.display()
                    ))
                })?;
            }
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
        try_from(vars).unwrap()
    }

    fn try_from(vars: &[(&str, &str)]) -> Result<Config, MessagingError> {
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
    }

    fn code_config() -> Config {
        Config::new("svc", "static://hub:7000", "kafka:19092", "mongodb://m/db")
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
    fn stream_credit_timeout_defaults_to_the_hubs_max_stream_duration() {
        assert_eq!(
            code_config().hub_stream_credit_timeout,
            Duration::from_secs(600)
        );
        assert_eq!(
            from(&[]).hub_stream_credit_timeout,
            Duration::from_secs(600)
        );
        assert_eq!(
            from(&[("KRAKEN_HUB_STREAM_CREDIT_TIMEOUT_MS", "2500")]).hub_stream_credit_timeout,
            Duration::from_millis(2500)
        );
        let e = try_from(&[("KRAKEN_HUB_STREAM_CREDIT_TIMEOUT_MS", "0")]).unwrap_err();
        assert!(
            e.to_string()
                .contains("KRAKEN_HUB_STREAM_CREDIT_TIMEOUT_MS"),
            "{e}"
        );
        let mut c = code_config();
        c.hub_stream_credit_timeout = Duration::ZERO;
        assert!(c.validate().is_err(), "zero rejected in code too");
    }

    #[test]
    fn max_subscriptions_defaults_to_the_hubs_limit_and_reads_the_environment() {
        assert_eq!(code_config().hub_max_subscriptions, 1024);
        assert_eq!(from(&[]).hub_max_subscriptions, 1024);
        assert_eq!(
            from(&[("KRAKEN_HUB_MAX_SUBSCRIPTIONS", "5")]).hub_max_subscriptions,
            5
        );
        for value in ["0", "-1", "many"] {
            let e = try_from(&[("KRAKEN_HUB_MAX_SUBSCRIPTIONS", value)]).unwrap_err();
            assert!(
                e.to_string().contains("KRAKEN_HUB_MAX_SUBSCRIPTIONS"),
                "{value}: {e}"
            );
        }
        let mut c = code_config();
        c.hub_max_subscriptions = 0;
        assert!(c.validate().is_err(), "zero rejected in code too");
    }

    #[test]
    fn flow_control_routing_and_retry_defaults() {
        let c = Config::new("svc", "static://hub:7000", "kafka:19092", "mongodb://m/db");
        assert_eq!(c.hub_load_factor, 100);
        assert!(c.hub_tags.is_empty());
        assert!(c.hub_priorities.is_empty());
        assert_eq!(c.hub_retry, None, "no retry unless configured");
        assert_eq!(from(&[]).hub_retry, None);
        assert_eq!(
            from(&[("KRAKEN_HUB_RETRY_ENABLED", "false")]).hub_retry,
            None
        );
        assert_eq!(
            from(&[("KRAKEN_HUB_RETRY_ENABLED", "true")]).hub_retry,
            Some(RetryPolicy::default()),
            "enabled alone: 3 attempts, 100 ms doubling to 2 s"
        );
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
            Some(RetryPolicy {
                max_attempts: 5,
                initial_backoff: Duration::from_millis(50),
                multiplier: 1.5,
                max_backoff: Duration::from_millis(800),
            })
        );
    }

    #[test]
    fn zero_retry_attempts_or_a_bad_multiplier_are_rejected_enabled_or_not() {
        for enabled in [None, Some("false"), Some("true")] {
            for (name, value) in [
                ("KRAKEN_HUB_RETRY_MAX_ATTEMPTS", "0"),
                ("KRAKEN_HUB_RETRY_MULTIPLIER", "0.5"),
                ("KRAKEN_HUB_RETRY_MULTIPLIER", "NaN"),
                ("KRAKEN_HUB_RETRY_MULTIPLIER", "inf"),
                ("KRAKEN_HUB_RETRY_MULTIPLIER", "-2"),
            ] {
                let mut vars = vec![(name, value)];
                vars.extend(enabled.map(|e| ("KRAKEN_HUB_RETRY_ENABLED", e)));
                let result = try_from(&vars);
                assert!(
                    matches!(result, Err(MessagingError::InvalidMessage(ref m)) if m.contains(name)),
                    "{name}={value} with KRAKEN_HUB_RETRY_ENABLED={enabled:?}: {result:?}"
                );
            }
        }
        // One attempt is valid: a configured policy that never retries.
        let one = from(&[
            ("KRAKEN_HUB_RETRY_ENABLED", "true"),
            ("KRAKEN_HUB_RETRY_MAX_ATTEMPTS", "1"),
        ]);
        assert_eq!(one.hub_retry.map(|p| p.max_attempts), Some(1));
    }

    #[test]
    fn validate_rejects_an_invalid_retry_policy_set_in_code() {
        for policy in [
            RetryPolicy {
                max_attempts: 0,
                ..RetryPolicy::default()
            },
            RetryPolicy {
                multiplier: 0.9,
                ..RetryPolicy::default()
            },
            RetryPolicy {
                multiplier: f64::NAN,
                ..RetryPolicy::default()
            },
        ] {
            let c = Config {
                hub_retry: Some(policy.clone()),
                ..code_config()
            };
            assert!(
                matches!(c.validate(), Err(MessagingError::InvalidMessage(_))),
                "{policy:?} accepted"
            );
        }
        let ok = Config {
            hub_retry: Some(RetryPolicy::NONE),
            ..code_config()
        };
        assert!(ok.validate().is_ok());
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
    fn the_reconnect_backoff_must_have_a_positive_max_not_below_the_min() {
        for (min, max) in [("500", "0"), ("6000", "5000"), ("0", "0")] {
            let result = try_from(&[
                ("KRAKEN_RECONNECT_BACKOFF_MIN_MS", min),
                ("KRAKEN_RECONNECT_BACKOFF_MAX_MS", max),
            ]);
            assert!(
                matches!(result, Err(MessagingError::InvalidMessage(ref m)) if m.contains("reconnect_backoff_max")),
                "min={min} max={max}: {result:?}"
            );
        }
        for (min, max) in [
            (Duration::from_millis(500), Duration::ZERO),
            (Duration::from_secs(6), Duration::from_secs(5)),
        ] {
            let in_code = Config {
                reconnect_backoff_min: min,
                reconnect_backoff_max: max,
                ..code_config()
            };
            assert!(
                matches!(in_code.validate(), Err(MessagingError::InvalidMessage(_))),
                "min={min:?} max={max:?} accepted"
            );
        }
        let c = from(&[
            ("KRAKEN_RECONNECT_BACKOFF_MIN_MS", "1000"),
            ("KRAKEN_RECONNECT_BACKOFF_MAX_MS", "1000"),
        ]);
        assert_eq!(c.reconnect_backoff_max, Duration::from_secs(1));
        assert!(code_config().validate().is_ok());
    }

    #[test]
    fn a_zero_reconnect_backoff_min_is_rejected() {
        // 0 doubles to 0 forever: the same hot loop as a zero max.
        let result = try_from(&[
            ("KRAKEN_RECONNECT_BACKOFF_MIN_MS", "0"),
            ("KRAKEN_RECONNECT_BACKOFF_MAX_MS", "5000"),
        ]);
        assert!(
            matches!(result, Err(MessagingError::InvalidMessage(ref m)) if m.contains("reconnect_backoff_min")),
            "{result:?}"
        );
        let in_code = Config {
            reconnect_backoff_min: Duration::ZERO,
            ..code_config()
        };
        assert!(
            matches!(in_code.validate(), Err(MessagingError::InvalidMessage(ref m)) if m.contains("reconnect_backoff_min")),
            "min 0 accepted"
        );
        let one = from(&[("KRAKEN_RECONNECT_BACKOFF_MIN_MS", "1")]);
        assert_eq!(one.reconnect_backoff_min, Duration::from_millis(1));
    }

    #[test]
    fn zero_concurrent_handlers_are_rejected() {
        let result = try_from(&[("KRAKEN_MAX_CONCURRENT_HANDLERS", "0")]);
        assert!(
            matches!(result, Err(MessagingError::InvalidMessage(ref m)) if m.contains("max_concurrent_handlers")),
            "{result:?}"
        );
        let in_code = Config {
            max_concurrent_handlers: 0,
            ..code_config()
        };
        assert!(
            matches!(in_code.validate(), Err(MessagingError::InvalidMessage(_))),
            "max_concurrent_handlers 0 accepted"
        );
        assert_eq!(
            from(&[("KRAKEN_MAX_CONCURRENT_HANDLERS", "1")]).max_concurrent_handlers,
            1
        );
    }

    #[test]
    fn a_negative_load_factor_is_rejected_and_zero_is_kept_for_the_hub() {
        for value in ["-1", "-100"] {
            let result = try_from(&[("KRAKEN_HUB_LOAD_FACTOR", value)]);
            assert!(
                matches!(result, Err(MessagingError::InvalidMessage(ref m)) if m.contains("KRAKEN_HUB_LOAD_FACTOR")),
                "{value}: {result:?}"
            );
        }
        // 0 is sent as is; the hub reads it as 100.
        assert_eq!(from(&[("KRAKEN_HUB_LOAD_FACTOR", "0")]).hub_load_factor, 0);
    }

    #[test]
    fn hub_address_takes_a_list_or_a_dns_name_and_a_status_interval() {
        let c = from(&[]);
        assert_eq!(c.hub_address, "static://kraken-hub:7000");
        assert_eq!(c.hub_status_interval, Duration::from_secs(5));
        let c = from(&[
            ("KRAKEN_HUB_ADDRESS", "static://hub:7000,hub-2:7000"),
            ("KRAKEN_HUB_STATUS_INTERVAL_MS", "2500"),
        ]);
        assert_eq!(c.hub_address, "static://hub:7000,hub-2:7000");
        assert_eq!(c.hub_status_interval, Duration::from_millis(2500));
        assert_eq!(
            from(&[("KRAKEN_HUB_ADDRESS", "dns:///kraken-hub:7000")]).hub_address,
            "dns:///kraken-hub:7000"
        );
        for (name, value) in [
            ("KRAKEN_HUB_ADDRESS", "dns:///kraken-hub"),
            ("KRAKEN_HUB_ADDRESS", "hub:7000,dns:///kraken-hub:7000"),
            ("KRAKEN_HUB_ADDRESS", ","),
            ("KRAKEN_HUB_STATUS_INTERVAL_MS", "0"),
            ("KRAKEN_HUB_STATUS_INTERVAL_MS", "soon"),
        ] {
            let result = try_from(&[(name, value)]);
            assert!(
                matches!(result, Err(MessagingError::InvalidMessage(_))),
                "{name}={value}: {result:?}"
            );
        }
        let in_code = Config {
            hub_address: " , ".into(),
            ..code_config()
        };
        assert!(in_code.validate().is_err(), "an empty hub list accepted");
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

    fn secret_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("kraken-cfg-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn the_token_comes_from_the_environment_and_never_prints() {
        assert!(from(&[]).hub_token.is_none());
        assert!(from(&[("KRAKEN_HUB_TOKEN", "  ")]).hub_token.is_none());
        let c = from(&[("KRAKEN_HUB_TOKEN", "s3cr3t-token-value\n")]);
        assert_eq!(c.hub_token.as_ref().unwrap().expose(), "s3cr3t-token-value");
        for printed in [format!("{c:?}"), format!("{c:#?}")] {
            assert!(!printed.contains("s3cr3t-token-value"), "{printed}");
            assert!(printed.contains("\"***\""), "{printed}");
        }
    }

    #[test]
    fn the_token_file_is_read_by_validate_and_may_be_missing_only_if_unset() {
        let dir = secret_dir();
        let path = dir.join("token");
        let missing = try_from(&[("KRAKEN_HUB_TOKEN_FILE", path.to_str().unwrap())]);
        assert!(
            matches!(missing, Err(MessagingError::InvalidMessage(ref m)) if m.contains("KRAKEN_HUB_TOKEN_FILE")),
            "{missing:?}"
        );
        std::fs::write(&path, "from-the-file\n").unwrap();
        let c = from(&[("KRAKEN_HUB_TOKEN_FILE", path.to_str().unwrap())]);
        assert_eq!(c.hub_token_file.as_deref(), Some(path.as_path()));
        assert!(
            c.hub_token.is_none(),
            "the file is read per connect, not stored here"
        );
        assert!(!format!("{c:?}").contains("from-the-file"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn tls_is_off_unless_asked_and_its_options_need_it() {
        assert_eq!(from(&[]).hub_tls, None);
        assert_eq!(from(&[("KRAKEN_HUB_TLS", "false")]).hub_tls, None);
        assert_eq!(
            from(&[("KRAKEN_HUB_TLS", "true")]).hub_tls,
            Some(HubTls::default())
        );
        let dir = secret_dir();
        let ca = dir.join("ca.pem");
        std::fs::write(
            &ca,
            "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        )
        .unwrap();
        let c = from(&[
            ("KRAKEN_HUB_TLS", "true"),
            ("KRAKEN_HUB_TLS_CA", ca.to_str().unwrap()),
            ("KRAKEN_HUB_TLS_SERVER_NAME", "kraken-hub"),
        ]);
        assert_eq!(
            c.hub_tls,
            Some(HubTls {
                ca_file: Some(ca.clone()),
                server_name: Some("kraken-hub".into())
            })
        );
        for vars in [
            vec![("KRAKEN_HUB_TLS_CA", ca.to_str().unwrap())],
            vec![("KRAKEN_HUB_TLS_SERVER_NAME", "kraken-hub")],
            vec![("KRAKEN_HUB_TLS", "maybe")],
            vec![
                ("KRAKEN_HUB_TLS", "true"),
                ("KRAKEN_HUB_TLS_CA", "/no/such/ca.pem"),
            ],
        ] {
            assert!(
                matches!(try_from(&vars), Err(MessagingError::InvalidMessage(_))),
                "{vars:?} accepted"
            );
        }
        // A CA file without a certificate is a startup error, not a handshake failure later.
        let junk = dir.join("junk.pem");
        std::fs::write(&junk, "not a certificate").unwrap();
        assert!(
            try_from(&[
                ("KRAKEN_HUB_TLS", "true"),
                ("KRAKEN_HUB_TLS_CA", junk.to_str().unwrap())
            ])
            .is_err()
        );
        let blank_name = Config {
            hub_tls: Some(HubTls {
                server_name: Some(" ".into()),
                ..HubTls::default()
            }),
            ..code_config()
        };
        assert!(blank_name.validate().is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
