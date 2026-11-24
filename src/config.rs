//! Runtime configuration, from code or from `SNODRA_*` environment variables (the names the
//! compose files use).

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use crate::error::MessagingError;
use crate::hub::HubTargets;
use crate::retry::RetryPolicy;
use crate::security::{HubOAuth2, HubTls, SecretString};

/// `Config.hub_stream_credit_timeout` when nothing sets it: the hub's default maximum stream
/// duration (`SNODRA_SERVER_MAX_STREAM_DURATION`, 10 minutes), after which the hub ends any stream
/// itself.
pub const DEFAULT_STREAM_CREDIT_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// What happens to a record an event processor cannot handle
/// (`SNODRA_EVENTS_DEAD_LETTER_MODE`, case-insensitive).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum DeadLetterMode {
    /// Parked in the `dead_letters` collection, sequenced per key, with the admin API
    /// ([`SnodraMessaging::dead_letters`](crate::SnodraMessaging::dead_letters)). The default.
    #[default]
    Collection,
    /// Published to the event store's dead-letter source (`<source>.dlt`) with `snodra-dlt-*`
    /// headers; it belongs to the operator's tooling. Needs an event store with the capability
    /// `native-dead-letter`.
    EventStore,
    /// Logged and skipped: nothing is kept.
    Log,
}

impl DeadLetterMode {
    /// The mode's configuration name, also `ProcessorStatus.dead_letter_mode`.
    pub fn as_str(self) -> &'static str {
        match self {
            DeadLetterMode::Collection => "collection",
            DeadLetterMode::EventStore => "event-store",
            DeadLetterMode::Log => "log",
        }
    }

    fn parse(name: &str, v: &str) -> Result<DeadLetterMode, MessagingError> {
        match v.trim().to_ascii_lowercase().as_str() {
            "collection" => Ok(DeadLetterMode::Collection),
            "event-store" => Ok(DeadLetterMode::EventStore),
            "log" => Ok(DeadLetterMode::Log),
            "topic" => Err(MessagingError::InvalidMessage(format!(
                "{name}: the mode `topic` is now `event-store`, got '{v}'"
            ))),
            _ => Err(MessagingError::InvalidMessage(format!(
                "{name} must be collection, event-store or log, got '{v}'"
            ))),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    /// Hub `Register.service_name`, event `source`, and the base of the event processors'
    /// subscription names (`<service_name>.<processor>`, `default` included).
    pub service_name: String,
    /// This instance's id: its hub `Register.instance_id` and the `service.instance.id` the
    /// application gives its meter provider's resource. A fresh UUID per `Config`
    /// unless set (`SNODRA_INSTANCE_ID`, e.g. a pod name); unique among the service's instances.
    pub instance_id: String,
    /// The hubs to connect to: `host:port` (`static://` prefix optional), a
    /// comma-separated list of them, or one `dns:///name:port` whose addresses are re-resolved
    /// every 30 s. One address is the single-hub setup.
    pub hub_address: String,
    /// Whether the service connects to the hubs (`SNODRA_SERVER_ENABLED`, default true). Off, it
    /// never connects (`hub_address` must still parse, nothing dials it): events, processors,
    /// dead letters, sagas and idempotent handlers work, a command or unary query of a type the
    /// service handles itself is still answered in-process (unless
    /// [`hub_always_route_through_hub`](Self::hub_always_route_through_hub)), and every other
    /// command, query and subscription sent fails at once with `HubUnavailable` naming the
    /// setting. The saga relay still delivers a command type this instance handles to its own
    /// handler; any other saga command goes into the relay's retries. For tests and local runs
    /// (`testing::InMemoryStores` turns it off).
    pub hub_enabled: bool,
    /// Send every command and query through the hub, also one of a type this instance handles
    /// itself (`SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB`, default false). Off: such a request (a
    /// command, or a unary query of a type not registered as shared) is answered in-process by
    /// this instance's handler, without the hub, and its routing key is ignored. On: the hub
    /// routes it like any other request, so a routing key keeps its instance affinity across the
    /// instances of this service, and the request gets the hub's flow control, priorities,
    /// deadline and metrics. Covers the saga relay's commands too. No effect while the hub is
    /// turned off ([`hub_enabled`](Self::hub_enabled) false): own types are then answered
    /// in-process.
    pub hub_always_route_through_hub: bool,
    /// How often an `InstanceStatus` goes to every hub (default 5 s, at least 1 ms).
    pub hub_status_interval: Duration,
    /// Request timeout sent as `timeout_ms` when a call sets none (`SendOptions.timeout` wins).
    /// `None` (default): the hub decides (its per-type override or its default, 5 s). It is also
    /// how long a call waits for a hub while none can take it (5 s when `None`).
    pub hub_request_timeout: Option<Duration>,
    /// Reconnect backoff floor; also where the backoff restarts once a connect reached the handshake.
    pub reconnect_backoff_min: Duration,
    /// Reconnect backoff ceiling (each wait is jittered by ±20 % and never exceeds it).
    pub reconnect_backoff_max: Duration,
    /// Handlers run concurrently, at least 1; also the permits granted to the hubs
    /// (`Register.initial_permits`), which then queue further requests for this instance.
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
    /// default `SNODRA_SERVER_MAX_STREAM_DURATION`; at least 1 ms ([`Config::validate`]).
    pub hub_stream_credit_timeout: Duration,
    /// The most subscription queries this instance serves at once: a
    /// subscription request beyond it is refused with `ReplyEnd{error = RESOURCE_EXHAUSTED
    /// "subscription limit (N) reached"}`. Subscriptions hold no handler permit, so this is
    /// their only bound here (the hub has its own per-instance limit,
    /// `SNODRA_SERVER_MAX_SUBSCRIPTIONS_PER_INSTANCE`, 1024). Default 1024; at least 1
    /// ([`Config::validate`]).
    pub hub_max_subscriptions: usize,
    /// The static token sent in `Register.credentials.token` on every hub stream
    /// (`SNODRA_SERVER_TOKEN`). Needed when the hub runs with `SNODRA_SERVER_AUTH_MODE=enforce`;
    /// ignored by a hub in `off` mode. Never logged (`Debug` prints `"***"`). An empty token is the
    /// same as none. Overridden by `hub_token_file` when that is set.
    pub hub_token: Option<SecretString>,
    /// A file holding the token (`SNODRA_SERVER_TOKEN_FILE`), for container secrets. Read at
    /// startup ([`Config::validate`]) and again on every reconnect, so a rotated secret is picked
    /// up without a restart. **It wins over `hub_token` when both are set.** Surrounding
    /// whitespace is trimmed; a file empty at startup is an error ([`Config::validate`]), one
    /// found empty at a reconnect keeps the last token read.
    pub hub_token_file: Option<PathBuf>,
    /// TLS to the hubs (`SNODRA_SERVER_TLS=true`, with `SNODRA_SERVER_TLS_CA` and
    /// `SNODRA_SERVER_TLS_SERVER_NAME`). `None` (the default): plain text, unless an address starts
    /// with `https://` (then TLS with the platform roots). Needed when the hub sets
    /// `SNODRA_SERVER_TLS_CERT`.
    pub hub_tls: Option<HubTls>,
    /// Authenticate to the hubs with an OAuth2 client-credentials access token (JWT) instead of a
    /// static token (`SNODRA_SERVER_OAUTH2_TOKEN_URI` set makes it `Some`). One token
    /// provider serves every hub connection and refreshes the token before it expires. Mutually
    /// exclusive with `hub_token` / `hub_token_file` ([`Config::validate`]).
    pub hub_oauth2: Option<HubOAuth2>,
    /// The event store's source the service publishes its events to and its processors read
    /// (`SNODRA_EVENTS_SOURCE`, default `snodra.events`).
    pub events_source: String,
    pub consumer_enabled: bool,
    pub consumer_max_attempts: u32,
    /// Where a record goes that an event processor cannot handle (`SNODRA_EVENTS_DEAD_LETTER_MODE`,
    /// default `collection`).
    pub dead_letter_mode: DeadLetterMode,
    /// How long a dead letter is kept (`SNODRA_EVENTS_DEAD_LETTER_RETENTION_MS`, default 30 d, at
    /// least 1 s): the TTL index on `expireAt` removes it then, which unblocks its key.
    pub dead_letter_retention: Duration,
    pub outbox_enabled: bool,
    pub outbox_poll_interval: Duration,
    pub outbox_batch_size: usize,
    pub outbox_max_attempts: u32,
    /// How long a batch the relay claims stays its own (`SNODRA_OUTBOX_LEASE_MS`, default 2 min):
    /// longer than one send can take ([`OUTBOX_SEND_TIMEOUT`](crate::events::OUTBOX_SEND_TIMEOUT),
    /// 40 s; a startup error otherwise). The relay stops a batch before its lease could run out
    /// under a send, and its next claim renews what it still holds.
    pub outbox_lease: Duration,
    /// Whether `start()` prepares the message store's schema (`SNODRA_STORE_INIT_SCHEMA`, default
    /// true): the MongoDB store's indexes, an SQL
    /// store's tables. `false` when the schema is managed elsewhere (migrations).
    pub store_init_schema: bool,
    /// The retentions (`outbox_retention`, `idempotency_retention`, `sagas_*_retention`,
    /// `deadlines_retention`, `external_calls_retention`) are the message store's to apply as it
    /// can (the MongoDB add-on's TTL indexes, built from this config); the core only
    /// reads `dead_letter_retention` (a letter's expiry).
    pub outbox_retention: Duration,
    pub idempotency_retention: Duration,
    /// When set (with `issuer`), forwarded tokens are verified and their callers are identities.
    /// Without it a forwarded token is never read: every caller is anonymous (the token is still
    /// forwarded on), and a handler with a guard is a startup error.
    pub jwks_url: Option<String>,
    pub issuer: Option<String>,
    /// Forward the current handler's bearer token on outgoing requests.
    pub forward_jwt: bool,
    /// Where a forwarded token's roles come from (`SNODRA_SECURITY_ROLES_CLAIM`): a dotted claim
    /// path, or `scope`. `roles` by default; `realm_access.roles` for providers that nest them, `groups` for
    /// providers that put them there.
    pub security_roles_claim: String,
    /// Sagas (`SNODRA_SAGAS_ENABLED`, default true): with `false` a service that
    /// registers sagas starts without their runtime, relay and scheduler (logged at warn).
    pub sagas_enabled: bool,
    /// Attempts of one (event, saga type) before the event is parked in `saga_failures`
    /// (`SNODRA_SAGAS_MAX_ATTEMPTS`, default 5; 200 ms doubling to 5 s between them).
    pub sagas_max_attempts: u32,
    /// How long an ended saga instance is kept (`SNODRA_SAGAS_ENDED_RETENTION_MS`, default 30 d).
    pub sagas_ended_retention: Duration,
    /// How long a dispatched `saga_dispatch` entry is kept (`SNODRA_SAGAS_DISPATCH_RETENTION_MS`,
    /// default 7 d; an entry that failed for good is kept by `sagas_failed_retention`).
    pub sagas_dispatch_retention: Duration,
    /// How long what failed for good is kept (`SNODRA_SAGAS_FAILED_RETENTION_MS`, default 30 d):
    /// `saga_dispatch` entries given up on (by `failedAt`) and parked events in `saga_failures`
    /// (by `lastFailedAt`, which a retry that fails again moves on).
    pub sagas_failed_retention: Duration,
    /// `SNODRA_SAGAS_RELAY_POLL_INTERVAL_MS`, default 500 ms.
    pub sagas_relay_poll_interval: Duration,
    /// How long a saga's dispatch lease holds (`SNODRA_SAGAS_RELAY_LEASE_MS`, default 30 s).
    pub sagas_relay_lease: Duration,
    /// Attempts of a saga command in all before `EXHAUSTED` (`SNODRA_SAGAS_RELAY_MAX_ATTEMPTS`,
    /// default 10).
    pub sagas_relay_max_attempts: u32,
    /// `SNODRA_SAGAS_RELAY_INITIAL_BACKOFF_MS`, default 1 s (doubling).
    pub sagas_relay_initial_backoff: Duration,
    /// `SNODRA_SAGAS_RELAY_MAX_BACKOFF_MS`, default 5 min.
    pub sagas_relay_max_backoff: Duration,
    /// `SNODRA_DEADLINES_POLL_INTERVAL_MS`, default 1 s.
    pub deadlines_poll_interval: Duration,
    /// How long a claimed deadline stays reserved (`SNODRA_DEADLINES_LEASE_MS`, default 30 s).
    pub deadlines_lease: Duration,
    /// How long a fired or cancelled deadline is kept (`SNODRA_DEADLINES_RETENTION_MS`, default
    /// 7 d).
    pub deadlines_retention: Duration,
    /// How long an idempotent call may take before its outcome counts as unknown
    /// (`SNODRA_EXTERNAL_CALLS_LEASE_MS`, default 5 min).
    pub external_calls_lease: Duration,
    /// How long an `external_calls` record is kept (`SNODRA_EXTERNAL_CALLS_RETENTION_MS`,
    /// default 30 d).
    pub external_calls_retention: Duration,
}

impl Config {
    pub fn new(service_name: &str, hub_address: &str) -> Config {
        Config {
            service_name: service_name.to_string(),
            instance_id: uuid::Uuid::new_v4().to_string(),
            hub_address: hub_address.to_string(),
            hub_enabled: true,
            hub_always_route_through_hub: false,
            hub_status_interval: Duration::from_secs(5),
            hub_request_timeout: None,
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
            hub_oauth2: None,
            events_source: crate::events::EVENTS_SOURCE.to_string(),
            consumer_enabled: true,
            consumer_max_attempts: 5,
            dead_letter_mode: DeadLetterMode::Collection,
            dead_letter_retention: Duration::from_secs(30 * 86_400),
            outbox_enabled: true,
            outbox_poll_interval: Duration::from_millis(200),
            outbox_batch_size: 100,
            outbox_max_attempts: 10,
            outbox_lease: Duration::from_secs(120),
            store_init_schema: true,
            outbox_retention: Duration::from_secs(7 * 86_400),
            idempotency_retention: Duration::from_secs(30 * 86_400),
            jwks_url: None,
            issuer: None,
            forward_jwt: true,
            security_roles_claim: "roles".to_string(),
            sagas_enabled: true,
            sagas_max_attempts: 5,
            sagas_ended_retention: Duration::from_secs(30 * 86_400),
            sagas_dispatch_retention: Duration::from_secs(7 * 86_400),
            sagas_failed_retention: Duration::from_secs(30 * 86_400),
            sagas_relay_poll_interval: Duration::from_millis(500),
            sagas_relay_lease: Duration::from_secs(30),
            sagas_relay_max_attempts: 10,
            sagas_relay_initial_backoff: Duration::from_secs(1),
            sagas_relay_max_backoff: Duration::from_secs(300),
            deadlines_poll_interval: Duration::from_secs(1),
            deadlines_lease: Duration::from_secs(30),
            deadlines_retention: Duration::from_secs(7 * 86_400),
            external_calls_lease: Duration::from_secs(300),
            external_calls_retention: Duration::from_secs(30 * 86_400),
        }
    }

    /// The dispatch relay's settings: the `sagas_relay_*` fields, with fixed
    /// values: multiplier 2, parallelism 4 and batch size 100.
    pub fn relay_settings(&self) -> crate::saga::RelaySettings {
        crate::saga::RelaySettings {
            poll_interval: self.sagas_relay_poll_interval,
            lease: self.sagas_relay_lease,
            max_attempts: self.sagas_relay_max_attempts,
            initial_backoff: self.sagas_relay_initial_backoff,
            max_backoff: self.sagas_relay_max_backoff,
            ..crate::saga::RelaySettings::default()
        }
    }

    /// The deadline scheduler's settings: the `deadlines_*` fields (batch size 100).
    pub fn deadline_settings(&self) -> crate::saga::DeadlineSettings {
        crate::saga::DeadlineSettings {
            poll_interval: self.deadlines_poll_interval,
            lease: self.deadlines_lease,
            ..crate::saga::DeadlineSettings::default()
        }
    }

    /// The saga runtime's retry: `sagas_max_attempts`, 200 ms doubling to 5 s.
    pub fn saga_retry(&self) -> crate::saga::SagaRetry {
        crate::saga::SagaRetry::new(RetryPolicy {
            max_attempts: self.sagas_max_attempts,
            ..crate::saga::SagaRetry::default_policy()
        })
    }

    /// The idempotent handlers' settings.
    pub fn external_call_settings(&self) -> crate::external::ExternalCallSettings {
        crate::external::ExternalCallSettings {
            lease: self.external_calls_lease,
        }
    }

    /// `SNODRA_SERVICE_NAME` is required; `SNODRA_SERVER_ADDRESS` defaults to
    /// `static://snodra-server:7000`. Optional:
    /// `SNODRA_INSTANCE_ID`, `SNODRA_SERVER_ENABLED`, `SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB`, `SNODRA_EVENTS_SOURCE`, `SNODRA_JWKS_URL`, `SNODRA_ISSUER`, `SNODRA_SERVER_REQUEST_TIMEOUT_MS`,
    /// `SNODRA_SERVER_STATUS_INTERVAL_MS`, `SNODRA_RECONNECT_BACKOFF_MIN_MS`,
    /// `SNODRA_RECONNECT_BACKOFF_MAX_MS`,
    /// `SNODRA_MAX_CONCURRENT_HANDLERS`, `SNODRA_CONSUMER_ENABLED`, `SNODRA_CONSUMER_MAX_ATTEMPTS`,
    /// `SNODRA_EVENTS_DEAD_LETTER_MODE` (`collection`, `event-store` or `log`),
    /// `SNODRA_EVENTS_DEAD_LETTER_RETENTION_MS`,
    /// `SNODRA_OUTBOX_ENABLED`, `SNODRA_OUTBOX_POLL_INTERVAL_MS`, `SNODRA_OUTBOX_BATCH_SIZE`,
    /// `SNODRA_OUTBOX_MAX_ATTEMPTS`, `SNODRA_OUTBOX_LEASE_MS`, `SNODRA_STORE_INIT_SCHEMA`,
    /// `SNODRA_FORWARD_JWT`, `SNODRA_SERVER_LOAD_FACTOR`,
    /// `SNODRA_SERVER_TAGS` (`k=v,k=v`), `SNODRA_SERVER_PRIORITIES` (`type=n,type=n`),
    /// `SNODRA_SERVER_RETRY_ENABLED`, `SNODRA_SERVER_RETRY_MAX_ATTEMPTS`,
    /// `SNODRA_SERVER_RETRY_INITIAL_BACKOFF_MS`, `SNODRA_SERVER_RETRY_MULTIPLIER`,
    /// `SNODRA_SERVER_RETRY_MAX_BACKOFF_MS`, `SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS`,
    /// `SNODRA_SERVER_TOKEN`, `SNODRA_SERVER_TOKEN_FILE`, `SNODRA_SERVER_TLS` (`true` turns TLS on),
    /// `SNODRA_SERVER_TLS_CA`, `SNODRA_SERVER_TLS_SERVER_NAME` (both need `SNODRA_SERVER_TLS=true`),
    /// `SNODRA_SERVER_TLS_CLIENT_CERT` and `SNODRA_SERVER_TLS_CLIENT_KEY` (a client certificate for
    /// mutual TLS, both or neither; they turn TLS on and exclude a hub token and OAuth2),
    /// `SNODRA_SERVER_MAX_SUBSCRIPTIONS`, `SNODRA_SERVER_OAUTH2_TOKEN_URI` (turns JWT
    /// authentication on; then `SNODRA_SERVER_OAUTH2_CLIENT_SECRET` or
    /// `SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE` is needed, and `SNODRA_SERVER_OAUTH2_CLIENT_ID`
    /// defaults to the service name), `SNODRA_SERVER_OAUTH2_SCOPE`,
    /// `SNODRA_SERVER_OAUTH2_CA`; the saga settings `SNODRA_SAGAS_ENABLED`,
    /// `SNODRA_SAGAS_MAX_ATTEMPTS`, `SNODRA_SAGAS_ENDED_RETENTION_MS`,
    /// `SNODRA_SAGAS_DISPATCH_RETENTION_MS`, `SNODRA_SAGAS_FAILED_RETENTION_MS`,
    /// `SNODRA_SAGAS_RELAY_POLL_INTERVAL_MS`,
    /// `SNODRA_SAGAS_RELAY_LEASE_MS`, `SNODRA_SAGAS_RELAY_MAX_ATTEMPTS`,
    /// `SNODRA_SAGAS_RELAY_INITIAL_BACKOFF_MS`, `SNODRA_SAGAS_RELAY_MAX_BACKOFF_MS`,
    /// `SNODRA_DEADLINES_POLL_INTERVAL_MS`, `SNODRA_DEADLINES_LEASE_MS`,
    /// `SNODRA_DEADLINES_RETENTION_MS`, `SNODRA_EXTERNAL_CALLS_LEASE_MS`,
    /// `SNODRA_EXTERNAL_CALLS_RETENTION_MS`.
    /// `SNODRA_SERVER_RETRY_ENABLED=true` sets `hub_retry` to the
    /// policy the other `SNODRA_SERVER_RETRY_*` variables describe (defaults: 3 attempts, 100 ms
    /// doubling to 2 s); those are validated whether or not retry is enabled.
    /// A removed setting still set (`SNODRA_KAFKA_TOPIC`, `SNODRA_KAFKA_DEAD_LETTER_MODE`,
    /// `SNODRA_KAFKA_DEAD_LETTER_RETENTION_MS`) is an error naming every one and its replacement,
    /// and so is the old dead-letter mode `topic` (now `event-store`).
    pub fn from_env() -> Result<Config, MessagingError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// [`Config::from_env`] over an arbitrary variable lookup (hermetic in tests).
    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Config, MessagingError> {
        removed_settings(&lookup)?;
        let required = |name: &str| {
            lookup(name)
                .filter(|v| !v.trim().is_empty())
                .ok_or_else(|| MessagingError::InvalidMessage(format!("{name} is required")))
        };
        let optional = |name: &str| lookup(name).filter(|v| !v.trim().is_empty());
        let mut c = Config::new(
            &required("SNODRA_SERVICE_NAME")?,
            &optional("SNODRA_SERVER_ADDRESS")
                .unwrap_or_else(|| "static://snodra-server:7000".into()),
        );
        if let Some(id) = optional("SNODRA_INSTANCE_ID") {
            c.instance_id = id;
        }
        if let Some(v) = optional("SNODRA_SERVER_ENABLED") {
            c.hub_enabled = flag("SNODRA_SERVER_ENABLED", &v)?;
        }
        if let Some(v) = optional("SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB") {
            c.hub_always_route_through_hub = flag("SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB", &v)?;
        }
        if let Some(t) = optional("SNODRA_EVENTS_SOURCE") {
            c.events_source = t;
        }
        if let Some(v) = optional("SNODRA_SECURITY_ROLES_CLAIM") {
            c.security_roles_claim = v;
        }
        c.jwks_url = optional("SNODRA_JWKS_URL");
        c.issuer = optional("SNODRA_ISSUER");
        if let Some(v) = optional("SNODRA_SERVER_REQUEST_TIMEOUT_MS") {
            // 0 means "not chosen", like leaving it unset (the hub decides).
            c.hub_request_timeout =
                Some(millis("SNODRA_SERVER_REQUEST_TIMEOUT_MS", &v)?).filter(|d| !d.is_zero());
        }
        if let Some(v) = optional("SNODRA_SERVER_STATUS_INTERVAL_MS") {
            c.hub_status_interval = millis("SNODRA_SERVER_STATUS_INTERVAL_MS", &v)?;
        }
        if let Some(v) = optional("SNODRA_RECONNECT_BACKOFF_MIN_MS") {
            c.reconnect_backoff_min = millis("SNODRA_RECONNECT_BACKOFF_MIN_MS", &v)?;
        }
        if let Some(v) = optional("SNODRA_RECONNECT_BACKOFF_MAX_MS") {
            c.reconnect_backoff_max = millis("SNODRA_RECONNECT_BACKOFF_MAX_MS", &v)?;
        }
        if let Some(v) = optional("SNODRA_MAX_CONCURRENT_HANDLERS") {
            c.max_concurrent_handlers = number("SNODRA_MAX_CONCURRENT_HANDLERS", &v)? as usize;
        }
        if let Some(v) = optional("SNODRA_SERVER_LOAD_FACTOR") {
            c.hub_load_factor =
                number("SNODRA_SERVER_LOAD_FACTOR", &v)?.min(u32::MAX as u64) as u32;
        }
        if let Some(v) = optional("SNODRA_SERVER_TAGS") {
            c.hub_tags = pairs("SNODRA_SERVER_TAGS", &v, |s| Some(s.to_string()))?;
        }
        if let Some(v) = optional("SNODRA_SERVER_PRIORITIES") {
            c.hub_priorities = pairs("SNODRA_SERVER_PRIORITIES", &v, |s| s.parse::<i32>().ok())?;
        }
        let retry_enabled = match optional("SNODRA_SERVER_RETRY_ENABLED") {
            Some(v) => flag("SNODRA_SERVER_RETRY_ENABLED", &v)?,
            None => false,
        };
        let mut retry = RetryPolicy::default();
        if let Some(v) = optional("SNODRA_SERVER_RETRY_MAX_ATTEMPTS") {
            retry.max_attempts = Some(number("SNODRA_SERVER_RETRY_MAX_ATTEMPTS", &v)?)
                .filter(|&n| n >= 1)
                .ok_or_else(|| {
                    MessagingError::InvalidMessage(format!(
                        "SNODRA_SERVER_RETRY_MAX_ATTEMPTS must be at least 1 (1 = no retry), got '{v}'"
                    ))
                })?
                .min(u32::MAX as u64) as u32;
        }
        if let Some(v) = optional("SNODRA_SERVER_RETRY_INITIAL_BACKOFF_MS") {
            retry.initial_backoff = millis("SNODRA_SERVER_RETRY_INITIAL_BACKOFF_MS", &v)?;
        }
        if let Some(v) = optional("SNODRA_SERVER_RETRY_MULTIPLIER") {
            retry.multiplier = v
                .trim()
                .parse::<f64>()
                .ok()
                .filter(|m| m.is_finite() && *m >= 1.0)
                .ok_or_else(|| {
                    MessagingError::InvalidMessage(format!(
                        "SNODRA_SERVER_RETRY_MULTIPLIER must be a number >= 1, got '{v}'"
                    ))
                })?;
        }
        if let Some(v) = optional("SNODRA_SERVER_RETRY_MAX_BACKOFF_MS") {
            retry.max_backoff = millis("SNODRA_SERVER_RETRY_MAX_BACKOFF_MS", &v)?;
        }
        retry.validate()?;
        c.hub_retry = retry_enabled.then_some(retry);
        if let Some(v) = optional("SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS") {
            c.hub_stream_credit_timeout = millis("SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS", &v)?;
        }
        if let Some(v) = optional("SNODRA_SERVER_MAX_SUBSCRIPTIONS") {
            c.hub_max_subscriptions = number("SNODRA_SERVER_MAX_SUBSCRIPTIONS", &v)? as usize;
        }
        c.hub_token = optional("SNODRA_SERVER_TOKEN").map(|v| SecretString::new(v.trim()));
        c.hub_token_file = optional("SNODRA_SERVER_TOKEN_FILE").map(|v| PathBuf::from(v.trim()));
        let tls_flag = match optional("SNODRA_SERVER_TLS") {
            Some(v) => Some(flag("SNODRA_SERVER_TLS", &v)?),
            None => None,
        };
        let tls_ca = optional("SNODRA_SERVER_TLS_CA").map(|v| PathBuf::from(v.trim()));
        let tls_name = optional("SNODRA_SERVER_TLS_SERVER_NAME").map(|v| v.trim().to_string());
        let client_cert =
            optional("SNODRA_SERVER_TLS_CLIENT_CERT").map(|v| PathBuf::from(v.trim()));
        let client_key = optional("SNODRA_SERVER_TLS_CLIENT_KEY").map(|v| PathBuf::from(v.trim()));
        let client_certificate = client_cert.is_some() || client_key.is_some();
        if client_certificate && tls_flag == Some(false) {
            return Err(MessagingError::InvalidMessage(
                "SNODRA_SERVER_TLS_CLIENT_CERT and SNODRA_SERVER_TLS_CLIENT_KEY need TLS, but SNODRA_SERVER_TLS=false".into(),
            ));
        }
        // A client certificate implies TLS, as SNODRA_SERVER_TLS=true does.
        if tls_flag.unwrap_or(false) || client_certificate {
            c.hub_tls = Some(HubTls {
                ca_file: tls_ca,
                server_name: tls_name,
                client_cert_file: client_cert,
                client_key_file: client_key,
            });
        } else if tls_ca.is_some() || tls_name.is_some() {
            return Err(MessagingError::InvalidMessage(
                "SNODRA_SERVER_TLS_CA and SNODRA_SERVER_TLS_SERVER_NAME need SNODRA_SERVER_TLS=true".into(),
            ));
        }
        let oauth2_secret = optional("SNODRA_SERVER_OAUTH2_CLIENT_SECRET");
        let oauth2_secret_file = optional("SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE");
        let oauth2_client_id = optional("SNODRA_SERVER_OAUTH2_CLIENT_ID");
        let oauth2_scope = optional("SNODRA_SERVER_OAUTH2_SCOPE");
        let oauth2_ca = optional("SNODRA_SERVER_OAUTH2_CA");
        match optional("SNODRA_SERVER_OAUTH2_TOKEN_URI") {
            Some(uri) => {
                c.hub_oauth2 = Some(HubOAuth2 {
                    token_uri: uri.trim().to_string(),
                    client_id: oauth2_client_id
                        .map(|v| v.trim().to_string())
                        .unwrap_or_else(|| c.service_name.clone()),
                    client_secret: oauth2_secret.map(|v| SecretString::new(v.trim())),
                    client_secret_file: oauth2_secret_file.map(|v| PathBuf::from(v.trim())),
                    scope: oauth2_scope.map(|v| v.trim().to_string()),
                    ca_file: oauth2_ca.map(|v| PathBuf::from(v.trim())),
                });
            }
            None if oauth2_secret.is_some()
                || oauth2_secret_file.is_some()
                || oauth2_client_id.is_some()
                || oauth2_scope.is_some()
                || oauth2_ca.is_some() =>
            {
                return Err(MessagingError::InvalidMessage(
                    "the SNODRA_SERVER_OAUTH2_* variables need SNODRA_SERVER_OAUTH2_TOKEN_URI"
                        .into(),
                ));
            }
            None => {}
        }
        if let Some(v) = optional("SNODRA_CONSUMER_ENABLED") {
            c.consumer_enabled = flag("SNODRA_CONSUMER_ENABLED", &v)?;
        }
        if let Some(v) = optional("SNODRA_CONSUMER_MAX_ATTEMPTS") {
            c.consumer_max_attempts = number("SNODRA_CONSUMER_MAX_ATTEMPTS", &v)? as u32;
        }
        if let Some(v) = optional("SNODRA_EVENTS_DEAD_LETTER_MODE") {
            c.dead_letter_mode = DeadLetterMode::parse("SNODRA_EVENTS_DEAD_LETTER_MODE", &v)?;
        }
        if let Some(v) = optional("SNODRA_EVENTS_DEAD_LETTER_RETENTION_MS") {
            c.dead_letter_retention = millis("SNODRA_EVENTS_DEAD_LETTER_RETENTION_MS", &v)?;
        }
        if let Some(v) = optional("SNODRA_OUTBOX_ENABLED") {
            c.outbox_enabled = flag("SNODRA_OUTBOX_ENABLED", &v)?;
        }
        if let Some(v) = optional("SNODRA_OUTBOX_POLL_INTERVAL_MS") {
            c.outbox_poll_interval = millis("SNODRA_OUTBOX_POLL_INTERVAL_MS", &v)?;
        }
        if let Some(v) = optional("SNODRA_OUTBOX_BATCH_SIZE") {
            c.outbox_batch_size = number("SNODRA_OUTBOX_BATCH_SIZE", &v)? as usize;
        }
        if let Some(v) = optional("SNODRA_OUTBOX_MAX_ATTEMPTS") {
            c.outbox_max_attempts = number("SNODRA_OUTBOX_MAX_ATTEMPTS", &v)? as u32;
        }
        if let Some(v) = optional("SNODRA_OUTBOX_LEASE_MS") {
            c.outbox_lease = millis("SNODRA_OUTBOX_LEASE_MS", &v)?;
        }
        if let Some(v) = optional("SNODRA_STORE_INIT_SCHEMA") {
            c.store_init_schema = flag("SNODRA_STORE_INIT_SCHEMA", &v)?;
        }
        if let Some(v) = optional("SNODRA_FORWARD_JWT") {
            c.forward_jwt = flag("SNODRA_FORWARD_JWT", &v)?;
        }
        if let Some(v) = optional("SNODRA_SAGAS_ENABLED") {
            c.sagas_enabled = flag("SNODRA_SAGAS_ENABLED", &v)?;
        }
        if let Some(v) = optional("SNODRA_SAGAS_MAX_ATTEMPTS") {
            c.sagas_max_attempts = small_number("SNODRA_SAGAS_MAX_ATTEMPTS", &v)?;
        }
        if let Some(v) = optional("SNODRA_SAGAS_RELAY_MAX_ATTEMPTS") {
            c.sagas_relay_max_attempts = small_number("SNODRA_SAGAS_RELAY_MAX_ATTEMPTS", &v)?;
        }
        for (name, field) in [
            (
                "SNODRA_SAGAS_ENDED_RETENTION_MS",
                &mut c.sagas_ended_retention,
            ),
            (
                "SNODRA_SAGAS_DISPATCH_RETENTION_MS",
                &mut c.sagas_dispatch_retention,
            ),
            (
                "SNODRA_SAGAS_FAILED_RETENTION_MS",
                &mut c.sagas_failed_retention,
            ),
            (
                "SNODRA_SAGAS_RELAY_POLL_INTERVAL_MS",
                &mut c.sagas_relay_poll_interval,
            ),
            ("SNODRA_SAGAS_RELAY_LEASE_MS", &mut c.sagas_relay_lease),
            (
                "SNODRA_SAGAS_RELAY_INITIAL_BACKOFF_MS",
                &mut c.sagas_relay_initial_backoff,
            ),
            (
                "SNODRA_SAGAS_RELAY_MAX_BACKOFF_MS",
                &mut c.sagas_relay_max_backoff,
            ),
            (
                "SNODRA_DEADLINES_POLL_INTERVAL_MS",
                &mut c.deadlines_poll_interval,
            ),
            ("SNODRA_DEADLINES_LEASE_MS", &mut c.deadlines_lease),
            ("SNODRA_DEADLINES_RETENTION_MS", &mut c.deadlines_retention),
            (
                "SNODRA_EXTERNAL_CALLS_LEASE_MS",
                &mut c.external_calls_lease,
            ),
            (
                "SNODRA_EXTERNAL_CALLS_RETENTION_MS",
                &mut c.external_calls_retention,
            ),
        ] {
            if let Some(v) = optional(name) {
                *field = millis(name, &v)?;
            }
        }
        c.validate()?;
        Ok(c)
    }

    /// Rejects what cannot work: an empty service name, a `hub_address` that names no hub (or a
    /// malformed `dns:///` target), a zero `reconnect_backoff_max`, a zero `reconnect_backoff_min` or one above the max, a zero `hub_status_interval`, `jwks_url` without `issuer`,
    /// `max_concurrent_handlers` 0, an invalid `hub_retry` policy, a zero
    /// `hub_stream_credit_timeout`, a zero `hub_max_subscriptions`, a `hub_token_file` that cannot
    /// be read, a `hub_tls` CA file that cannot be read or holds no certificate, an empty
    /// `hub_tls` server name, `hub_oauth2` together with a static hub token, without a token URI
    /// that is `http(s)://`, without a secret or with an unreadable secret file or CA file, a
    /// `hub_tls` client certificate without its key (or the reverse), together with a static hub
    /// token or `hub_oauth2`, or one the hub would refuse (unreadable files, a key that is not the
    /// certificate's, a leaf that does not carry exactly one `snodra://service/<service_name>`
    /// URI SAN, an expired one).
    /// Called by `from_env` and by `SnodraMessaging::builder`; error
    /// messages never carry the token.
    pub fn validate(&self) -> Result<(), MessagingError> {
        if self.service_name.trim().is_empty() {
            return Err(MessagingError::InvalidMessage(
                "service_name must not be empty".into(),
            ));
        }
        if self.instance_id.trim().is_empty() {
            return Err(MessagingError::InvalidMessage(
                "instance_id (SNODRA_INSTANCE_ID) must not be empty".into(),
            ));
        }
        HubTargets::parse(&self.hub_address)?;
        if self.reconnect_backoff_max.is_zero() {
            return Err(MessagingError::InvalidMessage(
                "reconnect_backoff_max (SNODRA_RECONNECT_BACKOFF_MAX_MS) must be at least 1 ms"
                    .into(),
            ));
        }
        if self.reconnect_backoff_min.is_zero() {
            return Err(MessagingError::InvalidMessage(
                "reconnect_backoff_min (SNODRA_RECONNECT_BACKOFF_MIN_MS) must be at least 1 ms (0 doubles to 0 forever)"
                    .into(),
            ));
        }
        if self.reconnect_backoff_min > self.reconnect_backoff_max {
            return Err(MessagingError::InvalidMessage(format!(
                "reconnect_backoff_min (SNODRA_RECONNECT_BACKOFF_MIN_MS, {:?}) must not exceed reconnect_backoff_max (SNODRA_RECONNECT_BACKOFF_MAX_MS, {:?})",
                self.reconnect_backoff_min, self.reconnect_backoff_max
            )));
        }
        if self.outbox_lease <= crate::events::OUTBOX_SEND_TIMEOUT {
            return Err(MessagingError::InvalidMessage(format!(
                "outbox_lease (SNODRA_OUTBOX_LEASE_MS, {:?}) must be longer than a send ({} s)",
                self.outbox_lease,
                crate::events::OUTBOX_SEND_TIMEOUT.as_secs()
            )));
        }
        if self.hub_status_interval.is_zero() {
            return Err(MessagingError::InvalidMessage(
                "hub_status_interval (SNODRA_SERVER_STATUS_INTERVAL_MS) must be at least 1 ms"
                    .into(),
            ));
        }
        if self.jwks_url.is_some() && self.issuer.is_none() {
            return Err(MessagingError::InvalidMessage(
                "issuer is required when jwks_url is set".into(),
            ));
        }
        if self.max_concurrent_handlers == 0 {
            return Err(MessagingError::InvalidMessage(
                "max_concurrent_handlers (SNODRA_MAX_CONCURRENT_HANDLERS) must be at least 1"
                    .into(),
            ));
        }
        if let Some(retry) = &self.hub_retry {
            retry.validate()?;
        }
        if self.hub_stream_credit_timeout.is_zero() {
            return Err(MessagingError::InvalidMessage(
                "hub_stream_credit_timeout (SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS) must be at least 1 ms"
                    .into(),
            ));
        }
        if self.hub_max_subscriptions == 0 {
            return Err(MessagingError::InvalidMessage(
                "hub_max_subscriptions (SNODRA_SERVER_MAX_SUBSCRIPTIONS) must be at least 1".into(),
            ));
        }
        if let Some(path) = &self.hub_token_file {
            let token = crate::security::read_token_file(path).map_err(|e| {
                MessagingError::InvalidMessage(format!(
                    "hub_token_file (SNODRA_SERVER_TOKEN_FILE) '{}' cannot be read: {e}",
                    path.display()
                ))
            })?;
            if token.is_none() {
                return Err(MessagingError::InvalidMessage(format!(
                    "hub_token_file (SNODRA_SERVER_TOKEN_FILE) '{}' is empty",
                    path.display()
                )));
            }
        }
        self.validate_sagas()?;
        if self.dead_letter_retention < Duration::from_secs(1) {
            return Err(MessagingError::InvalidMessage(
                "dead_letter_retention (SNODRA_EVENTS_DEAD_LETTER_RETENTION_MS) must be at least 1 s"
                    .into(),
            ));
        }
        if let Some(oauth2) = &self.hub_oauth2 {
            self.validate_oauth2(oauth2)?;
        }
        if let Some(tls) = &self.hub_tls {
            if tls
                .server_name
                .as_deref()
                .is_some_and(|n| n.trim().is_empty())
            {
                return Err(MessagingError::InvalidMessage(
                    "hub_tls.server_name (SNODRA_SERVER_TLS_SERVER_NAME) must not be empty".into(),
                ));
            }
            if let Some(ca) = &tls.ca_file {
                crate::hub::read_ca_file(ca).map_err(|e| {
                    MessagingError::InvalidMessage(format!(
                        "hub_tls.ca_file (SNODRA_SERVER_TLS_CA) '{}': {e}",
                        ca.display()
                    ))
                })?;
            }
            if tls.has_client_certificate() {
                self.validate_client_certificate(tls)?;
            }
        }
        Ok(())
    }

    /// The saga, deadline and external-call settings: attempts at least 1,
    /// intervals, leases and backoffs at least 1 ms, the relay's initial backoff not above its
    /// maximum, retentions at least 1 s (a TTL index counts in seconds). Checked whether or not
    /// sagas are registered, so a typo is found before the first saga is.
    fn validate_sagas(&self) -> Result<(), MessagingError> {
        let invalid = |m: String| Err(MessagingError::InvalidMessage(m));
        for (field, env, value) in [
            (
                "sagas_max_attempts",
                "SNODRA_SAGAS_MAX_ATTEMPTS",
                self.sagas_max_attempts,
            ),
            (
                "sagas_relay_max_attempts",
                "SNODRA_SAGAS_RELAY_MAX_ATTEMPTS",
                self.sagas_relay_max_attempts,
            ),
        ] {
            if value == 0 {
                return invalid(format!("{field} ({env}) must be at least 1"));
            }
        }
        for (field, env, value) in [
            (
                "sagas_relay_poll_interval",
                "SNODRA_SAGAS_RELAY_POLL_INTERVAL_MS",
                self.sagas_relay_poll_interval,
            ),
            (
                "sagas_relay_lease",
                "SNODRA_SAGAS_RELAY_LEASE_MS",
                self.sagas_relay_lease,
            ),
            (
                "sagas_relay_initial_backoff",
                "SNODRA_SAGAS_RELAY_INITIAL_BACKOFF_MS",
                self.sagas_relay_initial_backoff,
            ),
            (
                "sagas_relay_max_backoff",
                "SNODRA_SAGAS_RELAY_MAX_BACKOFF_MS",
                self.sagas_relay_max_backoff,
            ),
            (
                "deadlines_poll_interval",
                "SNODRA_DEADLINES_POLL_INTERVAL_MS",
                self.deadlines_poll_interval,
            ),
            (
                "deadlines_lease",
                "SNODRA_DEADLINES_LEASE_MS",
                self.deadlines_lease,
            ),
            (
                "external_calls_lease",
                "SNODRA_EXTERNAL_CALLS_LEASE_MS",
                self.external_calls_lease,
            ),
        ] {
            if value.is_zero() {
                return invalid(format!("{field} ({env}) must be at least 1 ms"));
            }
        }
        for (field, env, value) in [
            (
                "sagas_ended_retention",
                "SNODRA_SAGAS_ENDED_RETENTION_MS",
                self.sagas_ended_retention,
            ),
            (
                "sagas_dispatch_retention",
                "SNODRA_SAGAS_DISPATCH_RETENTION_MS",
                self.sagas_dispatch_retention,
            ),
            (
                "sagas_failed_retention",
                "SNODRA_SAGAS_FAILED_RETENTION_MS",
                self.sagas_failed_retention,
            ),
            (
                "deadlines_retention",
                "SNODRA_DEADLINES_RETENTION_MS",
                self.deadlines_retention,
            ),
            (
                "external_calls_retention",
                "SNODRA_EXTERNAL_CALLS_RETENTION_MS",
                self.external_calls_retention,
            ),
        ] {
            if value < Duration::from_secs(1) {
                return invalid(format!(
                    "{field} ({env}) must be at least 1000 ms (a TTL index counts in seconds)"
                ));
            }
        }
        if self.sagas_relay_initial_backoff > self.sagas_relay_max_backoff {
            return invalid(format!(
                "sagas_relay_initial_backoff (SNODRA_SAGAS_RELAY_INITIAL_BACKOFF_MS, {:?}) must not exceed sagas_relay_max_backoff (SNODRA_SAGAS_RELAY_MAX_BACKOFF_MS, {:?})",
                self.sagas_relay_initial_backoff, self.sagas_relay_max_backoff
            ));
        }
        self.relay_settings().validate()?;
        self.deadline_settings().validate()?;
        self.external_call_settings().validate()
    }

    /// The client certificate (mTLS): both files, no other credential, and a pair the
    /// hub would accept (it parses, the key is the certificate's, the leaf names this service, it
    /// has not expired).
    fn validate_client_certificate(&self, tls: &HubTls) -> Result<(), MessagingError> {
        let invalid = |m: String| Err(MessagingError::InvalidMessage(m));
        let (Some(cert), Some(key)) = (&tls.client_cert_file, &tls.client_key_file) else {
            return invalid(if tls.client_cert_file.is_none() {
                "hub_tls.client_key_file (SNODRA_SERVER_TLS_CLIENT_KEY) is set without hub_tls.client_cert_file (SNODRA_SERVER_TLS_CLIENT_CERT): set both or neither".into()
            } else {
                "hub_tls.client_cert_file (SNODRA_SERVER_TLS_CLIENT_CERT) is set without hub_tls.client_key_file (SNODRA_SERVER_TLS_CLIENT_KEY): set both or neither".into()
            });
        };
        if self.hub_token.as_ref().is_some_and(|t| !t.is_blank()) || self.hub_token_file.is_some() {
            return invalid("a client certificate (hub_tls.client_cert_file, SNODRA_SERVER_TLS_CLIENT_CERT) and a static hub token (hub_token / hub_token_file, SNODRA_SERVER_TOKEN / SNODRA_SERVER_TOKEN_FILE) are both set: use one credential".into());
        }
        if self.hub_oauth2.is_some() {
            return invalid("a client certificate (hub_tls.client_cert_file, SNODRA_SERVER_TLS_CLIENT_CERT) and hub_oauth2 (SNODRA_SERVER_OAUTH2_TOKEN_URI) are both set: use one credential".into());
        }
        let invalid = |e: crate::hub::LoadError| MessagingError::InvalidMessage(e.to_string());
        let identity =
            crate::hub::load_client_certificate(cert, key, &self.service_name).map_err(invalid)?;
        crate::hub::check_valid_at(&identity, std::time::SystemTime::now()).map_err(invalid)
    }

    fn validate_oauth2(&self, oauth2: &HubOAuth2) -> Result<(), MessagingError> {
        let invalid = |m: String| Err(MessagingError::InvalidMessage(m));
        if self.hub_token.as_ref().is_some_and(|t| !t.is_blank()) || self.hub_token_file.is_some() {
            return invalid("hub_oauth2 (SNODRA_SERVER_OAUTH2_TOKEN_URI) and a static hub token (hub_token / hub_token_file, SNODRA_SERVER_TOKEN / SNODRA_SERVER_TOKEN_FILE) are both set: use one".into());
        }
        let scheme_ok = reqwest::Url::parse(oauth2.token_uri.trim())
            .is_ok_and(|u| matches!(u.scheme(), "http" | "https") && u.host_str().is_some());
        if !scheme_ok {
            return invalid(format!(
                "hub_oauth2.token_uri (SNODRA_SERVER_OAUTH2_TOKEN_URI) must be an http:// or https:// URL, got '{}'",
                oauth2.token_uri
            ));
        }
        if oauth2.client_id.trim().is_empty() {
            return invalid(
                "hub_oauth2.client_id (SNODRA_SERVER_OAUTH2_CLIENT_ID) must not be empty".into(),
            );
        }
        let inline = oauth2.client_secret.as_ref().is_some_and(|s| !s.is_blank());
        match &oauth2.client_secret_file {
            Some(path) => {
                let secret = crate::security::read_token_file(path).map_err(|e| {
                    MessagingError::InvalidMessage(format!(
                        "hub_oauth2.client_secret_file (SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE) '{}' cannot be read: {e}",
                        path.display()
                    ))
                })?;
                if secret.is_none() {
                    return invalid(format!(
                        "hub_oauth2.client_secret_file (SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE) '{}' is empty",
                        path.display()
                    ));
                }
            }
            None if !inline => {
                return invalid("hub_oauth2 needs a client secret: set hub_oauth2.client_secret (SNODRA_SERVER_OAUTH2_CLIENT_SECRET) or hub_oauth2.client_secret_file (SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE)".into());
            }
            None => {}
        }
        if let Some(ca) = &oauth2.ca_file {
            crate::hub::read_ca_file(ca).map_err(|e| {
                MessagingError::InvalidMessage(format!(
                    "hub_oauth2.ca_file (SNODRA_SERVER_OAUTH2_CA) '{}': {e}",
                    ca.display()
                ))
            })?;
        }
        Ok(())
    }
}

fn number(name: &str, v: &str) -> Result<u64, MessagingError> {
    v.trim().parse::<u64>().map_err(|_| {
        MessagingError::InvalidMessage(format!("{name} must be a non-negative integer, got '{v}'"))
    })
}

/// A count that fits a `u32` (attempts).
fn small_number(name: &str, v: &str) -> Result<u32, MessagingError> {
    u32::try_from(number(name, v)?)
        .map_err(|_| MessagingError::InvalidMessage(format!("{name} is too large, got '{v}'")))
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

/// Settings the library read before the event store became an add-on, each with what
/// replaced it. They are not read (no alias before v1), and one still set fails
/// [`Config::from_env`] ([`removed_settings`]): ignored, a source or a mode would misroute events.
/// `SNODRA_MONGODB_URI`, also no longer read, is not here: a service may keep it for its own
/// MongoDB client.
const REMOVED_SETTINGS: [(&str, &str); 3] = [
    ("SNODRA_KAFKA_TOPIC", "SNODRA_EVENTS_SOURCE"),
    (
        "SNODRA_KAFKA_DEAD_LETTER_MODE",
        "SNODRA_EVENTS_DEAD_LETTER_MODE, where `topic` is now `event-store`",
    ),
    (
        "SNODRA_KAFKA_DEAD_LETTER_RETENTION_MS",
        "SNODRA_EVENTS_DEAD_LETTER_RETENTION_MS",
    ),
];

/// The error for the removed settings the environment (`lookup`) still sets (blank is unset),
/// naming every one and its replacement; `Ok` when none is set.
fn removed_settings(lookup: &impl Fn(&str) -> Option<String>) -> Result<(), MessagingError> {
    let set: Vec<String> = REMOVED_SETTINGS
        .iter()
        .filter(|(name, _)| lookup(name).is_some_and(|v| !v.trim().is_empty()))
        .map(|(name, replacement)| format!("{name}: use {replacement}"))
        .collect();
    if set.is_empty() {
        return Ok(());
    }
    Err(MessagingError::InvalidMessage(format!(
        "removed settings are set; they are no longer read, unset them. {}",
        set.join("; ")
    )))
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
        let vars: Vec<(String, String)> = [("SNODRA_SERVICE_NAME", "svc")]
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
        Config::new("svc", "static://hub:7000")
    }

    #[test]
    fn the_instance_id_is_a_fresh_uuid_unless_set() {
        let (a, b) = (code_config(), code_config());
        uuid::Uuid::parse_str(&a.instance_id).expect("a UUID");
        assert_ne!(a.instance_id, b.instance_id, "one per Config");
        uuid::Uuid::parse_str(&from(&[]).instance_id).expect("a UUID from the environment too");
        assert_eq!(
            from(&[("SNODRA_INSTANCE_ID", "pod-7")]).instance_id,
            "pod-7"
        );
        let mut c = code_config();
        c.instance_id = " ".into();
        assert!(c.validate().is_err(), "an empty id");
    }

    #[test]
    fn zero_request_timeout_means_unset() {
        assert_eq!(from(&[]).hub_request_timeout, None);
        assert_eq!(
            from(&[("SNODRA_SERVER_REQUEST_TIMEOUT_MS", "0")]).hub_request_timeout,
            None
        );
        assert_eq!(
            from(&[("SNODRA_SERVER_REQUEST_TIMEOUT_MS", "2500")]).hub_request_timeout,
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
            from(&[("SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS", "2500")]).hub_stream_credit_timeout,
            Duration::from_millis(2500)
        );
        let e = try_from(&[("SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS", "0")]).unwrap_err();
        assert!(
            e.to_string()
                .contains("SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS"),
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
            from(&[("SNODRA_SERVER_MAX_SUBSCRIPTIONS", "5")]).hub_max_subscriptions,
            5
        );
        for value in ["0", "-1", "many"] {
            let e = try_from(&[("SNODRA_SERVER_MAX_SUBSCRIPTIONS", value)]).unwrap_err();
            assert!(
                e.to_string().contains("SNODRA_SERVER_MAX_SUBSCRIPTIONS"),
                "{value}: {e}"
            );
        }
        let mut c = code_config();
        c.hub_max_subscriptions = 0;
        assert!(c.validate().is_err(), "zero rejected in code too");
    }

    #[test]
    fn flow_control_routing_and_retry_defaults() {
        let c = Config::new("svc", "static://hub:7000");
        assert_eq!(c.hub_load_factor, 100);
        assert!(c.hub_tags.is_empty());
        assert!(c.hub_priorities.is_empty());
        assert_eq!(c.hub_retry, None, "no retry unless configured");
        assert_eq!(from(&[]).hub_retry, None);
        assert_eq!(
            from(&[("SNODRA_SERVER_RETRY_ENABLED", "false")]).hub_retry,
            None
        );
        assert_eq!(
            from(&[("SNODRA_SERVER_RETRY_ENABLED", "true")]).hub_retry,
            Some(RetryPolicy::default()),
            "enabled alone: 3 attempts, 100 ms doubling to 2 s"
        );
    }

    #[test]
    fn flow_control_routing_and_retry_from_env() {
        let c = from(&[
            ("SNODRA_SERVER_LOAD_FACTOR", "250"),
            ("SNODRA_SERVER_TAGS", " zone=eu-1 , tier = gold,"),
            (
                "SNODRA_SERVER_PRIORITIES",
                "orders.Cancel=10,reports.Build=-5",
            ),
            ("SNODRA_SERVER_RETRY_ENABLED", "true"),
            ("SNODRA_SERVER_RETRY_MAX_ATTEMPTS", "5"),
            ("SNODRA_SERVER_RETRY_INITIAL_BACKOFF_MS", "50"),
            ("SNODRA_SERVER_RETRY_MULTIPLIER", "1.5"),
            ("SNODRA_SERVER_RETRY_MAX_BACKOFF_MS", "800"),
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
                ("SNODRA_SERVER_RETRY_MAX_ATTEMPTS", "0"),
                ("SNODRA_SERVER_RETRY_MULTIPLIER", "0.5"),
                ("SNODRA_SERVER_RETRY_MULTIPLIER", "NaN"),
                ("SNODRA_SERVER_RETRY_MULTIPLIER", "inf"),
                ("SNODRA_SERVER_RETRY_MULTIPLIER", "-2"),
            ] {
                let mut vars = vec![(name, value)];
                vars.extend(enabled.map(|e| ("SNODRA_SERVER_RETRY_ENABLED", e)));
                let result = try_from(&vars);
                assert!(
                    matches!(result, Err(MessagingError::InvalidMessage(ref m)) if m.contains(name)),
                    "{name}={value} with SNODRA_SERVER_RETRY_ENABLED={enabled:?}: {result:?}"
                );
            }
        }
        // One attempt is valid: a configured policy that never retries.
        let one = from(&[
            ("SNODRA_SERVER_RETRY_ENABLED", "true"),
            ("SNODRA_SERVER_RETRY_MAX_ATTEMPTS", "1"),
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
            ("SNODRA_SERVER_TAGS", "zone"),
            ("SNODRA_SERVER_TAGS", "=x"),
            ("SNODRA_SERVER_PRIORITIES", "t.X=high"),
            ("SNODRA_SERVER_RETRY_MULTIPLIER", "fast"),
        ] {
            let vars = [
                ("SNODRA_SERVICE_NAME".to_string(), "svc".to_string()),
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
                ("SNODRA_RECONNECT_BACKOFF_MIN_MS", min),
                ("SNODRA_RECONNECT_BACKOFF_MAX_MS", max),
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
            ("SNODRA_RECONNECT_BACKOFF_MIN_MS", "1000"),
            ("SNODRA_RECONNECT_BACKOFF_MAX_MS", "1000"),
        ]);
        assert_eq!(c.reconnect_backoff_max, Duration::from_secs(1));
        assert!(code_config().validate().is_ok());
    }

    #[test]
    fn a_zero_reconnect_backoff_min_is_rejected() {
        // 0 doubles to 0 forever: the same hot loop as a zero max.
        let result = try_from(&[
            ("SNODRA_RECONNECT_BACKOFF_MIN_MS", "0"),
            ("SNODRA_RECONNECT_BACKOFF_MAX_MS", "5000"),
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
        let one = from(&[("SNODRA_RECONNECT_BACKOFF_MIN_MS", "1")]);
        assert_eq!(one.reconnect_backoff_min, Duration::from_millis(1));
    }

    #[test]
    fn zero_concurrent_handlers_are_rejected() {
        let result = try_from(&[("SNODRA_MAX_CONCURRENT_HANDLERS", "0")]);
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
            from(&[("SNODRA_MAX_CONCURRENT_HANDLERS", "1")]).max_concurrent_handlers,
            1
        );
    }

    #[test]
    fn a_negative_load_factor_is_rejected_and_zero_is_kept_for_the_hub() {
        for value in ["-1", "-100"] {
            let result = try_from(&[("SNODRA_SERVER_LOAD_FACTOR", value)]);
            assert!(
                matches!(result, Err(MessagingError::InvalidMessage(ref m)) if m.contains("SNODRA_SERVER_LOAD_FACTOR")),
                "{value}: {result:?}"
            );
        }
        // 0 is sent as is; the hub reads it as 100.
        assert_eq!(
            from(&[("SNODRA_SERVER_LOAD_FACTOR", "0")]).hub_load_factor,
            0
        );
    }

    #[test]
    fn hub_address_takes_a_list_or_a_dns_name_and_a_status_interval() {
        let c = from(&[]);
        assert_eq!(c.hub_address, "static://snodra-server:7000");
        assert_eq!(c.hub_status_interval, Duration::from_secs(5));
        let c = from(&[
            ("SNODRA_SERVER_ADDRESS", "static://hub:7000,hub-2:7000"),
            ("SNODRA_SERVER_STATUS_INTERVAL_MS", "2500"),
        ]);
        assert_eq!(c.hub_address, "static://hub:7000,hub-2:7000");
        assert_eq!(c.hub_status_interval, Duration::from_millis(2500));
        assert_eq!(
            from(&[("SNODRA_SERVER_ADDRESS", "dns:///snodra-server:7000")]).hub_address,
            "dns:///snodra-server:7000"
        );
        for (name, value) in [
            ("SNODRA_SERVER_ADDRESS", "dns:///snodra-server"),
            (
                "SNODRA_SERVER_ADDRESS",
                "hub:7000,dns:///snodra-server:7000",
            ),
            ("SNODRA_SERVER_ADDRESS", ","),
            ("SNODRA_SERVER_STATUS_INTERVAL_MS", "0"),
            ("SNODRA_SERVER_STATUS_INTERVAL_MS", "soon"),
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
    fn hub_defaults() {
        let c = Config::new("svc", "static://hub:7000");
        // No chosen timeout: the hub decides (per-type override or its default).
        assert_eq!(c.hub_request_timeout, None);
        // F2: the backoff is capped at 5 s.
        assert_eq!(c.reconnect_backoff_min, Duration::from_millis(500));
        assert_eq!(c.reconnect_backoff_max, Duration::from_secs(5));
    }

    fn secret_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("snodra-cfg-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn the_token_comes_from_the_environment_and_never_prints() {
        assert!(from(&[]).hub_token.is_none());
        assert!(from(&[("SNODRA_SERVER_TOKEN", "  ")]).hub_token.is_none());
        let c = from(&[("SNODRA_SERVER_TOKEN", "s3cr3t-token-value\n")]);
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
        let missing = try_from(&[("SNODRA_SERVER_TOKEN_FILE", path.to_str().unwrap())]);
        assert!(
            matches!(missing, Err(MessagingError::InvalidMessage(ref m)) if m.contains("SNODRA_SERVER_TOKEN_FILE")),
            "{missing:?}"
        );
        std::fs::write(&path, "from-the-file\n").unwrap();
        let c = from(&[("SNODRA_SERVER_TOKEN_FILE", path.to_str().unwrap())]);
        assert_eq!(c.hub_token_file.as_deref(), Some(path.as_path()));
        assert!(
            c.hub_token.is_none(),
            "the file is read per connect, not stored here"
        );
        assert!(!format!("{c:?}").contains("from-the-file"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A token file empty at startup (after trimming) fails it, naming the setting and the path,
    /// as the other secret files; set in code, the same.
    #[test]
    fn an_empty_token_file_at_startup_is_an_error() {
        let dir = secret_dir();
        for (name, content) in [("empty", ""), ("blank", " \n\t\r\n")] {
            let path = dir.join(name);
            std::fs::write(&path, content).unwrap();
            let refused = try_from(&[("SNODRA_SERVER_TOKEN_FILE", path.to_str().unwrap())]);
            assert!(
                matches!(&refused, Err(MessagingError::InvalidMessage(m))
                    if m.contains("SNODRA_SERVER_TOKEN_FILE") && m.contains(path.to_str().unwrap()) && m.contains("empty")),
                "{name}: {refused:?}"
            );
            let mut c = code_config();
            c.hub_token_file = Some(path.clone());
            assert!(c.validate().is_err(), "{name}, set in code");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn tls_is_off_unless_asked_and_its_options_need_it() {
        assert_eq!(from(&[]).hub_tls, None);
        assert_eq!(from(&[("SNODRA_SERVER_TLS", "false")]).hub_tls, None);
        assert_eq!(
            from(&[("SNODRA_SERVER_TLS", "true")]).hub_tls,
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
            ("SNODRA_SERVER_TLS", "true"),
            ("SNODRA_SERVER_TLS_CA", ca.to_str().unwrap()),
            ("SNODRA_SERVER_TLS_SERVER_NAME", "snodra-server"),
        ]);
        assert_eq!(
            c.hub_tls,
            Some(HubTls {
                ca_file: Some(ca.clone()),
                server_name: Some("snodra-server".into()),
                ..HubTls::default()
            })
        );
        for vars in [
            vec![("SNODRA_SERVER_TLS_CA", ca.to_str().unwrap())],
            vec![("SNODRA_SERVER_TLS_SERVER_NAME", "snodra-server")],
            vec![("SNODRA_SERVER_TLS", "maybe")],
            vec![
                ("SNODRA_SERVER_TLS", "true"),
                ("SNODRA_SERVER_TLS_CA", "/no/such/ca.pem"),
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
                ("SNODRA_SERVER_TLS", "true"),
                ("SNODRA_SERVER_TLS_CA", junk.to_str().unwrap())
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

    #[test]
    fn oauth2_is_off_unless_a_token_uri_is_set_and_defaults_the_client_id_to_the_service() {
        assert_eq!(from(&[]).hub_oauth2, None);
        let c = from(&[
            (
                "SNODRA_SERVER_OAUTH2_TOKEN_URI",
                "https://idp.example/realms/snodra/protocol/openid-connect/token",
            ),
            (
                "SNODRA_SERVER_OAUTH2_CLIENT_SECRET",
                " s3cr3t-client-secret\n",
            ),
        ]);
        let o = c.hub_oauth2.as_ref().expect("oauth2 on");
        assert_eq!(
            o.token_uri,
            "https://idp.example/realms/snodra/protocol/openid-connect/token"
        );
        assert_eq!(o.client_id, "svc", "the service name");
        assert_eq!(
            o.client_secret.as_ref().map(SecretString::expose),
            Some("s3cr3t-client-secret")
        );
        assert_eq!(
            (&o.client_secret_file, &o.scope, &o.ca_file),
            (&None, &None, &None)
        );
        for printed in [format!("{c:?}"), format!("{c:#?}"), format!("{o:?}")] {
            assert!(!printed.contains("s3cr3t-client-secret"), "{printed}");
            assert!(printed.contains("\"***\""), "{printed}");
        }
    }

    #[test]
    fn every_oauth2_variable_is_read() {
        let dir = secret_dir();
        let (secret, ca) = (dir.join("secret"), dir.join("ca.pem"));
        std::fs::write(&secret, "from-the-file\n").unwrap();
        std::fs::write(
            &ca,
            "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        )
        .unwrap();
        let c = from(&[
            (
                "SNODRA_SERVER_OAUTH2_TOKEN_URI",
                "http://auth.example:8080/token",
            ),
            ("SNODRA_SERVER_OAUTH2_CLIENT_ID", "orders-service"),
            ("SNODRA_SERVER_OAUTH2_CLIENT_SECRET", "inline"),
            (
                "SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE",
                secret.to_str().unwrap(),
            ),
            ("SNODRA_SERVER_OAUTH2_SCOPE", "hub orders"),
            ("SNODRA_SERVER_OAUTH2_CA", ca.to_str().unwrap()),
        ]);
        let o = c.hub_oauth2.unwrap();
        assert_eq!(o.client_id, "orders-service");
        assert_eq!(o.client_secret_file.as_deref(), Some(secret.as_path()));
        assert_eq!(o.scope.as_deref(), Some("hub orders"));
        assert_eq!(o.ca_file.as_deref(), Some(ca.as_path()));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn oauth2_needs_a_secret_a_readable_secret_file_and_an_http_token_uri() {
        let uri = (
            "SNODRA_SERVER_OAUTH2_TOKEN_URI",
            "http://auth.example:8080/token",
        );
        let mentions = |vars: &[(&str, &str)], text: &str| {
            let r = try_from(vars);
            assert!(
                matches!(r, Err(MessagingError::InvalidMessage(ref m)) if m.contains(text)),
                "{vars:?}: {r:?}"
            );
        };
        mentions(&[uri], "SNODRA_SERVER_OAUTH2_CLIENT_SECRET");
        mentions(
            &[uri, ("SNODRA_SERVER_OAUTH2_CLIENT_SECRET", "  ")],
            "SNODRA_SERVER_OAUTH2_CLIENT_SECRET",
        );
        mentions(
            &[
                uri,
                ("SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE", "/no/such/secret"),
            ],
            "SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE",
        );
        mentions(
            &[
                ("SNODRA_SERVER_OAUTH2_TOKEN_URI", "ftp://auth.example/token"),
                ("SNODRA_SERVER_OAUTH2_CLIENT_SECRET", "x"),
            ],
            "SNODRA_SERVER_OAUTH2_TOKEN_URI",
        );
        mentions(
            &[
                ("SNODRA_SERVER_OAUTH2_TOKEN_URI", "not a uri"),
                ("SNODRA_SERVER_OAUTH2_CLIENT_SECRET", "x"),
            ],
            "SNODRA_SERVER_OAUTH2_TOKEN_URI",
        );
        mentions(
            &[
                uri,
                ("SNODRA_SERVER_OAUTH2_CLIENT_SECRET", "x"),
                ("SNODRA_SERVER_OAUTH2_CA", "/no/such/ca.pem"),
            ],
            "SNODRA_SERVER_OAUTH2_CA",
        );
        // The other variables mean nothing without the token URI: a typo must not pass silently.
        mentions(
            &[("SNODRA_SERVER_OAUTH2_CLIENT_SECRET", "x")],
            "SNODRA_SERVER_OAUTH2_TOKEN_URI",
        );
    }

    #[test]
    fn oauth2_and_a_static_hub_token_together_are_a_startup_error_naming_both() {
        let oauth2 = [
            (
                "SNODRA_SERVER_OAUTH2_TOKEN_URI",
                "http://auth.example:8080/token",
            ),
            ("SNODRA_SERVER_OAUTH2_CLIENT_SECRET", "x"),
        ];
        let dir = secret_dir();
        let file = dir.join("token");
        std::fs::write(&file, "tok\n").unwrap();
        for (name, value) in [
            ("SNODRA_SERVER_TOKEN", "a-static-token"),
            ("SNODRA_SERVER_TOKEN_FILE", file.to_str().unwrap()),
        ] {
            let mut vars = oauth2.to_vec();
            vars.push((name, value));
            let r = try_from(&vars);
            assert!(
                matches!(r, Err(MessagingError::InvalidMessage(ref m))
                    if m.contains("SNODRA_SERVER_OAUTH2_TOKEN_URI") && m.contains(name)),
                "{name}: {r:?}"
            );
        }
        // A blank static token is no token (as everywhere else).
        let mut vars = oauth2.to_vec();
        vars.push(("SNODRA_SERVER_TOKEN", " "));
        assert!(try_from(&vars).is_ok());
        // Set in code, the same.
        let mut c = from(&oauth2);
        c.hub_token = Some(SecretString::new("static"));
        assert!(c.validate().is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A certificate pair for service `name` in a fresh directory.
    fn cert_pair(name: &str) -> (crate::hub::test_pki::Dir, String, String) {
        let dir = crate::hub::test_pki::Dir::new();
        let (cert, key) = dir.pair(&crate::hub::test_pki::issue(
            &crate::hub::test_pki::Spec::service(name),
        ));
        let (cert, key) = (
            cert.to_str().unwrap().to_string(),
            key.to_str().unwrap().to_string(),
        );
        (dir, cert, key)
    }

    #[test]
    fn a_client_certificate_turns_tls_on_and_needs_both_files() {
        let (_dir, cert, key) = cert_pair("svc");
        let c = from(&[
            ("SNODRA_SERVER_TLS_CLIENT_CERT", &cert),
            ("SNODRA_SERVER_TLS_CLIENT_KEY", &key),
        ]);
        assert_eq!(
            c.hub_tls,
            Some(HubTls {
                client_cert_file: Some(cert.clone().into()),
                client_key_file: Some(key.clone().into()),
                ..HubTls::default()
            })
        );
        // The other TLS settings stay as they were.
        let c = from(&[
            ("SNODRA_SERVER_TLS", "true"),
            ("SNODRA_SERVER_TLS_SERVER_NAME", "snodra-server"),
            ("SNODRA_SERVER_TLS_CLIENT_CERT", &cert),
            ("SNODRA_SERVER_TLS_CLIENT_KEY", &key),
        ]);
        let tls = c.hub_tls.unwrap();
        assert_eq!(tls.server_name.as_deref(), Some("snodra-server"));
        assert!(tls.client_cert_file.is_some() && tls.client_key_file.is_some());
        // One alone names the one that is missing.
        for (vars, missing) in [
            (
                vec![("SNODRA_SERVER_TLS_CLIENT_CERT", cert.as_str())],
                "SNODRA_SERVER_TLS_CLIENT_KEY",
            ),
            (
                vec![("SNODRA_SERVER_TLS_CLIENT_KEY", key.as_str())],
                "SNODRA_SERVER_TLS_CLIENT_CERT",
            ),
        ] {
            let r = try_from(&vars);
            assert!(
                matches!(r, Err(MessagingError::InvalidMessage(ref m)) if m.contains(missing)),
                "{vars:?}: {r:?}"
            );
        }
        // Explicitly off while a certificate is set contradicts itself.
        let r = try_from(&[
            ("SNODRA_SERVER_TLS", "false"),
            ("SNODRA_SERVER_TLS_CLIENT_CERT", &cert),
            ("SNODRA_SERVER_TLS_CLIENT_KEY", &key),
        ]);
        assert!(matches!(r, Err(MessagingError::InvalidMessage(_))), "{r:?}");
        // Set in code, one file alone is the same error.
        let in_code = Config {
            hub_tls: Some(HubTls {
                client_cert_file: Some(cert.into()),
                ..HubTls::default()
            }),
            ..code_config()
        };
        assert!(in_code.validate().is_err());
    }

    #[test]
    fn a_client_certificate_and_a_token_or_oauth2_together_are_a_startup_error_naming_both() {
        let (dir, cert, key) = cert_pair("svc");
        let token_file = dir.write("token", "tok\n");
        let token_file = token_file.to_str().unwrap().to_string();
        let certificate = [
            ("SNODRA_SERVER_TLS_CLIENT_CERT", cert.as_str()),
            ("SNODRA_SERVER_TLS_CLIENT_KEY", key.as_str()),
        ];
        assert!(try_from(&certificate).is_ok());
        for extra in [
            vec![("SNODRA_SERVER_TOKEN", "a-static-token")],
            vec![("SNODRA_SERVER_TOKEN_FILE", token_file.as_str())],
            vec![
                (
                    "SNODRA_SERVER_OAUTH2_TOKEN_URI",
                    "http://auth.example:8080/token",
                ),
                ("SNODRA_SERVER_OAUTH2_CLIENT_SECRET", "x"),
            ],
        ] {
            let mut vars = certificate.to_vec();
            vars.extend(extra.iter().copied());
            let r = try_from(&vars);
            assert!(
                matches!(r, Err(MessagingError::InvalidMessage(ref m))
                    if m.contains("SNODRA_SERVER_TLS_CLIENT_CERT") && m.contains(extra[0].0)),
                "{extra:?}: {r:?}"
            );
        }
        // A blank static token is no token.
        let mut vars = certificate.to_vec();
        vars.push(("SNODRA_SERVER_TOKEN", " "));
        assert!(try_from(&vars).is_ok());
        // Set in code, the same.
        let mut c = from(&certificate);
        c.hub_token = Some(SecretString::new("static"));
        assert!(c.validate().is_err());
    }

    #[test]
    fn the_client_certificate_files_are_checked_at_startup() {
        let dir = crate::hub::test_pki::Dir::new();
        use crate::hub::test_pki::{Spec, issue};
        let vars = |cert: &std::path::Path, key: &std::path::Path| {
            try_from(&[
                ("SNODRA_SERVER_TLS_CLIENT_CERT", cert.to_str().unwrap()),
                ("SNODRA_SERVER_TLS_CLIENT_KEY", key.to_str().unwrap()),
            ])
        };
        let mentions = |r: Result<Config, MessagingError>, parts: &[&str]| {
            let Err(MessagingError::InvalidMessage(m)) = r else {
                panic!("accepted");
            };
            for p in parts {
                assert!(m.contains(p), "{p} not in {m}");
            }
        };
        let good = issue(&Spec::service("svc"));
        let (cert, key) = dir.pair(&good);
        assert!(vars(&cert, &key).is_ok());
        // Unreadable, empty.
        mentions(
            vars(&dir.0.join("absent.pem"), &key),
            &["absent.pem", "cannot be read"],
        );
        let empty = dir.write("empty.pem", "");
        mentions(vars(&cert, &empty), &["empty.pem", "no PEM private key"]);
        // A key that is not the certificate's.
        let other = dir.write("other.pem", &issue(&Spec::service("svc")).key_pem);
        mentions(vars(&cert, &other), &["does not match"]);
        // No service SAN, or another service's.
        let dns_only = issue(&Spec {
            uris: vec![],
            dns: vec!["svc".into()],
            ..Spec::service("x")
        });
        let (c, k) = (
            dir.write("c2.pem", &dns_only.cert_pem),
            dir.write("k2.pem", &dns_only.key_pem),
        );
        mentions(vars(&c, &k), &["snodra://service/"]);
        let orders = issue(&Spec::service("orders"));
        let (c, k) = (
            dir.write("c3.pem", &orders.cert_pem),
            dir.write("k3.pem", &orders.key_pem),
        );
        mentions(vars(&c, &k), &["orders", "svc"]);
        // Already expired: the hub would refuse it at the handshake.
        let now = time::OffsetDateTime::now_utc();
        let expired = issue(&Spec {
            not_before: now - std::time::Duration::from_secs(7200),
            not_after: now - std::time::Duration::from_secs(60),
            ..Spec::service("svc")
        });
        let (c, k) = (
            dir.write("c4.pem", &expired.cert_pem),
            dir.write("k4.pem", &expired.key_pem),
        );
        mentions(vars(&c, &k), &["expired"]);
    }
    #[test]
    fn saga_settings_have_their_defaults() {
        let c = code_config();
        assert!(c.sagas_enabled);
        assert_eq!(c.sagas_max_attempts, 5);
        assert_eq!(c.sagas_ended_retention, Duration::from_secs(30 * 86_400));
        assert_eq!(c.sagas_dispatch_retention, Duration::from_secs(7 * 86_400));
        assert_eq!(c.sagas_failed_retention, Duration::from_secs(30 * 86_400));
        assert_eq!(c.sagas_relay_poll_interval, Duration::from_millis(500));
        assert_eq!(c.sagas_relay_lease, Duration::from_secs(30));
        assert_eq!(c.sagas_relay_max_attempts, 10);
        assert_eq!(c.sagas_relay_initial_backoff, Duration::from_secs(1));
        assert_eq!(c.sagas_relay_max_backoff, Duration::from_secs(300));
        assert_eq!(c.deadlines_poll_interval, Duration::from_secs(1));
        assert_eq!(c.deadlines_lease, Duration::from_secs(30));
        assert_eq!(c.deadlines_retention, Duration::from_secs(7 * 86_400));
        assert_eq!(c.external_calls_lease, Duration::from_secs(300));
        assert_eq!(c.external_calls_retention, Duration::from_secs(30 * 86_400));
        let relay = c.relay_settings();
        assert_eq!(
            (relay.multiplier, relay.parallelism, relay.batch_size),
            (2.0, 4, 100)
        );
        assert_eq!(c.deadline_settings().batch_size, 100);
        assert_eq!(c.saga_retry().max_attempts(), 5);
        assert_eq!(
            from(&[]).sagas_relay_max_attempts,
            10,
            "the environment's defaults too"
        );
    }

    #[test]
    fn every_saga_variable_is_read() {
        let c = from(&[
            ("SNODRA_SAGAS_ENABLED", "false"),
            ("SNODRA_SAGAS_MAX_ATTEMPTS", "7"),
            ("SNODRA_SAGAS_ENDED_RETENTION_MS", "86400000"),
            ("SNODRA_SAGAS_DISPATCH_RETENTION_MS", "3600000"),
            ("SNODRA_SAGAS_FAILED_RETENTION_MS", "172800000"),
            ("SNODRA_SAGAS_RELAY_POLL_INTERVAL_MS", "250"),
            ("SNODRA_SAGAS_RELAY_LEASE_MS", "10000"),
            ("SNODRA_SAGAS_RELAY_MAX_ATTEMPTS", "4"),
            ("SNODRA_SAGAS_RELAY_INITIAL_BACKOFF_MS", "100"),
            ("SNODRA_SAGAS_RELAY_MAX_BACKOFF_MS", "2000"),
            ("SNODRA_DEADLINES_POLL_INTERVAL_MS", "300"),
            ("SNODRA_DEADLINES_LEASE_MS", "6000"),
            ("SNODRA_DEADLINES_RETENTION_MS", "7200000"),
            ("SNODRA_EXTERNAL_CALLS_LEASE_MS", "60000"),
            ("SNODRA_EXTERNAL_CALLS_RETENTION_MS", "1000"),
        ]);
        assert!(!c.sagas_enabled);
        assert_eq!(c.sagas_max_attempts, 7);
        assert_eq!(c.saga_retry().max_attempts(), 7);
        assert_eq!(c.sagas_ended_retention, Duration::from_secs(86_400));
        assert_eq!(c.sagas_dispatch_retention, Duration::from_secs(3600));
        assert_eq!(c.sagas_failed_retention, Duration::from_secs(2 * 86_400));
        let relay = c.relay_settings();
        assert_eq!(relay.poll_interval, Duration::from_millis(250));
        assert_eq!(relay.lease, Duration::from_secs(10));
        assert_eq!(relay.max_attempts, 4);
        assert_eq!(relay.initial_backoff, Duration::from_millis(100));
        assert_eq!(relay.max_backoff, Duration::from_secs(2));
        let deadlines = c.deadline_settings();
        assert_eq!(deadlines.poll_interval, Duration::from_millis(300));
        assert_eq!(deadlines.lease, Duration::from_secs(6));
        assert_eq!(c.deadlines_retention, Duration::from_secs(7200));
        assert_eq!(c.external_call_settings().lease, Duration::from_secs(60));
        assert_eq!(c.external_calls_retention, Duration::from_secs(1));
    }

    #[test]
    fn invalid_saga_settings_are_rejected_naming_the_variable() {
        for (name, value) in [
            ("SNODRA_SAGAS_ENABLED", "maybe"),
            ("SNODRA_SAGAS_MAX_ATTEMPTS", "0"),
            ("SNODRA_SAGAS_MAX_ATTEMPTS", "five"),
            ("SNODRA_SAGAS_MAX_ATTEMPTS", "99999999999"),
            ("SNODRA_SAGAS_RELAY_MAX_ATTEMPTS", "0"),
            ("SNODRA_SAGAS_ENDED_RETENTION_MS", "999"),
            ("SNODRA_SAGAS_DISPATCH_RETENTION_MS", "0"),
            ("SNODRA_SAGAS_FAILED_RETENTION_MS", "0"),
            ("SNODRA_SAGAS_FAILED_RETENTION_MS", "999"),
            ("SNODRA_SAGAS_RELAY_POLL_INTERVAL_MS", "0"),
            ("SNODRA_SAGAS_RELAY_LEASE_MS", "0"),
            ("SNODRA_SAGAS_RELAY_INITIAL_BACKOFF_MS", "0"),
            ("SNODRA_SAGAS_RELAY_INITIAL_BACKOFF_MS", "600000"),
            ("SNODRA_SAGAS_RELAY_MAX_BACKOFF_MS", "-1"),
            ("SNODRA_DEADLINES_POLL_INTERVAL_MS", "0"),
            ("SNODRA_DEADLINES_LEASE_MS", "0"),
            ("SNODRA_DEADLINES_RETENTION_MS", "10"),
            ("SNODRA_EXTERNAL_CALLS_LEASE_MS", "0"),
            ("SNODRA_EXTERNAL_CALLS_RETENTION_MS", "soon"),
        ] {
            let e = try_from(&[(name, value)]).unwrap_err().to_string();
            assert!(e.contains(name), "{name}={value}: {e}");
        }
        // Set in code, validate() checks them as well.
        let mut c = code_config();
        c.deadlines_lease = Duration::ZERO;
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("deadlines_lease")
        );
    }

    #[test]
    fn dead_letter_mode_and_retention_default_to_a_collection_kept_30_days() {
        let c = from(&[]);
        assert_eq!(c.dead_letter_mode, DeadLetterMode::Collection);
        assert_eq!(c.dead_letter_retention, Duration::from_secs(30 * 86_400));
        assert_eq!(code_config().dead_letter_mode, DeadLetterMode::Collection);
        assert_eq!(
            code_config().dead_letter_retention,
            Duration::from_secs(30 * 86_400)
        );
    }

    #[test]
    fn dead_letter_mode_and_retention_from_env() {
        for (value, mode) in [
            ("collection", DeadLetterMode::Collection),
            ("event-store", DeadLetterMode::EventStore),
            ("log", DeadLetterMode::Log),
            (" EVENT-STORE ", DeadLetterMode::EventStore),
            ("Log", DeadLetterMode::Log),
        ] {
            let c = from(&[("SNODRA_EVENTS_DEAD_LETTER_MODE", value)]);
            assert_eq!(c.dead_letter_mode, mode, "{value}");
        }
        assert_eq!(DeadLetterMode::Collection.as_str(), "collection");
        assert_eq!(DeadLetterMode::EventStore.as_str(), "event-store");
        assert_eq!(DeadLetterMode::Log.as_str(), "log");
        let c = from(&[("SNODRA_EVENTS_DEAD_LETTER_RETENTION_MS", "1000")]);
        assert_eq!(c.dead_letter_retention, Duration::from_secs(1));
    }

    #[test]
    fn an_unknown_mode_or_a_retention_under_one_second_is_rejected() {
        for (name, value) in [
            ("SNODRA_EVENTS_DEAD_LETTER_MODE", "queue"),
            ("SNODRA_EVENTS_DEAD_LETTER_RETENTION_MS", "999"),
            ("SNODRA_EVENTS_DEAD_LETTER_RETENTION_MS", "0"),
            ("SNODRA_EVENTS_DEAD_LETTER_RETENTION_MS", "later"),
        ] {
            let e = try_from(&[(name, value)]).unwrap_err().to_string();
            assert!(e.contains(name), "{name}={value}: {e}");
        }
        let mut c = code_config();
        c.dead_letter_retention = Duration::from_millis(500);
        let e = c.validate().unwrap_err().to_string();
        assert!(e.contains("dead_letter_retention"), "{e}");
        assert!(e.contains("SNODRA_EVENTS_DEAD_LETTER_RETENTION_MS"), "{e}");
    }

    #[test]
    fn the_outbox_lease_and_the_schema_setup_default_and_come_from_env() {
        let c = code_config();
        assert_eq!(c.outbox_lease, Duration::from_secs(120));
        assert!(c.store_init_schema);
        let c = from(&[
            ("SNODRA_OUTBOX_LEASE_MS", "90000"),
            ("SNODRA_STORE_INIT_SCHEMA", "false"),
        ]);
        assert_eq!(c.outbox_lease, Duration::from_secs(90));
        assert!(!c.store_init_schema);
    }

    #[test]
    fn the_hub_can_be_turned_off() {
        assert!(from(&[]).hub_enabled, "on by default");
        assert!(!from(&[("SNODRA_SERVER_ENABLED", "false")]).hub_enabled);
        assert!(try_from(&[("SNODRA_SERVER_ENABLED", "maybe")]).is_err());
    }

    #[test]
    fn an_outbox_lease_not_longer_than_a_send_is_rejected() {
        let e = try_from(&[("SNODRA_OUTBOX_LEASE_MS", "40000")])
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("SNODRA_OUTBOX_LEASE_MS") && e.contains("40 s"),
            "{e}"
        );
        let mut c = code_config();
        c.outbox_lease = Duration::from_secs(41);
        assert!(c.validate().is_ok());
    }

    /// A removed setting still set fails the configuration, one message naming every offender
    /// and its replacement (a silently ignored source or mode would misroute events);
    /// `SNODRA_MONGODB_URI` is not one of them (a service may read it for its own client).
    #[test]
    fn a_removed_setting_still_set_fails_naming_every_offender_and_its_replacement() {
        let removed = [
            ("SNODRA_KAFKA_TOPIC", "orders.events"),
            ("SNODRA_KAFKA_DEAD_LETTER_MODE", "topic"),
            ("SNODRA_KAFKA_DEAD_LETTER_RETENTION_MS", "1000"),
        ];
        let refused = try_from(&removed);
        let Err(MessagingError::InvalidMessage(message)) = &refused else {
            panic!("{refused:?}");
        };
        for (name, replacement) in [
            ("SNODRA_KAFKA_TOPIC", "SNODRA_EVENTS_SOURCE"),
            (
                "SNODRA_KAFKA_DEAD_LETTER_MODE",
                "SNODRA_EVENTS_DEAD_LETTER_MODE",
            ),
            (
                "SNODRA_KAFKA_DEAD_LETTER_RETENTION_MS",
                "SNODRA_EVENTS_DEAD_LETTER_RETENTION_MS",
            ),
        ] {
            assert!(
                message.contains(name) && message.contains(replacement),
                "{name} -> {replacement}: {message}"
            );
        }
        assert_eq!(
            message,
            "removed settings are set; they are no longer read, unset them. \
             SNODRA_KAFKA_TOPIC: use SNODRA_EVENTS_SOURCE; \
             SNODRA_KAFKA_DEAD_LETTER_MODE: use SNODRA_EVENTS_DEAD_LETTER_MODE, where `topic` is now `event-store`; \
             SNODRA_KAFKA_DEAD_LETTER_RETENTION_MS: use SNODRA_EVENTS_DEAD_LETTER_RETENTION_MS"
        );
        // One alone is refused too; blank is unset.
        assert!(try_from(&[("SNODRA_KAFKA_TOPIC", "t")]).is_err());
        assert!(try_from(&[("SNODRA_KAFKA_TOPIC", " ")]).is_ok());
        // The service's own MongoDB URI is neither refused nor read.
        assert!(try_from(&[("SNODRA_MONGODB_URI", "mongodb://mongo:27017/orders")]).is_ok());
    }

    /// The v0.4.0 value `topic` of the dead-letter mode is refused naming its replacement.
    #[test]
    fn the_old_dead_letter_mode_topic_fails_naming_event_store() {
        let refused = try_from(&[("SNODRA_EVENTS_DEAD_LETTER_MODE", "topic")]);
        assert!(
            matches!(&refused, Err(MessagingError::InvalidMessage(m))
                if m.contains("SNODRA_EVENTS_DEAD_LETTER_MODE") && m.contains("`topic`") && m.contains("event-store")),
            "{refused:?}"
        );
    }
}
