//! The add-on's settings ([`RabbitConfig`]), in code or from `KRAKEN_RABBITMQ_*` variables.

use std::path::PathBuf;
use std::time::Duration;

use kraken_messaging::event_store::EventStoreError;
use kraken_messaging::event_store::browse::BrowseSettings;

pub use crate::subscription::StorePolicy;

/// The stream port without TLS.
pub const DEFAULT_PORT: u16 = 5552;
/// The stream port with TLS.
pub const DEFAULT_TLS_PORT: u16 = 5551;
/// The host when none is configured.
pub const DEFAULT_HOST: &str = "rabbitmq";

/// The management HTTP API (the plugins `rabbitmq_management` and `rabbitmq_stream_management`,
/// with metrics collection on: `management.disable_stats` false): who reads a subscription now.
/// Its `Debug` never shows the password.
#[derive(Clone, PartialEq, Eq)]
pub struct Management {
    /// `http(s)://host:port` (the API's root, without `/api`), never with credentials in it.
    pub url: String,
    /// `None`: the stream user's.
    pub username: Option<String>,
    /// `None`: the stream user's.
    pub password: Option<String>,
    /// PEM CA certificates an `https` API is verified against (besides the platform's roots).
    pub ca_file: Option<PathBuf>,
}

impl std::fmt::Debug for Management {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Management")
            .field("url", &redacted(&self.url))
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<hidden>"))
            .field("ca_file", &self.ca_file)
            .finish()
    }
}

/// `url` without the credentials it may carry (`scheme://<hidden>@host`).
pub(crate) fn redacted(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let authority = rest.split('/').next().unwrap_or_default();
    match authority.rsplit_once('@') {
        Some((_, host)) => format!("{scheme}://<hidden>@{host}{}", &rest[authority.len()..]),
        None => url.to_string(),
    }
}

/// How the store reaches RabbitMQ. Its `Debug` never shows a password.
#[derive(Clone, PartialEq, Eq)]
pub struct RabbitConfig {
    /// The nodes to connect to first (`host` or `host:port`), tried in order; the store then
    /// connects to each stream's leader as the broker advertises it (or through these, with
    /// [`load_balancer`](Self::load_balancer)).
    pub hosts: Vec<String>,
    /// The port of a host given without one (`None`: 5552, or 5551 with TLS).
    pub port: Option<u16>,
    pub virtual_host: String,
    pub username: String,
    pub password: String,
    /// TLS to every node, verified against [`tls_ca_file`](Self::tls_ca_file) (required: the
    /// client verifies only against a CA file, and the store never connects unverified).
    pub tls: bool,
    /// PEM file of the CA certificates the nodes' certificates are checked against.
    pub tls_ca_file: Option<PathBuf>,
    /// The hosts are a load balancer in front of the nodes: the store connects through it until
    /// it lands on the node it needs (the broker's advertised host), instead of connecting to
    /// that node directly.
    pub load_balancer: bool,
    /// The management HTTP API; without it the store cannot tell who reads a subscription, so it
    /// does not declare `replay` (see the crate docs).
    pub management: Option<Management>,
    /// Subscription names the browser's lag view always lists (with a stored offset), besides the
    /// ones this process subscribed and the ones the management API shows reading.
    pub known_subscriptions: Vec<String>,
    /// When a subscription stores its acknowledged offset (every stored offset is an entry in
    /// the stream).
    pub offset_store: StorePolicy,
    /// The browser's sources (the first is the default) and bounds.
    pub browse: BrowseSettings,
}

impl RabbitConfig {
    /// RabbitMQ at `host` (`host` or `host:port`), user `guest`, virtual host `/`, no TLS, no
    /// management API.
    pub fn new(host: &str) -> RabbitConfig {
        RabbitConfig {
            hosts: vec![host.to_string()],
            port: None,
            virtual_host: "/".into(),
            username: "guest".into(),
            password: "guest".into(),
            tls: false,
            tls_ca_file: None,
            load_balancer: false,
            management: None,
            known_subscriptions: Vec::new(),
            offset_store: StorePolicy::default(),
            browse: BrowseSettings::default(),
        }
    }

    /// From the environment (see the crate docs for every variable); `Invalid` for a value that
    /// does not parse or a password file that cannot be read.
    pub fn from_env() -> Result<RabbitConfig, EventStoreError> {
        RabbitConfig::from_vars(std::env::vars())
    }

    /// [`from_env`](Self::from_env) over `vars` (name, value).
    pub fn from_vars(
        vars: impl IntoIterator<Item = (String, String)>,
    ) -> Result<RabbitConfig, EventStoreError> {
        let vars: std::collections::HashMap<String, String> = vars
            .into_iter()
            .filter(|(name, _)| name.starts_with("KRAKEN_RABBITMQ_"))
            .collect();
        let get = |name: &str| {
            vars.get(&format!("KRAKEN_RABBITMQ_{name}"))
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let list = |value: Option<String>| -> Vec<String> {
            value
                .map(|v| {
                    v.split(',')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default()
        };
        let mut config = RabbitConfig::new(DEFAULT_HOST);
        let hosts = list(get("HOSTS"));
        if !hosts.is_empty() {
            config.hosts = hosts;
        }
        if let Some(port) = get("PORT") {
            config.port = Some(port.parse().map_err(|_| {
                EventStoreError::Invalid(format!("KRAKEN_RABBITMQ_PORT is not a port: {port}"))
            })?);
        }
        if let Some(vhost) = get("VIRTUAL_HOST") {
            config.virtual_host = vhost;
        }
        if let Some(user) = get("USERNAME") {
            config.username = user;
        }
        if let Some(password) = secret(&vars, "PASSWORD")? {
            config.password = password;
        }
        if let Some(tls) = get("TLS") {
            config.tls = flag("KRAKEN_RABBITMQ_TLS", &tls)?;
        }
        config.tls_ca_file = get("TLS_CA_FILE").map(PathBuf::from);
        if let Some(lb) = get("LOAD_BALANCER") {
            config.load_balancer = flag("KRAKEN_RABBITMQ_LOAD_BALANCER", &lb)?;
        }
        if let Some(url) = get("MANAGEMENT_URL") {
            config.management = Some(Management {
                url,
                username: get("MANAGEMENT_USERNAME"),
                password: secret(&vars, "MANAGEMENT_PASSWORD")?,
                ca_file: get("MANAGEMENT_CA_FILE").map(PathBuf::from),
            });
        }
        if let Some(n) = get("OFFSET_STORE_MESSAGES") {
            config.offset_store.messages = n.parse().map_err(|_| {
                EventStoreError::Invalid(format!(
                    "KRAKEN_RABBITMQ_OFFSET_STORE_MESSAGES is not a number: {n}"
                ))
            })?;
        }
        if let Some(ms) = get("OFFSET_STORE_INTERVAL_MS") {
            config.offset_store.interval = Duration::from_millis(ms.parse().map_err(|_| {
                EventStoreError::Invalid(format!(
                    "KRAKEN_RABBITMQ_OFFSET_STORE_INTERVAL_MS is not a number: {ms}"
                ))
            })?);
        }
        config.known_subscriptions = list(get("KNOWN_SUBSCRIPTIONS"));
        Ok(config)
    }

    /// The user and password.
    pub fn credentials(mut self, username: &str, password: &str) -> RabbitConfig {
        self.username = username.to_string();
        self.password = password.to_string();
        self
    }

    pub fn virtual_host(mut self, virtual_host: &str) -> RabbitConfig {
        self.virtual_host = virtual_host.to_string();
        self
    }

    /// The port of a host given without one.
    pub fn port(mut self, port: u16) -> RabbitConfig {
        self.port = Some(port);
        self
    }

    /// TLS, verified against the CA certificates of `ca_file` (PEM).
    pub fn tls(mut self, ca_file: impl Into<PathBuf>) -> RabbitConfig {
        self.tls = true;
        self.tls_ca_file = Some(ca_file.into());
        self
    }

    /// The hosts are a load balancer (see [`load_balancer`](Self::load_balancer)).
    pub fn load_balancer(mut self, load_balancer: bool) -> RabbitConfig {
        self.load_balancer = load_balancer;
        self
    }

    /// The management HTTP API at `url`, with the stream user's credentials (whenever they are
    /// set) unless [`management_credentials`](Self::management_credentials) says otherwise.
    pub fn management(mut self, url: &str) -> RabbitConfig {
        self.management = Some(Management {
            url: url.to_string(),
            username: None,
            password: None,
            ca_file: None,
        });
        self
    }

    /// The management API's own user and password (after [`management`](Self::management)).
    pub fn management_credentials(mut self, username: &str, password: &str) -> RabbitConfig {
        if let Some(m) = &mut self.management {
            m.username = Some(username.to_string());
            m.password = Some(password.to_string());
        }
        self
    }

    /// The CA certificates of an `https` management API (after [`management`](Self::management)).
    pub fn management_ca(mut self, ca_file: impl Into<PathBuf>) -> RabbitConfig {
        if let Some(m) = &mut self.management {
            m.ca_file = Some(ca_file.into());
        }
        self
    }

    /// When subscriptions store their acknowledged offsets.
    pub fn offset_store(mut self, policy: StorePolicy) -> RabbitConfig {
        self.offset_store = policy;
        self
    }

    /// The management API's user and password: its own, else the stream user's.
    pub(crate) fn management_login(&self) -> (String, String) {
        let m = self.management.as_ref();
        (
            m.and_then(|m| m.username.clone())
                .unwrap_or_else(|| self.username.clone()),
            m.and_then(|m| m.password.clone())
                .unwrap_or_else(|| self.password.clone()),
        )
    }

    /// Subscription names the browser's lag view always lists.
    pub fn known_subscriptions(mut self, names: &[&str]) -> RabbitConfig {
        self.known_subscriptions = names.iter().map(|n| n.to_string()).collect();
        self
    }

    /// The browser's sources and bounds.
    pub fn browse(mut self, settings: BrowseSettings) -> RabbitConfig {
        self.browse = settings;
        self
    }

    /// The port of a host given without one.
    pub fn effective_port(&self) -> u16 {
        self.port.unwrap_or(if self.tls {
            DEFAULT_TLS_PORT
        } else {
            DEFAULT_PORT
        })
    }

    /// The hosts as `(host, port)` (an IPv6 address with a port in brackets: `[::1]:5552`).
    pub fn addresses(&self) -> Result<Vec<(String, u16)>, EventStoreError> {
        self.hosts
            .iter()
            .map(|h| {
                let h = h.trim();
                let (host, port) = match h.rsplit_once(':') {
                    Some((host, port)) if !host.contains(':') || host.ends_with(']') => {
                        (host, Some(port))
                    }
                    _ => (h, None),
                };
                let host = host.trim_start_matches('[').trim_end_matches(']');
                if host.is_empty() {
                    return Err(EventStoreError::Invalid(format!(
                        "the RabbitMQ host {h:?} is not host or host:port"
                    )));
                }
                let port = match port {
                    None => self.effective_port(),
                    Some(p) => p.parse().map_err(|_| {
                        EventStoreError::Invalid(format!(
                            "the RabbitMQ host {h} is not host or host:port"
                        ))
                    })?,
                };
                Ok((host.to_string(), port))
            })
            .collect()
    }

    pub(crate) fn validate(&self) -> Result<(), EventStoreError> {
        if self.hosts.is_empty() {
            return Err(EventStoreError::Invalid(
                "no RabbitMQ host is configured".into(),
            ));
        }
        self.addresses()?;
        if self.tls && self.tls_ca_file.is_none() {
            return Err(EventStoreError::Invalid(
                "TLS to RabbitMQ needs a CA file (KRAKEN_RABBITMQ_TLS_CA_FILE): the stream client \
                 verifies the nodes only against one, and the store never connects unverified"
                    .into(),
            ));
        }
        if let Some(m) = &self.management {
            let rest = m
                .url
                .strip_prefix("http://")
                .or_else(|| m.url.strip_prefix("https://"));
            match rest {
                None => {
                    return Err(EventStoreError::Invalid(
                        "the RabbitMQ management URL is not http(s)://host:port".into(),
                    ));
                }
                // Credentials only through their settings: a URL carrying them is refused, and
                // never shown.
                Some(rest)
                    if rest
                        .split('/')
                        .next()
                        .is_some_and(|auth| auth.contains('@')) =>
                {
                    return Err(EventStoreError::Invalid(
                        "the RabbitMQ management URL carries credentials: set \
                         KRAKEN_RABBITMQ_MANAGEMENT_USERNAME and _PASSWORD instead"
                            .into(),
                    ));
                }
                Some(_) => {}
            }
        }
        if self.offset_store.messages == 0 || self.offset_store.interval.is_zero() {
            return Err(EventStoreError::Invalid(
                "the offset store policy needs at least 1 message and 1 ms".into(),
            ));
        }
        self.browse.validate().map_err(EventStoreError::Invalid)
    }
}

impl std::fmt::Debug for RabbitConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RabbitConfig")
            .field("hosts", &self.hosts)
            .field("port", &self.port)
            .field("virtual_host", &self.virtual_host)
            .field("username", &self.username)
            .field("password", &"<hidden>")
            .field("tls", &self.tls)
            .field("tls_ca_file", &self.tls_ca_file)
            .field("load_balancer", &self.load_balancer)
            .field("management", &self.management)
            .field("known_subscriptions", &self.known_subscriptions)
            .field("offset_store", &self.offset_store)
            .field("browse", &self.browse)
            .finish()
    }
}

/// `true`/`false` (`1`/`0`, `yes`/`no`, any case).
fn flag(name: &str, value: &str) -> Result<bool, EventStoreError> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(EventStoreError::Invalid(format!(
            "{name} is not true or false: {value}"
        ))),
    }
}

/// `KRAKEN_RABBITMQ_<name>`, else the content of the file `KRAKEN_RABBITMQ_<name>_FILE` (its
/// trailing line break removed); the variable wins when both are set.
fn secret(
    vars: &std::collections::HashMap<String, String>,
    name: &str,
) -> Result<Option<String>, EventStoreError> {
    if let Some(value) = vars
        .get(&format!("KRAKEN_RABBITMQ_{name}"))
        .filter(|v| !v.is_empty())
    {
        return Ok(Some(value.clone()));
    }
    let file_var = format!("KRAKEN_RABBITMQ_{name}_FILE");
    let Some(path) = vars
        .get(&file_var)
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
    else {
        return Ok(None);
    };
    let content = std::fs::read_to_string(path)
        .map_err(|e| EventStoreError::Invalid(format!("{file_var}: cannot read {path}: {e}")))?;
    Ok(Some(content.trim_end_matches(['\r', '\n']).to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn the_environment_configures_every_setting() {
        let dir = std::env::temp_dir().join(format!("kraken-rmq-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("password");
        std::fs::write(&file, "from-the-file\n").unwrap();
        let config = RabbitConfig::from_vars(vars(&[
            ("KRAKEN_RABBITMQ_HOSTS", "r1, r2:5560"),
            ("KRAKEN_RABBITMQ_PORT", "5553"),
            ("KRAKEN_RABBITMQ_VIRTUAL_HOST", "orders"),
            ("KRAKEN_RABBITMQ_USERNAME", "svc"),
            ("KRAKEN_RABBITMQ_PASSWORD_FILE", file.to_str().unwrap()),
            ("KRAKEN_RABBITMQ_TLS", "true"),
            ("KRAKEN_RABBITMQ_TLS_CA_FILE", "/etc/ca.pem"),
            ("KRAKEN_RABBITMQ_LOAD_BALANCER", "yes"),
            ("KRAKEN_RABBITMQ_MANAGEMENT_URL", "http://r1:15672"),
            (
                "KRAKEN_RABBITMQ_KNOWN_SUBSCRIPTIONS",
                "orders.projector, orders.audit",
            ),
            ("PATH", "/bin"),
        ]))
        .unwrap();
        assert_eq!(
            config.addresses().unwrap(),
            [("r1".to_string(), 5553), ("r2".to_string(), 5560)]
        );
        assert_eq!(config.virtual_host, "orders");
        assert_eq!(config.username, "svc");
        assert_eq!(config.password, "from-the-file");
        assert!(config.tls && config.load_balancer);
        assert_eq!(config.tls_ca_file, Some(PathBuf::from("/etc/ca.pem")));
        assert_eq!(config.management.as_ref().unwrap().url, "http://r1:15672");
        assert_eq!(
            config.management_login(),
            ("svc".to_string(), "from-the-file".to_string())
        );
        assert_eq!(
            config.known_subscriptions,
            ["orders.projector", "orders.audit"]
        );
        assert!(config.validate().is_ok());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_defaults_and_the_tls_port() {
        let config = RabbitConfig::from_vars(vec![]).unwrap();
        assert_eq!(
            config.addresses().unwrap(),
            [("rabbitmq".to_string(), 5552)]
        );
        assert_eq!(
            (config.username.as_str(), config.virtual_host.as_str()),
            ("guest", "/")
        );
        assert!(config.management.is_none() && !config.tls);
        let tls = RabbitConfig::new("r").tls("/ca.pem");
        assert_eq!(tls.addresses().unwrap(), [("r".to_string(), 5551)]);
    }

    #[test]
    fn bad_values_are_refused() {
        for bad in [
            vars(&[("KRAKEN_RABBITMQ_PORT", "x")]),
            vars(&[("KRAKEN_RABBITMQ_TLS", "maybe")]),
            vars(&[("KRAKEN_RABBITMQ_PASSWORD_FILE", "/no/such/kraken/file")]),
        ] {
            assert!(
                matches!(
                    RabbitConfig::from_vars(bad.clone()),
                    Err(EventStoreError::Invalid(_))
                ),
                "{bad:?}"
            );
        }
        let mut no_ca = RabbitConfig::new("r");
        no_ca.tls = true;
        assert!(
            matches!(no_ca.validate(), Err(EventStoreError::Invalid(m)) if m.contains("CA file"))
        );
        assert!(RabbitConfig::new("r:x").validate().is_err());
        assert!(RabbitConfig::new(" ").validate().is_err());
        assert!(
            RabbitConfig::new("r")
                .management("r:15672")
                .validate()
                .is_err()
        );
    }

    #[test]
    fn debug_never_shows_a_password() {
        let config = RabbitConfig::new("r")
            .credentials("svc", "s3cr3t-value")
            .management("http://r:15672")
            .management_credentials("admin", "other-s3cr3t");
        let shown = format!("{config:?}");
        assert!(shown.contains("svc") && shown.contains("admin"), "{shown}");
        for secret in ["s3cr3t-value", "other-s3cr3t"] {
            assert!(!shown.contains(secret), "{shown}");
        }
    }

    #[test]
    fn the_variable_wins_over_the_file() {
        let config = RabbitConfig::from_vars(vars(&[
            ("KRAKEN_RABBITMQ_PASSWORD", "direct"),
            ("KRAKEN_RABBITMQ_PASSWORD_FILE", "/no/such/kraken/file"),
            ("KRAKEN_RABBITMQ_MANAGEMENT_URL", "http://m:15672"),
            ("KRAKEN_RABBITMQ_MANAGEMENT_USERNAME", "admin"),
            ("KRAKEN_RABBITMQ_MANAGEMENT_PASSWORD", "m-pass"),
        ]))
        .unwrap();
        assert_eq!(config.password, "direct");
        assert_eq!(
            config.management_login(),
            ("admin".to_string(), "m-pass".to_string())
        );
    }
}

#[cfg(test)]
mod order {
    use super::*;

    /// The stream user's credentials set after the management API still reach it; its own,
    /// set explicitly, win in either order.
    #[test]
    fn credentials_set_in_any_order_reach_the_management_api() {
        let late = RabbitConfig::new("r")
            .management("http://r:15672")
            .credentials("svc", "pw");
        assert_eq!(
            late.management_login(),
            ("svc".to_string(), "pw".to_string())
        );
        let own = RabbitConfig::new("r")
            .management("http://r:15672")
            .management_credentials("admin", "m")
            .credentials("svc", "pw");
        assert_eq!(
            own.management_login(),
            ("admin".to_string(), "m".to_string())
        );
    }

    #[test]
    fn the_offset_store_policy_comes_from_the_environment() {
        let config = RabbitConfig::from_vars(vec![
            (
                "KRAKEN_RABBITMQ_OFFSET_STORE_MESSAGES".to_string(),
                "50".to_string(),
            ),
            (
                "KRAKEN_RABBITMQ_OFFSET_STORE_INTERVAL_MS".to_string(),
                "250".to_string(),
            ),
        ])
        .unwrap();
        assert_eq!(
            config.offset_store,
            StorePolicy {
                messages: 50,
                interval: Duration::from_millis(250)
            }
        );
        assert_eq!(RabbitConfig::new("r").offset_store, StorePolicy::default());
    }
}
