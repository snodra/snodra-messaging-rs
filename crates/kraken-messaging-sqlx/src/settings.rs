//! The store's own settings (kraken-messaging-jdbc's `kraken.messaging.store.jdbc.*`); the
//! retentions and `KRAKEN_STORE_INIT_SCHEMA` are the core `Config`'s.

use std::time::Duration;

use kraken_messaging::error::MessagingError;

/// `KRAKEN_STORE_SQL_NOTIFY` (`kraken.messaging.store.jdbc.notify`).
pub const NOTIFY_VAR: &str = "KRAKEN_STORE_SQL_NOTIFY";
/// `KRAKEN_STORE_SQL_PURGE_INTERVAL_MS` (`kraken.messaging.store.jdbc.purge-interval`).
pub const PURGE_INTERVAL_VAR: &str = "KRAKEN_STORE_SQL_PURGE_INTERVAL_MS";

/// How the SQL store runs beside its tables.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlxSettings {
    /// Whether appends wake the relays up (PostgreSQL `NOTIFY kraken_outbox`, and the relay's
    /// `LISTEN`); off, the relays poll. No effect on MySQL. Default true.
    pub notify: bool,
    /// How often the purge deletes what the retentions let go; zero turns it off (the service
    /// purges by other means). Default one minute.
    pub purge_interval: Duration,
}

impl Default for SqlxSettings {
    fn default() -> Self {
        SqlxSettings {
            notify: true,
            purge_interval: Duration::from_secs(60),
        }
    }
}

impl SqlxSettings {
    /// The defaults, with `KRAKEN_STORE_SQL_NOTIFY` (`true`/`false`) and `KRAKEN_STORE_SQL_PURGE_INTERVAL_MS`
    /// (milliseconds, 0 = off) when set.
    pub fn from_env() -> Result<SqlxSettings, MessagingError> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<SqlxSettings, MessagingError> {
        let mut s = SqlxSettings::default();
        if let Some(v) = var(NOTIFY_VAR) {
            s.notify = match v.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" => true,
                "false" | "0" | "no" => false,
                _ => {
                    return Err(MessagingError::InvalidMessage(format!(
                        "{NOTIFY_VAR} must be true or false, got '{v}'"
                    )));
                }
            };
        }
        if let Some(v) = var(PURGE_INTERVAL_VAR) {
            let ms: u64 = v.trim().parse().map_err(|_| {
                MessagingError::InvalidMessage(format!(
                    "{PURGE_INTERVAL_VAR} must be a number of milliseconds, got '{v}'"
                ))
            })?;
            s.purge_interval = Duration::from_millis(ms);
        }
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn read(vars: &[(&str, &str)]) -> Result<SqlxSettings, MessagingError> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        SqlxSettings::from_vars(|name| vars.get(name).cloned())
    }

    #[test]
    fn the_defaults_notify_and_purge_every_minute() {
        assert_eq!(read(&[]).unwrap(), SqlxSettings::default());
        assert!(SqlxSettings::default().notify);
        assert_eq!(
            SqlxSettings::default().purge_interval,
            Duration::from_secs(60)
        );
    }

    #[test]
    fn the_variables_set_them() {
        let s = read(&[(NOTIFY_VAR, "false"), (PURGE_INTERVAL_VAR, "0")]).unwrap();
        assert_eq!(
            s,
            SqlxSettings {
                notify: false,
                purge_interval: Duration::ZERO
            }
        );
        assert!(read(&[(NOTIFY_VAR, "maybe")]).is_err());
        assert!(read(&[(PURGE_INTERVAL_VAR, "1m")]).is_err());
    }
}
