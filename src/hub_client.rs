//! Client side of `Hub.Connect`: registers this instance's handlers, sends commands and queries,
//! correlates replies. Skeleton: the public surface is in place, transport is not implemented.

use std::sync::Arc;
use std::time::Duration;

use crate::error::MessagingError;
use crate::handlers::{Command, HandlerContext, HandlerRegistry, Query};

/// Outbound metadata attached to a command or query.
#[derive(Debug, Clone, Default)]
pub struct SendOptions {
    /// Consistent-hash routing key (typically the aggregate id). Empty means round-robin.
    pub routing_key: String,
    /// `Bearer <JWT>` of the acting user, forwarded in `Request.headers["authorization"]`.
    pub authorization: Option<String>,
    pub correlation_id: Option<String>,
    pub traceparent: Option<String>,
    /// Per-request timeout; `None` means the hub default (5 s).
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct HubConfig {
    /// For example `http://hub:7000`. Env: `KRAKEN_HUB_ADDRESS`.
    pub address: String,
    pub heartbeat_interval: Duration,
}

impl HubConfig {
    pub fn from_env() -> Self {
        Self {
            address: std::env::var("KRAKEN_HUB_ADDRESS")
                .unwrap_or_else(|_| "http://localhost:7000".to_string()),
            heartbeat_interval: Duration::from_secs(10),
        }
    }
}

pub struct HubClient {
    config: HubConfig,
    registry: Arc<HandlerRegistry>,
    instance_id: String,
    // TODO: tonic channel, outbound mpsc sender, pending-reply map keyed by request id.
}

impl HubClient {
    /// Opens the stream, sends `Register` with the registry's command and query types, starts the
    /// heartbeat task and the inbound dispatch task.
    pub async fn connect(config: HubConfig, registry: HandlerRegistry) -> Result<Self, MessagingError> {
        let client = Self {
            config,
            registry: Arc::new(registry),
            instance_id: uuid::Uuid::now_v7().to_string(),
        };
        tracing::info!(address = %client.config.address, instance = %client.instance_id,
            "hub client created (transport not implemented)");
        // TODO: tonic::transport::Channel::from_shared(address).connect(), then
        // proto::hub_client::HubClient::connect(stream) and the dispatch loop.
        Ok(client)
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn registry(&self) -> &HandlerRegistry {
        &self.registry
    }

    pub async fn send_command<C: Command>(
        &self,
        _command: C,
        _options: SendOptions,
    ) -> Result<C::Response, MessagingError> {
        Err(MessagingError::NotImplemented("HubClient::send_command"))
    }

    pub async fn send_query<Q: Query>(
        &self,
        _query: Q,
        _options: SendOptions,
    ) -> Result<Q::Response, MessagingError> {
        Err(MessagingError::NotImplemented("HubClient::send_query"))
    }

    /// Builds the handler context from a forwarded hub request's headers.
    pub(crate) fn context_from_headers(
        headers: &std::collections::HashMap<String, String>,
    ) -> HandlerContext {
        use crate::envelope::hub_headers as h;
        HandlerContext {
            correlation_id: headers.get(h::CORRELATION_ID).cloned().unwrap_or_default(),
            authorization: headers.get(h::AUTHORIZATION).cloned(),
            user_id: None,
            traceparent: headers.get(h::TRACEPARENT).cloned(),
        }
    }
}
