//! Outgoing commands and queries: one `Request` per call, correlated reply, timeout, error mapping.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::oneshot;

use crate::codec;
use crate::context::{MessageContext, hub_headers};
use crate::error::MessagingError;
use crate::proto::{self, ClientMessage, RequestKind, client_message, reply};
use crate::types::{Command, Query};

use super::connection::{HubConnection, Inbound};

#[derive(Clone, Debug, Default)]
pub struct SendOptions {
    /// Same key, same owning instance (consistent hashing on the hub); typically the aggregate id.
    pub routing_key: Option<String>,
    /// Overrides the default request timeout.
    pub timeout: Option<Duration>,
    /// Full header value (`Bearer <jwt>`) to send instead of the one forwarded from the current handler context.
    pub authorization: Option<String>,
}

type Pending = oneshot::Sender<Result<Vec<u8>, MessagingError>>;

/// The `CommandBus`/`QueryBus` of the Rust side. Replies arrive through [`Sender::on_inbound`].
pub struct Sender {
    conn: Arc<HubConnection>,
    pending: Mutex<HashMap<String, (String, Pending)>>,
    default_timeout: Duration,
    forward_jwt: bool,
}

impl Sender {
    pub fn new(
        conn: Arc<HubConnection>,
        default_timeout: Duration,
        forward_jwt: bool,
    ) -> Arc<Sender> {
        Arc::new(Sender {
            conn,
            pending: Mutex::new(HashMap::new()),
            default_timeout,
            forward_jwt,
        })
    }

    pub async fn send_command<C: Command>(
        &self,
        command: &C,
        opts: SendOptions,
    ) -> Result<C::Reply, MessagingError> {
        let bytes = self
            .request(RequestKind::Command, C::TYPE, codec::encode(command)?, opts)
            .await?;
        codec::decode_reply(&bytes)
    }

    pub async fn send_query<Q: Query>(
        &self,
        query: &Q,
        opts: SendOptions,
    ) -> Result<Q::Reply, MessagingError> {
        let bytes = self
            .request(RequestKind::Query, Q::TYPE, codec::encode(query)?, opts)
            .await?;
        codec::decode_reply(&bytes)
    }

    /// Completes the pending request a `Reply`/`Error` refers to; a `Disconnected` fails them all.
    pub fn on_inbound(&self, msg: Inbound) {
        match msg {
            Inbound::Reply(r) => {
                if let Some((type_name, tx)) = self.take(&r.request_id) {
                    let result = match r.result {
                        Some(reply::Result::Payload(p)) => Ok(p),
                        Some(reply::Result::Error(e)) => Err(MessagingError::from_hub(
                            &type_name, e.code, &e.message, &e.details,
                        )),
                        None => Ok(b"{}".to_vec()),
                    };
                    let _ = tx.send(result);
                }
            }
            Inbound::Error(e) => {
                if let Some((type_name, tx)) = self.take(&e.request_id) {
                    let _ = tx.send(Err(MessagingError::from_hub(
                        &type_name, e.code, &e.message, &e.details,
                    )));
                }
            }
            Inbound::Disconnected => {
                let all: Vec<(String, (String, Pending))> =
                    self.pending.lock().unwrap().drain().collect();
                for (_, (_, tx)) in all {
                    let _ = tx.send(Err(MessagingError::HubUnavailable(
                        "hub stream closed".into(),
                    )));
                }
            }
            Inbound::Request(_) => {} // the dispatcher's business
        }
    }

    async fn request(
        &self,
        kind: RequestKind,
        type_name: &str,
        payload: Vec<u8>,
        opts: SendOptions,
    ) -> Result<Vec<u8>, MessagingError> {
        let id = uuid::Uuid::new_v4().to_string();
        let current = MessageContext::current();
        let mut headers = HashMap::new();
        headers.insert(
            hub_headers::CORRELATION_ID.to_string(),
            current
                .as_ref()
                .map(|c| c.correlation_id.clone())
                .filter(|c| !c.is_empty())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        );
        let authorization = opts.authorization.or_else(|| {
            if self.forward_jwt {
                current.as_ref().and_then(|c| c.authorization.clone())
            } else {
                None
            }
        });
        if let Some(auth) = authorization {
            headers.insert(hub_headers::AUTHORIZATION.to_string(), auth);
        }
        if let Some(tp) = current
            .as_ref()
            .and_then(|c| c.headers.get(hub_headers::TRACEPARENT))
        {
            headers.insert(hub_headers::TRACEPARENT.to_string(), tp.clone());
        }
        let timeout = opts.timeout.unwrap_or(self.default_timeout);
        let request = proto::Request {
            id: id.clone(),
            kind: kind as i32,
            r#type: type_name.to_string(),
            routing_key: opts.routing_key.unwrap_or_default(),
            payload,
            headers,
            timeout_ms: timeout.as_millis().min(u32::MAX as u128) as u32,
        };

        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .unwrap()
            .insert(id.clone(), (type_name.to_string(), tx));
        if let Err(e) = self
            .conn
            .send(ClientMessage {
                message: Some(client_message::Message::Request(request)),
            })
            .await
        {
            self.take(&id);
            return Err(e);
        }
        // The hub emits TIMEOUT itself; the extra second covers a hub that never answers at all.
        match tokio::time::timeout(timeout + Duration::from_secs(1), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(MessagingError::HubUnavailable("sender closed".into())),
            Err(_) => {
                self.take(&id);
                Err(MessagingError::Timeout(type_name.to_string()))
            }
        }
    }

    fn take(&self, request_id: &str) -> Option<(String, Pending)> {
        self.pending.lock().unwrap().remove(request_id)
    }
}
