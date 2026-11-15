//! Publishing: one publisher per stream on its leader's connection, no producer name (no
//! deduplication), each event confirmed by the broker before `publish` returns.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use kraken_messaging::event_store::{EventStoreError, OutgoingEvent};
use rabbitmq_stream_client::types::ResponseCode;
use tokio::sync::Mutex;

use crate::conn::{Broker, Conn, Nodes, Part, failed};
use crate::wire;

/// How long a source's partitions are published to before they are read again.
pub const LAYOUT_FOR: Duration = Duration::from_secs(30);
/// The longest a publish waits for the broker's confirm, by default
/// ([`RabbitConfig::confirm_timeout`](crate::RabbitConfig::confirm_timeout)).
pub const SEND_TIMEOUT: Duration = Duration::from_secs(30);
/// The largest publish frame the store sends: the client's and the broker's default frame size
/// (1 MiB). A larger event is refused for good.
pub const MAX_FRAME: usize = 1_048_576;

/// A sent event: its stream, its connection, its publishing id, and the broker's answer to come.
type Sent = (
    String,
    Arc<Conn>,
    u64,
    tokio::sync::oneshot::Receiver<Result<(), ResponseCode>>,
);

struct State {
    nodes: Nodes,
    /// Per source: its streams, and when they were read (read again after [`LAYOUT_FOR`], so a
    /// partition added to a super stream is published to).
    layouts: HashMap<String, (Vec<Part>, std::time::Instant)>,
    /// Per stream: its leader's connection and the publisher declared there.
    publishers: HashMap<String, (Arc<Conn>, u8)>,
    next_id: u64,
}

pub(crate) struct Publisher {
    state: Mutex<State>,
    confirm_timeout: Duration,
}

impl Publisher {
    pub fn new(broker: Broker) -> Publisher {
        let confirm_timeout = broker.config().confirm_timeout;
        Publisher {
            confirm_timeout,
            state: Mutex::new(State {
                nodes: Nodes::new(broker),
                layouts: HashMap::new(),
                publishers: HashMap::new(),
                next_id: 1,
            }),
        }
    }

    pub async fn publish(&self, event: &OutgoingEvent) -> Result<(), EventStoreError> {
        if event.source.trim().is_empty() {
            return Err(EventStoreError::Invalid("the source is empty".into()));
        }
        let (stream, conn, id, confirm) = {
            let mut state = self.state.lock().await;
            match state.send(event).await {
                Ok(sent) => sent,
                Err(e) => {
                    state.forget_broken();
                    return Err(e);
                }
            }
        };
        let outcome = tokio::time::timeout(self.confirm_timeout, confirm).await;
        match outcome {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(code))) => {
                // The publisher may be gone (its stream deleted or moved): declared again next
                // time.
                let mut state = self.state.lock().await;
                state.publishers.remove(&stream);
                state.layouts.clear();
                Err(refused(&stream, code))
            }
            Ok(Err(_)) => {
                self.state.lock().await.forget_broken();
                Err(failed(format!(
                    "the connection closed before the broker confirmed an event to {}",
                    event.source
                )))
            }
            Err(_) => {
                // A connection that confirms nothing in time is replaced: its read task may
                // have stopped, and every later publish on it would wait as long.
                conn.forget_confirm(id);
                let mut state = self.state.lock().await;
                state.publishers.retain(|_, (c, _)| !Arc::ptr_eq(c, &conn));
                state.layouts.clear();
                conn.abandon("no confirm in time");
                state.nodes.prune();
                Err(failed(format!(
                    "the broker did not confirm an event to {} within {:?}: the connection is replaced",
                    event.source, self.confirm_timeout
                )))
            }
        }
    }

    pub async fn close(&self) {
        self.state.lock().await.nodes.close().await;
    }

    /// The connections the publisher holds now.
    pub async fn connections(&self) -> usize {
        self.state.lock().await.nodes.held()
    }
}

impl State {
    /// Sends `event` to its stream: the connection it went on and its publishing id.
    async fn send(&mut self, event: &OutgoingEvent) -> Result<Sent, EventStoreError> {
        let id = self.next_id;
        self.next_id += 1;
        let message = wire::message(event, id);
        let size = wire::frame_size(&message);
        if size > MAX_FRAME {
            return Err(EventStoreError::Rejected(format!(
                "the event {} is {size} bytes on the wire, over the frame size of {MAX_FRAME}",
                event.id
            )));
        }
        let parts = match self
            .layouts
            .get(&event.source)
            .filter(|(_, at)| at.elapsed() < LAYOUT_FOR)
        {
            Some((parts, _)) => parts.clone(),
            None => {
                let parts = self.nodes.layout(&event.source).await?.ok_or_else(|| {
                    failed(format!(
                        "neither a super stream nor a stream named {} exists: create it \
                         (rabbitmq-streams add_super_stream {} --partitions 6)",
                        event.source, event.source
                    ))
                })?;
                self.layouts.insert(
                    event.source.clone(),
                    (parts.clone(), std::time::Instant::now()),
                );
                parts
            }
        };
        let stream = parts[wire::partition_index(wire::routing_key(event), parts.len())]
            .stream
            .clone();
        let (conn, publisher) = self.publisher(&stream).await?;
        // Registered before the send, so a confirm cannot come first.
        let confirm = conn.confirm(id);
        if let Err(e) = conn.client.publish(publisher, message, 1).await {
            conn.forget_confirm(id);
            return Err(failed(format!("publishing to {stream}: {e}")));
        }
        Ok((stream, conn, id, confirm))
    }

    async fn publisher(&mut self, stream: &str) -> Result<(Arc<Conn>, u8), EventStoreError> {
        if let Some((conn, id)) = self.publishers.get(stream).filter(|(c, _)| !c.is_closed()) {
            return Ok((Arc::clone(conn), *id));
        }
        let conn = self
            .nodes
            .leader(stream)
            .await?
            .ok_or_else(|| failed(format!("the stream {stream} does not exist")))?;
        let Some(id) = conn.publisher_id() else {
            self.publishers.retain(|_, (c, _)| !Arc::ptr_eq(c, &conn));
            self.nodes.forget(&conn);
            return Err(failed(format!(
                "256 publishers were declared on the connection to {}:{}: it is replaced",
                conn.node.0, conn.node.1
            )));
        };
        let response = conn
            .timed(conn.client.declare_publisher(id, None, stream))
            .await
            .map_err(|e| failed(format!("declaring a publisher on {stream}: {e}")))?;
        if !response.is_ok() {
            return Err(refused(stream, response.code().clone()));
        }
        self.publishers
            .insert(stream.to_string(), (Arc::clone(&conn), id));
        Ok((conn, id))
    }

    /// Drops what a lost connection held: its publishers, the layouts (a stream may have moved).
    fn forget_broken(&mut self) {
        self.publishers.retain(|_, (c, _)| !c.is_closed());
        self.layouts.clear();
        self.nodes.prune();
    }
}

/// The broker's refusal as an error: for good (`Rejected`) when publishing again cannot succeed
/// (not authorized, a frame too large), else `Failed` (a stream missing now may be created).
fn refused(stream: &str, code: ResponseCode) -> EventStoreError {
    match code {
        ResponseCode::AccessRefused | ResponseCode::FrameTooLarge => {
            EventStoreError::Rejected(format!("rabbitmq: {stream}: {code:?}"))
        }
        code => failed(format!("{stream}: {code:?}")),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::config::RabbitConfig;

    /// An event over the frame size is refused for good before anything connects (the broker is
    /// unreachable here).
    #[tokio::test]
    async fn an_event_over_the_frame_size_is_rejected_at_once() {
        let config = Arc::new(RabbitConfig::new("127.0.0.1:1"));
        let publisher = Publisher::new(Broker::new(config, "test").unwrap());
        let event = OutgoingEvent {
            source: "kraken.events".into(),
            id: "e-1".into(),
            type_name: "t".into(),
            key: Some("k".into()),
            headers: HashMap::new(),
            payload: vec![b'x'; MAX_FRAME],
        };
        let refused = publisher.publish(&event).await;
        assert!(
            matches!(&refused, Err(EventStoreError::Rejected(m)) if m.contains("frame size")),
            "{refused:?}"
        );
        let small = OutgoingEvent {
            payload: b"{}".to_vec(),
            ..event
        };
        assert!(matches!(
            publisher.publish(&small).await,
            Err(EventStoreError::Failed(_))
        ));
        assert!(matches!(
            publisher
                .publish(&OutgoingEvent {
                    source: " ".into(),
                    ..small
                })
                .await,
            Err(EventStoreError::Invalid(_))
        ));
    }

    #[test]
    fn only_a_refusal_for_good_is_rejected() {
        for code in [ResponseCode::AccessRefused, ResponseCode::FrameTooLarge] {
            assert!(matches!(refused("s", code), EventStoreError::Rejected(_)));
        }
        for code in [
            ResponseCode::StreamDoesNotExist,
            ResponseCode::PublisherDoesNotExist,
        ] {
            assert!(matches!(refused("s", code), EventStoreError::Failed(_)));
        }
    }
}
