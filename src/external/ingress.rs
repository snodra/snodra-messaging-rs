//! The webhook side of the integration toolkit (ADR 0021 ruling 18, spec "Inbound 2"): a service
//! exposes its own endpoint, verifies the request with [`verify`](super::webhook::verify), turns
//! the body into an event and calls [`ExternalEventIngress::accept`]. The Rust form of the Kotlin
//! `ExternalEventIngress`.

use std::collections::HashMap;
use std::sync::Arc;

use mongodb::Database;
use mongodb::bson::{Document, doc};

use crate::codec;
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::kafka::dispatch::Outcome;
use crate::kafka::{LocalEventDispatcher, PROCESSED_EVENTS_COLLECTION, headers};
use crate::saga::Clock;
use crate::types::Event;

/// The prefix of the marker id (in `processed_events`) that remembers a finished webhook call,
/// next to the dispatcher's own markers.
pub const INGRESS_MARKER_PREFIX: &str = "ingress:";

/// What [`ExternalEventIngress::accept`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngressResult {
    /// The event was dispatched now (or nobody here handles its type).
    Accepted,
    /// An earlier call with the same source and dedupe key already returned: nothing was done.
    Duplicate,
}

/// See the module docs. Cheap to clone.
#[derive(Clone)]
pub struct ExternalEventIngress {
    dispatcher: Arc<LocalEventDispatcher>,
    db: Database,
    clock: Clock,
}

impl ExternalEventIngress {
    pub fn new(
        dispatcher: Arc<LocalEventDispatcher>,
        db: Database,
        clock: Clock,
    ) -> ExternalEventIngress {
        ExternalEventIngress {
            dispatcher,
            db,
            clock,
        }
    }

    /// Dispatches `event` as the one `source` and `dedupe_key` (the partner's own event or
    /// delivery id, stable across its retries) identify, **before returning**: the ordinary event
    /// handlers of this service, then its sagas, each in the dispatcher's transactions, keyed by
    /// the dedupe id `<source>:<dedupeKey>`. `headers` are extra message headers for the handlers
    /// (a saga's header association reads them; `correlationId` becomes the correlation id).
    ///
    /// Then the `ingress:<source>:<dedupeKey>` marker is inserted and the call is
    /// [`Accepted`](IngressResult::Accepted); a call that finds the marker is
    /// [`Duplicate`](IngressResult::Duplicate) without dispatching. A handler failure is returned
    /// (answer 5xx: the partner retries, and the markers of what already succeeded make the retry
    /// apply only the rest). An event nobody handles is `Accepted` with a warning and no marker.
    /// Two simultaneous deliveries of one key may both dispatch: the markers make the second a
    /// no-op and only one is told `Accepted`. A blank source or key, or a reserved source name
    /// ([`RESERVED_SOURCE_NAMES`](super::RESERVED_SOURCE_NAMES)), is `InvalidMessage`.
    pub async fn accept<E: Event>(
        &self,
        source: &str,
        dedupe_key: &str,
        event: &E,
        headers: &HashMap<String, String>,
    ) -> Result<IngressResult, MessagingError> {
        if source.trim().is_empty() || dedupe_key.trim().is_empty() {
            return Err(MessagingError::InvalidMessage(
                "a webhook event needs a source and a dedupe key".into(),
            ));
        }
        if super::is_reserved_source_name(source) {
            return Err(MessagingError::InvalidMessage(format!(
                "webhook source '{source}': the name is reserved (the library's marker families: {})",
                super::RESERVED_SOURCE_NAMES.join(", ")
            )));
        }
        let dedupe_id = format!("{source}:{dedupe_key}");
        let marker = format!("{INGRESS_MARKER_PREFIX}{dedupe_id}");
        let markers = self.db.collection::<Document>(PROCESSED_EVENTS_COLLECTION);
        if markers.count_documents(doc! { "_id": &marker }).await? > 0 {
            return Ok(IngressResult::Duplicate);
        }

        let mut all = headers.clone();
        let correlation_id = headers
            .get(headers::CORRELATION_ID)
            .filter(|c| !c.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| dedupe_id.clone());
        all.insert(headers::ID.to_string(), dedupe_id.clone());
        all.insert(headers::TYPE.to_string(), E::TYPE.to_string());
        all.insert(headers::SOURCE.to_string(), source.to_string());
        let ctx = MessageContext {
            message_id: dedupe_id.clone(),
            type_name: E::TYPE.to_string(),
            correlation_id,
            identity: None,
            authorization: None,
            headers: all,
        };
        let payload = codec::encode(event)?;
        let outcome = self
            .dispatcher
            .dispatch(&dedupe_id, E::TYPE, &payload, &ctx)
            .await?;
        if outcome == Outcome::NoInterest {
            tracing::warn!(source = %source, dedupe_key = %dedupe_key, r#type = E::TYPE, "webhook event has no handler or saga in this service");
            return Ok(IngressResult::Accepted);
        }
        let inserted = markers
            .insert_one(
                doc! { "_id": &marker, "type": E::TYPE, "processedAt": self.clock.now_bson() },
            )
            .await;
        match inserted {
            Ok(_) => Ok(IngressResult::Accepted),
            // A simultaneous delivery finished first; the dispatch above skipped everything by
            // its markers.
            Err(e) if crate::saga::ports::is_duplicate_key(&e) => Ok(IngressResult::Duplicate),
            Err(e) => Err(e.into()),
        }
    }
}
