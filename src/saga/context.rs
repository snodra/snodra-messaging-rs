//! `SagaContext`: what a saga handler is handed. It only records; the runtime applies the recorded
//! [`Effects`], in one transaction, after every handler of the attempt returned, so a handler that
//! fails leaves nothing behind (ADR 0021 ruling 6).

use std::time::{Duration, SystemTime};

use serde::Serialize;

use super::store::DispatchKind;
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::types::{Command, Event};

/// Entries of one saga handling take `seq = version * SEQ_STRIDE + index`, so the index must stay
/// below it (ruling 6: at most 999 messages per handling).
pub(crate) const SEQ_STRIDE: i64 = 1000;

/// A command or event a handler recorded; the runtime turns it into a `saga_dispatch` entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecordedDispatch {
    pub kind: DispatchKind,
    /// The wire type name.
    pub type_name: &'static str,
    /// The message as JSON text.
    pub payload: String,
    /// A command's routing key or an event's key.
    pub routing_key: Option<String>,
}

/// An association change, in the order the handlers asked for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AssociationChange {
    Add { key: String, value: String },
    Remove { key: String, value: String },
}

/// A deadline change recorded by a handler, applied in call order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DeadlineOp {
    Schedule {
        id: String,
        name: String,
        due_at: SystemTime,
        /// The payload as JSON text (any JSON value).
        payload: Option<String>,
        /// The payload's Rust type name (informational: the handler that fires it knows its type).
        payload_type: Option<String>,
    },
    Cancel(String),
    CancelAll(String),
}

/// Everything one handling recorded.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Effects {
    pub associations: Vec<AssociationChange>,
    /// `end()` was called.
    pub end: bool,
    pub dispatches: Vec<RecordedDispatch>,
    pub deadlines: Vec<DeadlineOp>,
}

/// What a saga handler can do to its own instance, and the commands, events and deadlines it
/// wants: a handler parameter, built per call by the saga runtime. Handlers are synchronous and
/// get no `Sender` or `EventBus`: sending from inside the saga's transaction is impossible by
/// construction (ADR 0021 ruling 32); a closure that captures one anyway is a bug (it would send
/// for a state that may roll back, without order, retry or idempotency key).
pub struct SagaContext {
    saga_id: String,
    message: MessageContext,
    now: SystemTime,
    effects: Effects,
}

impl SagaContext {
    /// A context for one handler call of instance `saga_id`; `now` is what `schedule_after` counts
    /// from (the runtime's clock).
    pub(crate) fn new(saga_id: String, message: MessageContext, now: SystemTime) -> SagaContext {
        SagaContext {
            saga_id,
            message,
            now,
            effects: Effects::default(),
        }
    }

    /// The id of this saga instance (`sagas._id`).
    pub fn saga_id(&self) -> &str {
        &self.saga_id
    }

    /// The metadata of the message being handled (for a deadline: its own id and the correlation
    /// id of the message that scheduled it).
    pub fn message(&self) -> &MessageContext {
        &self.message
    }

    /// Routes later events whose association `key = value` to this instance.
    pub fn associate_with(&mut self, key: &str, value: &str) {
        self.effects.associations.push(AssociationChange::Add {
            key: key.to_string(),
            value: value.to_string(),
        });
    }

    /// Stops routing events with the association `key = value` to this instance.
    pub fn remove_association_with(&mut self, key: &str, value: &str) {
        self.effects.associations.push(AssociationChange::Remove {
            key: key.to_string(),
            value: value.to_string(),
        });
    }

    /// Ends the instance after the handler returns, as [`SagaDefinition::ending`](super::SagaDefinition::ending) does.
    pub fn end(&mut self) {
        self.effects.end = true;
    }

    /// Records a command: dispatched after commit, in call order, with the idempotency key
    /// `<sagaId>:<seq>`; never for a state that rolled back. A saga does not wait for the reply.
    pub fn send<C: Command>(
        &mut self,
        command: &C,
        routing_key: Option<&str>,
    ) -> Result<(), MessagingError> {
        self.record(DispatchKind::Command, C::TYPE, command, routing_key)
    }

    /// Records an event: published through the outbox after commit, in call order.
    pub fn publish<E: Event>(
        &mut self,
        event: &E,
        key: Option<&str>,
    ) -> Result<(), MessagingError> {
        // The relay turns the entry into an outbox document: what cannot become one (a unit
        // struct, a payload that is no JSON object, a number BSON cannot hold) fails the handler
        // now, naming the event, instead of the entry later (ruling 69).
        crate::kafka::payload_document(E::TYPE, &serde_json::to_value(event)?)?;
        self.record(DispatchKind::Event, E::TYPE, event, key)
    }

    fn record<M: Serialize>(
        &mut self,
        kind: DispatchKind,
        type_name: &'static str,
        message: &M,
        routing_key: Option<&str>,
    ) -> Result<(), MessagingError> {
        let recorded = self.effects.dispatches.len();
        if recorded >= (SEQ_STRIDE - 1) as usize {
            return Err(MessagingError::InvalidMessage(format!(
                "a saga handling may send and publish at most {} messages, '{type_name}' would be number {}",
                SEQ_STRIDE - 1,
                recorded + 1
            )));
        }
        self.effects.dispatches.push(RecordedDispatch {
            kind,
            type_name,
            payload: serde_json::to_string(message)?,
            routing_key: routing_key.map(str::to_string),
        });
        Ok(())
    }

    /// Schedules a deadline named `name` to fire `after` from now; returns its id.
    pub fn schedule_after<P: Serialize>(
        &mut self,
        after: Duration,
        name: &str,
        payload: Option<&P>,
    ) -> Result<String, MessagingError> {
        self.schedule_at(self.now + after, name, payload)
    }

    /// Schedules a deadline named `name` to fire at `at`; returns its id.
    pub fn schedule_at<P: Serialize>(
        &mut self,
        at: SystemTime,
        name: &str,
        payload: Option<&P>,
    ) -> Result<String, MessagingError> {
        let id = uuid::Uuid::new_v4().to_string();
        self.effects.deadlines.push(DeadlineOp::Schedule {
            id: id.clone(),
            name: name.to_string(),
            due_at: at,
            payload: payload.map(serde_json::to_string).transpose()?,
            payload_type: payload.map(|_| std::any::type_name::<P>().to_string()),
        });
        Ok(id)
    }

    /// Cancels the deadline `id` (no effect when it already fired or was cancelled).
    pub fn cancel(&mut self, id: &str) {
        self.effects
            .deadlines
            .push(DeadlineOp::Cancel(id.to_string()));
    }

    /// Cancels every scheduled deadline of this saga named `name`.
    pub fn cancel_all(&mut self, name: &str) {
        self.effects
            .deadlines
            .push(DeadlineOp::CancelAll(name.to_string()));
    }

    /// What was recorded so far.
    pub(crate) fn effects(&self) -> &Effects {
        &self.effects
    }

    /// Takes what was recorded, leaving the context empty.
    pub(crate) fn take_effects(&mut self) -> Effects {
        std::mem::take(&mut self.effects)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_type;
    use serde::Deserialize;

    #[derive(Serialize, Deserialize)]
    struct Charge {
        amount: u32,
    }
    message_type!(Charge, "t.Charge");
    impl Command for Charge {
        type Reply = ();
    }

    #[derive(Serialize, Deserialize)]
    struct Charged {
        amount: u32,
    }
    message_type!(Charged, "t.Charged");
    impl Event for Charged {}

    fn ctx() -> SagaContext {
        SagaContext::new(
            "saga-1".into(),
            MessageContext {
                correlation_id: "corr-1".into(),
                ..Default::default()
            },
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_000),
        )
    }

    #[test]
    fn the_context_knows_its_saga_and_message() {
        let c = ctx();
        assert_eq!(c.saga_id(), "saga-1");
        assert_eq!(c.message().correlation_id, "corr-1");
    }

    #[test]
    fn association_changes_and_end_are_recorded_in_call_order() {
        let mut c = ctx();
        c.associate_with("paymentRef", "p-1");
        c.remove_association_with("orderId", "o-1");
        c.end();
        assert_eq!(
            c.effects().associations,
            vec![
                AssociationChange::Add {
                    key: "paymentRef".into(),
                    value: "p-1".into()
                },
                AssociationChange::Remove {
                    key: "orderId".into(),
                    value: "o-1".into()
                },
            ]
        );
        assert!(c.effects().end);
    }

    #[test]
    fn commands_and_events_are_recorded_as_json_text_in_call_order() {
        let mut c = ctx();
        c.send(&Charge { amount: 5 }, Some("o-1")).unwrap();
        c.publish(&Charged { amount: 5 }, None).unwrap();
        assert_eq!(
            c.effects().dispatches,
            vec![
                RecordedDispatch {
                    kind: DispatchKind::Command,
                    type_name: "t.Charge",
                    payload: r#"{"amount":5}"#.into(),
                    routing_key: Some("o-1".into()),
                },
                RecordedDispatch {
                    kind: DispatchKind::Event,
                    type_name: "t.Charged",
                    payload: r#"{"amount":5}"#.into(),
                    routing_key: None,
                },
            ]
        );
    }

    #[test]
    fn a_handling_records_at_most_999_messages() {
        let mut c = ctx();
        for _ in 0..999 {
            c.send(&Charge { amount: 1 }, None).unwrap();
        }
        let err = c.send(&Charge { amount: 1 }, None).unwrap_err();
        assert!(err.to_string().contains("999"), "{err}");
        assert_eq!(c.effects().dispatches.len(), 999);
    }

    #[test]
    fn deadlines_are_scheduled_relative_to_now_and_cancelled_in_call_order() {
        let mut c = ctx();
        let id = c
            .schedule_after(
                Duration::from_secs(30),
                "timeout",
                Some(&Charged { amount: 2 }),
            )
            .unwrap();
        let at = SystemTime::UNIX_EPOCH + Duration::from_secs(9_000);
        let id2 = c.schedule_at::<()>(at, "poll", None).unwrap();
        c.cancel(&id);
        c.cancel_all("poll");
        assert_ne!(id, id2);
        assert_eq!(c.effects().deadlines.len(), 4);
        match &c.effects().deadlines[0] {
            DeadlineOp::Schedule {
                id: got,
                name,
                due_at,
                payload,
                payload_type,
            } => {
                assert_eq!(got, &id);
                assert_eq!(name, "timeout");
                assert_eq!(*due_at, SystemTime::UNIX_EPOCH + Duration::from_secs(1_030));
                assert_eq!(payload.as_deref(), Some(r#"{"amount":2}"#));
                assert!(payload_type.as_deref().unwrap().ends_with("Charged"));
            }
            other => panic!("{other:?}"),
        }
        match &c.effects().deadlines[1] {
            DeadlineOp::Schedule {
                due_at,
                payload,
                payload_type,
                ..
            } => {
                assert_eq!(*due_at, at);
                assert!(payload.is_none() && payload_type.is_none());
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(c.effects().deadlines[2], DeadlineOp::Cancel(id));
        assert_eq!(
            c.effects().deadlines[3],
            DeadlineOp::CancelAll("poll".into())
        );
    }

    #[test]
    fn taking_the_effects_empties_the_context() {
        let mut c = ctx();
        c.end();
        let taken = c.take_effects();
        assert!(taken.end);
        assert_eq!(c.effects(), &Effects::default());
    }
}
