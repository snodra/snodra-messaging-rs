//! The saga definition API (S2-R1) and the erased model the runtime works with.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::context::SagaContext;
use super::failed::SagaCommandFailed;
use crate::codec;
use crate::context::MessageContext;
use crate::error::MessagingError;
use crate::types::{Event, MessageType};

type ValueFn = Arc<dyn Fn(&[u8]) -> Result<Option<String>, MessagingError> + Send + Sync>;
type EventInvoke =
    Box<dyn Fn(&mut String, &[u8], &mut SagaContext) -> Result<(), MessagingError> + Send + Sync>;
type DeadlineInvoke = Box<
    dyn Fn(&mut String, Option<&str>, &mut SagaContext) -> Result<(), MessagingError> + Send + Sync,
>;

/// Where a handler reads its association value from.
#[derive(Clone)]
pub(crate) enum ValueSource {
    /// A property of the event: decodes the payload and applies the user's function.
    Property(ValueFn),
    /// A message header of this name.
    Header(String),
}

/// One event handler of a saga, with the state and the event typed away: `invoke` decodes the
/// state (JSON text) into the saga's `S` and the payload into the event, calls the user's closure
/// and writes the state back as text, only when the closure succeeded. The text is the codec's
/// exact output (ADR 0021 ruling 25), never a `serde_json::Value` in between, so a number or a
/// map key survives as written.
pub(crate) struct ErasedHandler {
    pub event_type: &'static str,
    /// The association key (`keyName`): the handler runs on instances associated `key = value`.
    pub key: String,
    pub value: ValueSource,
    /// May create an instance when no live instance matched the event.
    pub start: bool,
    /// Always creates an instance (carrying no start key), also when one matched.
    pub force_new: bool,
    /// The instance ends after the handler returns.
    pub end: bool,
    pub invoke: EventInvoke,
}

impl ErasedHandler {
    /// The association value of the event: the header's value for a header handler, else the
    /// user's function applied to the decoded event; `None` when absent.
    pub(crate) fn association_value(
        &self,
        payload: &[u8],
        ctx: &MessageContext,
    ) -> Result<Option<String>, MessagingError> {
        match &self.value {
            ValueSource::Header(name) => Ok(ctx.headers.get(name).cloned()),
            ValueSource::Property(value) => value(payload),
        }
    }
}

/// A deadline handler: `invoke` gets the deadline's payload as JSON text (`None` when it was
/// scheduled without one).
pub(crate) struct ErasedDeadlineHandler {
    pub name: &'static str,
    pub end: bool,
    pub invoke: DeadlineInvoke,
}

/// A registered saga type: its wire name, its handlers by event type name (each list in
/// registration order, the order they run in), its deadline handlers by deadline name, the
/// handler of [`SagaCommandFailed`] (routed by `sagaId`, ruling 27, so it is not in `handlers`) and
/// a fresh instance's state text.
///
/// Opaque outside the crate: build one with [`SagaDefinition::build`] and hand it to
/// [`SagaRuntime::new`](super::SagaRuntime::new) (the builder does so for the sagas registered on
/// it; tests and the test fixture build their own).
pub struct SagaType {
    pub(crate) name: &'static str,
    pub(crate) handlers: HashMap<&'static str, Vec<ErasedHandler>>,
    pub(crate) deadlines: HashMap<&'static str, ErasedDeadlineHandler>,
    pub(crate) command_failed: Option<ErasedHandler>,
    pub(crate) new_state: fn() -> String,
}

impl SagaType {
    /// The saga's wire name (`sagas.type`).
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Whether any handler of this type reacts to events of `type_name`.
    #[cfg(test)]
    pub(crate) fn interested_in(&self, type_name: &str) -> bool {
        self.handlers.contains_key(type_name)
            || (self.command_failed.is_some() && type_name == SagaCommandFailed::TYPE)
    }
}

impl std::fmt::Debug for SagaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SagaType({})", self.name)
    }
}

enum Last {
    Event(usize),
    Deadline(usize),
    CommandFailed,
}

/// The definition of one saga type, built with the closure given to
/// [`SnodraMessagingBuilder::saga`](crate::SnodraMessagingBuilder::saga).
///
/// Every handler is a synchronous closure `Fn(&mut S, &E, &mut SagaContext) -> Result<(), MessagingError>`:
/// it changes the state and records what it wants done on the context; the runtime does the I/O
/// after the saga's transaction committed. Handlers of one event type run in registration order
/// (Kotlin runs them in method-name order, a reflection limit: an S2 divergence).
///
/// The association of a handler is `key = value`: the handler runs on the live instances of this
/// saga type that are associated with it. `start_on` handlers may create an instance when no live
/// instance matched the event (at most one live instance per start `key = value`);
/// `start_new_on` handlers always create one. The `*_header` forms read the value from a message
/// header instead of the event.
pub struct SagaDefinition<S> {
    name: &'static str,
    handlers: Vec<ErasedHandler>,
    deadlines: Vec<ErasedDeadlineHandler>,
    command_failed: Option<ErasedHandler>,
    last: Option<Last>,
    problems: Vec<String>,
    state: PhantomData<fn() -> S>,
}

impl<S: Serialize + DeserializeOwned + Default + Send + 'static> SagaDefinition<S> {
    /// An empty definition of the saga type `name` (its `sagas.type`).
    pub fn new(name: &'static str) -> Self {
        SagaDefinition {
            name,
            handlers: Vec::new(),
            deadlines: Vec::new(),
            command_failed: None,
            last: None,
            problems: Vec::new(),
            state: PhantomData,
        }
    }

    fn add<E: Event>(
        mut self,
        key: &'static str,
        value: ValueSource,
        start: bool,
        force_new: bool,
        handler: impl Fn(&mut S, &E, &mut SagaContext) -> Result<(), MessagingError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.handlers.push(ErasedHandler {
            event_type: E::TYPE,
            key: key.to_string(),
            value,
            start,
            force_new,
            end: false,
            invoke: erase_event::<S, E>(handler),
        });
        self.last = Some(Last::Event(self.handlers.len() - 1));
        self
    }

    /// A handler that may start an instance: the saga's event `E` whose `value(&event)` is the
    /// association `key = value`. When no live instance has it, a new one is created and this
    /// handler runs on it.
    pub fn start_on<E: Event>(
        self,
        key: &'static str,
        value: fn(&E) -> Option<String>,
        handler: impl Fn(&mut S, &E, &mut SagaContext) -> Result<(), MessagingError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.add(key, property::<E>(value), true, false, handler)
    }

    /// [`start_on`](Self::start_on) with the association value read from the message header `header`.
    pub fn start_on_header<E: Event>(
        self,
        key: &'static str,
        header: &'static str,
        handler: impl Fn(&mut S, &E, &mut SagaContext) -> Result<(), MessagingError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.add(
            key,
            ValueSource::Header(header.into()),
            true,
            false,
            handler,
        )
    }

    /// A handler that always starts a new instance (`forceNew`), also when one is associated with
    /// the event; the new instance carries no start key, so any number may be live.
    pub fn start_new_on<E: Event>(
        self,
        key: &'static str,
        value: fn(&E) -> Option<String>,
        handler: impl Fn(&mut S, &E, &mut SagaContext) -> Result<(), MessagingError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.add(key, property::<E>(value), true, true, handler)
    }

    /// [`start_new_on`](Self::start_new_on) with the association value read from a header.
    pub fn start_new_on_header<E: Event>(
        self,
        key: &'static str,
        header: &'static str,
        handler: impl Fn(&mut S, &E, &mut SagaContext) -> Result<(), MessagingError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.add(key, ValueSource::Header(header.into()), true, true, handler)
    }

    /// A handler that runs on the live instances associated with `key = value(&event)`.
    pub fn on<E: Event>(
        self,
        key: &'static str,
        value: fn(&E) -> Option<String>,
        handler: impl Fn(&mut S, &E, &mut SagaContext) -> Result<(), MessagingError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.add(key, property::<E>(value), false, false, handler)
    }

    /// [`on`](Self::on) with the association value read from the message header `header`.
    pub fn on_header<E: Event>(
        self,
        key: &'static str,
        header: &'static str,
        handler: impl Fn(&mut S, &E, &mut SagaContext) -> Result<(), MessagingError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.add(
            key,
            ValueSource::Header(header.into()),
            false,
            false,
            handler,
        )
    }

    /// Marks the handler just added (an event, deadline or command-failed handler) as ending the
    /// instance after it returns (`@EndSaga`). Calling it before any handler is a definition error
    /// reported by `start()`.
    pub fn ending(mut self) -> Self {
        match self.last {
            Some(Last::Event(i)) => self.handlers[i].end = true,
            Some(Last::Deadline(i)) => self.deadlines[i].end = true,
            Some(Last::CommandFailed) => {
                if let Some(h) = self.command_failed.as_mut() {
                    h.end = true;
                }
            }
            None => self.problems.push(format!(
                "Saga {}: ending() must follow the handler it marks (start_on, on, on_header, \
                 on_deadline or on_command_failed)",
                self.name
            )),
        }
        self
    }

    /// The handler of the deadlines named `name` that this instance scheduled; `payload` is
    /// `None` when it was scheduled without one. A handler that ignores the payload (or whose
    /// deadline never carries one) takes `on_deadline::<serde::de::IgnoredAny>`: `()` reads only
    /// a JSON `null`, so a deadline scheduled with a payload would fail to decode.
    pub fn on_deadline<P: DeserializeOwned + 'static>(
        mut self,
        name: &'static str,
        handler: impl Fn(&mut S, Option<P>, &mut SagaContext) -> Result<(), MessagingError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        if self.deadlines.iter().any(|d| d.name == name) {
            self.problems.push(format!(
                "Saga handler {}.{name}: two handlers for the deadline '{name}'",
                self.name
            ));
        }
        self.deadlines.push(ErasedDeadlineHandler {
            name,
            end: false,
            invoke: Box::new(move |state, payload, ctx| {
                let payload: Option<P> = payload.map(serde_json::from_str).transpose()?;
                let mut saga: S = serde_json::from_str(state)?;
                handler(&mut saga, payload, ctx)?;
                *state = serde_json::to_string(&saga)?;
                Ok(())
            }),
        });
        self.last = Some(Last::Deadline(self.deadlines.len() - 1));
        self
    }

    /// The handler of [`SagaCommandFailed`] for this saga's own commands. The runtime routes it
    /// by `saga_id` to the instance that sent the command, with no association needed (ADR 0021
    /// ruling 27); an ended instance is not reached.
    pub fn on_command_failed(
        mut self,
        handler: impl Fn(&mut S, &SagaCommandFailed, &mut SagaContext) -> Result<(), MessagingError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        if self.command_failed.is_some() {
            self.problems.push(format!(
                "Saga handler {}.{}: two handlers for the failure of a command",
                self.name,
                SagaCommandFailed::TYPE
            ));
        }
        self.command_failed = Some(ErasedHandler {
            event_type: SagaCommandFailed::TYPE,
            key: "sagaId".to_string(),
            value: property::<SagaCommandFailed>(|e| Some(e.saga_id.clone())),
            start: false,
            force_new: false,
            end: false,
            invoke: erase_event::<S, SagaCommandFailed>(handler),
        });
        self.last = Some(Last::CommandFailed);
        self
    }

    /// The startup checks (S2-R2), then the erased type; the error names the saga and the handler.
    pub fn build(self) -> Result<SagaType, MessagingError> {
        let name = self.name;
        let invalid = |message: String| Err(MessagingError::InvalidMessage(message));
        if name.trim().is_empty() {
            return invalid("A saga needs a name (its sagas.type)".to_string());
        }
        if let Some(problem) = self.problems.into_iter().next() {
            return invalid(problem);
        }
        if !self.handlers.iter().any(|h| h.start) {
            return invalid(format!(
                "Saga {name} has no start handler (start_on or start_new_on): no instance could \
                 ever be created"
            ));
        }
        if let Err(why) = check_round_trip::<S>() {
            return invalid(format!(
                "Saga {name} does not round-trip through serde_json (its state is stored as that \
                 JSON text): {why}"
            ));
        }
        let mut handlers: HashMap<&'static str, Vec<ErasedHandler>> = HashMap::new();
        for h in self.handlers {
            handlers.entry(h.event_type).or_default().push(h);
        }
        Ok(SagaType {
            name,
            handlers,
            deadlines: self.deadlines.into_iter().map(|d| (d.name, d)).collect(),
            command_failed: self.command_failed,
            new_state: new_state::<S>,
        })
    }
}

/// A fresh instance's state as stored: the JSON text of `S::default()`.
fn new_state<S: Serialize + Default>() -> String {
    serde_json::to_string(&S::default()).expect("checked when the saga was registered")
}

/// The stored form of a fresh instance (the codec's exact JSON text) must be an object that
/// decodes and encodes to the identical text.
fn check_round_trip<S: Serialize + DeserializeOwned + Default>() -> Result<(), String> {
    let stored = serde_json::to_string(&S::default()).map_err(|e| e.to_string())?;
    if !serde_json::from_str::<serde_json::Value>(&stored).is_ok_and(|v| v.is_object()) {
        return Err(format!("it is not a JSON object: {stored}"));
    }
    let decoded: S =
        serde_json::from_str(&stored).map_err(|e| format!("{e} (reading back {stored})"))?;
    let again = serde_json::to_string(&decoded).map_err(|e| e.to_string())?;
    if again != stored {
        return Err(format!(
            "encoding it again gave a different document: {stored} then {again}"
        ));
    }
    Ok(())
}

fn property<E: Event>(value: fn(&E) -> Option<String>) -> ValueSource {
    ValueSource::Property(Arc::new(move |payload| {
        let event: E = codec::decode(payload)?;
        Ok(value(&event))
    }))
}

fn erase_event<S, E>(
    handler: impl Fn(&mut S, &E, &mut SagaContext) -> Result<(), MessagingError> + Send + Sync + 'static,
) -> EventInvoke
where
    S: Serialize + DeserializeOwned + 'static,
    E: Event,
{
    Box::new(move |state, payload, ctx| {
        let event: E = codec::decode(payload)?;
        let mut saga: S = serde_json::from_str(state)?;
        handler(&mut saga, &event, ctx)?;
        *state = serde_json::to_string(&saga)?;
        Ok(())
    })
}

/// The registrations of a builder, checked together: the first definition error, else a saga
/// name used twice, else the types.
pub(crate) fn validate_registrations(
    registrations: Vec<Result<SagaType, MessagingError>>,
) -> Result<Vec<SagaType>, MessagingError> {
    let types = registrations.into_iter().collect::<Result<Vec<_>, _>>()?;
    let mut seen = std::collections::HashSet::new();
    for t in &types {
        if !seen.insert(t.name) {
            return Err(MessagingError::InvalidMessage(format!(
                "Saga name '{}' is registered twice; every saga needs its own name",
                t.name
            )));
        }
    }
    Ok(types)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_type;
    use serde::Deserialize;

    #[derive(Default, Serialize, Deserialize)]
    struct Payment {
        order_id: Option<String>,
        log: Vec<String>,
    }

    #[derive(Serialize, Deserialize)]
    struct OrderPlaced {
        order_id: String,
    }
    message_type!(OrderPlaced, "t.OrderPlaced");
    impl Event for OrderPlaced {}

    #[derive(Serialize, Deserialize)]
    struct ChargeSucceeded {
        payment_ref: String,
    }
    message_type!(ChargeSucceeded, "t.ChargeSucceeded");
    impl Event for ChargeSucceeded {}

    #[derive(Serialize, Deserialize)]
    struct Note {
        text: String,
    }
    message_type!(Note, "t.Note");
    impl Event for Note {}

    fn order_id(e: &OrderPlaced) -> Option<String> {
        Some(e.order_id.clone())
    }

    fn ctx_with(headers: &[(&str, &str)]) -> MessageContext {
        MessageContext {
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Default::default()
        }
    }

    fn saga_context() -> SagaContext {
        SagaContext::new(
            "s-1".into(),
            MessageContext::default(),
            std::time::SystemTime::UNIX_EPOCH,
        )
    }

    fn payment() -> SagaDefinition<Payment> {
        SagaDefinition::<Payment>::new("payment")
            .start_on::<OrderPlaced>("orderId", order_id, |s, e, _| {
                s.order_id = Some(e.order_id.clone());
                Ok(())
            })
            .on_header::<ChargeSucceeded>("paymentRef", "payment-ref", |s, _, _| {
                s.log.push("charged".into());
                Ok(())
            })
            .ending()
            .on_deadline::<String>("charge-timeout", |s, payload, _| {
                s.log.push(format!("timeout {payload:?}"));
                Ok(())
            })
            .on_command_failed(|s, failed, _| {
                s.log.push(format!("failed {}", failed.command_type));
                Ok(())
            })
    }

    #[test]
    fn a_saga_with_a_start_handler_registers_with_its_name_and_event_types() {
        let t = payment().build().unwrap();
        assert_eq!(t.name, "payment");
        let mut types: Vec<_> = t.handlers.keys().copied().collect();
        types.sort();
        assert_eq!(types, ["t.ChargeSucceeded", "t.OrderPlaced"]);
        let start = &t.handlers["t.OrderPlaced"][0];
        assert!(start.start && !start.force_new && !start.end);
        assert_eq!(start.key, "orderId");
        assert!(t.deadlines.contains_key("charge-timeout"));
        assert!(t.command_failed.is_some());
        assert!(t.interested_in("t.OrderPlaced"));
        assert!(t.interested_in("snodra.SagaCommandFailed"));
        assert!(!t.interested_in("t.Unknown"));
        assert_eq!((t.new_state)(), r#"{"order_id":null,"log":[]}"#);
    }

    #[test]
    fn a_property_handler_takes_the_association_value_from_the_event() {
        let t = payment().build().unwrap();
        let h = &t.handlers["t.OrderPlaced"][0];
        let payload = br#"{"order_id":"o-7"}"#;
        assert_eq!(
            h.association_value(payload, &ctx_with(&[])).unwrap(),
            Some("o-7".to_string())
        );
        assert!(h.association_value(b"nope", &ctx_with(&[])).is_err());
    }

    #[test]
    fn on_header_takes_the_association_value_from_the_message_header() {
        let t = payment().build().unwrap();
        let h = &t.handlers["t.ChargeSucceeded"][0];
        assert_eq!(h.key, "paymentRef");
        let payload = br#"{"payment_ref":"from-the-body"}"#;
        assert_eq!(
            h.association_value(payload, &ctx_with(&[("payment-ref", "from-the-header")]))
                .unwrap(),
            Some("from-the-header".to_string())
        );
        assert_eq!(h.association_value(payload, &ctx_with(&[])).unwrap(), None);
    }

    #[test]
    fn ending_marks_the_handler_just_added() {
        let t = payment().build().unwrap();
        assert!(t.handlers["t.ChargeSucceeded"][0].end);
        assert!(!t.handlers["t.OrderPlaced"][0].end);
        assert!(!t.deadlines["charge-timeout"].end);

        let on_deadline = SagaDefinition::<Payment>::new("p")
            .start_on::<OrderPlaced>("orderId", order_id, |_, _, _| Ok(()))
            .on_deadline::<()>("t", |_, _, _| Ok(()))
            .ending()
            .build()
            .unwrap();
        assert!(on_deadline.deadlines["t"].end);
        assert!(!on_deadline.handlers["t.OrderPlaced"][0].end);
    }

    #[test]
    fn start_new_marks_a_forced_start_and_the_header_forms_are_starts_too() {
        let t = SagaDefinition::<Payment>::new("p")
            .start_new_on::<OrderPlaced>("orderId", order_id, |_, _, _| Ok(()))
            .start_on_header::<Note>("noteId", "note-id", |_, _, _| Ok(()))
            .build()
            .unwrap();
        let forced = &t.handlers["t.OrderPlaced"][0];
        assert!(forced.start && forced.force_new);
        let by_header = &t.handlers["t.Note"][0];
        assert!(by_header.start && !by_header.force_new);
    }

    #[test]
    fn a_handler_decodes_the_state_text_and_writes_it_back() {
        let t = payment().build().unwrap();
        let mut state = (t.new_state)();
        let mut ctx = saga_context();
        (t.handlers["t.OrderPlaced"][0].invoke)(&mut state, br#"{"order_id":"o-1"}"#, &mut ctx)
            .unwrap();
        (t.handlers["t.ChargeSucceeded"][0].invoke)(
            &mut state,
            br#"{"payment_ref":"p"}"#,
            &mut ctx,
        )
        .unwrap();
        assert_eq!(state, r#"{"order_id":"o-1","log":["charged"]}"#);
        (t.deadlines["charge-timeout"].invoke)(&mut state, Some(r#""x""#), &mut ctx).unwrap();
        (t.deadlines["charge-timeout"].invoke)(&mut state, None, &mut ctx).unwrap();
        (t.command_failed.as_ref().unwrap().invoke)(
            &mut state,
            br#"{"sagaId":"s","commandType":"t.Charge","idempotencyKey":"s:1","reason":"FAILED","message":"m"}"#,
            &mut ctx,
        )
        .unwrap();
        assert_eq!(
            state,
            r#"{"order_id":"o-1","log":["charged","timeout Some(\"x\")","timeout None","failed t.Charge"]}"#
        );
    }

    #[test]
    fn a_failing_handler_leaves_the_state_text_as_it_was() {
        let t = SagaDefinition::<Payment>::new("p")
            .start_on::<OrderPlaced>("orderId", order_id, |s, _, _| {
                s.log.push("touched".into());
                Err(MessagingError::InvalidMessage("boom".into()))
            })
            .build()
            .unwrap();
        let mut state = (t.new_state)();
        let before = state.clone();
        let err = (t.handlers["t.OrderPlaced"][0].invoke)(
            &mut state,
            br#"{"order_id":"o"}"#,
            &mut saga_context(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("boom"));
        assert_eq!(state, before);
    }

    #[test]
    fn registration_order_is_the_handlers_run_order() {
        let t = SagaDefinition::<Payment>::new("p")
            .start_on::<OrderPlaced>("orderId", order_id, |s, _, _| {
                s.log.push("b".into());
                Ok(())
            })
            .on::<OrderPlaced>("orderId", order_id, |s, _, _| {
                s.log.push("a".into());
                Ok(())
            })
            .on::<OrderPlaced>("orderId", order_id, |s, _, _| {
                s.log.push("c".into());
                Ok(())
            })
            .build()
            .unwrap();
        let mut state = (t.new_state)();
        for h in &t.handlers["t.OrderPlaced"] {
            (h.invoke)(&mut state, br#"{"order_id":"o"}"#, &mut saga_context()).unwrap();
        }
        assert_eq!(state, r#"{"order_id":null,"log":["b","a","c"]}"#);
    }
}
