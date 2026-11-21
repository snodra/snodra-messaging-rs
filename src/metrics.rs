//! The library's metrics, through the OpenTelemetry metrics API: the library records,
//! the application installs the meter provider (the SDK with an exporter, e.g. OTLP to
//! otel-lgtm). [`SnodraMessaging::builder`](crate::SnodraMessaging::builder) records on
//! `opentelemetry::global::meter("snodra-messaging-rs")` as the global provider is when it runs,
//! so the provider is installed before it; with none installed that is the API's no-op meter:
//! nothing is recorded, an observable gauge's callback never runs, and a measurement costs a
//! virtual call. [`SnodraMessaging::builder_with_meter`](crate::SnodraMessaging::builder_with_meter)
//! takes the meter instead (tests read it back from an in-memory exporter).
//!
//! Instruments are named with dots, as Kotlin's Micrometer meters; the OTLP→Prometheus translation
//! turns the dots into underscores, adds the unit suffix (`s` → `_seconds`) and `_total` to a
//! counter, which gives the series of the README's Metrics section (`snodra.outbox.published` is
//! `snodra_outbox_published_total`). Every label value comes from a bounded set: a type this
//! service registered (a foreign one is [`OTHER`]), a processor name, an outcome, a state.
//!
//! Gauges are observable: their callbacks read values the library holds anyway (the processor
//! monitors, the outbox relay's last poll, the hub pool), never Kafka or MongoDB, and hold only a
//! weak reference to what they read, so an application that is dropped stops reporting.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::{KeyValue, Value, global};

use crate::error::MessagingError;
use crate::proto::RequestKind;

/// The instrumentation scope of every instrument of the library.
pub const METER_NAME: &str = "snodra-messaging-rs";

/// The `type` label of a type this service did not register (a foreign producer's).
pub const OTHER: &str = "other";

/// `snodra_outbox_pending`: outbox documents not yet published (neither sent nor failed), as the
/// relay's last poll counted them.
pub const OUTBOX_PENDING: &str = "snodra.outbox.pending";
/// `snodra_outbox_oldest_age_seconds`: the age of the oldest of them, 0 when none.
pub const OUTBOX_OLDEST_AGE: &str = "snodra.outbox.oldest.age";
/// `snodra_outbox_published_total{outcome}`: publish attempts, `ok` or `failed`.
pub const OUTBOX_PUBLISHED: &str = "snodra.outbox.published";
/// `snodra_events_consumed_total{processor,type,outcome}`: stage-1 outcomes per record.
pub const EVENTS_CONSUMED: &str = "snodra.events.consumed";
/// `snodra_event_handler_duration_seconds{processor,type}`: one stage-1 attempt that ran the
/// handlers.
pub const EVENT_HANDLER_DURATION: &str = "snodra.event.handler.duration";
/// `snodra_processor_lag{processor,segment}`: the lag of an assigned segment, as the status.
pub const PROCESSOR_LAG: &str = "snodra.processor.lag";
/// `snodra_processor_state{processor,state}`: 1 for the processor's state now, 0 for the others.
pub const PROCESSOR_STATE: &str = "snodra.processor.state";
/// `snodra_dead_letters{processor}`: the processor's letters, as the status samples them.
pub const DEAD_LETTERS: &str = "snodra.dead.letters";
/// `snodra_hub_client_request_duration_seconds{kind,type,outcome}`: a command or query sent
/// through the hub, from the call to its result (retries included).
pub const HUB_CLIENT_REQUEST_DURATION: &str = "snodra.hub.client.request.duration";
/// `snodra_hub_client_handlers_in_flight`: requests from the hubs whose handler runs now.
pub const HUB_CLIENT_HANDLERS_IN_FLIGHT: &str = "snodra.hub.client.handlers.in.flight";
/// `snodra_hub_client_connected_hubs`: hubs with an acknowledged stream.
pub const HUB_CLIENT_CONNECTED_HUBS: &str = "snodra.hub.client.connected.hubs";

/// `snodra_hub_client_token_fetches_total{result}`: access token fetches (OAuth2 only) by result
/// (`ok`, `http_error`, `oauth_error`, `io_error`, `bad_response`).
pub const HUB_CLIENT_TOKEN_FETCHES: &str = "snodra.hub.client.token.fetches";
/// `snodra_hub_client_certificate_rotations_total`: client certificate pairs adopted after the
/// first (client certificate only).
pub const HUB_CLIENT_CERTIFICATE_ROTATIONS: &str = "snodra.hub.client.certificate.rotations";
/// `snodra_hub_client_credentials_refreshes_sent_total`: `ClientMessage.credentials` written to
/// the hubs.
pub const HUB_CLIENT_CREDENTIALS_REFRESHES_SENT: &str =
    "snodra.hub.client.credentials.refreshes.sent";

/// The bucket boundaries of the duration histograms, in seconds (the hub's).
pub const DURATION_BOUNDARIES: [f64; 15] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

/// How one record ended for one processor (`snodra.events.consumed{outcome}`): one per record
/// and processor, at its final outcome there, never per attempt (a stop in the middle of the
/// attempts records nothing: the record comes again).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Consumed {
    /// The dispatch succeeded: stage 1 ran (in `default` the saga stage too, including for an
    /// event no saga instance claimed), or the saga stage alone for a type only sagas handle.
    Handled,
    /// The processor's stage-1 marker already existed and no saga stage ran, or the record's own
    /// letter already existed (a redelivery after a crash).
    Duplicate,
    /// The processor does not handle the type.
    Skipped,
    /// Given up without a letter: mode `log` after the attempts or for a malformed record, or
    /// only the saga stage failed for good (in `default`).
    Failed,
    /// Taken by the dead letters (mode `collection` or `event-store`): it failed on every attempt
    /// (`HANDLER_FAILED`), was malformed (`INVALID`) or its key had a parked letter
    /// (`SEQUENCED`).
    Parked,
}

impl Consumed {
    pub fn as_str(self) -> &'static str {
        match self {
            Consumed::Handled => "handled",
            Consumed::Duplicate => "duplicate",
            Consumed::Skipped => "skipped",
            Consumed::Failed => "failed",
            Consumed::Parked => "parked",
        }
    }
}

/// The `outcome` of a hub request: `ok`, or what its error is: the hub's outcome names, plus
/// `unavailable` (no live hub, or the hub connection broke) and `error` (anything else), as
/// Kotlin's. A reply that does not decode is `invalid_request` (see [`DECODE_FAILED`]).
pub fn request_outcome(result: Result<(), &MessagingError>) -> &'static str {
    let Err(e) = result else {
        return "ok";
    };
    match e {
        MessagingError::NoHandler(_) => "no_handler",
        MessagingError::Timeout(_) => "timeout",
        MessagingError::HandlerDisconnected(_) => "handler_disconnected",
        MessagingError::HandlerBusy(_) => "resource_exhausted",
        MessagingError::RemoteHandler { .. }
        | MessagingError::RetryableExternal(_)
        | MessagingError::UnknownOutcome(_) => "handler_error",
        MessagingError::Unauthenticated(_) | MessagingError::AccessDenied(_) => "denied",
        MessagingError::InvalidMessage(_) => "invalid_request",
        MessagingError::Cancelled(_) => "cancelled",
        MessagingError::HubUnavailable(_) | MessagingError::HubConnectionLost(_) => "unavailable",
        MessagingError::HubRegistrationRejected(_)
        | MessagingError::Serialization(_)
        | MessagingError::Storage(_)
        | MessagingError::Transport(_) => "error",
    }
}

/// The `outcome` of a hub request whose reply did not decode.
pub const DECODE_FAILED: &str = "invalid_request";

/// The bounded values of a `type` label: the types this service registered, interned (a label
/// costs no allocation), anything else [`OTHER`].
#[derive(Clone, Debug, Default)]
pub(crate) struct TypeLabels(Arc<HashSet<Arc<str>>>);

impl TypeLabels {
    pub(crate) fn new(types: impl IntoIterator<Item = String>) -> TypeLabels {
        TypeLabels(Arc::new(types.into_iter().map(Arc::from).collect()))
    }

    pub(crate) fn label(&self, type_name: &str) -> Value {
        match self.0.get(type_name) {
            Some(t) => Value::from(Arc::clone(t)),
            None => Value::from(OTHER),
        }
    }
}

struct Instruments {
    outbox_published: Counter<u64>,
    events_consumed: Counter<u64>,
    handler_duration: Histogram<f64>,
    request_duration: Histogram<f64>,
}

/// What the library records on: the meter and its synchronous instruments. Cheap to clone.
#[derive(Clone)]
pub struct Metrics {
    meter: Meter,
    instruments: Arc<Instruments>,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Metrics")
    }
}

impl Default for Metrics {
    /// [`Metrics::global`].
    fn default() -> Self {
        Metrics::global()
    }
}

impl Metrics {
    /// The library's instruments on `meter`.
    pub fn new(meter: Meter) -> Metrics {
        let instruments = Instruments {
            outbox_published: meter
                .u64_counter(OUTBOX_PUBLISHED)
                .with_description("Outbox publish attempts, per outcome (ok, failed)")
                .build(),
            events_consumed: meter
                .u64_counter(EVENTS_CONSUMED)
                .with_description("Records of snodra.events per processor, type and stage-1 outcome (handled, duplicate, skipped, failed, parked)")
                .build(),
            handler_duration: meter
                .f64_histogram(EVENT_HANDLER_DURATION)
                .with_unit("s")
                .with_boundaries(DURATION_BOUNDARIES.to_vec())
                .with_description("Stage-1 attempts that ran the event handlers (marker, handlers and commit), per processor and type")
                .build(),
            request_duration: meter
                .f64_histogram(HUB_CLIENT_REQUEST_DURATION)
                .with_unit("s")
                .with_boundaries(DURATION_BOUNDARIES.to_vec())
                .with_description("Commands and queries sent through the hub, from the call to its result (retries included), per kind, type and outcome")
                .build(),
        };
        Metrics {
            meter,
            instruments: Arc::new(instruments),
        }
    }

    /// The instruments on `opentelemetry::global::meter("snodra-messaging-rs")`, as the global
    /// provider is now (the no-op meter when none is installed).
    pub fn global() -> Metrics {
        Metrics::new(global::meter(METER_NAME))
    }

    /// The meter, for the observable gauges of the library's parts.
    pub fn meter(&self) -> &Meter {
        &self.meter
    }

    /// One publish attempt of an outbox document.
    pub(crate) fn outbox_published(&self, ok: bool) {
        let outcome = if ok { "ok" } else { "failed" };
        self.instruments
            .outbox_published
            .add(1, &[KeyValue::new("outcome", outcome)]);
    }

    /// How a record ended for processor `processor`'s stage 1.
    pub(crate) fn event_consumed(&self, processor: Value, type_label: Value, outcome: Consumed) {
        self.instruments.events_consumed.add(
            1,
            &[
                KeyValue::new("processor", processor),
                KeyValue::new("type", type_label),
                KeyValue::new("outcome", outcome.as_str()),
            ],
        );
    }

    /// One stage-1 attempt that ran the handlers, and how long it took.
    pub(crate) fn event_handled(&self, processor: Value, type_label: Value, took: Duration) {
        self.instruments.handler_duration.record(
            took.as_secs_f64(),
            &[
                KeyValue::new("processor", processor),
                KeyValue::new("type", type_label),
            ],
        );
    }

    /// One command or query call through the hub (`type_name` is its registered wire type),
    /// with its final outcome ([`request_outcome`], or [`DECODE_FAILED`]).
    pub(crate) fn hub_request(
        &self,
        kind: RequestKind,
        type_name: &str,
        outcome: &'static str,
        took: Duration,
    ) {
        let kind = match kind {
            RequestKind::Command => "command",
            RequestKind::Query | RequestKind::Unspecified => "query",
        };
        self.instruments.request_duration.record(
            took.as_secs_f64(),
            &[
                KeyValue::new("kind", kind),
                KeyValue::new("type", type_name.to_string()),
                KeyValue::new("outcome", outcome),
            ],
        );
    }
}

#[cfg(test)]
#[path = "../tests/support/metrics.rs"]
pub(crate) mod test_meter;

#[cfg(test)]
mod tests {
    use super::test_meter::{Kind, TestMeter};
    use super::*;

    #[test]
    fn publish_attempts_are_counted_by_outcome() {
        let t = TestMeter::new();
        let metrics = Metrics::new(t.meter());
        metrics.outbox_published(true);
        metrics.outbox_published(true);
        metrics.outbox_published(false);
        let ok = t.point(OUTBOX_PUBLISHED, &[("outcome", "ok")]).expect("ok");
        assert_eq!((ok.kind, ok.value), (Kind::Counter, 2.0));
        assert_eq!(
            t.value(OUTBOX_PUBLISHED, &[("outcome", "failed")]),
            Some(1.0)
        );
    }

    #[test]
    fn consumed_records_and_handler_time_carry_the_processor_and_a_bounded_type() {
        let t = TestMeter::new();
        let metrics = Metrics::new(t.meter());
        let types = TypeLabels::new(["o.Placed".to_string()]);
        metrics.event_consumed("orders".into(), types.label("o.Placed"), Consumed::Handled);
        metrics.event_consumed(
            "orders".into(),
            types.label("o.Placed"),
            Consumed::Duplicate,
        );
        metrics.event_consumed("orders".into(), types.label("x.Foreign"), Consumed::Skipped);
        metrics.event_consumed("orders".into(), types.label("y.Foreign"), Consumed::Skipped);
        metrics.event_handled(
            "orders".into(),
            types.label("o.Placed"),
            Duration::from_millis(30),
        );

        for (t_, outcome, n) in [
            ("o.Placed", "handled", 1.0),
            ("o.Placed", "duplicate", 1.0),
            ("other", "skipped", 2.0),
        ] {
            assert_eq!(
                t.value(
                    EVENTS_CONSUMED,
                    &[("processor", "orders"), ("type", t_), ("outcome", outcome)]
                ),
                Some(n),
                "{t_} {outcome}"
            );
        }
        let handled = t
            .point(
                EVENT_HANDLER_DURATION,
                &[("processor", "orders"), ("type", "o.Placed")],
            )
            .expect("a duration");
        assert_eq!((handled.kind, handled.count), (Kind::Histogram, 1));
        assert_eq!(handled.unit, "s");
        assert!((handled.value - 0.03).abs() < 1e-9, "{}", handled.value);
    }

    #[test]
    fn hub_requests_are_timed_by_kind_type_and_outcome() {
        let t = TestMeter::new();
        let metrics = Metrics::new(t.meter());
        metrics.hub_request(
            RequestKind::Command,
            "t.Do",
            request_outcome(Ok(())),
            Duration::from_millis(5),
        );
        metrics.hub_request(
            RequestKind::Query,
            "t.Get",
            request_outcome(Err(&MessagingError::Timeout("t.Get".into()))),
            Duration::from_secs(2),
        );
        let ok = t
            .point(
                HUB_CLIENT_REQUEST_DURATION,
                &[("kind", "command"), ("type", "t.Do"), ("outcome", "ok")],
            )
            .expect("the command");
        assert_eq!(
            (ok.kind, ok.count, ok.unit.as_str()),
            (Kind::Histogram, 1, "s")
        );
        assert_eq!(
            t.value(
                HUB_CLIENT_REQUEST_DURATION,
                &[("kind", "query"), ("type", "t.Get"), ("outcome", "timeout")]
            ),
            Some(1.0)
        );
    }

    #[test]
    fn every_error_has_a_bounded_outcome() {
        assert_eq!(request_outcome(Ok(())), "ok");
        assert_eq!(
            request_outcome(Err(&MessagingError::HandlerBusy("t".into()))),
            "resource_exhausted"
        );
        assert_eq!(
            request_outcome(Err(&MessagingError::AccessDenied("no".into()))),
            "denied"
        );
        assert_eq!(
            request_outcome(Err(&MessagingError::HubConnectionLost("x".into()))),
            "unavailable",
            "the hub connection broke: as Kotlin's"
        );
        assert_eq!(
            request_outcome(Err(&MessagingError::HubUnavailable("x".into()))),
            "unavailable"
        );
        assert_eq!(
            request_outcome(Err(&MessagingError::Storage("x".into()))),
            "error"
        );
    }

    #[test]
    fn without_a_provider_nothing_is_recorded_and_nothing_breaks() {
        // No meter provider is installed in this test binary: the global meter is the no-op one.
        let metrics = Metrics::global();
        metrics.outbox_published(true);
        metrics.event_consumed("p".into(), Value::from(OTHER), Consumed::Parked);
        metrics.event_handled("p".into(), Value::from(OTHER), Duration::from_millis(1));
        metrics.hub_request(RequestKind::Command, "t", "ok", Duration::ZERO);
        let gauge_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ran = Arc::clone(&gauge_ran);
        metrics
            .meter()
            .u64_observable_gauge("snodra.test.gauge")
            .with_callback(move |_| ran.store(true, std::sync::atomic::Ordering::SeqCst))
            .build();
        assert!(!gauge_ran.load(std::sync::atomic::Ordering::SeqCst));
    }
}
