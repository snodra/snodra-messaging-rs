//! The messages and sagas of the saga runtime and failure-admin tests ("rt.*", the Rust form of
//! the Kotlin `SagaRuntimeFixtures`).
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

use kraken_messaging::saga::{EventOutcome, SagaDefinition, SagaMetrics, SagaType};
use kraken_messaging::{Command, Event, MessagingError, message_type};
use serde::{Deserialize, Serialize};

use super::saga::t0;

macro_rules! event {
    ($name:ident, $wire:literal, { $($field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Clone, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct $name { $(pub $field: $ty),* }
        message_type!($name, $wire);
        impl Event for $name {}
    };
}

event!(OrderPlaced, "rt.OrderPlaced", { order_id: String });
event!(PaymentTaken, "rt.PaymentTaken", { payment_ref: String });
event!(Bump, "rt.Bump", { order_id: String, tag: String });
event!(Forced, "rt.Forced", { order_id: String });
event!(Finish, "rt.Finish", { order_id: String });
event!(Assoc, "rt.Assoc", { order_id: String, key: String, value: String, remove: bool });
event!(ByRef, "rt.ByRef", { r#ref: String, tag: String });
event!(Note, "rt.Note", { text: String });
event!(Done, "rt.Done", { order_id: String });
event!(Priced, "rt.Priced", { order_id: String, amount: String, tags: BTreeMap<String, String> });
event!(PriceCheck, "rt.PriceCheck", { order_id: String });

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DoIt {
    pub order_id: String,
}
message_type!(DoIt, "rt.DoIt");
impl Command for DoIt {
    type Reply = ();
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChargeAmount {
    pub amount: String,
    pub tags: BTreeMap<String, String>,
}
message_type!(ChargeAmount, "rt.ChargeAmount");
impl Command for ChargeAmount {
    type Reply = ();
}

/// A deadline payload without a wire name.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Nudge {
    pub order_id: String,
}

pub fn assoc(order_id: &str, key: &str, value: &str, remove: bool) -> Assoc {
    Assoc {
        order_id: order_id.into(),
        key: key.into(),
        value: value.into(),
        remove,
    }
}

/// Test switches the handlers consult: a barrier that makes the first handlers wait for each
/// other, and a failure counter.
#[derive(Default)]
pub struct Gate {
    barrier: Mutex<Option<Arc<Barrier>>>,
    arrivals: AtomicUsize,
    parties: AtomicUsize,
    pub fail_first: AtomicUsize,
    /// How often the broken saga's handler ran.
    pub broken_calls: AtomicUsize,
}

impl Gate {
    pub fn new() -> Arc<Gate> {
        Arc::new(Gate::default())
    }

    /// The next `parties` handler calls wait for each other (retries pass through).
    pub fn meet_in_pairs(&self, parties: usize) {
        *self.barrier.lock().unwrap() = Some(Arc::new(Barrier::new(parties)));
        self.parties.store(parties, Ordering::SeqCst);
        self.arrivals.store(0, Ordering::SeqCst);
    }

    fn meet(&self) {
        let barrier = self.barrier.lock().unwrap().clone();
        if let Some(barrier) = barrier
            && self.arrivals.fetch_add(1, Ordering::SeqCst) < self.parties.load(Ordering::SeqCst)
        {
            // Blocking a worker thread of a multi-thread runtime starves the tasks parked on it
            // (the driver's own); say so to the runtime.
            tokio::task::block_in_place(|| {
                barrier.wait();
            });
        }
    }

    fn fail_if_asked(&self) -> Result<(), MessagingError> {
        let left = self.fail_first.load(Ordering::SeqCst);
        if left > 0 {
            self.fail_first.store(left - 1, Ordering::SeqCst);
            return Err(MessagingError::InvalidMessage(
                "handler failed on purpose".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Order {
    pub order_id: Option<String>,
    pub placed: u32,
    pub forced: u32,
    pub log: Vec<String>,
    pub saw_saga_id: Option<String>,
    pub saw_message_id: Option<String>,
    pub saw_correlation_id: Option<String>,
}

pub fn order_saga(gate: &Arc<Gate>) -> SagaType {
    let on_placed = Arc::clone(gate);
    let on_bump = Arc::clone(gate);
    SagaDefinition::<Order>::new("rt.Order")
        .start_on::<OrderPlaced>(
            "orderId",
            |e| Some(e.order_id.clone()),
            move |s, e, ctx| {
                on_placed.meet();
                s.order_id = Some(e.order_id.clone());
                s.placed += 1;
                s.saw_saga_id = Some(ctx.saga_id().to_string());
                s.saw_message_id = Some(ctx.message().message_id.clone());
                s.saw_correlation_id = Some(ctx.message().correlation_id.clone());
                ctx.associate_with("paymentRef", &format!("pay-{}", e.order_id));
                ctx.send(
                    &DoIt {
                        order_id: e.order_id.clone(),
                    },
                    None,
                )?;
                ctx.publish(
                    &Done {
                        order_id: e.order_id.clone(),
                    },
                    Some(&format!("k-{}", e.order_id)),
                )?;
                ctx.schedule_after(
                    Duration::from_secs(600),
                    "timeout",
                    Some(&format!("p-{}", e.order_id)),
                )?;
                ctx.schedule_at(
                    t0() + Duration::from_secs(2 * 3600),
                    "nudge",
                    Some(&Nudge {
                        order_id: e.order_id.clone(),
                    }),
                )?;
                on_placed.fail_if_asked()
            },
        )
        .start_new_on::<Forced>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |s, _, _| {
                s.forced += 1;
                Ok(())
            },
        )
        .on::<Bump>(
            "orderId",
            |e| Some(e.order_id.clone()),
            move |s, e, ctx| {
                on_bump.meet();
                s.log.push(e.tag.clone());
                ctx.send(
                    &DoIt {
                        order_id: format!("{}-{}", e.order_id, e.tag),
                    },
                    None,
                )
            },
        )
        .on::<PaymentTaken>(
            "paymentRef",
            |e| Some(e.payment_ref.clone()),
            |s, _, _| {
                s.log.push("paid".into());
                Ok(())
            },
        )
        .ending()
        .on::<Finish>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |_, _, ctx| {
                ctx.end();
                Ok(())
            },
        )
        .on::<Assoc>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |_, e, ctx| {
                if e.remove {
                    ctx.remove_association_with(&e.key, &e.value);
                } else {
                    ctx.associate_with(&e.key, &e.value);
                }
                Ok(())
            },
        )
        .on::<ByRef>(
            "ref",
            |e| Some(e.r#ref.clone()),
            |s, e, _| {
                s.log.push(e.tag.clone());
                Ok(())
            },
        )
        .build()
        .unwrap()
}

/// Throws on every event, as a broken saga type does.
#[derive(Default, Serialize, Deserialize)]
pub struct Broken {}

pub fn broken_saga(gate: &Arc<Gate>) -> SagaType {
    let gate = Arc::clone(gate);
    SagaDefinition::<Broken>::new("rt.Broken")
        .start_on::<OrderPlaced>(
            "orderId",
            |e| Some(e.order_id.clone()),
            move |_, e, _| {
                gate.broken_calls.fetch_add(1, Ordering::SeqCst);
                Err(MessagingError::InvalidMessage(format!(
                    "broken on {}",
                    e.order_id
                )))
            },
        )
        .build()
        .unwrap()
}

/// Associates by the message header `x-order` instead of an event property.
#[derive(Default, Serialize, Deserialize)]
pub struct Header {
    pub notes: u32,
}

pub fn header_saga() -> SagaType {
    SagaDefinition::<Header>::new("rt.Header")
        .start_on_header::<Note>("x-order", "x-order", |s, _, _| {
            s.notes += 1;
            Ok(())
        })
        .build()
        .unwrap()
}

/// Keeps a decimal as text and a map with awkward keys (`a.b`, `$c`) in its state and sends them
/// in commands.
#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Price {
    pub order_id: Option<String>,
    pub amount: String,
    pub tags: BTreeMap<String, String>,
}

pub fn price_saga() -> SagaType {
    SagaDefinition::<Price>::new("rt.Price")
        .start_on::<Priced>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |s, e, ctx| {
                s.order_id = Some(e.order_id.clone());
                s.amount = e.amount.clone();
                s.tags = e.tags.clone();
                ctx.send(
                    &ChargeAmount {
                        amount: s.amount.clone(),
                        tags: s.tags.clone(),
                    },
                    None,
                )
            },
        )
        .on::<PriceCheck>(
            "orderId",
            |e| Some(e.order_id.clone()),
            |s, _, ctx| {
                ctx.send(
                    &ChargeAmount {
                        amount: s.amount.clone(),
                        tags: s.tags.clone(),
                    },
                    None,
                )
            },
        )
        .build()
        .unwrap()
}

/// Fails while `broken` is set, as a saga with a bug (or an unavailable collaborator) does.
#[derive(Default, Serialize, Deserialize)]
pub struct Flaky {
    pub placed: u32,
}

pub fn flaky_saga(broken: &Arc<std::sync::atomic::AtomicBool>) -> SagaType {
    let broken = Arc::clone(broken);
    SagaDefinition::<Flaky>::new("rt.Flaky")
        .start_on::<OrderPlaced>(
            "orderId",
            |e| Some(e.order_id.clone()),
            move |s, e, ctx| {
                if broken.load(Ordering::SeqCst) {
                    return Err(MessagingError::InvalidMessage(format!(
                        "flaky on {}",
                        e.order_id
                    )));
                }
                s.placed += 1;
                ctx.send(
                    &DoIt {
                        order_id: e.order_id.clone(),
                    },
                    None,
                )
            },
        )
        .build()
        .unwrap()
}

/// Records the metrics calls of a runtime.
#[derive(Default)]
pub struct RecordingMetrics {
    pub started: Mutex<Vec<String>>,
    pub ended: Mutex<Vec<String>>,
    pub events: Mutex<Vec<(String, EventOutcome)>>,
    pub fired: Mutex<Vec<String>>,
    pub overdue: Mutex<HashMap<String, u64>>,
}

impl RecordingMetrics {
    pub fn new() -> Arc<RecordingMetrics> {
        Arc::new(RecordingMetrics::default())
    }

    pub fn count(&self, saga_type: &str, outcome: EventOutcome) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|(t, o)| t == saga_type && *o == outcome)
            .count()
    }

    pub fn started(&self) -> Vec<String> {
        self.started.lock().unwrap().clone()
    }

    pub fn ended(&self) -> Vec<String> {
        self.ended.lock().unwrap().clone()
    }
}

impl SagaMetrics for RecordingMetrics {
    fn saga_started(&self, saga_type: &str) {
        self.started.lock().unwrap().push(saga_type.into());
    }

    fn saga_ended(&self, saga_type: &str) {
        self.ended.lock().unwrap().push(saga_type.into());
    }

    fn event(&self, saga_type: &str, outcome: EventOutcome) {
        self.events
            .lock()
            .unwrap()
            .push((saga_type.into(), outcome));
    }

    fn deadline_fired(&self, name: &str) {
        self.fired.lock().unwrap().push(name.into());
    }

    fn deadlines_overdue(&self, by_name: &HashMap<String, u64>) {
        *self.overdue.lock().unwrap() = by_name.clone();
    }
}
