//! `SagaTestFixture`: the spec's PaymentSaga driven with events and time, in memory, over
//! the real saga runtime. The Rust form of the Kotlin `SagaTestFixtureTest`. Needs the feature
//! `testing` (`cargo test --all-features`); without it the file compiles to nothing, so the
//! manifest names no test target and a build that copies only `src/` works.
#![cfg(feature = "testing")]

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use snodra_messaging::saga::FailureReason;
use snodra_messaging::saga::testing::SagaTestFixture;
use snodra_messaging::{Command, Event, MessagingError, SagaDefinition, message_type};

macro_rules! event {
    ($name:ident, $wire:literal, { $($field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct $name { $($field: $ty),* }
        message_type!($name, $wire);
        impl Event for $name {}
    };
}

macro_rules! command {
    ($name:ident, $wire:literal, { $($field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct $name { $($field: $ty),* }
        message_type!($name, $wire);
        impl Command for $name { type Reply = (); }
    };
}

event!(OrderPlaced, "fx.OrderPlaced", { order_id: String, amount: i32 });
event!(ChargeSucceeded, "fx.ChargeSucceeded", { payment_ref: String });
event!(OrderCancelled, "fx.OrderCancelled", { order_id: String });
event!(Unrelated, "fx.Unrelated", { order_id: String });
event!(ChargeFailed, "fx.ChargeFailed", {
    reason: FailureReason, command_type: String, message: String, key_belongs_to_saga: bool,
});
event!(SourceSeen, "fx.SourceSeen", { source: String });
command!(RequestCharge, "fx.RequestCharge", { payment_ref: String, amount: i32 });
command!(CancelCharge, "fx.CancelCharge", { payment_ref: String });

fn order_id(e: &OrderPlaced) -> Option<String> {
    Some(e.order_id.clone())
}

/// The spec's PaymentSaga, plus a published event on the timeout.
#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Payment {
    order_id: Option<String>,
    payment_ref: Option<String>,
    attempts: u32,
    timeout_deadline: Option<String>,
}

fn payment_saga(d: SagaDefinition<Payment>) -> SagaDefinition<Payment> {
    d.start_on::<OrderPlaced>("orderId", order_id, |s, e, ctx| {
        let payment_ref = format!("pay-{}", e.order_id);
        s.order_id = Some(e.order_id.clone());
        s.payment_ref = Some(payment_ref.clone());
        ctx.associate_with("paymentRef", &payment_ref);
        ctx.send(
            &RequestCharge {
                payment_ref,
                amount: e.amount,
            },
            None,
        )?;
        s.timeout_deadline =
            Some(ctx.schedule_after::<()>(Duration::from_secs(15 * 60), "charge-timeout", None)?);
        Ok(())
    })
    .on::<ChargeSucceeded>(
        "paymentRef",
        |e| Some(e.payment_ref.clone()),
        |s, _, ctx| {
            if let Some(id) = &s.timeout_deadline {
                ctx.cancel(id);
            }
            Ok(())
        },
    )
    .ending()
    .on_deadline::<IgnoredAny>("charge-timeout", |s, _, ctx| {
        let payment_ref = s.payment_ref.clone().unwrap_or_default();
        let order_id = s.order_id.clone().unwrap_or_default();
        ctx.send(&CancelCharge { payment_ref }, None)?;
        ctx.publish(
            &OrderCancelled {
                order_id: order_id.clone(),
            },
            Some(&order_id),
        )?;
        ctx.end();
        Ok(())
    })
}

/// Reacts to its command failing, as a saga does after the relay reports `SagaCommandFailed`.
#[derive(Default, Serialize, Deserialize)]
struct Failing {
    order_id: Option<String>,
}

fn failing_saga(d: SagaDefinition<Failing>) -> SagaDefinition<Failing> {
    d.start_on::<OrderPlaced>("orderId", order_id, |s, e, ctx| {
        s.order_id = Some(e.order_id.clone());
        ctx.send(
            &RequestCharge {
                payment_ref: format!("pay-{}", e.order_id),
                amount: e.amount,
            },
            None,
        )
    })
    .on_command_failed(|_, e, ctx| {
        let own_key = format!("{}:0", ctx.saga_id());
        ctx.publish(
            &ChargeFailed {
                reason: e.reason,
                command_type: e.command_type.clone(),
                message: e.message.clone(),
                key_belongs_to_saga: e.idempotency_key == own_key,
            },
            None,
        )?;
        ctx.end();
        Ok(())
    })
}

/// Reads a header and reschedules itself like a polling saga.
#[derive(Default, Serialize, Deserialize)]
struct Poller {
    polls: u32,
}

fn poller_saga(d: SagaDefinition<Poller>) -> SagaDefinition<Poller> {
    d.start_on::<OrderPlaced>("orderId", order_id, |_, _, ctx| {
        let source = ctx.message().headers.get("x-source").cloned();
        if let Some(source) = source {
            ctx.publish(&SourceSeen { source }, None)?;
        }
        ctx.schedule_after::<()>(Duration::from_secs(2), "poll", None)?;
        Ok(())
    })
    .on_deadline::<IgnoredAny>("poll", |s, _, ctx| {
        s.polls += 1;
        ctx.send(
            &CancelCharge {
                payment_ref: format!("poll-{}", s.polls),
            },
            None,
        )?;
        if s.polls < 3 {
            ctx.schedule_after::<()>(Duration::from_secs(2), "poll", None)?;
        } else {
            ctx.end();
        }
        Ok(())
    })
}

/// State serde_json cannot write once the handler has put something in it: a map with non-string
/// keys (the empty default passes the registration check).
#[derive(Default, Serialize, Deserialize)]
struct Unwritable {
    cells: HashMap<(i32, i32), String>,
}

fn unwritable_saga(d: SagaDefinition<Unwritable>) -> SagaDefinition<Unwritable> {
    d.start_on::<OrderPlaced>("orderId", order_id, |s, e, _| {
        s.cells.insert((1, 2), e.order_id.clone());
        Ok(())
    })
}

#[derive(Default, Serialize, Deserialize)]
struct Throwing {}

fn throwing_saga(d: SagaDefinition<Throwing>) -> SagaDefinition<Throwing> {
    d.start_on::<OrderPlaced>("orderId", order_id, |_, e, _| {
        Err(MessagingError::InvalidMessage(format!(
            "the handler is broken for {}",
            e.order_id
        )))
    })
}

fn t0() -> SystemTime {
    // 2026-11-03T10:00:00Z
    UNIX_EPOCH + Duration::from_secs(1_793_700_000)
}

fn payment() -> SagaTestFixture<Payment> {
    let mut fixture = SagaTestFixture::new("fx.Payment", payment_saga);
    fixture.given_current_time(t0());
    fixture
}

/// The panic message of `f`, which must panic.
fn panic_of(f: impl FnOnce()) -> String {
    let panic = catch_unwind(AssertUnwindSafe(f)).expect_err("expected the step to fail");
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default()
}

fn order(id: &str, amount: i32) -> OrderPlaced {
    OrderPlaced {
        order_id: id.into(),
        amount,
    }
}

#[test]
fn a_start_event_creates_the_saga_associates_it_records_the_command_and_schedules_the_deadline() {
    payment()
        .when_event(&order("o1", 42))
        .expect_active_sagas(1)
        .expect_association("orderId", "o1")
        .expect_association("paymentRef", "pay-o1")
        .expect_dispatched_commands(&[&RequestCharge {
            payment_ref: "pay-o1".into(),
            amount: 42,
        }])
        .expect_published_events(&[])
        .expect_scheduled_deadline("charge-timeout", t0() + Duration::from_secs(15 * 60));
}

#[test]
fn the_success_path_ends_the_saga_and_cancels_its_deadline() {
    payment()
        .given_events(&[&order("o1", 42)])
        .when_event(&ChargeSucceeded {
            payment_ref: "pay-o1".into(),
        })
        .expect_saga_ended()
        .expect_active_sagas(0)
        .expect_no_scheduled_deadlines()
        .expect_dispatched_commands(&[]);
}

#[test]
fn elapsed_time_fires_the_timeout_which_sends_cancel_charge_publishes_and_ends_the_saga() {
    payment()
        .given_events(&[&order("o1", 42)])
        .when_time_elapses(Duration::from_secs(14 * 60))
        .expect_active_sagas(1)
        .expect_dispatched_commands(&[])
        .when_time_elapses(Duration::from_secs(60))
        .expect_dispatched_commands(&[&CancelCharge {
            payment_ref: "pay-o1".into(),
        }])
        .expect_published_events(&[&OrderCancelled {
            order_id: "o1".into(),
        }])
        .expect_saga_ended()
        .expect_no_scheduled_deadlines();
}

#[test]
fn what_the_given_events_sent_is_not_part_of_the_expected_commands() {
    payment()
        .given_events(&[&order("o1", 42)])
        .when_event(&Unrelated {
            order_id: "o1".into(),
        })
        .expect_dispatched_commands(&[])
        .expect_active_sagas(1);
}

#[test]
fn an_event_the_saga_does_not_handle_changes_nothing() {
    payment()
        .when_event(&Unrelated {
            order_id: "o1".into(),
        })
        .expect_active_sagas(0)
        .expect_dispatched_commands(&[])
        .expect_no_scheduled_deadlines();
}

#[test]
fn a_deadline_that_reschedules_itself_fires_once_per_elapsed_interval_at_its_own_due_time() {
    let poll = |n: u32| CancelCharge {
        payment_ref: format!("poll-{n}"),
    };
    SagaTestFixture::new("fx.Poller", poller_saga)
        .given_current_time(t0())
        .when_event(&order("o1", 1))
        .expect_scheduled_deadline("poll", t0() + Duration::from_secs(2))
        .when_time_elapses(Duration::from_secs(5))
        .expect_dispatched_commands(&[&poll(1), &poll(2)])
        .expect_scheduled_deadline("poll", t0() + Duration::from_secs(6))
        .when_time_elapses(Duration::from_secs(1))
        .expect_dispatched_commands(&[&poll(1), &poll(2), &poll(3)])
        .expect_saga_ended();
}

#[test]
fn the_headers_of_the_event_reach_the_saga() {
    SagaTestFixture::new("fx.Poller", poller_saga)
        .when_event_with_headers(&order("o1", 1), &[("x-source", "webhook")])
        .expect_active_sagas(1)
        .expect_published_events(&[&SourceSeen {
            source: "webhook".into(),
        }]);
}

#[test]
fn state_that_cannot_be_written_fails_the_step_naming_the_saga_the_event_and_the_cause() {
    let message = panic_of(|| {
        SagaTestFixture::new("fx.Unwritable", unwritable_saga).when_event(&order("o1", 1));
    });
    assert!(message.contains("fx.Unwritable"), "{message}");
    assert!(message.contains("fx.OrderPlaced"), "{message}");
    assert!(message.contains("JSON"), "{message}");
    assert!(message.contains("key must be a string"), "{message}");
}

#[test]
fn a_handler_error_fails_the_step_at_once_with_the_handlers_message() {
    let message = panic_of(|| {
        SagaTestFixture::new("fx.Throwing", throwing_saga).when_event(&order("o1", 1));
    });
    assert!(
        message.contains("the handler is broken for o1"),
        "{message}"
    );
}

#[test]
fn a_failing_step_leaves_nothing_behind() {
    let mut fixture = SagaTestFixture::new("fx.Throwing", throwing_saga);
    let _ = catch_unwind(AssertUnwindSafe(|| {
        fixture.when_event(&order("o1", 1));
    }));
    fixture
        .expect_active_sagas(0)
        .expect_dispatched_commands(&[])
        .expect_no_scheduled_deadlines();
}

#[test]
fn a_failed_expectation_panics_saying_what_was_expected_and_what_was_found() {
    let mut fixture = payment();
    fixture.when_event(&order("o1", 42));
    let mut check = |f: &dyn Fn(&mut SagaTestFixture<Payment>), wanted: &str| {
        let message = panic_of(|| f(&mut fixture));
        assert!(message.contains(wanted), "{wanted} not in: {message}");
        assert!(message.contains("fx.Payment"), "{message}");
    };
    check(
        &|f| {
            f.expect_active_sagas(2);
        },
        "expected 2 active sagas but found 1",
    );
    check(
        &|f| {
            f.expect_association("orderId", "zzz");
        },
        "orderId=zzz",
    );
    check(
        &|f| {
            f.expect_dispatched_commands(&[&RequestCharge {
                payment_ref: "pay-o1".into(),
                amount: 43,
            }]);
        },
        "fx.RequestCharge",
    );
    check(
        &|f| {
            f.expect_dispatched_commands(&[]);
        },
        "expected no commands",
    );
    check(
        &|f| {
            f.expect_published_events(&[&OrderCancelled {
                order_id: "o1".into(),
            }]);
        },
        "fx.OrderCancelled",
    );
    check(
        &|f| {
            f.expect_scheduled_deadline("charge-timeout", t0());
        },
        "charge-timeout",
    );
    check(
        &|f| {
            f.expect_no_scheduled_deadlines();
        },
        "charge-timeout",
    );
    check(
        &|f| {
            f.expect_saga_ended();
        },
        "not ended",
    );
}

#[test]
fn given_current_time_sets_the_time_deadlines_are_scheduled_against() {
    // 2030-01-01T00:00:00Z
    let later = UNIX_EPOCH + Duration::from_secs(1_893_456_000);
    SagaTestFixture::new("fx.Payment", payment_saga)
        .given_current_time(later)
        .when_event(&order("o1", 1))
        .expect_scheduled_deadline("charge-timeout", later + Duration::from_secs(15 * 60));
}

#[test]
fn a_definition_the_startup_checks_refuse_is_refused_when_the_fixture_is_built() {
    let message = panic_of(|| {
        SagaTestFixture::new("fx.NoStart", |d: SagaDefinition<Payment>| {
            d.on::<OrderPlaced>("orderId", order_id, |_, _, _| Ok(()))
        });
    });
    assert!(message.contains("fx.NoStart"), "{message}");
    assert!(message.contains("no start handler"), "{message}");
}

#[test]
fn when_command_failed_tells_the_saga_that_sent_the_command_with_the_key_the_relay_would_have_used()
{
    SagaTestFixture::new("fx.Failing", failing_saga)
        .when_event(&order("o1", 5))
        .when_event(&order("o2", 6))
        .when_command_failed(
            &RequestCharge {
                payment_ref: "pay-o2".into(),
                amount: 6,
            },
            FailureReason::UnknownOutcome,
            "the printer crashed",
        )
        .expect_published_events(&[&ChargeFailed {
            reason: FailureReason::UnknownOutcome,
            command_type: "fx.RequestCharge".into(),
            message: "the printer crashed".into(),
            key_belongs_to_saga: true,
        }])
        .expect_active_sagas(1); // o2's saga ended, o1's did not hear of it
}

#[test]
fn when_command_failed_for_a_command_no_saga_sent_fails_the_step_saying_so() {
    let message = panic_of(|| {
        SagaTestFixture::new("fx.Failing", failing_saga)
            .when_event(&order("o1", 5))
            .when_command_failed(
                &RequestCharge {
                    payment_ref: "pay-zzz".into(),
                    amount: 5,
                },
                FailureReason::Failed,
                "",
            );
    });
    assert!(message.contains("fx.RequestCharge"), "{message}");
    assert!(message.contains("pay-zzz"), "{message}");
}

/// Its start handler hands the instance over from its start association to a payment reference.
#[derive(Default, Serialize, Deserialize)]
struct Handover {}

fn handover_saga(d: SagaDefinition<Handover>) -> SagaDefinition<Handover> {
    d.start_on::<OrderPlaced>("orderId", order_id, |_, e, ctx| {
        ctx.associate_with("paymentRef", &format!("pay-{}", e.order_id));
        ctx.remove_association_with("orderId", &e.order_id);
        Ok(())
    })
}

#[test]
fn a_start_handler_that_removes_its_own_start_association_lets_the_next_start_event_start_another_saga()
 {
    SagaTestFixture::new("fx.Handover", handover_saga)
        .given_events(&[&order("o1", 5)])
        .when_event(&order("o1", 5))
        .expect_active_sagas(2);
}

/// An event that serializes to `null`, and one with a number BSON cannot hold.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Tick;
message_type!(Tick, "fx.Tick");
impl Event for Tick {}

event!(Huge, "fx.Huge", { n: u64 });

#[derive(Default, Serialize, Deserialize)]
struct Ticking {}

fn ticking_saga(d: SagaDefinition<Ticking>) -> SagaDefinition<Ticking> {
    d.start_on::<OrderPlaced>("orderId", order_id, |_, e, ctx| {
        if e.amount == 1 {
            ctx.publish(&Tick, None)
        } else {
            ctx.publish(&Huge { n: u64::MAX }, None)
        }
    })
}

#[test]
fn publishing_an_event_whose_payload_is_not_a_json_object_is_a_handler_error_naming_the_event() {
    for (amount, wire) in [(1, "fx.Tick"), (2, "fx.Huge")] {
        let message = panic_of(|| {
            SagaTestFixture::new("fx.Ticking", ticking_saga).when_event(&order("o1", amount));
        });
        assert!(message.contains(wire), "{message}");
        assert!(message.contains("outbox"), "{message}");
    }
}
