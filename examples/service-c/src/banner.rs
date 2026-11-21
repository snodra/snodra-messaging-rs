//! The banner saga: every shouted text is printed as a banner by the sandbox's fake
//! printer, whatever way the printer answers. The Rust counterpart of service-b's postcard saga,
//! with service-c's own message types (a command type has one owner).
//!
//! It starts on [`TextShouted`] (the job id is `banner-<text in lower case>`), sends
//! [`PrintBanner`] to service-c's own idempotent adapter and gives the printer 30 seconds. The
//! printer's answer arrives as [`BannerPrinted`] by one of three channels, none of which the saga
//! knows: the webhook (`POST /printer/webhook`), the printer's topic for service-c
//! (`printer.banner.events`, the `printer-banner` source), or a poll the saga asks for (a text
//! `poll-...` makes it send [`PollBannerJob`] every 2 seconds; the adapter publishes
//! [`BannerPrinted`] when the job is done). Printed ends the saga, which cancels its deadlines.
//! Otherwise the saga publishes [`BannerFailed`] and ends: when the timeout falls due (`TIMEOUT`),
//! or when a command it sent failed for good (`SagaCommandFailed`, for example `UNKNOWN_OUTCOME`
//! when the printer crashed in the middle of the call).
//!
//! The fake printer takes its instructions from a lower-case prefix of the job's name, so the
//! banner carries the shout in lower case (`WEBHOOK-1` is printed as `webhook-1`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use snodra_messaging::SagaDefinition;
use snodra_messaging::saga::FailureReason;

use crate::messages::{BannerFailed, BannerPrinted, PollBannerJob, PrintBanner, TextShouted};

/// The saga's wire name (`sagas.type`).
pub const SAGA: &str = "examples.Banner";
/// How long the printer has before the saga gives up.
pub const TIMEOUT: Duration = Duration::from_secs(30);
/// How often a `poll-*` banner asks the printer.
pub const POLL_EVERY: Duration = Duration::from_secs(2);
pub const TIMEOUT_DEADLINE: &str = "print-timeout";
pub const POLL_DEADLINE: &str = "poll-printer";
const POLL_PREFIX: &str = "poll-";

/// The saga's state, stored as JSON.
#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Banner {
    job_id: Option<String>,
    text: Option<String>,
    polls: u32,
}

/// The job id of the banner for a shouted text.
pub fn job_id_of(shouted: &str) -> String {
    format!("banner-{}", shouted.to_lowercase())
}

fn job_of(e: &TextShouted) -> Option<String> {
    Some(job_id_of(&e.text))
}

/// The wire name of a failure reason (`UNKNOWN_OUTCOME`), as Kotlin's `reason.name`.
fn reason_name(reason: FailureReason) -> String {
    serde_json::to_value(reason)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{reason:?}"))
}

fn failed(s: &Banner, reason: String) -> BannerFailed {
    BannerFailed {
        job_id: s.job_id.clone().unwrap_or_default(),
        text: s.text.clone().unwrap_or_default(),
        reason,
    }
}

/// The saga, as registered with `builder.saga(SAGA, banner_saga)`.
pub fn banner_saga(d: SagaDefinition<Banner>) -> SagaDefinition<Banner> {
    d.start_on::<TextShouted>("jobId", job_of, |s, e, ctx| {
        if s.job_id.is_some() {
            // The same text shouted again while its banner is in flight: one banner is enough.
            return Ok(());
        }
        let job_id = job_id_of(&e.text);
        let text = e.text.to_lowercase();
        s.job_id = Some(job_id.clone());
        s.text = Some(text.clone());
        ctx.send(
            &PrintBanner {
                job_id,
                text: text.clone(),
            },
            None,
        )?;
        ctx.schedule_after::<()>(TIMEOUT, TIMEOUT_DEADLINE, None)?;
        if text.starts_with(POLL_PREFIX) {
            ctx.schedule_after::<()>(POLL_EVERY, POLL_DEADLINE, None)?;
        }
        Ok(())
    })
    .on::<BannerPrinted>("jobId", |e| Some(e.job_id.clone()), |_, _, _| Ok(()))
    .ending()
    .on_deadline::<IgnoredAny>(POLL_DEADLINE, |s, _, ctx| {
        s.polls += 1;
        ctx.send(
            &PollBannerJob {
                job_id: s.job_id.clone().unwrap_or_default(),
                attempt: s.polls,
            },
            None,
        )?;
        ctx.schedule_after::<()>(POLL_EVERY, POLL_DEADLINE, None)?;
        Ok(())
    })
    .on_deadline::<IgnoredAny>(TIMEOUT_DEADLINE, |s, _, ctx| {
        ctx.publish(&failed(s, "TIMEOUT".into()), s.text.as_deref())
    })
    .ending()
    .on_command_failed(|s, e, ctx| {
        ctx.publish(&failed(s, reason_name(e.reason)), s.text.as_deref())?;
        ctx.end();
        Ok(())
    })
}

/// How a banner ended: `status` `PRINTED` or `FAILED` (with its `reason`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BannerOutcome {
    pub job_id: String,
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// What the sandbox (the smoke test, a curl) reads to see how a banner ended: an in-memory
/// projection of the two events the saga's story ends with, lost on restart.
#[derive(Clone, Default)]
pub struct BannerOutcomes(Arc<Mutex<HashMap<String, BannerOutcome>>>);

impl BannerOutcomes {
    pub fn printed(&self, e: &BannerPrinted) {
        self.put(BannerOutcome {
            job_id: e.job_id.clone(),
            status: "PRINTED",
            reason: None,
        });
    }

    pub fn failed(&self, e: &BannerFailed) {
        self.put(BannerOutcome {
            job_id: e.job_id.clone(),
            status: "FAILED",
            reason: Some(e.reason.clone()),
        });
    }

    pub fn of(&self, job_id: &str) -> Option<BannerOutcome> {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(job_id)
            .cloned()
    }

    fn put(&self, outcome: BannerOutcome) {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(outcome.job_id.clone(), outcome);
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use chrono::Utc;
    use snodra_messaging::saga::FailureReason;
    use snodra_messaging::saga::testing::SagaTestFixture;

    use super::{Banner, BannerOutcomes, POLL_EVERY, SAGA, TIMEOUT, banner_saga, job_id_of};
    use crate::messages::{BannerFailed, BannerPrinted, PollBannerJob, PrintBanner, TextShouted};

    /// 2026-11-03T10:00:00Z.
    fn t0() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(1_793_700_000)
    }

    fn fixture() -> SagaTestFixture<Banner> {
        let mut fixture = SagaTestFixture::new(SAGA, banner_saga);
        fixture.given_current_time(t0());
        fixture
    }

    fn shouted(text: &str) -> TextShouted {
        TextShouted {
            text: text.to_string(),
            at: Utc::now(),
        }
    }

    fn print(text: &str) -> PrintBanner {
        PrintBanner {
            job_id: format!("banner-{text}"),
            text: text.to_string(),
        }
    }

    fn printed(text: &str) -> BannerPrinted {
        BannerPrinted {
            job_id: format!("banner-{text}"),
            text: text.to_string(),
        }
    }

    #[test]
    fn the_job_id_is_the_shout_in_lower_case() {
        assert_eq!(job_id_of("WEBHOOK-17"), "banner-webhook-17");
    }

    #[test]
    fn a_shout_starts_the_saga_sends_print_banner_and_gives_the_printer_30_seconds() {
        fixture()
            .when_event(&shouted("WEBHOOK-1"))
            .expect_active_sagas(1)
            .expect_association("jobId", "banner-webhook-1")
            .expect_dispatched_commands(&[&print("webhook-1")])
            .expect_published_events(&[])
            .expect_scheduled_deadline("print-timeout", t0() + TIMEOUT);
    }

    #[test]
    fn banner_printed_ends_the_saga_and_cancels_the_timeout() {
        fixture()
            .given_events(&[&shouted("WEBHOOK-1")])
            .when_event(&printed("webhook-1"))
            .expect_saga_ended()
            .expect_no_scheduled_deadlines()
            .expect_dispatched_commands(&[])
            .expect_published_events(&[]);
    }

    #[test]
    fn a_poll_banner_polls_every_2_seconds_until_it_is_printed() {
        let poll = |attempt| PollBannerJob {
            job_id: "banner-poll-1".into(),
            attempt,
        };
        fixture()
            .given_events(&[&shouted("POLL-1")])
            .expect_scheduled_deadline("poll-printer", t0() + POLL_EVERY)
            .when_time_elapses(Duration::from_secs(5))
            .expect_dispatched_commands(&[&poll(1), &poll(2)])
            .expect_active_sagas(1)
            .when_event(&printed("poll-1"))
            .expect_saga_ended()
            .expect_no_scheduled_deadlines();
    }

    #[test]
    fn a_banner_that_is_not_a_poll_banner_never_polls() {
        fixture()
            .given_events(&[&shouted("WEBHOOK-1")])
            .when_time_elapses(Duration::from_secs(29))
            .expect_dispatched_commands(&[])
            .expect_active_sagas(1);
    }

    #[test]
    fn a_printer_that_never_answers_fails_the_banner_with_timeout_after_30_seconds() {
        fixture()
            .given_events(&[&shouted("SILENT-1")])
            .when_time_elapses(TIMEOUT)
            .expect_published_events(&[&BannerFailed {
                job_id: "banner-silent-1".into(),
                text: "silent-1".into(),
                reason: "TIMEOUT".into(),
            }])
            .expect_saga_ended()
            .expect_no_scheduled_deadlines();
    }

    #[test]
    fn a_print_with_an_unknown_outcome_fails_the_banner_at_once() {
        fixture()
            .given_events(&[&shouted("CRASH-1")])
            .when_command_failed(
                &print("crash-1"),
                FailureReason::UnknownOutcome,
                "the printer broke the connection",
            )
            .expect_published_events(&[&BannerFailed {
                job_id: "banner-crash-1".into(),
                text: "crash-1".into(),
                reason: "UNKNOWN_OUTCOME".into(),
            }])
            .expect_saga_ended()
            .expect_no_scheduled_deadlines();
    }

    #[test]
    fn the_same_text_shouted_again_while_its_banner_is_in_flight_prints_once() {
        fixture()
            .when_event(&shouted("WEBHOOK-1"))
            .when_event(&shouted("WEBHOOK-1"))
            .expect_active_sagas(1)
            .expect_dispatched_commands(&[&print("webhook-1")]);
    }

    #[test]
    fn a_text_shouted_after_its_banner_was_printed_prints_again() {
        fixture()
            .given_events(&[&shouted("WEBHOOK-1"), &printed("webhook-1")])
            .when_event(&shouted("WEBHOOK-1"))
            .expect_active_sagas(1)
            .expect_dispatched_commands(&[&print("webhook-1")]);
    }

    #[test]
    fn the_outcomes_remember_how_each_banner_ended() {
        let outcomes = BannerOutcomes::default();
        outcomes.printed(&printed("webhook-1"));
        outcomes.failed(&BannerFailed {
            job_id: "banner-silent-1".into(),
            text: "silent-1".into(),
            reason: "TIMEOUT".into(),
        });
        assert_eq!(outcomes.of("banner-webhook-1").unwrap().status, "PRINTED");
        let failed = outcomes.of("banner-silent-1").unwrap();
        assert_eq!(failed.status, "FAILED");
        assert_eq!(failed.reason.as_deref(), Some("TIMEOUT"));
        assert_eq!(
            serde_json::to_value(&failed).unwrap(),
            serde_json::json!({"jobId":"banner-silent-1","status":"FAILED","reason":"TIMEOUT"})
        );
        assert!(outcomes.of("banner-nope").is_none());
    }
}
