//! The edge of service-c towards the fake printer: the HTTP client its idempotent adapters call,
//! the translator of the printer's topic, and the check of the printer's webhook.
//!
//! The adapters (registered in `main.rs` with `idempotent_command`) are command handlers made
//! idempotent over `external_calls`, so a command the saga relay sends twice (a crash between
//! sending and marking it sent) reaches the printer once.

use std::collections::HashMap;
use std::time::Duration;

use kraken_messaging::MessagingError;
use kraken_messaging::external::TranslatedEvent;
use kraken_messaging::external::kafka::{BorrowedMessage, Message};
use kraken_messaging::external::webhook;
use kraken_messaging::kafka::EventBus;
use serde::{Deserialize, Serialize};

use crate::messages::{BannerPrinted, PollBannerJob};

/// The name of the printer's topic source (`builder.external_source`) and of the webhook's
/// ingress source: one dedupe namespace for both channels, so a notification the printer sends
/// both ways would apply once.
pub const SOURCE: &str = "printer-banner";

/// A job as service-c submits it: `replyWebhook` and `replyTopic` tell the printer where this
/// customer wants its notifications (service-b's postcards use the printer's defaults).
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct JobRequest {
    pub job_id: String,
    pub name: String,
    pub reply_webhook: String,
    pub reply_topic: String,
}

/// A job as the printer reports it: `DONE` once printed.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct JobStatus {
    pub job_id: String,
    pub name: String,
    pub status: String,
}

/// What the printer tells its customer when a job is printed: on the webhook and on the topic
/// alike. `deliveryId` is the printer's id of the notification, stable across its retries.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PrinterNotification {
    pub delivery_id: String,
    pub job_id: String,
    pub name: String,
}

impl PrinterNotification {
    pub fn printed(&self) -> BannerPrinted {
        BannerPrinted {
            job_id: self.job_id.clone(),
            text: self.name.clone(),
        }
    }
}

/// Where the printer should answer service-c.
#[derive(Clone, Debug)]
pub struct ReplyTo {
    pub webhook: String,
    pub topic: String,
}

/// The printer over HTTP (`POST /jobs`, `GET /jobs/{id}`), classifying what goes wrong the way an
/// adapter must: nothing reached the printer (the connection was refused), a 5xx or a 429 are
/// [`MessagingError::RetryableExternal`] (the saga relay backs off and calls again); another 4xx
/// is permanent; a submit whose connection broke **after** it went out leaves the outcome unknown
/// ([`MessagingError::UnknownOutcome`]: the printer may or may not have printed, so nobody calls
/// it again by itself and the saga is told `UNKNOWN_OUTCOME`). A read can always be repeated, so
/// a failed [`status`](Self::status) is retryable.
#[derive(Clone)]
pub struct Printer {
    http: reqwest::Client,
    url: String,
    reply_to: ReplyTo,
}

impl Printer {
    pub fn new(url: &str, reply_to: ReplyTo) -> Result<Printer, MessagingError> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(5))
            // A fresh connection per call: a pooled connection the printer closed while idle must
            // never look like a call that broke midway (an unknown outcome).
            .pool_max_idle_per_host(0)
            .build()
            .map_err(|e| MessagingError::Transport(format!("printer client: {e}")))?;
        Ok(Printer {
            http,
            url: url.trim_end_matches('/').to_string(),
            reply_to,
        })
    }

    /// The request for a banner job.
    pub fn job(&self, job_id: &str, text: &str) -> JobRequest {
        JobRequest {
            job_id: job_id.to_string(),
            name: text.to_string(),
            reply_webhook: self.reply_to.webhook.clone(),
            reply_topic: self.reply_to.topic.clone(),
        }
    }

    /// Submits the job under `idempotency_key`: the printer prints once per key however often it
    /// is asked.
    pub async fn submit(
        &self,
        job: &JobRequest,
        idempotency_key: &str,
    ) -> Result<(), MessagingError> {
        let sent = self
            .http
            .post(format!("{}/jobs", self.url))
            .header("Idempotency-Key", idempotency_key)
            .json(job)
            .send()
            .await;
        let response = match sent {
            Ok(response) => response,
            Err(e) if e.is_connect() => {
                return Err(MessagingError::RetryableExternal(format!(
                    "the printer could not be reached submitting {}: {e}",
                    job.job_id
                )));
            }
            Err(e) => {
                return Err(MessagingError::UnknownOutcome(format!(
                    "the connection to the printer broke after job {} was sent ({e}): it may or may not have printed",
                    job.job_id
                )));
            }
        };
        classify(
            response.status().as_u16(),
            &format!("submitting {}", job.job_id),
        )
    }

    /// The printer's view of the job, `None` when it does not know it.
    pub async fn status(&self, job_id: &str) -> Result<Option<JobStatus>, MessagingError> {
        let retryable = |e: reqwest::Error| {
            MessagingError::RetryableExternal(format!(
                "the printer could not be read polling {job_id}: {e}"
            ))
        };
        let response = self
            .http
            .get(self.job_url(job_id)?)
            .send()
            .await
            .map_err(retryable)?;
        let code = response.status().as_u16();
        if code == 404 {
            return Ok(None);
        }
        classify(code, &format!("polling {job_id}"))?;
        Ok(Some(response.json().await.map_err(retryable)?))
    }

    /// `GET /jobs/{id}` of `job_id`, the id one percent-encoded path segment (it carries the
    /// shout's text: a `/`, `?` or `#` in it must not change the path or add a query).
    fn job_url(&self, job_id: &str) -> Result<reqwest::Url, MessagingError> {
        let invalid = || {
            MessagingError::InvalidMessage(format!(
                "the printer URL {} is not a base URL",
                self.url
            ))
        };
        let mut url = reqwest::Url::parse(&self.url).map_err(|_| invalid())?;
        url.path_segments_mut()
            .map_err(|()| invalid())?
            .pop_if_empty()
            .extend(["jobs", job_id]);
        Ok(url)
    }
}

/// The poll adapter's body (`PollBannerJob`): asks the printer about the job and publishes
/// `BannerPrinted` when it is done.
pub async fn poll(
    printer: &Printer,
    events: &EventBus,
    cmd: &PollBannerJob,
) -> Result<(), MessagingError> {
    let Some(job) = printer.status(&cmd.job_id).await? else {
        return Ok(());
    };
    if job.status == "DONE" {
        let printed = BannerPrinted {
            job_id: job.job_id,
            text: job.name,
        };
        // The printer was only read: a repeat is safe, so a publish that fails (the outbox is
        // unreachable) is retryable and the relay sends this poll again, rather than FAILED,
        // which would end the banner.
        events
            .publish(&printed, Some(&printed.text))
            .await
            .map_err(|e| {
                MessagingError::RetryableExternal(format!(
                    "the printer said {} is printed but BannerPrinted could not be published: {e}",
                    printed.job_id
                ))
            })?;
    }
    Ok(())
}

/// An HTTP status of the printer as the adapter's outcome.
fn classify(code: u16, what: &str) -> Result<(), MessagingError> {
    match code {
        200..=299 => Ok(()),
        429 => Err(MessagingError::RetryableExternal(format!(
            "the printer is rate limiting (429) {what}"
        ))),
        500..=599 => Err(MessagingError::RetryableExternal(format!(
            "the printer failed with {code} {what}"
        ))),
        _ => Err(MessagingError::RemoteHandler {
            type_name: "printer".to_string(),
            message: format!("the printer refused {what} with {code}"),
            details: Vec::new(),
        }),
    }
}

/// The translator of the `printer-banner` source: a record of the printer's topic for service-c
/// becomes [`BannerPrinted`], with the printer's delivery id as the dedupe key (a record produced
/// twice applies once). A record that is no notification is skipped (`InvalidMessage`).
pub fn translate(record: &BorrowedMessage<'_>) -> Result<Option<TranslatedEvent>, MessagingError> {
    translate_payload(record.payload().unwrap_or_default())
}

pub fn translate_payload(payload: &[u8]) -> Result<Option<TranslatedEvent>, MessagingError> {
    let notification: PrinterNotification = serde_json::from_slice(payload)
        .map_err(|e| MessagingError::InvalidMessage(format!("not a printer notification: {e}")))?;
    Ok(Some(TranslatedEvent::new(
        &notification.printed(),
        Some(notification.delivery_id),
    )?))
}

/// The webhook's verdict on a delivery before anything is dispatched: the notification, or the
/// HTTP status to answer (401 for a bad signature, 400 for a body that is no notification).
pub fn check_webhook<I, K, V>(
    secret: &str,
    headers: I,
    body: &[u8],
) -> Result<PrinterNotification, u16>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<[u8]>,
{
    if !webhook::verify(secret, None, headers, body) {
        return Err(401);
    }
    serde_json::from_slice(body).map_err(|_| 400)
}

/// The extra message headers of a webhook event: none (the HTTP headers carry the signature,
/// which has no business in a message context or a parked event).
pub fn no_headers() -> HashMap<String, String> {
    HashMap::new()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::{
        Printer, PrinterNotification, ReplyTo, check_webhook, classify, poll, translate_payload,
    };
    use crate::messages::PollBannerJob;
    use kraken_messaging::MessagingError;
    use kraken_messaging::kafka::EventBus;

    /// A printer on a free loopback port that answers every request with `DONE` for `jobId` and
    /// records the path and query it was asked for.
    async fn done_printer() -> (Printer, Arc<Mutex<Vec<String>>>) {
        let asked = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&asked);
        let app = axum::Router::new().fallback(move |uri: axum::http::Uri| {
            let seen = Arc::clone(&seen);
            async move {
                seen.lock().unwrap().push(uri.to_string());
                (
                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                    r#"{"jobId":"banner-x","name":"x","status":"DONE"}"#,
                )
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await });
        let reply_to = ReplyTo {
            webhook: String::new(),
            topic: String::new(),
        };
        (Printer::new(&url, reply_to).unwrap(), asked)
    }

    #[tokio::test]
    async fn a_job_id_is_one_encoded_path_segment_of_the_status_request() {
        let (printer, asked) = done_printer().await;

        printer.status("banner-a/b?c#d e").await.unwrap();

        assert_eq!(
            *asked.lock().unwrap(),
            ["/jobs/banner-a%2Fb%3Fc%23d%20e"],
            "a shout's text must not change the printer's path or add a query"
        );
    }

    #[tokio::test]
    async fn a_poll_whose_publish_fails_is_retryable_so_the_saga_keeps_polling() {
        let (printer, _) = done_printer().await;
        // No outbox: the publish fails after the printer said DONE.
        let cmd = PollBannerJob {
            job_id: "banner-x".into(),
            attempt: 1,
        };

        let polled = poll(&printer, &EventBus::new(), &cmd).await;

        assert!(
            matches!(polled, Err(MessagingError::RetryableExternal(_))),
            "a failed publish must not make the poll FAILED: {polled:?}"
        );
    }

    const SECRET: &str = "test-secret";
    const BODY: &[u8] = br#"{"deliveryId":"printed-banner-webhook-1","jobId":"banner-webhook-1","name":"webhook-1"}"#;

    #[test]
    fn a_webhook_with_the_shared_secret_is_the_printers_notification() {
        let notification =
            check_webhook(SECRET, [("X-Kraken-Webhook-Secret", SECRET)], BODY).unwrap();
        assert_eq!(
            notification,
            PrinterNotification {
                delivery_id: "printed-banner-webhook-1".into(),
                job_id: "banner-webhook-1".into(),
                name: "webhook-1".into(),
            }
        );
        assert_eq!(notification.printed().job_id, "banner-webhook-1");
    }

    #[test]
    fn a_webhook_signed_with_the_secret_passes_the_signature_check() {
        // The fake printer's own vector (WebhookSignatureTest): HMAC-SHA256 keyed by the secret.
        let body = br#"{"deliveryId":"printed-g1"}"#;
        let signature = "sha256=9f69fed2d08d376e0365531041e58607a2b2120e6509f22664dbe31be53defc1";
        // Signed correctly, but no whole notification: past the 401, refused as a bad body.
        assert_eq!(
            check_webhook(SECRET, [("X-Kraken-Signature", signature)], body),
            Err(400)
        );
        assert_eq!(
            check_webhook("another-secret", [("X-Kraken-Signature", signature)], body),
            Err(401)
        );
    }

    #[test]
    fn a_webhook_with_a_wrong_secret_is_401_and_a_body_that_is_no_notification_400() {
        assert_eq!(
            check_webhook(SECRET, [("X-Kraken-Webhook-Secret", "nope")], BODY),
            Err(401)
        );
        assert_eq!(
            check_webhook(SECRET, Vec::<(&str, &str)>::new(), BODY),
            Err(401)
        );
        assert_eq!(
            check_webhook(SECRET, [("X-Kraken-Webhook-Secret", SECRET)], b"{}"),
            Err(400)
        );
    }

    #[test]
    fn a_topic_record_becomes_banner_printed_deduplicated_by_the_delivery_id() {
        let event = translate_payload(BODY).unwrap().unwrap();
        assert_eq!(event.type_name(), "examples.BannerPrinted");
        assert_eq!(event.dedupe_key(), Some("printed-banner-webhook-1"));
        assert!(matches!(
            translate_payload(b"not json"),
            Err(MessagingError::InvalidMessage(_))
        ));
    }

    #[test]
    fn printer_statuses_are_classified_as_an_adapter_must() {
        assert!(classify(200, "x").is_ok());
        assert!(matches!(
            classify(503, "x"),
            Err(MessagingError::RetryableExternal(_))
        ));
        assert!(matches!(
            classify(429, "x"),
            Err(MessagingError::RetryableExternal(_))
        ));
        assert!(matches!(
            classify(400, "x"),
            Err(MessagingError::RemoteHandler { .. })
        ));
    }

    #[test]
    fn the_job_request_names_where_the_printer_should_answer() {
        let printer = Printer::new(
            "http://printer:8080/",
            ReplyTo {
                webhook: "http://service-c:8080/printer/webhook".into(),
                topic: "printer.banner.events".into(),
            },
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(printer.job("banner-poll-1", "poll-1")).unwrap(),
            serde_json::json!({
                "jobId": "banner-poll-1",
                "name": "poll-1",
                "replyWebhook": "http://service-c:8080/printer/webhook",
                "replyTopic": "printer.banner.events",
            })
        );
    }

    #[tokio::test]
    async fn a_printer_that_cannot_be_reached_is_retryable() {
        // Port 1 on loopback: nothing listens, the connection is refused before anything is sent.
        let printer = Printer::new(
            "http://127.0.0.1:1",
            ReplyTo {
                webhook: String::new(),
                topic: String::new(),
            },
        )
        .unwrap();
        let job = printer.job("banner-x", "x");
        assert!(matches!(
            printer.submit(&job, "k1").await,
            Err(MessagingError::RetryableExternal(_))
        ));
        assert!(matches!(
            printer.status("banner-x").await,
            Err(MessagingError::RetryableExternal(_))
        ));
    }
}
