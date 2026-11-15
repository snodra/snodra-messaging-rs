//! The management HTTP API (plugin `rabbitmq_stream_management`): the stream consumers
//! connected now, which the stream protocol cannot list. It tells who reads a subscription (the
//! reset's refusal, the lag view's members) and which subscriptions read a source.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kraken_messaging::event_store::EventStoreError;
use serde_json::Value;

use crate::config;
use crate::subscription::MEMBER_PROPERTY;

/// The longest one request to the management API may take.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// One stream consumer the management API lists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Consumer {
    /// Its name (the subscription's); `None` for a consumer without one.
    pub name: Option<String>,
    /// The stream it reads.
    pub stream: String,
    /// What tells the members of one subscription apart: the `kraken-member` property, else its
    /// connection.
    pub member: String,
}

/// The largest answer read from the management API.
pub const MAX_ANSWER: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct Management {
    http: reqwest::Client,
    /// The API's root, without a trailing `/`.
    url: String,
    username: String,
    password: String,
    virtual_host: String,
}

impl Management {
    /// The API of `config`'s management settings (`None` without them).
    pub fn of(config: &config::RabbitConfig) -> Result<Option<Management>, EventStoreError> {
        let Some(m) = &config.management else {
            return Ok(None);
        };
        let invalid = |e: String| EventStoreError::Invalid(format!("the management API: {e}"));
        let mut http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none());
        if let Some(ca) = &m.ca_file {
            let pem = std::fs::read(ca)
                .map_err(|e| invalid(format!("cannot read the CA file {}: {e}", ca.display())))?;
            let certificate = reqwest::Certificate::from_pem(&pem)
                .map_err(|e| invalid(format!("the CA file {}: {e}", ca.display())))?;
            http = http.add_root_certificate(certificate);
        }
        let (username, password) = config.management_login();
        Ok(Some(Management {
            http: http.build().map_err(|e| invalid(e.to_string()))?,
            url: m.url.trim_end_matches('/').to_string(),
            username,
            password,
            virtual_host: config.virtual_host.clone(),
        }))
    }

    /// The JSON answer of `GET <url>/api/<path>`, at most [`MAX_ANSWER`] bytes, never following
    /// a redirect.
    async fn get(&self, path: &str) -> Result<Value, EventStoreError> {
        let shown = format!("{}/api/{path}", config::redacted(&self.url));
        let unavailable = |e: String| {
            EventStoreError::Failed(format!("the RabbitMQ management API ({shown}): {e}"))
        };
        let mut response = self
            .http
            .get(format!("{}/api/{path}", self.url))
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|e| unavailable(e.without_url().to_string()))?;
        if !response.status().is_success() {
            return Err(unavailable(format!("HTTP {}", response.status())));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| unavailable(e.without_url().to_string()))?
        {
            if body.len() + chunk.len() > MAX_ANSWER {
                return Err(unavailable(format!("an answer over {MAX_ANSWER} bytes")));
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&body).map_err(|e| unavailable(format!("not JSON: {e}")))
    }

    /// Whether the broker collects the metrics the consumer list comes from. Observed on
    /// RabbitMQ 4.4: with `management.disable_stats = true` or
    /// `management_agent.disable_metrics_collector = true`, the overview reports
    /// `disable_stats: true` and the consumer list answers HTTP 400 ("Stats in management UI are
    /// disabled on this node"); both fail here, never "nobody reads".
    pub async fn stats_enabled(&self) -> Result<bool, EventStoreError> {
        let overview = self.get("overview").await?;
        match overview.get("disable_stats") {
            Some(Value::Bool(disabled)) => Ok(!disabled),
            None => Ok(true),
            Some(other) => Err(EventStoreError::Failed(format!(
                "the RabbitMQ management API's overview: disable_stats is {other}"
            ))),
        }
    }

    /// Every stream consumer of the virtual host. Fails (never "nobody") on an answer of
    /// another shape, a consumer without its stream or properties, or disabled metrics.
    pub async fn consumers(&self) -> Result<Vec<Consumer>, EventStoreError> {
        if !self.stats_enabled().await? {
            return Err(EventStoreError::Failed(
                "the RabbitMQ management API collects no metrics (management.disable_stats or \
                 management_agent.disable_metrics_collector): it cannot tell who reads a \
                 subscription"
                    .into(),
            ));
        }
        let body = self
            .get(&format!("stream/consumers/{}", encode(&self.virtual_host)))
            .await?;
        parse_consumers(&body)
    }

    /// The members reading the subscription `name` on any of `streams`, sorted.
    pub async fn members(
        &self,
        name: &str,
        streams: &[String],
    ) -> Result<Vec<String>, EventStoreError> {
        let members: BTreeSet<String> = self
            .consumers()
            .await?
            .into_iter()
            .filter(|c| c.name.as_deref() == Some(name) && streams.contains(&c.stream))
            .map(|c| c.member)
            .collect();
        Ok(members.into_iter().collect())
    }

    /// Per subscription name reading any of `streams`: its members.
    pub async fn subscriptions(
        &self,
        streams: &[String],
    ) -> Result<BTreeMap<String, BTreeSet<String>>, EventStoreError> {
        let mut all: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for c in self.consumers().await? {
            if let Some(name) = c.name.filter(|_| streams.contains(&c.stream)) {
                all.entry(name).or_default().insert(c.member);
            }
        }
        Ok(all)
    }
}

/// The consumers of the API's answer: an array of consumers (or a paged answer's `items`), each
/// with `queue.name` and a `properties` object; a consumer without `properties.name` is an
/// unnamed reader (the event browser's), never a subscription's member. Anything else fails.
fn parse_consumers(body: &Value) -> Result<Vec<Consumer>, EventStoreError> {
    let shape = |what: String| {
        EventStoreError::Failed(format!(
            "the RabbitMQ management API's consumer list has an unexpected shape: {what}"
        ))
    };
    let items = body
        .as_array()
        .or_else(|| body.get("items").and_then(Value::as_array))
        .ok_or_else(|| shape("not an array of consumers".into()))?;
    items
        .iter()
        .map(|c| {
            let stream = c
                .get("queue")
                .and_then(|q| q.get("name"))
                .and_then(Value::as_str)
                .ok_or_else(|| shape("a consumer without queue.name".into()))?;
            let properties = c
                .get("properties")
                .and_then(Value::as_object)
                .ok_or_else(|| shape("a consumer without properties".into()))?;
            let text = |name: &str| -> Result<Option<String>, EventStoreError> {
                match properties.get(name) {
                    None => Ok(None),
                    Some(Value::String(v)) => Ok(Some(v.clone())),
                    Some(other) => Err(shape(format!("properties.{name} is {other}"))),
                }
            };
            let name = text("name")?;
            let member = match text(MEMBER_PROPERTY)? {
                Some(member) => member,
                None => format!(
                    "{}:{}",
                    c.get("connection_details")
                        .and_then(|d| d.get("name"))
                        .and_then(Value::as_str)
                        .unwrap_or("?"),
                    c.get("subscription_id").cloned().unwrap_or(Value::Null)
                ),
            };
            Ok(Consumer {
                name,
                stream: stream.to_string(),
                member,
            })
        })
        .collect()
}

/// `s` percent-encoded as one path segment.
fn encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_virtual_host_is_one_path_segment() {
        assert_eq!(encode("/"), "%2F");
        assert_eq!(encode("orders eu"), "orders%20eu");
        assert_eq!(encode("a-b_c.d~"), "a-b_c.d~");
    }

    #[test]
    fn consumers_are_read_from_the_api_answer() {
        let body = serde_json::json!([
            {"properties": {"name": "svc.p", "single-active-consumer": "true",
                            "kraken-member": "m-1", "super-stream": "kraken.events"},
             "queue": {"name": "kraken.events-0", "vhost": "/"},
             "connection_details": {"name": "10.0.0.1:5000 -> 10.0.0.2:5552"},
             "subscription_id": 0, "active": true},
            {"properties": {"name": "other"},
             "queue": {"name": "kraken.events-1", "vhost": "/"},
             "connection_details": {"name": "c-2"}, "subscription_id": 3},
            {"properties": {}, "queue": {"name": "x"}}
        ]);
        let consumers = parse_consumers(&body).unwrap();
        assert_eq!(consumers.len(), 3);
        assert_eq!(
            consumers[0],
            Consumer {
                name: Some("svc.p".into()),
                stream: "kraken.events-0".into(),
                member: "m-1".into()
            }
        );
        assert_eq!(consumers[1].member, "c-2:3");
        assert_eq!(consumers[2].name, None);
        assert_eq!(
            parse_consumers(&serde_json::json!({"items": body}))
                .unwrap()
                .len(),
            3
        );
    }

    /// Never "nobody reads" on an answer it cannot read.
    #[test]
    fn an_unexpected_answer_is_an_error() {
        for body in [
            serde_json::json!({}),
            serde_json::json!("consumers"),
            serde_json::json!([{"properties": {"name": "svc.p"}}]),
            serde_json::json!([{"queue": {"name": "s-0"}}]),
            serde_json::json!([{"queue": {"name": "s-0"}, "properties": {"name": 7}}]),
        ] {
            assert!(
                matches!(parse_consumers(&body), Err(EventStoreError::Failed(_))),
                "{body}"
            );
        }
    }

    mod fake {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        use super::super::*;

        async fn api(overview: serde_json::Value, consumers: ResponseTemplate) -> MockServer {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/api/overview"))
                .respond_with(ResponseTemplate::new(200).set_body_json(overview))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/api/stream/consumers/%2F"))
                .respond_with(consumers)
                .mount(&server)
                .await;
            server
        }

        fn management(server: &MockServer) -> Management {
            let config = config::RabbitConfig::new("r").management(&server.uri());
            Management::of(&config).unwrap().unwrap()
        }

        fn stats_on() -> serde_json::Value {
            serde_json::json!({"disable_stats": false, "rates_mode": "basic"})
        }

        #[tokio::test]
        async fn members_are_read_from_a_well_formed_answer() {
            let server = api(
                stats_on(),
                ResponseTemplate::new(200).set_body_json(serde_json::json!([
                    {"properties": {"name": "svc.p", "kraken-member": "m-1"},
                     "queue": {"name": "s-0"}},
                    {"properties": {"name": "svc.p", "kraken-member": "m-1"},
                     "queue": {"name": "s-1"}},
                    {"properties": {"name": "svc.p", "kraken-member": "m-2"},
                     "queue": {"name": "elsewhere"}},
                    {"properties": {}, "queue": {"name": "s-0"}}
                ])),
            )
            .await;
            let streams = ["s-0".to_string(), "s-1".to_string()];
            assert_eq!(
                management(&server)
                    .members("svc.p", &streams)
                    .await
                    .unwrap(),
                ["m-1"]
            );
        }

        /// An answer it cannot read, or metrics switched off, is an error: never "nobody
        /// reads" (a reset would then run under a live instance).
        #[tokio::test]
        async fn an_answer_it_cannot_trust_fails() {
            let streams = ["s-0".to_string()];
            for (overview, consumers) in [
                (
                    stats_on(),
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({})),
                ),
                (
                    stats_on(),
                    ResponseTemplate::new(200).set_body_string("<html>"),
                ),
                (
                    stats_on(),
                    ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!([{"queue": {"name": "s-0"}}])),
                ),
                (
                    stats_on(),
                    ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!([{"properties": {"name": "svc.p"}}])),
                ),
                (stats_on(), ResponseTemplate::new(500)),
                (
                    stats_on(),
                    ResponseTemplate::new(302).insert_header("location", "http://elsewhere/"),
                ),
                (
                    serde_json::json!({"disable_stats": true}),
                    ResponseTemplate::new(200).set_body_json(serde_json::json!([])),
                ),
            ] {
                let server = api(overview, consumers).await;
                let m = management(&server);
                assert!(
                    matches!(
                        m.members("svc.p", &streams).await,
                        Err(EventStoreError::Failed(_))
                    ),
                    "an untrusted answer gave members"
                );
            }
        }

        #[tokio::test]
        async fn an_answer_over_the_bound_is_refused() {
            let big = vec![b' '; MAX_ANSWER + 1];
            let server = api(stats_on(), ResponseTemplate::new(200).set_body_bytes(big)).await;
            let refused = management(&server).consumers().await;
            assert!(
                matches!(&refused, Err(EventStoreError::Failed(m)) if m.contains("bytes")),
                "{refused:?}"
            );
        }

        #[tokio::test]
        async fn the_store_opened_on_an_api_without_metrics_declares_no_replay() {
            let server = api(
                serde_json::json!({"disable_stats": true}),
                ResponseTemplate::new(200).set_body_json(serde_json::json!([])),
            )
            .await;
            use kraken_messaging::event_store::{Capability, EventStore};
            let config = config::RabbitConfig::new("127.0.0.1:1").management(&server.uri());
            let store = crate::RabbitEventStore::open(config.clone()).await.unwrap();
            assert!(!store.supports(Capability::Replay));
            let on = api(stats_on(), ResponseTemplate::new(200)).await;
            let store = crate::RabbitEventStore::open(
                config::RabbitConfig::new("127.0.0.1:1").management(&on.uri()),
            )
            .await
            .unwrap();
            assert!(store.supports(Capability::Replay));
            // An API that does not answer: the store does not open.
            let gone = config::RabbitConfig::new("127.0.0.1:1").management("http://127.0.0.1:1");
            assert!(matches!(
                crate::RabbitEventStore::open(gone).await.err(),
                Some(EventStoreError::Failed(_))
            ));
        }
    }
}
