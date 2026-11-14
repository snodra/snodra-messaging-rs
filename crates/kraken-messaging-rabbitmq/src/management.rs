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
    /// Its name (the subscription's); empty for a consumer without one.
    pub name: String,
    /// The stream it reads.
    pub stream: String,
    /// What tells the members of one subscription apart: the `kraken-member` property, else its
    /// connection.
    pub member: String,
}

#[derive(Clone)]
pub(crate) struct Management {
    http: reqwest::Client,
    config: config::Management,
    virtual_host: String,
}

impl Management {
    pub fn new(
        config: &config::Management,
        virtual_host: &str,
    ) -> Result<Management, EventStoreError> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| EventStoreError::Invalid(format!("the management API client: {e}")))?;
        Ok(Management {
            http,
            config: config.clone(),
            virtual_host: virtual_host.to_string(),
        })
    }

    /// Every stream consumer of the virtual host.
    pub async fn consumers(&self) -> Result<Vec<Consumer>, EventStoreError> {
        let url = format!(
            "{}/api/stream/consumers/{}",
            self.config.url.trim_end_matches('/'),
            encode(&self.virtual_host)
        );
        let unavailable = |e: String| {
            EventStoreError::Failed(format!("the RabbitMQ management API ({url}): {e}"))
        };
        let response = self
            .http
            .get(&url)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .send()
            .await
            .map_err(|e| unavailable(e.to_string()))?;
        if !response.status().is_success() {
            return Err(unavailable(format!("HTTP {}", response.status())));
        }
        let body: Value = response
            .json()
            .await
            .map_err(|e| unavailable(e.to_string()))?;
        Ok(parse_consumers(&body))
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
            .filter(|c| c.name == name && streams.contains(&c.stream))
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
            if !c.name.is_empty() && streams.contains(&c.stream) {
                all.entry(c.name).or_default().insert(c.member);
            }
        }
        Ok(all)
    }
}

/// The consumers of the API's answer (an array of consumers; a paged answer's `items`).
fn parse_consumers(body: &Value) -> Vec<Consumer> {
    let items = body
        .as_array()
        .or_else(|| body.get("items").and_then(Value::as_array));
    let text = |v: Option<&Value>| v.and_then(Value::as_str).unwrap_or_default().to_string();
    items
        .map(|items| {
            items
                .iter()
                .map(|c| {
                    let properties = c.get("properties");
                    let property = |name: &str| text(properties.and_then(|p| p.get(name)));
                    let connection = c.get("connection_details");
                    let member = Some(property(MEMBER_PROPERTY))
                        .filter(|m| !m.is_empty())
                        .unwrap_or_else(|| {
                            format!(
                                "{}:{}",
                                text(connection.and_then(|d| d.get("name"))),
                                c.get("subscription_id").cloned().unwrap_or(Value::Null)
                            )
                        });
                    Consumer {
                        name: property("name"),
                        stream: text(c.get("queue").and_then(|q| q.get("name"))),
                        member,
                    }
                })
                .collect()
        })
        .unwrap_or_default()
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
        let consumers = parse_consumers(&body);
        assert_eq!(consumers.len(), 3);
        assert_eq!(
            consumers[0],
            Consumer {
                name: "svc.p".into(),
                stream: "kraken.events-0".into(),
                member: "m-1".into()
            }
        );
        assert_eq!(consumers[1].member, "c-2:3");
        assert_eq!(consumers[2].name, "");
        assert_eq!(
            parse_consumers(&serde_json::json!({"items": body})).len(),
            3
        );
    }
}
