//! service-c's small HTTP side (the banner saga's edge): the printer's webhook and the banner
//! outcomes the sandbox reads.
//!
//! - `POST /printer/webhook`: verify the raw body against the shared secret, then hand the event
//!   to the library's ingress, which applies it once however often the printer sends it
//!   (`deliveryId` is the printer's id of the delivery). 200 for a new and for a repeated
//!   delivery, 401 for a bad signature, 400 for a body that is no notification, 500 for any other
//!   failure (the printer retries).
//! - `GET /banners/{jobId}`: how the banner ended (`PRINTED`, or `FAILED` with its reason), 404
//!   while it is in flight (or unknown).
//! - `GET /greetings/{name}/count`: service-b's `examples.CountGreetings` through the hub
//!   (`{"name","count"}`; 502 with the error when it fails). The
//!   examples smoke uses it for service-c's side of `snodra_hub_client_request_duration_seconds`.

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use snodra_messaging::SendOptions;
use snodra_messaging::external::ExternalEventIngress;
use snodra_messaging::hub::Sender;
use std::sync::Arc;

use crate::banner::BannerOutcomes;
use crate::messages::CountGreetings;
use crate::printer::{self, SOURCE};

#[derive(Clone)]
pub struct Http {
    pub ingress: ExternalEventIngress,
    pub secret: String,
    pub outcomes: BannerOutcomes,
    pub sender: Arc<Sender>,
}

pub fn router(http: Http) -> Router {
    Router::new()
        .route("/printer/webhook", post(webhook))
        .route("/banners/{job_id}", get(banner))
        .route("/greetings/{name}/count", get(count_greetings))
        .with_state(http)
}

async fn count_greetings(State(http): State<Http>, Path(name): Path<String>) -> Response {
    match http
        .sender
        .send_query(&CountGreetings { name }, SendOptions::default())
        .await
    {
        Ok(count) => (
            [(header::CONTENT_TYPE, "application/json")],
            serde_json::to_vec(&count).unwrap_or_default(),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            format!("CountGreetings failed: {e}"),
        )
            .into_response(),
    }
}

async fn webhook(State(http): State<Http>, headers: HeaderMap, body: Bytes) -> StatusCode {
    let notification = match printer::check_webhook(&http.secret, &headers, &body) {
        Ok(notification) => notification,
        Err(code) => return StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_REQUEST),
    };
    match http
        .ingress
        .accept(
            SOURCE,
            &notification.delivery_id,
            &notification.printed(),
            &printer::no_headers(),
        )
        .await
    {
        Ok(result) => {
            tracing::info!(
                "C: printer webhook {} for {}: {result:?}",
                notification.delivery_id,
                notification.job_id
            );
            StatusCode::OK
        }
        Err(e) => {
            tracing::warn!(
                "C: printer webhook {} failed, the printer will retry: {e}",
                notification.delivery_id
            );
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn banner(State(http): State<Http>, Path(job_id): Path<String>) -> Response {
    match http.outcomes.of(&job_id) {
        Some(outcome) => (
            [(header::CONTENT_TYPE, "application/json")],
            serde_json::to_vec(&outcome).unwrap_or_default(),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            format!("no outcome for banner {job_id} yet"),
        )
            .into_response(),
    }
}
