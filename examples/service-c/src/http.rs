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

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use kraken_messaging::external::ExternalEventIngress;

use crate::banner::BannerOutcomes;
use crate::printer::{self, SOURCE};

#[derive(Clone)]
pub struct Http {
    pub ingress: ExternalEventIngress,
    pub secret: String,
    pub outcomes: BannerOutcomes,
}

pub fn router(http: Http) -> Router {
    Router::new()
        .route("/printer/webhook", post(webhook))
        .route("/banners/{job_id}", get(banner))
        .with_state(http)
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
