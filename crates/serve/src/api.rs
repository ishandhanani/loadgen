// SPDX-License-Identifier: Apache-2.0

//! HTTP handlers. Every response is JSON; errors are `{"error": "..."}` with a
//! status code.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, FromRequestParts, State};
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::Deserialize;
use serde_json::json;
use tracing::{info, warn};

use crate::AppState;
use crate::metrics::{Rejection, metrics_handler, track_http};
use crate::plan::{MAX_TOML_BYTES, run_plan, toml_sha256};

/// JSON error response.
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn internal(error: anyhow::Error) -> Self {
        warn!("internal error: {error:#}");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("{error:#}"),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response =
            (self.status, axum::Json(json!({ "error": self.message }))).into_response();
        if self.status == StatusCode::BAD_REQUEST {
            response.extensions_mut().insert(Rejection("bad_request"));
        }
        response
    }
}

/// Client identity for usage metrics: first `X-Forwarded-For` hop (set by the
/// reverse proxy) or the peer address.
pub struct ClientAddr(pub String);

impl<S: Send + Sync> FromRequestParts<S> for ClientAddr {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        let forwarded = parts
            .headers
            .get("x-forwarded-for")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(',').next())
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let peer = parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|info| info.0.ip().to_string());
        Ok(Self(
            forwarded.or(peer).unwrap_or_else(|| "unknown".to_string()),
        ))
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/plan", post(plan))
        .route("/metrics", get(metrics_handler))
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            track_http,
        ))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    axum::Json(json!({
        "ok": true,
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

#[derive(Deserialize)]
struct PlanBody {
    toml: String,
}

async fn plan(
    State(state): State<Arc<AppState>>,
    ClientAddr(client): ClientAddr,
    body: Bytes,
) -> Result<Response, ApiError> {
    let PlanBody { toml } = serde_json::from_slice(&body)
        .map_err(|error| ApiError::bad_request(format!("invalid JSON body: {error}")))?;
    if toml.len() > MAX_TOML_BYTES {
        return Err(ApiError::bad_request("config too large"));
    }
    let metrics = &state.metrics;
    metrics.job_submitted(&client, "plan");
    let sha = toml_sha256(&toml);
    let inflight = metrics.jobs_inflight.with_label_values(&["plan"]);
    inflight.inc();
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || run_plan(&toml)).await;
    inflight.dec();
    metrics
        .plan_duration
        .observe(started.elapsed().as_secs_f64());
    let result = result
        .map_err(|error| ApiError::internal(anyhow::anyhow!("plan task panicked: {error}")))?;
    match result {
        Ok(response) => {
            metrics.job_finished("plan", "done");
            info!(
                toml_sha256 = %sha,
                trees = response.timeline.trees,
                elapsed_ms = response.elapsed_ms,
                "planned profile"
            );
            Ok(axum::Json(response).into_response())
        }
        Err(message) => {
            metrics.job_finished("plan", "error");
            Ok((
                StatusCode::UNPROCESSABLE_ENTITY,
                axum::Json(json!({ "error": message })),
            )
                .into_response())
        }
    }
}
