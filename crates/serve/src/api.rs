// SPDX-License-Identifier: Apache-2.0

//! HTTP handlers. Every response is JSON except the artifact downloads; errors
//! are `{"error": "..."}` with a status code.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, FromRequestParts, Path, State};
use axum::http::header::{CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::Deserialize;
use serde_json::json;
use tokio_util::io::ReaderStream;
use tracing::{info, warn};

use crate::AppState;
use crate::bundle::{BundleJob, BundleRequest, resolve_bundle, run_bundle_job};
use crate::hours::TooManyHours;
use crate::jobs::{JobState, is_safe_id};
use crate::metrics::{Rejection, metrics_handler, track_http};
use crate::plan::{MAX_TOML_BYTES, run_plan, toml_sha256};
use crate::shards::list_model_shards;

const LOG_TAIL_CHARS: usize = 4000;

/// JSON error response.
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
    /// `loadgen_serve_rejected_total{reason}` label, when the error is a refusal.
    pub reason: Option<&'static str>,
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        let reason = match status {
            StatusCode::BAD_REQUEST => Some("bad_request"),
            StatusCode::NOT_FOUND => Some("not_found"),
            StatusCode::TOO_MANY_REQUESTS => Some("per_ip_cap"),
            _ => None,
        };
        Self {
            status,
            message: message.into(),
            reason,
        }
    }

    fn with_reason(mut self, reason: &'static str) -> Self {
        self.reason = Some(reason);
        self
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }

    fn internal(error: anyhow::Error) -> Self {
        warn!("internal error: {error:#}");
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}"))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response =
            (self.status, axum::Json(json!({ "error": self.message }))).into_response();
        if let Some(reason) = self.reason {
            response.extensions_mut().insert(Rejection(reason));
        }
        response
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self::internal(error)
    }
}

/// Client identity for the per-client job cap: first `X-Forwarded-For` hop
/// (set by the reverse proxy) or the peer address.
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
        .route("/replay-bundle", post(replay_bundle))
        .route("/plan", post(plan))
        .route("/jobs/{id}", get(job))
        .route("/jobs/{id}/bundle", get(bundle))
        .route("/metrics", get(metrics_handler))
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            track_http,
        ))
        .with_state(state)
}

fn parse_json<T: for<'de> Deserialize<'de>>(body: &Bytes) -> Result<T, ApiError> {
    serde_json::from_slice(body)
        .map_err(|error| ApiError::bad_request(format!("invalid JSON body: {error}")))
}

async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    axum::Json(json!({
        "ok": true,
        "loadgen": true,
        "s5cmd": false,
        "creds": state.creds_loaded,
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

async fn replay_bundle(
    State(state): State<Arc<AppState>>,
    ClientAddr(client): ClientAddr,
    body: Bytes,
) -> Result<Response, ApiError> {
    let request: BundleRequest = parse_json(&body)?;
    if !is_safe_id(&request.model) || !is_safe_id(&request.session_id) {
        return Err(ApiError::bad_request("bad model/session id"));
    }
    let web_dir = state.web_dir.clone();
    let resolve_request = request.clone();
    let resolved = tokio::task::spawn_blocking(move || resolve_bundle(&web_dir, &resolve_request))
        .await
        .map_err(|error| ApiError::internal(anyhow::anyhow!("resolve task panicked: {error}")))?
        .map_err(|error| {
            let rejection = ApiError::bad_request(format!("{error:#}"));
            if error.is::<TooManyHours>() {
                rejection.with_reason("too_many_hours")
            } else {
                rejection
            }
        })?
        .ok_or_else(|| ApiError::not_found("session not found"))?;
    state.metrics.job_submitted(&client, "replay");
    let key = resolved.key(&request.model);

    if let Some(job_id) = state.store.in_flight(&key) {
        info!(job = %job_id, "joined in-flight bundle job");
        state.metrics.job_finished("replay", "cached");
        return Ok(axum::Json(json!({ "job_id": job_id })).into_response());
    }

    let mut prelisted = None;
    if let Some((cached_id, recorded)) = state.store.cached_bundle(&key) {
        let listing = tokio::time::timeout(
            state.cache_list_timeout,
            list_model_shards(state.shards.as_ref(), &request.model, &resolved.hours),
        )
        .await;
        match listing {
            Ok(Ok(current)) => {
                if current == recorded {
                    info!(job = %cached_id, "bundle cache hit");
                    state.metrics.job_finished("replay", "cached");
                    return Ok(axum::Json(json!({ "job_id": cached_id })).into_response());
                }
                info!(job = %cached_id, "bundle cache stale; shard listing changed");
                prelisted = Some(current);
            }
            Ok(Err(error)) => {
                state.metrics.s3_errors.inc();
                return Err(ApiError::internal(error));
            }
            Err(_) => warn!("shard listing for cache check timed out; scheduling a fresh job"),
        }
    }

    let job = state.store.create("r")?;
    if let Err(count) = state
        .store
        .start(&job.id, key, &client, state.per_client_cap)
    {
        let _ = std::fs::remove_dir_all(&job.path);
        return Err(ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "{count} bundle job(s) already queued or running for this client (limit {})",
                state.per_client_cap
            ),
        ));
    }
    let mut job_state = JobState::new("replay", "queued");
    job_state.model = Some(request.model.clone());
    job_state.session_id = Some(request.session_id.clone());
    job.write_state(&mut job_state)?;
    let job_id = job.id.clone();
    tokio::spawn(run_bundle_job(
        Arc::clone(&state),
        BundleJob {
            job,
            request,
            resolved,
            prelisted,
        },
    ));
    Ok(axum::Json(json!({ "job_id": job_id })).into_response())
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
    let PlanBody { toml } = parse_json(&body)?;
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

async fn job(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let job = state
        .store
        .job_dir(&id)
        .map_err(|_| ApiError::bad_request("bad job id"))?;
    let job_state = job
        .read_state()?
        .ok_or_else(|| ApiError::not_found("no such job"))?;
    let mut value =
        serde_json::to_value(&job_state).map_err(|error| ApiError::internal(error.into()))?;
    if let Some(object) = value.as_object_mut() {
        if job_state.state == "queued" {
            if let Some(position) = state.store.queue_position(&id) {
                object.insert("queue_position".to_string(), json!(position));
            }
        }
        object.insert("log".to_string(), json!(job.log_tail(LOG_TAIL_CHARS)));
    }
    Ok(axum::Json(value).into_response())
}

async fn bundle(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let job = state
        .store
        .job_dir(&id)
        .map_err(|_| ApiError::bad_request("bad job id"))?;
    let path = job.path.join("bundle.jsonl.gz");
    if !path.is_file() {
        return Err(ApiError::not_found("bundle not ready"));
    }
    let filename = job
        .read_state()?
        .and_then(|job_state| job_state.filename)
        .unwrap_or_else(|| "bundle.jsonl.gz".to_string());
    stream_file(
        &path,
        "application/gzip",
        Some(format!("attachment; filename=\"{filename}\"")),
    )
    .await
}

async fn stream_file(
    path: &std::path::Path,
    content_type: &'static str,
    disposition: Option<String>,
) -> Result<Response, ApiError> {
    let file = tokio::fs::File::open(path).await.map_err(|error| {
        ApiError::internal(anyhow::anyhow!(
            "failed to open {}: {error}",
            path.display()
        ))
    })?;
    let length = file
        .metadata()
        .await
        .map_err(|error| ApiError::internal(error.into()))?
        .len();
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, content_type.parse().expect("static header"));
    headers.insert(
        CONTENT_LENGTH,
        length.to_string().parse().expect("numeric header"),
    );
    if let Some(disposition) = disposition {
        headers.insert(
            CONTENT_DISPOSITION,
            disposition.parse().map_err(|_| {
                ApiError::internal(anyhow::anyhow!("filename is not a valid header value"))
            })?,
        );
    }
    Ok((headers, Body::from_stream(ReaderStream::new(file))).into_response())
}
