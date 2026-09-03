// SPDX-License-Identifier: Apache-2.0

//! HTTP handlers. Every response is JSON except the artifact downloads; errors
//! are `{"error": "..."}` with a status code.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, FromRequestParts, Path, State};
use axum::http::header::{CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE};
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
use crate::jobs::{JobState, is_safe_id};
use crate::plan::{
    MAX_TOML_BYTES, PlanFailure, materialize_scenario_blocking, run_plan_blocking, toml_sha256,
};
use crate::shards::list_model_shards;

const LOG_TAIL_CHARS: usize = 4000;

/// JSON error response.
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
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
        (self.status, axum::Json(json!({ "error": self.message }))).into_response()
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
        .route("/jobs/{id}/scenario.json", get(scenario))
        .route("/jobs/{id}/plan.svg", get(plan_svg))
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
        .map_err(|error| ApiError::bad_request(format!("{error:#}")))?
        .ok_or_else(|| ApiError::not_found("session not found"))?;
    let key = resolved.key(&request.model);

    if let Some(job_id) = state.store.in_flight(&key) {
        info!(job = %job_id, "joined in-flight bundle job");
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
                    return Ok(axum::Json(json!({ "job_id": cached_id })).into_response());
                }
                info!(job = %cached_id, "bundle cache stale; shard listing changed");
                prelisted = Some(current);
            }
            Ok(Err(error)) => return Err(ApiError::internal(error)),
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

async fn plan(State(state): State<Arc<AppState>>, body: Bytes) -> Result<Response, ApiError> {
    let PlanBody { toml } = parse_json(&body)?;
    if toml.len() > MAX_TOML_BYTES {
        return Err(ApiError::bad_request("config too large"));
    }
    let sha = toml_sha256(&toml);
    if let Some(job_id) = state.store.cached_plan(&sha) {
        let job = state.store.job_dir(&job_id)?;
        let cached = tokio::fs::read(job.path.join("plan.json")).await;
        match cached {
            Ok(bytes) => {
                info!(job = %job_id, "plan cache hit");
                return Ok(([(CONTENT_TYPE, "application/json")], bytes).into_response());
            }
            Err(error) => warn!(job = %job_id, %error, "cached plan.json unreadable; replanning"),
        }
    }

    let job = state.store.create("p")?;
    let mut job_state = JobState::new("plan", "running");
    job_state.toml_sha256 = Some(sha.clone());
    job.write_state(&mut job_state)?;
    let worker_job = job.clone();
    let result = tokio::task::spawn_blocking(move || run_plan_blocking(&worker_job, &toml))
        .await
        .map_err(|error| ApiError::internal(anyhow::anyhow!("plan task panicked: {error}")))?;
    let response = match result {
        Ok(response) => response,
        Err(PlanFailure::Invalid(message)) => {
            job_state.state = "error".to_string();
            job_state.error = Some(message.clone());
            job.write_state(&mut job_state)?;
            state.store.prune();
            return Ok((
                StatusCode::UNPROCESSABLE_ENTITY,
                axum::Json(json!({ "job_id": job.id, "error": message })),
            )
                .into_response());
        }
        Err(PlanFailure::Internal(error)) => {
            job_state.state = "error".to_string();
            job_state.error = Some(format!("{error:#}"));
            job.write_state(&mut job_state)?;
            return Err(ApiError::internal(error));
        }
    };
    let body = serde_json::to_vec(&response).map_err(|error| ApiError::internal(error.into()))?;
    tokio::fs::write(job.path.join("plan.json"), &body)
        .await
        .map_err(|error| ApiError::internal(error.into()))?;
    job_state.state = "done".to_string();
    job_state.elapsed_ms = Some(response.elapsed_ms);
    job.write_state(&mut job_state)?;
    state.store.record_plan(&sha, &job.id);
    info!(
        job = %job.id,
        nodes = response.summary.nodes,
        sessions = response.summary.sessions,
        elapsed_ms = response.elapsed_ms,
        has_svg = response.has_svg,
        "planned scenario"
    );
    state.store.prune();
    Ok(([(CONTENT_TYPE, "application/json")], body).into_response())
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
        None,
    )
    .await
}

async fn scenario(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let job = state
        .store
        .job_dir(&id)
        .map_err(|_| ApiError::bad_request("bad job id"))?;
    let gz_path = job.path.join("scenario.json.gz");
    if gz_path.is_file() {
        return stream_file(&gz_path, "application/json", None, Some("gzip")).await;
    }
    let plain_path = job.path.join("scenario.json");
    if plain_path.is_file() {
        return stream_file(&plain_path, "application/json", None, None).await;
    }
    if !job.path.join("config.toml").is_file()
        || !job
            .read_state()?
            .is_some_and(|job_state| job_state.kind == "plan" && job_state.state == "done")
    {
        return Err(ApiError::not_found("no scenario"));
    }
    let worker_job = job.clone();
    let path = tokio::task::spawn_blocking(move || materialize_scenario_blocking(&worker_job))
        .await
        .map_err(|error| {
            ApiError::internal(anyhow::anyhow!("scenario task panicked: {error}"))
        })??;
    stream_file(&path, "application/json", None, None).await
}

async fn plan_svg(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let job = state
        .store
        .job_dir(&id)
        .map_err(|_| ApiError::bad_request("bad job id"))?;
    let path = job.path.join("plan.svg");
    if !path.is_file() {
        return Err(ApiError::not_found("no plan.svg (graphviz missing?)"));
    }
    stream_file(&path, "image/svg+xml", None, None).await
}

async fn stream_file(
    path: &std::path::Path,
    content_type: &'static str,
    disposition: Option<String>,
    content_encoding: Option<&'static str>,
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
    if let Some(encoding) = content_encoding {
        headers.insert(CONTENT_ENCODING, encoding.parse().expect("static header"));
    }
    Ok((headers, Body::from_stream(ReaderStream::new(file))).into_response())
}
