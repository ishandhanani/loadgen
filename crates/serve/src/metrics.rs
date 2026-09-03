// SPDX-License-Identifier: Apache-2.0

//! Prometheus metrics for the service, exposed at `GET /metrics`.
//!
//! Metric names and labels are a contract with the Grafana dashboard; change
//! them only together with it.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{MatchedPath, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};

use crate::AppState;

/// Why a request was refused; attached to error responses as an extension so the
/// HTTP middleware can count it under `loadgen_serve_rejected_total{reason}`.
#[derive(Debug, Clone, Copy)]
pub struct Rejection(pub &'static str);

/// Route label for requests that matched no route.
const UNMATCHED_ROUTE: &str = "unmatched";
/// Distinct `client` label values kept for `loadgen_serve_client_jobs_total`;
/// further clients are attributed to [`OTHER_CLIENT`].
pub const MAX_CLIENT_LABELS: usize = 500;
const OTHER_CLIENT: &str = "other";
/// Windows reported by `loadgen_serve_active_clients{window}`.
const ACTIVE_WINDOWS: [(&str, Duration); 2] = [
    ("15m", Duration::from_secs(15 * 60)),
    ("24h", Duration::from_secs(24 * 60 * 60)),
];

/// Per-client usage state behind the scrape-time gauges.
#[derive(Default)]
struct Clients {
    /// Client -> last accepted job submission.
    last_seen: HashMap<String, Instant>,
    /// Clients that own a `client` label value.
    labelled: HashSet<String>,
}

pub struct Metrics {
    registry: Registry,
    clients: Mutex<Clients>,
    pub active_clients: IntGaugeVec,
    pub client_jobs: IntCounterVec,
    pub http_requests: IntCounterVec,
    pub http_request_duration: HistogramVec,
    pub jobs: IntCounterVec,
    pub shards_scanned: IntCounter,
    pub shard_bytes: IntCounter,
    pub lines_scanned: IntCounter,
    pub records_kept: IntCounter,
    pub zero_output_dropped: IntCounter,
    pub s3_errors: IntCounter,
    pub rejected: IntCounterVec,
    pub jobs_inflight: IntGaugeVec,
    pub jobs_queued: IntGauge,
    pub plan_duration: Histogram,
    pub bundle_job_duration: HistogramVec,
}

fn counter(registry: &Registry, name: &str, help: &str) -> IntCounter {
    let metric = IntCounter::new(name, help).expect("valid metric definition");
    registry
        .register(Box::new(metric.clone()))
        .expect("unique metric name");
    metric
}

fn counter_vec(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let metric =
        IntCounterVec::new(Opts::new(name, help), labels).expect("valid metric definition");
    registry
        .register(Box::new(metric.clone()))
        .expect("unique metric name");
    metric
}

fn histogram_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
    buckets: Vec<f64>,
) -> HistogramVec {
    let metric = HistogramVec::new(HistogramOpts::new(name, help).buckets(buckets), labels)
        .expect("valid metric definition");
    registry
        .register(Box::new(metric.clone()))
        .expect("unique metric name");
    metric
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let http_requests = counter_vec(
            &registry,
            "loadgen_serve_http_requests_total",
            "HTTP requests by matched route, method, and status code.",
            &["route", "method", "status"],
        );
        let http_request_duration = histogram_vec(
            &registry,
            "loadgen_serve_http_request_duration_seconds",
            "HTTP request latency by matched route.",
            &["route"],
            vec![
                0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        );
        let jobs = counter_vec(
            &registry,
            "loadgen_serve_jobs_total",
            "Jobs by kind (replay, plan) and result (done, error, cached).",
            &["kind", "result"],
        );
        let shards_scanned = counter(
            &registry,
            "loadgen_serve_shards_scanned_total",
            "Trace shards scanned by bundle jobs.",
        );
        let shard_bytes = counter(
            &registry,
            "loadgen_serve_shard_bytes_total",
            "Compressed shard bytes downloaded from the trace store.",
        );
        let lines_scanned = counter(
            &registry,
            "loadgen_serve_lines_scanned_total",
            "Trace lines read while scanning shards.",
        );
        let records_kept = counter(
            &registry,
            "loadgen_serve_records_kept_total",
            "Trace records written to bundles after merge and deduplication.",
        );
        let zero_output_dropped = counter(
            &registry,
            "loadgen_serve_zero_output_dropped_total",
            "request_end records dropped for zero output tokens.",
        );
        let s3_errors = counter(
            &registry,
            "loadgen_serve_s3_errors_total",
            "Failed shard listing or download calls.",
        );
        let rejected = counter_vec(
            &registry,
            "loadgen_serve_rejected_total",
            "Requests refused by reason (per_ip_cap, too_many_hours, bad_request, not_found).",
            &["reason"],
        );
        let jobs_inflight = IntGaugeVec::new(
            Opts::new(
                "loadgen_serve_jobs_inflight",
                "Jobs currently running, by kind.",
            ),
            &["kind"],
        )
        .expect("valid metric definition");
        registry
            .register(Box::new(jobs_inflight.clone()))
            .expect("unique metric name");
        let jobs_queued = IntGauge::new(
            "loadgen_serve_jobs_queued",
            "Bundle jobs waiting for a scan permit.",
        )
        .expect("valid metric definition");
        registry
            .register(Box::new(jobs_queued.clone()))
            .expect("unique metric name");
        let build_info = IntGaugeVec::new(
            Opts::new("loadgen_serve_build_info", "Build information; always 1."),
            &["version"],
        )
        .expect("valid metric definition");
        registry
            .register(Box::new(build_info.clone()))
            .expect("unique metric name");
        build_info
            .with_label_values(&[env!("CARGO_PKG_VERSION")])
            .set(1);
        let plan_duration = Histogram::with_opts(
            HistogramOpts::new(
                "loadgen_serve_plan_duration_seconds",
                "Wall time of in-process plan generation.",
            )
            .buckets(vec![
                0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ]),
        )
        .expect("valid metric definition");
        registry
            .register(Box::new(plan_duration.clone()))
            .expect("unique metric name");
        let active_clients = IntGaugeVec::new(
            Opts::new(
                "loadgen_serve_active_clients",
                "Distinct clients that submitted a job within the window.",
            ),
            &["window"],
        )
        .expect("valid metric definition");
        registry
            .register(Box::new(active_clients.clone()))
            .expect("unique metric name");
        for (window, _) in ACTIVE_WINDOWS {
            active_clients.with_label_values(&[window]).set(0);
        }
        let client_jobs = counter_vec(
            &registry,
            "loadgen_serve_client_jobs_total",
            "Accepted job submissions by client and kind, including cache hits.",
            &["client", "kind"],
        );
        let bundle_job_duration = histogram_vec(
            &registry,
            "loadgen_serve_bundle_job_duration_seconds",
            "Wall time of bundle jobs including queue time, by result (done, error).",
            &["result"],
            vec![
                0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 900.0,
            ],
        );
        // Pre-create the label sets the dashboard queries so they exist at 0.
        for kind in ["replay", "plan"] {
            jobs_inflight.with_label_values(&[kind]).set(0);
            for result in ["done", "error", "cached"] {
                jobs.with_label_values(&[kind, result]).reset();
            }
        }
        for reason in ["per_ip_cap", "too_many_hours", "bad_request", "not_found"] {
            rejected.with_label_values(&[reason]).reset();
        }
        Self {
            registry,
            clients: Mutex::new(Clients::default()),
            active_clients,
            client_jobs,
            http_requests,
            http_request_duration,
            jobs,
            shards_scanned,
            shard_bytes,
            lines_scanned,
            records_kept,
            zero_output_dropped,
            s3_errors,
            rejected,
            jobs_inflight,
            jobs_queued,
            plan_duration,
            bundle_job_duration,
        }
    }

    pub fn job_finished(&self, kind: &str, result: &str) {
        self.jobs.with_label_values(&[kind, result]).inc();
    }

    /// Record an accepted `POST /plan` or `POST /replay-bundle` from `client`.
    pub fn job_submitted(&self, client: &str, kind: &str) {
        self.job_submitted_at(client, kind, Instant::now());
    }

    fn job_submitted_at(&self, client: &str, kind: &str, now: Instant) {
        let mut clients = self.clients.lock().expect("client index poisoned");
        clients.last_seen.insert(client.to_string(), now);
        let label = if clients.labelled.contains(client) {
            client
        } else if clients.labelled.len() < MAX_CLIENT_LABELS {
            clients.labelled.insert(client.to_string());
            client
        } else {
            OTHER_CLIENT
        };
        self.client_jobs.with_label_values(&[label, kind]).inc();
    }

    /// Refresh `loadgen_serve_active_clients` and evict clients idle past the
    /// longest window.
    fn refresh_active_clients(&self, now: Instant) {
        let mut clients = self.clients.lock().expect("client index poisoned");
        let horizon = ACTIVE_WINDOWS
            .iter()
            .map(|(_, window)| *window)
            .max()
            .expect("at least one window");
        clients
            .last_seen
            .retain(|_, seen| now.duration_since(*seen) <= horizon);
        for (label, window) in ACTIVE_WINDOWS {
            let count = clients
                .last_seen
                .values()
                .filter(|seen| now.duration_since(**seen) <= window)
                .count();
            self.active_clients
                .with_label_values(&[label])
                .set(count as i64);
        }
    }

    /// Text exposition of every registered metric.
    pub fn render(&self) -> Result<Vec<u8>, prometheus::Error> {
        self.refresh_active_clients(Instant::now());
        let mut buffer = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buffer)?;
        Ok(buffer)
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// `GET /metrics`.
pub async fn metrics_handler(State(state): State<Arc<AppState>>) -> Response {
    match state.metrics.render() {
        Ok(body) => (
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            body,
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to encode metrics: {error}"),
        )
            .into_response(),
    }
}

/// Middleware recording the request counter, latency histogram, and rejections.
pub async fn track_http(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_string())
        .unwrap_or_else(|| UNMATCHED_ROUTE.to_string());
    let method = request.method().as_str().to_string();
    let started = Instant::now();
    let response = next.run(request).await;
    let metrics = &state.metrics;
    metrics
        .http_requests
        .with_label_values(&[&route, &method, response.status().as_str()])
        .inc();
    metrics
        .http_request_duration
        .with_label_values(&[&route])
        .observe(started.elapsed().as_secs_f64());
    if let Some(Rejection(reason)) = response.extensions().get::<Rejection>() {
        metrics.rejected.with_label_values(&[reason]).inc();
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gauge(metrics: &Metrics, window: &str) -> i64 {
        metrics.active_clients.with_label_values(&[window]).get()
    }

    #[test]
    fn active_clients_follow_windows_and_evict() {
        let metrics = Metrics::new();
        let now = Instant::now();
        metrics.job_submitted_at("10.0.0.1", "plan", now);
        metrics.job_submitted_at("10.0.0.2", "replay", now - Duration::from_secs(20 * 60));
        metrics.job_submitted_at("10.0.0.3", "plan", now - Duration::from_secs(25 * 60 * 60));
        metrics.refresh_active_clients(now);
        assert_eq!(gauge(&metrics, "15m"), 1);
        assert_eq!(gauge(&metrics, "24h"), 2);
        assert_eq!(
            metrics.clients.lock().unwrap().last_seen.len(),
            2,
            "clients idle past 24h are evicted"
        );
        assert_eq!(
            metrics
                .client_jobs
                .with_label_values(&["10.0.0.1", "plan"])
                .get(),
            1
        );
    }

    #[test]
    fn client_labels_are_capped() {
        let metrics = Metrics::new();
        let now = Instant::now();
        for i in 0..MAX_CLIENT_LABELS + 3 {
            metrics.job_submitted_at(&format!("10.1.{}.{}", i / 256, i % 256), "plan", now);
        }
        metrics.job_submitted_at("10.1.0.0", "plan", now);
        assert_eq!(
            metrics
                .client_jobs
                .with_label_values(&[OTHER_CLIENT, "plan"])
                .get(),
            3
        );
        assert_eq!(
            metrics
                .client_jobs
                .with_label_values(&["10.1.0.0", "plan"])
                .get(),
            2,
            "known clients keep their own label"
        );
        metrics.refresh_active_clients(now);
        assert_eq!(gauge(&metrics, "15m"), (MAX_CLIENT_LABELS + 3) as i64);
    }
}
