// SPDX-License-Identifier: Apache-2.0

//! `agent-loadgen serve`: an HTTP service that turns dashboard sessions into
//! replay bundles and TOML profiles into plan previews, in process.
//!
//! The service produces artifacts and copy-paste commands only; it never sends
//! load anywhere itself.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::Semaphore;
use tracing::info;

pub mod api;
pub mod bundle;
pub mod creds;
pub mod hours;
pub mod jobs;
pub mod plan;
pub mod shards;

pub use api::router;
use jobs::JobStore;
use shards::ShardSource;

/// Tunables shared by the CLI and tests.
#[derive(Debug, Clone)]
pub struct ServeLimits {
    /// Bundle jobs scanning shards at once.
    pub bundle_concurrency: usize,
    /// Job directories retained under `<root>/loadgen-jobs`.
    pub keep_jobs: usize,
    /// Queued or running bundle jobs allowed per client address.
    pub per_client_cap: usize,
    /// Wall-clock limit for one bundle job, including queue time.
    pub job_timeout: Duration,
    /// Limit for the shard listing performed to validate a cache hit.
    pub cache_list_timeout: Duration,
}

impl Default for ServeLimits {
    fn default() -> Self {
        Self {
            bundle_concurrency: 3,
            keep_jobs: 50,
            per_client_cap: 2,
            job_timeout: Duration::from_secs(15 * 60),
            cache_list_timeout: Duration::from_secs(30),
        }
    }
}

/// Shared handler state.
pub struct AppState {
    pub root: PathBuf,
    pub web_dir: PathBuf,
    pub store: JobStore,
    pub shards: Arc<dyn ShardSource>,
    pub bundle_semaphore: Arc<Semaphore>,
    pub per_client_cap: usize,
    pub job_timeout: Duration,
    pub cache_list_timeout: Duration,
    pub creds_loaded: bool,
}

impl AppState {
    pub fn new(
        root: &Path,
        shards: Arc<dyn ShardSource>,
        limits: &ServeLimits,
        creds_loaded: bool,
    ) -> Result<Arc<Self>> {
        if limits.bundle_concurrency == 0 {
            anyhow::bail!("--bundle-concurrency must be at least 1");
        }
        if limits.keep_jobs == 0 {
            anyhow::bail!("--keep-jobs must be at least 1");
        }
        let web_dir = root.join("web").join("data");
        if !web_dir.is_dir() {
            anyhow::bail!(
                "{} is not a directory; --root must point at the dynamo-traces checkout",
                web_dir.display()
            );
        }
        Ok(Arc::new(Self {
            root: root.to_path_buf(),
            web_dir,
            store: JobStore::open(root.join("loadgen-jobs"), limits.keep_jobs)?,
            shards,
            bundle_semaphore: Arc::new(Semaphore::new(limits.bundle_concurrency)),
            per_client_cap: limits.per_client_cap,
            job_timeout: limits.job_timeout,
            cache_list_timeout: limits.cache_list_timeout,
            creds_loaded,
        }))
    }
}

/// CLI options for `agent-loadgen serve`.
#[derive(Debug, Clone)]
pub struct ServeOptions {
    pub root: PathBuf,
    pub listen: SocketAddr,
    pub creds: PathBuf,
    pub limits: ServeLimits,
}

/// Install a stderr `tracing` subscriber honoring `RUST_LOG` (default `info`).
pub fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .with_writer(std::io::stderr)
        .try_init();
}

/// Run the service until the process exits.
pub async fn run(options: ServeOptions) -> Result<()> {
    let creds = creds::load_credentials(&options.creds)?;
    info!(endpoint = %creds.endpoint_url, region = %creds.region, "loaded S3 credentials");
    let shards: Arc<dyn ShardSource> = Arc::new(shards::S3ShardSource::new(&creds, shards::BUCKET));
    let state = AppState::new(&options.root, shards, &options.limits, true)?;
    let listener = tokio::net::TcpListener::bind(options.listen)
        .await
        .with_context(|| format!("failed to bind {}", options.listen))?;
    info!(
        listen = %options.listen,
        root = %options.root.display(),
        jobs = %state.store.jobs_dir().display(),
        bundle_concurrency = options.limits.bundle_concurrency,
        keep_jobs = options.limits.keep_jobs,
        "agent-loadgen serve ready"
    );
    axum::serve(
        listener,
        router(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .context("HTTP server failed")
}
