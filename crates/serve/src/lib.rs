// SPDX-License-Identifier: Apache-2.0

//! `agent-loadgen serve`: an HTTP service that turns TOML profiles into plan
//! previews for the traces dashboard, in process.
//!
//! The service plans only; it never sends load anywhere itself.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::info;

pub mod api;
pub mod metrics;
pub mod plan;

pub use api::router;
use metrics::Metrics;

/// Shared handler state.
#[derive(Default)]
pub struct AppState {
    pub metrics: Metrics,
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

/// Run the service on `listen` until the process exits.
pub async fn run(listen: SocketAddr) -> Result<()> {
    let state = Arc::new(AppState::default());
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("failed to bind {listen}"))?;
    info!(%listen, "agent-loadgen serve ready");
    axum::serve(
        listener,
        router(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .context("HTTP server failed")
}
