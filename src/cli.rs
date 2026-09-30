// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;
use std::path::PathBuf;

use agent_loadgen_core::HeaderDialect;
use agent_loadgen_replay::HttpTransport;
use agent_loadgen_replay::token_shape::{SafeTokenAlphabet, TokenAlphabetSource};
use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "agent-loadgen", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Args)]
pub struct TokenArgs {
    /// Local tokenizer file/directory or Hugging Face model ID.
    #[arg(long)]
    pub tokenizer: Option<String>,

    /// First ID in a caller-certified manual token range.
    #[arg(long)]
    pub token_start: Option<u32>,

    /// Maximum number of safe token IDs to use.
    #[arg(long, default_value_t = 1024)]
    pub token_alphabet_size: usize,

    /// Exclude one token ID. Repeat this option for more IDs.
    #[arg(long = "exclude-token-id")]
    pub excluded_token_ids: Vec<u32>,

    /// Accept a manual token range that the load generator cannot verify.
    #[arg(long)]
    pub allow_unverified_token_range: bool,
}

impl TokenArgs {
    pub fn load(self) -> Result<SafeTokenAlphabet> {
        let source = match (self.tokenizer, self.token_start) {
            (Some(tokenizer), None) => {
                if self.allow_unverified_token_range {
                    bail!("--allow-unverified-token-range requires --token-start");
                }
                TokenAlphabetSource::Tokenizer(tokenizer)
            }
            (None, Some(start)) => {
                if !self.allow_unverified_token_range {
                    bail!(
                        "--token-start requires --allow-unverified-token-range; use --tokenizer for verified token safety"
                    );
                }
                TokenAlphabetSource::UnverifiedRange {
                    start,
                    size: u32::try_from(self.token_alphabet_size)
                        .context("--token-alphabet-size does not fit u32")?,
                }
            }
            (Some(_), Some(_)) => bail!("use either --tokenizer or --token-start, not both"),
            (None, None) => bail!(
                "--tokenizer is required for verified token safety; a manual range requires --token-start and --allow-unverified-token-range"
            ),
        };
        SafeTokenAlphabet::load(source, self.token_alphabet_size, &self.excluded_token_ids)
    }
}

#[derive(Debug, Args)]
pub struct TraceSelectionArgs {
    /// Keep only this many earliest model requests.
    #[arg(long)]
    pub max_requests: Option<usize>,

    /// Replay one session in isolation.
    #[arg(long)]
    pub session_id: Option<String>,
}

#[derive(Debug, Args)]
pub struct FidelityArgs {
    /// Declare that supplied token IDs reach the engine without re-tokenization.
    #[arg(long)]
    pub token_path_verified: bool,

    /// Declare one engine cache setting as NAME=VALUE. Repeat for more settings.
    #[arg(long = "engine-cache-mode")]
    pub engine_cache_mode: Vec<String>,
}

#[derive(Debug, Args)]
pub struct RunOutputArgs {
    /// Width in seconds of each timeline.csv row.
    #[arg(long, default_value_t = 60.0)]
    pub timeline_seconds: f64,

    /// Mix this value into token content so the run does not reuse KV cached by
    /// earlier runs. Defaults to a random value; 0 sends the planned content.
    #[arg(long)]
    pub token_salt: Option<u64>,

    /// Stop sending after this many minutes, let in-flight requests finish, and
    /// report the requests that were sent.
    #[arg(long)]
    pub stop_after_minutes: Option<f64>,
}

impl RunOutputArgs {
    pub fn timeline_bucket(&self) -> Result<std::time::Duration> {
        if !self.timeline_seconds.is_finite() || self.timeline_seconds <= 0.0 {
            bail!("--timeline-seconds must be a positive number");
        }
        Ok(std::time::Duration::from_secs_f64(self.timeline_seconds))
    }

    pub fn stop_after(&self) -> Result<Option<std::time::Duration>> {
        self.stop_after_minutes
            .map(|minutes| {
                if !minutes.is_finite() || minutes <= 0.0 {
                    bail!("--stop-after-minutes must be a positive number");
                }
                Ok(std::time::Duration::from_secs_f64(minutes * 60.0))
            })
            .transpose()
    }

    pub fn token_salt(&self) -> u64 {
        self.token_salt
            .unwrap_or_else(|| uuid::Uuid::new_v4().as_u64_pair().0)
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Validate and summarize an agentic trace and its causal graph.
    Inspect {
        /// Raw or wrapped Dynamo request trace files.
        #[arg(required = true)]
        trace: Vec<PathBuf>,

        #[command(flatten)]
        selection: TraceSelectionArgs,

        #[command(flatten)]
        tokens: TokenArgs,
    },

    /// Replay a trace against a live Dynamo frontend.
    Replay {
        /// Raw or wrapped Dynamo request trace files.
        #[arg(required = true)]
        trace: Vec<PathBuf>,

        /// Coding-agent harness whose request headers to send.
        #[arg(long = "headers", value_enum)]
        header_dialect: HeaderDialect,

        /// Also mark each session's last request with `x-dynamo-session-final: true`.
        /// No harness sends this Dynamo extension; enable it only for deployments that consume it.
        #[arg(long)]
        dynamo_session_final: bool,

        /// Model name sent to Dynamo.
        #[arg(long)]
        model: String,

        /// Dynamo base URL or full Chat Completions URL.
        #[arg(long)]
        target: String,

        /// Output directory for run.json and requests.jsonl.
        #[arg(long)]
        output: PathBuf,

        #[command(flatten)]
        selection: TraceSelectionArgs,

        #[command(flatten)]
        tokens: TokenArgs,

        #[command(flatten)]
        fidelity: FidelityArgs,

        /// Maximum simultaneous HTTP requests.
        #[arg(long, default_value_t = 4096)]
        max_in_flight: usize,

        /// Number of idle HTTP connections prepared through /v1/models before replay.
        #[arg(long, default_value_t = 1)]
        warmup_connections: usize,

        /// HTTP transport used for target requests.
        #[arg(long, value_enum, default_value_t = HttpTransport::Http2PriorKnowledge)]
        http_transport: HttpTransport,

        /// Flush requests.jsonl after this many completed requests.
        #[arg(long, default_value_t = 1)]
        result_flush_interval: usize,

        /// Maximum allowed p99 client-offer lag.
        #[arg(long, default_value_t = 2.0)]
        max_dispatch_p99_ms: f64,

        /// Maximum allowed client-offer lag.
        #[arg(long, default_value_t = 5.0)]
        max_dispatch_max_ms: f64,

        /// Delay before the first scheduled request.
        #[arg(long, default_value_t = 100)]
        start_delay_ms: u64,

        /// Per-request HTTP timeout.
        #[arg(long, default_value_t = 600)]
        timeout_seconds: u64,

        /// Divide root offsets and dependency delays by this value.
        #[arg(long, default_value_t = 1.0)]
        time_scale: f64,

        /// Add a static target header as NAME=VALUE. Repeat the flag for more headers.
        #[arg(long = "header")]
        static_headers: Vec<String>,

        #[command(flatten)]
        run: RunOutputArgs,
    },

    /// Summarize a profile's traffic, and its request graph when small, without sending it.
    Plan {
        /// Versioned TOML generator config.
        #[arg(long)]
        config: PathBuf,

        /// Output directory for plan-summary.json and, when the plan fits one
        /// graph, scenario.json, plan.dot, and optional plan.svg.
        #[arg(long)]
        output: PathBuf,
    },

    /// Generate and run seeded coding-agent traffic, planning each session tree when it starts.
    Generate {
        /// Versioned TOML generator config.
        #[arg(long)]
        config: PathBuf,

        /// Coding-agent harness whose request headers to send.
        #[arg(long = "headers", value_enum)]
        header_dialect: HeaderDialect,

        /// Also mark each session's last request with `x-dynamo-session-final: true`.
        /// No harness sends this Dynamo extension; enable it only for deployments that consume it.
        #[arg(long)]
        dynamo_session_final: bool,

        /// Model name sent to Dynamo.
        #[arg(long)]
        model: String,

        /// Dynamo base URL or full Chat Completions URL.
        #[arg(long)]
        target: String,

        /// Output directory for plan and run artifacts: plan-summary.json, run.json,
        /// requests.jsonl, timeline.csv, and, for small plans, scenario.json and plan.dot.
        #[arg(long)]
        output: PathBuf,

        #[command(flatten)]
        tokens: TokenArgs,

        #[command(flatten)]
        fidelity: FidelityArgs,

        /// Maximum simultaneous HTTP requests.
        #[arg(long, default_value_t = 256)]
        max_in_flight: usize,

        /// Number of idle HTTP connections prepared through /v1/models.
        #[arg(long, default_value_t = 1)]
        warmup_connections: usize,

        /// HTTP transport used for target requests.
        #[arg(long, value_enum, default_value_t = HttpTransport::Http2PriorKnowledge)]
        http_transport: HttpTransport,

        /// Flush requests.jsonl after this many completed requests.
        #[arg(long, default_value_t = 1)]
        result_flush_interval: usize,

        /// Maximum allowed p99 dispatch lag after a graph node becomes ready.
        #[arg(long, default_value_t = 10.0)]
        max_dispatch_p99_ms: f64,

        /// Maximum allowed dispatch lag after a graph node becomes ready.
        #[arg(long, default_value_t = 50.0)]
        max_dispatch_max_ms: f64,

        /// Delay before the first top-level session tree starts.
        #[arg(long, default_value_t = 100)]
        start_delay_ms: u64,

        /// Per-request HTTP timeout.
        #[arg(long, default_value_t = 600)]
        timeout_seconds: u64,

        /// Divide startup, restart, tool, think, and join delays by this value.
        #[arg(long, default_value_t = 1.0)]
        time_scale: f64,

        /// Add a static target header as NAME=VALUE. Repeat the flag for more headers.
        #[arg(long = "header")]
        static_headers: Vec<String>,

        #[command(flatten)]
        run: RunOutputArgs,
    },

    /// Compare a source trace with a trace captured during replay.
    Compare {
        /// Original Dynamo request trace files.
        #[arg(long = "source", required = true)]
        source: Vec<PathBuf>,

        /// Dynamo request trace files captured during replay.
        #[arg(long = "replay", required = true)]
        replay: Vec<PathBuf>,

        /// requests.jsonl from the replay run.
        #[arg(long)]
        requests: PathBuf,

        /// Maximum allowed p99 frontend arrival error.
        #[arg(long, default_value_t = 5.0)]
        max_arrival_p99_ms: f64,

        /// Maximum allowed frontend arrival error.
        #[arg(long, default_value_t = 20.0)]
        max_arrival_max_ms: f64,
    },

    /// Serve plan previews and replay-bundle exports over HTTP for the traces dashboard.
    Serve {
        /// dynamo-traces root containing web/data; jobs go to <root>/loadgen-jobs (default ~/dynamo-traces).
        #[arg(long)]
        root: Option<PathBuf>,

        /// Address to listen on.
        #[arg(long, default_value = "127.0.0.1:8096")]
        listen: SocketAddr,

        /// `export NAME=value` file with AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and S3_ENDPOINT_URL
        /// (default ~/.config/dynamo-traces/env).
        #[arg(long)]
        creds: Option<PathBuf>,

        /// Replay-bundle jobs scanning shards at the same time.
        #[arg(long, default_value_t = 3)]
        bundle_concurrency: usize,

        /// Newest job directories to keep; older ones are pruned.
        #[arg(long, default_value_t = 50)]
        keep_jobs: usize,
    },

    /// Join request results with normalized engine telemetry by request ID.
    JoinTelemetry {
        /// requests.jsonl from a replay or generated run.
        #[arg(long)]
        requests: PathBuf,

        /// Engine telemetry JSONL. Repeat for more files.
        #[arg(long = "engine-telemetry", required = true)]
        engine_telemetry: Vec<PathBuf>,

        /// Joined JSONL output path.
        #[arg(long)]
        output: PathBuf,
    },
}
