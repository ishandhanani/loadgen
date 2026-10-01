// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;
use std::path::PathBuf;

use agent_loadgen_core::HeaderDialect;
use agent_loadgen_replay::token_shape::{SafeTokenAlphabet, TokenAlphabetSource};
use anyhow::{Result, bail};
use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "agent-loadgen", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Args)]
pub struct TokenArgs {
    /// Tokenizer of the served model: a Hugging Face model ID, or a local
    /// tokenizer.json or model directory. Synthetic prompts use only its
    /// ordinary (non-special) token IDs.
    #[arg(long)]
    pub tokenizer: String,
}

impl TokenArgs {
    pub fn load(self) -> Result<SafeTokenAlphabet> {
        SafeTokenAlphabet::load(TokenAlphabetSource::Tokenizer(self.tokenizer), 1024, &[])
    }
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Stop sending after this many minutes, let in-flight requests finish, and
    /// report the requests that were sent.
    #[arg(long)]
    pub stop_after_minutes: Option<f64>,
}

impl RunArgs {
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
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Summarize a profile's traffic without sending it.
    Plan {
        /// Versioned TOML generator config.
        #[arg(long)]
        config: PathBuf,

        /// Output directory for plan-summary.json.
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

        /// Output directory for plan-summary.json, run.json, requests.jsonl, and
        /// timeline.csv.
        #[arg(long)]
        output: PathBuf,

        #[command(flatten)]
        tokens: TokenArgs,

        /// Per-request HTTP timeout.
        #[arg(long, default_value_t = 600)]
        timeout_seconds: u64,

        /// Add a static target header as NAME=VALUE. Repeat the flag for more headers.
        #[arg(long = "header")]
        static_headers: Vec<String>,

        #[command(flatten)]
        run: RunArgs,
    },

    /// Serve plan previews over HTTP for the traces dashboard.
    Serve {
        /// Address to listen on.
        #[arg(long, default_value = "127.0.0.1:8096")]
        listen: SocketAddr,
    },
}
