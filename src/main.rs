// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use agent_loadgen_generate::scenario::{GeneratorConfig, PlanSummary, TreePlanner};
use agent_loadgen_replay::token_shape::TokenDictionary;
use agent_loadgen_replay::{ReplayOptions, run_agentic_replay, run_generated};
use agent_loadgen_trace::load_agentic_trace;
use anyhow::{Context, Result, bail};
use clap::Parser;

mod cli;

use cli::{Cli, Command};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Inspect {
            trace,
            selection,
            tokens,
        } => {
            let trace = load_agentic_trace(&trace, selection.session_id.as_deref())?;
            let dictionary = TokenDictionary::new(
                trace.manifest.trace_block_size,
                trace.manifest.distinct_sequence_hashes,
                tokens.load()?,
            )?;
            let output = serde_json::json!({
                "fidelity": "agentic-causal",
                "source": trace.manifest,
                "root_requests": trace.root_request_count(),
                "dependency_edges": trace.dependency_edge_count(),
                "token_dictionary": dictionary.manifest()
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        Command::Replay {
            trace,
            header_dialect,
            dynamo_session_final,
            model,
            target,
            output,
            selection,
            tokens,
            timeout_seconds,
            time_scale,
            static_headers,
            run,
        } => {
            let trace = load_agentic_trace(&trace, selection.session_id.as_deref())?;
            let dictionary = TokenDictionary::new(
                trace.manifest.trace_block_size,
                trace.manifest.distinct_sequence_hashes,
                tokens.load()?,
            )?;
            let summary = run_agentic_replay(
                trace,
                dictionary,
                ReplayOptions {
                    header_dialect,
                    dynamo_session_final,
                    model,
                    target,
                    output_dir: output,
                    timeout: Duration::from_secs(timeout_seconds),
                    time_scale,
                    static_headers: parse_headers(static_headers)?,
                    stop_after: run.stop_after()?,
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
            if !summary.passed {
                bail!("some replayed requests failed or missed their recorded output length");
            }
        }
        Command::Plan { config, output } => {
            let planner = TreePlanner::new(GeneratorConfig::load(&config)?.resolve()?)?;
            let output = write_plan_artifacts(&planner, &output)?;
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        Command::Generate {
            config,
            header_dialect,
            dynamo_session_final,
            model,
            target,
            output,
            tokens,
            timeout_seconds,
            static_headers,
            run,
        } => {
            let planner = TreePlanner::new(GeneratorConfig::load(&config)?.resolve()?)?;
            write_plan_artifacts(&planner, &output)?;
            let dictionary = TokenDictionary::new(planner.config().block_size, 0, tokens.load()?)?;
            let summary = run_generated(
                &planner,
                dictionary,
                ReplayOptions {
                    header_dialect,
                    dynamo_session_final,
                    model,
                    target,
                    output_dir: output,
                    timeout: Duration::from_secs(timeout_seconds),
                    time_scale: 1.0,
                    static_headers: parse_headers(static_headers)?,
                    stop_after: run.stop_after()?,
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
            if !summary.passed {
                bail!("some generated requests failed or missed their planned output length");
            }
        }
        Command::Serve {
            root,
            listen,
            creds,
            bundle_concurrency,
            keep_jobs,
        } => {
            agent_loadgen_serve::init_tracing();
            let root = match root {
                Some(root) => root,
                None => home_dir()?.join("dynamo-traces"),
            };
            let creds = match creds {
                Some(creds) => creds,
                None => home_dir()?.join(".config/dynamo-traces/env"),
            };
            agent_loadgen_serve::run(agent_loadgen_serve::ServeOptions {
                root,
                listen,
                creds,
                limits: agent_loadgen_serve::ServeLimits {
                    bundle_concurrency,
                    keep_jobs,
                    ..agent_loadgen_serve::ServeLimits::default()
                },
            })
            .await?;
        }
    }
    Ok(())
}

/// Write plan-summary.json.
fn write_plan_artifacts(
    planner: &TreePlanner,
    output: &std::path::Path,
) -> Result<serde_json::Value> {
    std::fs::create_dir_all(output)
        .with_context(|| format!("failed to create output directory {}", output.display()))?;
    let summary = PlanSummary::sample(planner)?;
    let summary_path = output.join("plan-summary.json");
    write_pretty_json(&summary_path, &summary)?;
    Ok(serde_json::json!({
        "summary": summary,
        "summary_path": summary_path,
    }))
}

fn write_pretty_json(path: &std::path::Path, value: &impl serde::Serialize) -> Result<()> {
    let writer = std::io::BufWriter::new(
        std::fs::File::create(path)
            .with_context(|| format!("failed to create {}", path.display()))?,
    );
    serde_json::to_writer_pretty(writer, value)
        .with_context(|| format!("failed to write {}", path.display()))
}

fn home_dir() -> Result<std::path::PathBuf> {
    std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .context("HOME is not set; pass --root and --creds explicitly")
}

fn parse_headers(headers: Vec<String>) -> Result<Vec<(String, String)>> {
    headers
        .into_iter()
        .map(|header| {
            let (name, value) = header
                .split_once('=')
                .with_context(|| format!("header {header:?} must use NAME=VALUE"))?;
            if name.is_empty() {
                bail!("header name must not be empty");
            }
            Ok((name.to_string(), value.to_string()))
        })
        .collect()
}
