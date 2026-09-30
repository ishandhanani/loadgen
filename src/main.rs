// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use agent_loadgen_generate::scenario::{
    GeneratorConfig, PlanSummary, TreePlanner, write_plan_graph,
};
use agent_loadgen_replay::telemetry::join_engine_telemetry;
use agent_loadgen_replay::token_shape::TokenDictionary;
use agent_loadgen_replay::{ReplayOptions, run_agentic_replay, run_generated};
use agent_loadgen_trace::compare::{CompareOptions, compare_traces};
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
            let trace = load_agentic_trace(
                &trace,
                selection.max_requests,
                selection.session_id.as_deref(),
            )?;
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
            fidelity,
            max_in_flight,
            warmup_connections,
            http_transport,
            result_flush_interval,
            max_dispatch_p99_ms,
            max_dispatch_max_ms,
            start_delay_ms,
            timeout_seconds,
            time_scale,
            static_headers,
            run,
        } => {
            let trace = load_agentic_trace(
                &trace,
                selection.max_requests,
                selection.session_id.as_deref(),
            )?;
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
                    max_in_flight,
                    warmup_connections,
                    http_transport,
                    result_flush_interval,
                    max_dispatch_p99_ms,
                    max_dispatch_max_ms,
                    start_delay: Duration::from_millis(start_delay_ms),
                    timeout: Duration::from_secs(timeout_seconds),
                    time_scale,
                    token_path_verified: fidelity.token_path_verified,
                    engine_cache_mode: parse_key_values(
                        fidelity.engine_cache_mode,
                        "engine cache mode",
                    )?,
                    static_headers: parse_headers(static_headers)?,
                    stop_after: run.stop_after()?,
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
            if !summary.passed {
                bail!("shape-strict replay failed request or dispatch-timing fidelity checks");
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
            fidelity,
            max_in_flight,
            warmup_connections,
            http_transport,
            result_flush_interval,
            max_dispatch_p99_ms,
            max_dispatch_max_ms,
            start_delay_ms,
            timeout_seconds,
            time_scale,
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
                    max_in_flight,
                    warmup_connections,
                    http_transport,
                    result_flush_interval,
                    max_dispatch_p99_ms,
                    max_dispatch_max_ms,
                    start_delay: Duration::from_millis(start_delay_ms),
                    timeout: Duration::from_secs(timeout_seconds),
                    time_scale,
                    token_path_verified: fidelity.token_path_verified,
                    engine_cache_mode: parse_key_values(
                        fidelity.engine_cache_mode,
                        "engine cache mode",
                    )?,
                    static_headers: parse_headers(static_headers)?,
                    stop_after: run.stop_after()?,
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
            if !summary.passed {
                bail!("generated traffic failed request or dispatch-timing checks");
            }
        }
        Command::Compare {
            source,
            replay,
            requests,
            max_arrival_p99_ms,
            max_arrival_max_ms,
        } => {
            let report = compare_traces(
                &source,
                &replay,
                &requests,
                CompareOptions {
                    max_arrival_p99_ms,
                    max_arrival_max_ms,
                },
            )?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            if !report.passed {
                bail!("replay fidelity comparison failed");
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
        Command::JoinTelemetry {
            requests,
            engine_telemetry,
            output,
        } => {
            let summary = join_engine_telemetry(&requests, &engine_telemetry, &output)?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
    }
    Ok(())
}

/// Write plan-summary.json and, when the plan fits one graph, scenario.json,
/// plan.dot, and plan.svg.
fn write_plan_artifacts(
    planner: &TreePlanner,
    output: &std::path::Path,
) -> Result<serde_json::Value> {
    std::fs::create_dir_all(output)
        .with_context(|| format!("failed to create output directory {}", output.display()))?;
    let summary = PlanSummary::sample(planner)?;
    let summary_path = output.join("plan-summary.json");
    write_pretty_json(&summary_path, &summary)?;
    let mut result = serde_json::json!({
        "summary": summary,
        "summary_path": summary_path,
    });
    // Only finite plans within limits.max_nodes are joined into one graph.
    let fits = planner.tree_count().is_some_and(|trees| {
        summary.per_tree.requests.mean * trees as f64 <= planner.config().max_nodes as f64
    });
    if fits && let Ok(scenario) = planner.materialize() {
        let scenario_path = output.join("scenario.json");
        write_pretty_json(&scenario_path, &scenario)?;
        let graph = write_plan_graph(output, &scenario)?;
        result["scenario_digest_sha256"] = serde_json::json!(scenario.scenario_digest_sha256);
        result["requests"] = serde_json::json!(scenario.nodes.len());
        result["session_topology"] = serde_json::json!(scenario.session_topology);
        result["tool_parallelism"] = serde_json::json!(scenario.tool_parallelism);
        result["scenario_path"] = serde_json::json!(scenario_path);
        result["graph_path"] = serde_json::json!(graph.dot_path);
        result["graphviz_svg_path"] = serde_json::json!(graph.svg_path);
    }
    Ok(result)
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

fn parse_key_values(
    values: Vec<String>,
    label: &str,
) -> Result<std::collections::BTreeMap<String, String>> {
    let mut parsed = std::collections::BTreeMap::new();
    for value in values {
        let (name, setting) = value
            .split_once('=')
            .with_context(|| format!("{label} {value:?} must use NAME=VALUE"))?;
        if name.is_empty() || setting.is_empty() {
            bail!("{label} names and values must not be empty");
        }
        if parsed
            .insert(name.to_string(), setting.to_string())
            .is_some()
        {
            bail!("{label} {name:?} is declared more than once");
        }
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_mode_rejects_duplicate_keys() {
        let result = parse_key_values(
            vec![
                "ownership=session".to_string(),
                "ownership=shared".to_string(),
            ],
            "engine cache mode",
        );
        assert!(result.is_err());
    }
}
