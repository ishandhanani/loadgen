// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use agent_loadgen_generate::scenario::{GeneratorConfig, PlanSummary, TreePlanner};
use agent_loadgen_replay::token_shape::TokenDictionary;
use agent_loadgen_replay::{ReplayOptions, run_generated};
use anyhow::{Context, Result, bail};
use clap::Parser;

mod cli;

use cli::{Cli, Command};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
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
            let dictionary = TokenDictionary::new(planner.config().block_size, tokens.load()?)?;
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
