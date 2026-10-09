// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;
use std::{fs::File, io::Write};

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
        Command::ExportPlan { config, output } => {
            let planner = TreePlanner::new(GeneratorConfig::load(&config)?.resolve()?)?;
            let mut artifacts = write_plan_artifacts(&planner, &output)?;
            artifacts["trees"] = write_tree_artifacts(&planner, &output)?;
            println!("{}", serde_json::to_string_pretty(&artifacts)?);
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

/// Stream every tree from a finite plan to one JSONL artifact.
fn write_tree_artifacts(
    planner: &TreePlanner,
    output: &std::path::Path,
) -> Result<serde_json::Value> {
    let tree_count = planner.tree_count().context(
        "cannot export a duration-bounded stream; set load.num_sessions or use an arrival schedule",
    )?;
    let path = output.join("trees.jsonl");
    let mut writer = std::io::BufWriter::new(
        File::create(&path).with_context(|| format!("failed to create {}", path.display()))?,
    );
    for index in 0..tree_count {
        serde_json::to_writer(&mut writer, &planner.plan_tree(index)?)
            .with_context(|| format!("failed to write tree {index} to {}", path.display()))?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    Ok(serde_json::json!({
        "path": path,
        "tree_count": tree_count,
    }))
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

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader};

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn exports_one_jsonl_row_per_tree() {
        let profile = tempdir().unwrap();
        let config = profile.path().join("profile.toml");
        std::fs::write(
            &config,
            r#"
                schema_version = 5
                seed = 9

                [load]
                num_sessions = 2
                concurrent_sessions = 1

                [trajectory]
                turns = { kind = "fixed", value = 2 }
                output_tokens = { kind = "fixed", value = 2 }

                [tokens]
                system_prefix_tokens = { kind = "fixed", value = 16 }
                tool_catalog_tokens = { kind = "fixed", value = 16 }
                repository_tokens = { kind = "fixed", value = 16 }
                session_tokens = { kind = "fixed", value = 16 }
                user_tokens = { kind = "fixed", value = 16 }

                [behavior]
                tool_probability = 1.0
                parallel_tool_probability = 0.0
                subagent_probability = 0.0
                swarm_probability = 0.0
                completion_probability = 0.0

                [compaction]
                enabled = false

                [subagents]
                max_depth = 0

                [tools]
                classes = [{ name = "shell", weight = 1.0, latency_ms = { kind = "fixed", value = 5 }, result_tokens = { kind = "fixed", value = 16 } }]
            "#,
        )
        .unwrap();
        let planner =
            TreePlanner::new(GeneratorConfig::load(&config).unwrap().resolve().unwrap()).unwrap();
        let output = tempdir().unwrap();

        let artifact = write_tree_artifacts(&planner, output.path()).unwrap();
        let rows = BufReader::new(File::open(&artifact["path"].as_str().unwrap()).unwrap())
            .lines()
            .collect::<std::io::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(artifact["tree_count"], 2);
        assert_eq!(rows.len(), 2);
        for (index, row) in rows.iter().enumerate() {
            let tree: serde_json::Value = serde_json::from_str(row).unwrap();
            assert_eq!(tree["index"], index);
            assert!(tree["top_level_arrival_ms"].is_null());
            assert_eq!(tree["nodes"].as_array().unwrap().len(), 2);
        }
    }
}
