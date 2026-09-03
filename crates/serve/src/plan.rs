// SPDX-License-Identifier: Apache-2.0

//! In-process `plan` previews: strict TOML -> generated scenario -> compact
//! swimlane data for the dashboard, plus `plan.dot` / optional `plan.svg` and a
//! stored `scenario.json`.

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

use agent_loadgen_generate::scenario::{
    GeneratedScenario, GeneratedToolParallelism, GeneratorConfig, render_plan_dot, write_plan_graph,
};
use anyhow::{Context, Result};
use flate2::write::GzEncoder;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::jobs::JobDir;

/// Largest accepted TOML body.
pub const MAX_TOML_BYTES: usize = 64_000;
/// Graphviz is only invoked for scenarios up to this many nodes.
pub const SVG_NODE_LIMIT: usize = 500;
/// Scenarios up to this serialized size are stored gzipped; larger ones are
/// regenerated on the first `GET`.
pub const SCENARIO_STORE_LIMIT: usize = 64 * 1024 * 1024;

pub const GENERATE_COMMAND: &str = "agent-loadgen generate --config scenario.toml \\\n  --headers dynamo --model <served-model-name> \\\n  --target http://<your-dynamo-frontend>:8000 \\\n  --tokenizer <hf-model-id-or-local-dir> \\\n  --output ./generate-run";

#[derive(Debug, Clone, Serialize)]
pub struct PlanResponse {
    pub job_id: String,
    pub summary: PlanSummary,
    pub timeline: Timeline,
    pub has_svg: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub svg_error: Option<String>,
    pub scenario_bytes: usize,
    pub command: &'static str,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlanSummary {
    pub sessions: usize,
    pub nodes: usize,
    pub top_level_sessions: usize,
    pub compaction_operations: usize,
    pub total_input_tokens: u64,
    pub total_output_budget_tokens: u64,
    pub tool_parallelism: GeneratedToolParallelism,
    pub scenario_digest: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Timeline {
    pub sessions: Vec<TimelineSession>,
    pub nodes: Vec<TimelineNode>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TimelineSession {
    pub id: String,
    pub parent: Option<usize>,
    pub depth: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TimelineNode {
    /// Session index into `Timeline::sessions`.
    pub s: usize,
    /// Request ordinal within the scenario.
    pub o: usize,
    /// Action name as serialized in `scenario.json`.
    pub a: String,
    #[serde(rename = "in")]
    pub input_tokens: usize,
    pub out: u32,
    pub tools: usize,
    pub wait: u64,
    pub spawn: Vec<usize>,
    pub compaction: bool,
}

pub fn summary(scenario: &GeneratedScenario) -> PlanSummary {
    PlanSummary {
        sessions: scenario.sessions.len(),
        nodes: scenario.nodes.len(),
        top_level_sessions: scenario
            .sessions
            .iter()
            .filter(|session| session.parent_session_id.is_none())
            .count(),
        compaction_operations: scenario.compaction_operations.len(),
        total_input_tokens: scenario
            .nodes
            .iter()
            .map(|node| node.request.input_tokens as u64)
            .sum(),
        total_output_budget_tokens: scenario
            .nodes
            .iter()
            .filter_map(|node| node.output_budget_tokens)
            .map(u64::from)
            .sum(),
        tool_parallelism: scenario.tool_parallelism.clone(),
        scenario_digest: scenario.scenario_digest_sha256.clone(),
    }
}

/// Compact swimlane data. Dependent nodes have no wall-clock arrival, so the
/// x-axis is request ordinal, not time.
pub fn timeline(scenario: &GeneratedScenario) -> Timeline {
    let index = |session_id: &str| {
        scenario
            .sessions
            .iter()
            .position(|session| session.session_id == session_id)
    };
    let sessions = scenario
        .sessions
        .iter()
        .map(|session| TimelineSession {
            id: session.session_id.clone(),
            parent: session.parent_session_id.as_deref().and_then(index),
            depth: session.depth,
        })
        .collect();
    let nodes = scenario
        .nodes
        .iter()
        .filter_map(|node| {
            let session_id = node.request.agent_context.as_ref()?.session_id.as_str();
            let s = index(session_id)?;
            Some(TimelineNode {
                s,
                o: node.request.ordinal,
                a: node.action.clone(),
                input_tokens: node.request.input_tokens,
                out: node
                    .output_budget_tokens
                    .unwrap_or(node.request.output_tokens),
                tools: node.tool_events.len(),
                wait: node.tool_events.iter().map(|event| event.latency_ms).sum(),
                spawn: node
                    .spawned_session_ids
                    .iter()
                    .filter_map(|id| index(id))
                    .collect(),
                compaction: node.compaction_attempt.is_some(),
            })
        })
        .collect();
    Timeline { sessions, nodes }
}

pub fn toml_sha256(text: &str) -> String {
    hex::encode(Sha256::digest(text.as_bytes()))
}

/// Why a plan request failed.
#[derive(Debug)]
pub enum PlanFailure {
    /// The profile is invalid or exceeds generator limits; reported as HTTP 422.
    Invalid(String),
    /// Filesystem or serialization failure inside the service.
    Internal(anyhow::Error),
}

impl From<anyhow::Error> for PlanFailure {
    fn from(error: anyhow::Error) -> Self {
        Self::Internal(error)
    }
}

/// Generate the scenario exactly as the `plan` subcommand does.
pub fn generate_scenario(toml_text: &str) -> Result<GeneratedScenario, String> {
    GeneratorConfig::parse(toml_text)
        .and_then(GeneratorConfig::resolve)
        .and_then(GeneratedScenario::generate)
        .map_err(|error| format!("{error:#}"))
}

/// Run a plan job synchronously on the current (blocking) thread.
pub fn run_plan_blocking(job: &JobDir, toml_text: &str) -> Result<PlanResponse, PlanFailure> {
    let started = Instant::now();
    fs::write(job.path.join("config.toml"), toml_text)
        .with_context(|| format!("failed to write {}", job.path.display()))?;
    let scenario = generate_scenario(toml_text).map_err(PlanFailure::Invalid)?;

    let serialized =
        serde_json::to_vec_pretty(&scenario).context("failed to serialize scenario")?;
    let scenario_bytes = serialized.len();
    if scenario_bytes <= SCENARIO_STORE_LIMIT {
        let gz_path = job.path.join("scenario.json.gz");
        let file = fs::File::create(&gz_path)
            .with_context(|| format!("failed to create {}", gz_path.display()))?;
        let mut encoder = GzEncoder::new(file, flate2::Compression::fast());
        encoder
            .write_all(&serialized)
            .and_then(|()| encoder.finish().map(drop))
            .context("failed to write scenario.json.gz")?;
    }
    drop(serialized);

    let (has_svg, svg_error) = if scenario.nodes.len() <= SVG_NODE_LIMIT {
        match write_plan_graph(&job.path, &scenario) {
            Ok(artifacts) => (artifacts.svg_path.is_some(), None),
            Err(error) => {
                let message = format!("{error:#}");
                tracing::warn!(job = %job.id, "plan graph: {message}");
                // Keep plan.dot even when Graphviz failed after the DOT write.
                if !job.path.join("plan.dot").is_file() {
                    fs::write(job.path.join("plan.dot"), render_plan_dot(&scenario)?)
                        .context("failed to write plan.dot")?;
                }
                (false, Some(message))
            }
        }
    } else {
        fs::write(job.path.join("plan.dot"), render_plan_dot(&scenario)?)
            .context("failed to write plan.dot")?;
        (false, None)
    };

    Ok(PlanResponse {
        job_id: job.id.clone(),
        summary: summary(&scenario),
        timeline: timeline(&scenario),
        has_svg,
        svg_error,
        scenario_bytes,
        command: GENERATE_COMMAND,
        elapsed_ms: started.elapsed().as_millis() as u64,
    })
}

/// Regenerate `scenario.json` from the stored `config.toml` for scenarios that
/// were too large to keep gzipped. Returns the plain file path.
pub fn materialize_scenario_blocking(job: &JobDir) -> Result<PathBuf> {
    let toml_text = fs::read_to_string(job.path.join("config.toml"))
        .with_context(|| format!("job {} has no config.toml", job.id))?;
    let scenario = generate_scenario(&toml_text)
        .map_err(|error| anyhow::anyhow!("stored config no longer plans: {error}"))?;
    let path = job.path.join("scenario.json");
    let tmp = job.path.join("scenario.json.tmp");
    let writer = std::io::BufWriter::new(fs::File::create(&tmp)?);
    serde_json::to_writer_pretty(writer, &scenario).context("failed to write scenario.json")?;
    fs::rename(&tmp, &path)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tools_heavy() -> String {
        fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../profiles/tools-heavy.toml"
        ))
        .unwrap()
    }

    #[test]
    fn timeline_and_summary_follow_the_scenario() {
        let scenario = generate_scenario(&tools_heavy()).unwrap();
        let timeline = timeline(&scenario);
        let summary = summary(&scenario);
        assert_eq!(timeline.sessions.len(), scenario.sessions.len());
        assert_eq!(timeline.nodes.len(), scenario.nodes.len());
        assert_eq!(summary.nodes, scenario.nodes.len());
        assert!(summary.total_input_tokens > 0);
        assert_eq!(summary.scenario_digest, scenario.scenario_digest_sha256);
        for (node, planned) in timeline.nodes.iter().zip(&scenario.nodes) {
            assert_eq!(node.o, planned.request.ordinal);
            assert_eq!(node.a, planned.action);
            assert_eq!(
                timeline.sessions[node.s].id,
                planned.request.agent_context.as_ref().unwrap().session_id
            );
            assert_eq!(node.tools, planned.tool_events.len());
            for spawned in &node.spawn {
                assert!(
                    planned
                        .spawned_session_ids
                        .contains(&timeline.sessions[*spawned].id)
                );
            }
        }
        for session in &timeline.sessions {
            if let Some(parent) = session.parent {
                assert!(timeline.sessions[parent].depth < session.depth);
            }
        }
        assert!(
            timeline.nodes.iter().all(|node| [
                "text",
                "tool",
                "parallel_tools",
                "subagent",
                "swarm",
                "complete"
            ]
            .contains(&node.a.as_str())),
            "unexpected action name"
        );
    }

    #[test]
    fn schema_four_reports_migration_message() {
        let error = generate_scenario("schema_version = 4\nseed = 1\n").unwrap_err();
        assert!(error.contains("schema 4 is no longer supported"), "{error}");
    }

    #[test]
    fn plan_job_writes_artifacts() {
        let dir = tempfile::tempdir().unwrap();
        let job = JobDir {
            id: "p-test".to_string(),
            path: dir.path().to_path_buf(),
        };
        let response = run_plan_blocking(&job, &tools_heavy()).unwrap();
        assert_eq!(response.command, GENERATE_COMMAND);
        assert!(response.scenario_bytes > 0);
        assert!(dir.path().join("plan.dot").is_file());
        assert!(dir.path().join("scenario.json.gz").is_file());
        assert!(dir.path().join("config.toml").is_file());
        assert_eq!(response.has_svg, dir.path().join("plan.svg").is_file());
        let materialized = materialize_scenario_blocking(&job).unwrap();
        assert_eq!(
            fs::metadata(materialized).unwrap().len() as usize,
            response.scenario_bytes
        );
    }
}
