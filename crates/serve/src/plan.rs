// SPDX-License-Identifier: Apache-2.0

//! In-process `plan` previews: strict TOML -> the CLI's `plan-summary.json`
//! plus compact swimlane data for the first top-level trees.

use std::time::Instant;

use agent_loadgen_generate::scenario::{GeneratorConfig, PlanSummary, TreePlanner};
use anyhow::Result;
use serde::Serialize;
use sha2::{Digest, Sha256};

/// Largest accepted TOML body.
pub const MAX_TOML_BYTES: usize = 64_000;
/// The swimlane adds whole trees until it holds at least this many requests.
pub const TIMELINE_NODE_LIMIT: usize = 2_000;

pub const GENERATE_COMMAND: &str = "agent-loadgen generate --config scenario.toml \\\n  --headers claude-code --model <served-model-name> \\\n  --target http://<your-dynamo-frontend>:8000 \\\n  --tokenizer <hf-model-id-or-local-dir> \\\n  --output ./generate-run";

#[derive(Debug, Clone, Serialize)]
pub struct PlanResponse {
    /// The `plan-summary.json` that `agent-loadgen plan` writes.
    pub summary: PlanSummary,
    pub timeline: Timeline,
    pub command: &'static str,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Timeline {
    /// Top-level trees drawn, from the first one.
    pub trees: usize,
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
    /// Request ordinal across the drawn trees.
    pub o: usize,
    /// Planned action: `text`, `tool`, `parallel_tools`, `subagent`, `swarm`,
    /// `compaction_attempt`, or `complete`.
    pub a: String,
    #[serde(rename = "in")]
    pub input_tokens: usize,
    pub out: u32,
    pub tools: usize,
    pub wait: u64,
    pub spawn: Vec<usize>,
    pub compaction: bool,
}

/// Swimlane data for the first trees. Dependent requests have no wall-clock
/// arrival, so the x-axis is request ordinal, not time.
pub fn timeline(planner: &TreePlanner) -> Result<Timeline> {
    let mut timeline = Timeline {
        trees: 0,
        sessions: Vec::new(),
        nodes: Vec::new(),
    };
    while planner
        .tree_count()
        .is_none_or(|count| timeline.trees < count)
        && timeline.nodes.len() < TIMELINE_NODE_LIMIT
    {
        let tree = planner.plan_tree(timeline.trees)?;
        let first_session = timeline.sessions.len();
        let index = |session_id: &str| {
            tree.sessions
                .iter()
                .position(|session| session.session_id == session_id)
                .map(|local| first_session + local)
        };
        let first_node = timeline.nodes.len();
        timeline
            .sessions
            .extend(tree.sessions.iter().map(|session| TimelineSession {
                id: session.session_id.clone(),
                parent: session.parent_session_id.as_deref().and_then(index),
                depth: session.depth,
            }));
        timeline
            .nodes
            .extend(tree.nodes.iter().enumerate().filter_map(|(local, node)| {
                let session_id = node.request.agent_context.as_ref()?.session_id.as_str();
                Some(TimelineNode {
                    s: index(session_id)?,
                    o: first_node + local,
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
            }));
        timeline.trees += 1;
    }
    Ok(timeline)
}

pub fn toml_sha256(text: &str) -> String {
    hex::encode(Sha256::digest(text.as_bytes()))
}

/// Plan a profile exactly as the `plan` subcommand does. Errors are invalid
/// profiles, reported as HTTP 422.
pub fn run_plan(toml_text: &str) -> Result<PlanResponse, String> {
    let started = Instant::now();
    let plan = || -> Result<PlanResponse> {
        let planner = TreePlanner::new(GeneratorConfig::parse(toml_text)?.resolve()?)?;
        Ok(PlanResponse {
            summary: PlanSummary::sample(&planner)?,
            timeline: timeline(&planner)?,
            command: GENERATE_COMMAND,
            elapsed_ms: 0,
        })
    };
    let mut response = plan().map_err(|error| format!("{error:#}"))?;
    response.elapsed_ms = started.elapsed().as_millis() as u64;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small plan: the neutral defaults with four top-level sessions.
    fn small_profile() -> String {
        "schema_version = 5\nseed = 42\n[load]\nnum_sessions = 4\nconcurrent_sessions = 2\n"
            .to_string()
    }

    #[test]
    fn timeline_follows_the_planned_trees() {
        let planner = TreePlanner::new(
            GeneratorConfig::parse(&small_profile())
                .unwrap()
                .resolve()
                .unwrap(),
        )
        .unwrap();
        let timeline = timeline(&planner).unwrap();
        assert_eq!(timeline.trees, 4);
        let trees = (0..4)
            .map(|index| planner.plan_tree(index).unwrap())
            .collect::<Vec<_>>();
        let sessions = trees.iter().flat_map(|tree| &tree.sessions);
        let nodes = trees
            .iter()
            .flat_map(|tree| &tree.nodes)
            .collect::<Vec<_>>();
        assert_eq!(timeline.sessions.len(), sessions.count());
        assert_eq!(timeline.nodes.len(), nodes.len());
        for (ordinal, (node, planned)) in timeline.nodes.iter().zip(&nodes).enumerate() {
            assert_eq!(node.o, ordinal);
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
    }

    #[test]
    fn plan_reports_the_cli_summary() {
        let response = run_plan(&small_profile()).unwrap();
        assert_eq!(response.command, GENERATE_COMMAND);
        assert_eq!(response.summary.top_level_sessions, Some(4));
        assert_eq!(response.summary.sampled_trees, 4);
        assert!(response.summary.per_request.input_tokens.mean > 0.0);
    }

    #[test]
    fn schema_four_reports_migration_message() {
        let error = run_plan("schema_version = 4\nseed = 1\n").unwrap_err();
        assert!(error.contains("schema 4 is no longer supported"), "{error}");
    }
}
