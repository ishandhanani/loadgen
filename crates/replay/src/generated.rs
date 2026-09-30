// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::sync::Arc;

use anyhow::{Context, Result};
use uuid::Uuid;

use super::artifacts::{SummaryIdentity, summarize, write_json, write_timeline};
use super::causal::{CausalWorkload, Chunk, ChunkNode, execute_causally};
use super::request::{
    connect, in_flight_limit, normalize_target, prepare_request, validate_options,
};
use super::{PreparedRequest, ReplayOptions, RequestExecution, RunSummary, TrafficKind};
use crate::headers::{HeaderInput, Headers, render, stable_uuid};
use crate::token_shape::TokenDictionary;
use agent_loadgen_core::{AgentContext, HeaderDialect, TraceManifest};
use agent_loadgen_generate::scenario::{
    GeneratedSessionTopology, PlannedTree, ResolvedLoad, TreePlanner,
};

/// Run a generated load, planning each top-level session tree when it starts.
pub async fn run_generated(
    planner: &TreePlanner,
    dictionary: TokenDictionary,
    options: ReplayOptions,
) -> Result<RunSummary> {
    validate_options(&options)?;
    let in_flight_limit = in_flight_limit()?;
    fs::create_dir_all(&options.output_dir).with_context(|| {
        format!(
            "failed to create output directory {}",
            options.output_dir.display()
        )
    })?;

    let target = normalize_target(&options.target);
    let (client, http_version) = connect(&options, &target, in_flight_limit).await?;
    let run_id = Uuid::new_v4();
    // A fresh salt per run keeps a repeated run from reusing KV cached by an
    // earlier one, while the shared system prompt and tool catalog stay warm.
    let dictionary = dictionary.with_salt(run_id.as_u64_pair().0, planner.shared_prefix_blocks());
    let run_id = run_id.to_string();
    let token_manifest = dictionary.manifest().clone();
    let options = Arc::new(options);
    let mut workload = GeneratedWorkload {
        planner,
        dictionary: &dictionary,
        client: client.clone(),
        target: target.clone(),
        run_id: run_id.clone(),
        options: Arc::clone(&options),
        next_scheduled: 0,
        pending: HashMap::new(),
        totals: Totals::default(),
    };
    let execution = execute_causally(&mut workload, client, &options, in_flight_limit).await?;
    let config = planner.config();
    let totals = workload.totals;
    let source = TraceManifest {
        request_count: totals.requests,
        session_count: totals.sessions,
        requests_with_agent_context: totals.requests,
        first_request_received_ms: 0,
        last_request_received_ms: totals.last_start_ms,
        duration_ms: config.load.duration_ms().unwrap_or(totals.last_start_ms),
        input_tokens: totals.input_tokens,
        output_tokens: totals.output_tokens,
        distinct_sequence_hashes: 0,
        trace_block_size: config.block_size,
        source_digest_sha256: planner.profile_digest_sha256().to_string(),
    };
    let mut summary = summarize(
        SummaryIdentity {
            traffic_kind: TrafficKind::SyntheticKvShape,
            run_id,
            target: &target,
            http_version,
            in_flight_limit,
            peak_in_flight: execution.peak_in_flight,
        },
        &source,
        &token_manifest,
        &options,
        &execution.accumulator,
        execution.elapsed,
        execution.stopped_early,
    );
    summary.session_topology = Some(GeneratedSessionTopology {
        configured_top_level_sessions: totals.trees,
        configured_concurrent_sessions: match &config.load {
            ResolvedLoad::Streams {
                concurrent_sessions,
                ..
            } => Some(*concurrent_sessions),
            ResolvedLoad::Arrivals { .. } => None,
        },
        generated_subagent_sessions: totals.sessions - totals.trees,
        total_protocol_sessions: totals.sessions,
    });
    write_json(&options.output_dir.join("run.json"), &summary)?;
    write_timeline(
        &options.output_dir.join("timeline.csv"),
        &execution.accumulator,
    )?;
    Ok(summary)
}

#[derive(Default)]
struct Totals {
    trees: usize,
    sessions: usize,
    requests: usize,
    input_tokens: u64,
    output_tokens: u64,
    last_start_ms: u64,
}

struct GeneratedWorkload<'a> {
    planner: &'a TreePlanner,
    dictionary: &'a TokenDictionary,
    client: reqwest::Client,
    target: String,
    run_id: String,
    options: Arc<ReplayOptions>,
    /// Next tree index with a scheduled start.
    next_scheduled: usize,
    /// Successor trees planned to read their restart delay.
    pending: HashMap<usize, PlannedTree>,
    totals: Totals,
}

struct TreeRequests {
    tree: PlannedTree,
    headers: Vec<Headers>,
}

impl CausalWorkload for GeneratedWorkload<'_> {
    type Payload = TreeRequests;

    fn name(&self) -> &'static str {
        "generated load"
    }

    fn next_scheduled(&mut self) -> Result<Option<(u64, usize)>> {
        let index = self.next_scheduled;
        let start_ms = match (&self.planner.config().load, self.planner.arrivals()) {
            (_, Some(arrivals)) => arrivals.arrival_ms.get(index).copied(),
            (
                ResolvedLoad::Streams {
                    num_sessions,
                    concurrent_sessions,
                    startup_interval_ms,
                    ..
                },
                None,
            ) => (index < *concurrent_sessions
                && num_sessions.is_none_or(|sessions| index < sessions))
            .then(|| index as u64 * startup_interval_ms),
            (ResolvedLoad::Arrivals { .. }, None) => None,
        };
        if start_ms.is_some() {
            self.next_scheduled += 1;
            self.totals.last_start_ms = self.totals.last_start_ms.max(start_ms.unwrap_or(0));
        }
        Ok(start_ms.map(|start_ms| (start_ms, index)))
    }

    fn successor(&mut self, tree: usize) -> Result<Option<(u64, usize)>> {
        let ResolvedLoad::Streams {
            num_sessions,
            concurrent_sessions,
            ..
        } = &self.planner.config().load
        else {
            return Ok(None);
        };
        let next = tree + concurrent_sessions;
        if num_sessions.is_some_and(|sessions| next >= sessions) {
            return Ok(None);
        }
        let planned = self.planner.plan_tree(next)?;
        let delay_ms = planned.restart_delay_ms;
        self.pending.insert(next, planned);
        Ok(Some((delay_ms, next)))
    }

    fn load(&mut self, index: usize) -> Result<Chunk<TreeRequests>> {
        let tree = match self.pending.remove(&index) {
            Some(tree) => tree,
            None => self.planner.plan_tree(index)?,
        };
        let headers = tree_headers(&tree, self.options.header_dialect, &self.run_id)?;
        self.totals.trees += 1;
        self.totals.sessions += tree.sessions.len();
        self.totals.requests += tree.nodes.len();
        self.totals.input_tokens += tree.total_input_tokens;
        self.totals.output_tokens += tree
            .nodes
            .iter()
            .map(|node| u64::from(node.request.output_tokens))
            .sum::<u64>();
        Ok(Chunk {
            nodes: tree
                .nodes
                .iter()
                .enumerate()
                .map(|(local, node)| ChunkNode {
                    dependencies: node.dependencies.clone(),
                    start_offset_ms: (local == 0).then_some(0),
                    delay_after_dependencies_ms: node.delay_after_dependencies_ms,
                })
                .collect(),
            anchor: Some(tree.root_final),
            payload: TreeRequests { tree, headers },
        })
    }

    fn prepare(
        &self,
        payload: &TreeRequests,
        node: usize,
        ordinal: usize,
    ) -> Result<PreparedRequest> {
        let planned = payload
            .tree
            .nodes
            .get(node)
            .with_context(|| format!("generated tree node {node} is missing"))?;
        let mut request = planned.request.clone();
        request.ordinal = ordinal;
        prepare_request(
            &self.client,
            &self.target,
            &self.run_id,
            &self.options,
            self.dictionary,
            request,
            RequestExecution {
                headers: payload.headers[node].clone(),
                output_budget_tokens: planned.output_budget_tokens,
                compaction_attempt: planned.compaction_attempt.clone(),
                session_final: planned.session_final,
            },
        )
    }

    fn start_deadline_ms(&self) -> Option<u64> {
        match &self.planner.config().load {
            ResolvedLoad::Streams { .. } => self.planner.config().load.duration_ms(),
            ResolvedLoad::Arrivals { .. } => None,
        }
    }
}

/// Render one tree's harness headers.
///
/// Wire session IDs are UUIDs derived from the run and the planned session,
/// so repeated runs of one plan never share session identity. A retried tool
/// call aggregates two attempts, so its batch reports no per-call durations.
fn tree_headers(tree: &PlannedTree, dialect: HeaderDialect, run_id: &str) -> Result<Vec<Headers>> {
    let session_ids = tree
        .sessions
        .iter()
        .map(|session| session.session_id.as_str())
        .collect::<BTreeSet<_>>();
    let wire = |id: &str| {
        if session_ids.contains(id) {
            stable_uuid(&[run_id, id])
        } else {
            id.to_string()
        }
    };
    let contexts = tree
        .nodes
        .iter()
        .map(|node| {
            let context = node
                .request
                .agent_context
                .as_ref()
                .context("generated node is missing agent_context")?;
            Ok(AgentContext {
                session_id: wire(&context.session_id),
                parent_session_id: context.parent_session_id.as_deref().map(wire),
                ..context.clone()
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let requests = tree
        .nodes
        .iter()
        .zip(&contexts)
        .map(|(node, context)| HeaderInput {
            context,
            dependencies: &node.dependencies,
            launched_tools: if node.tool_events.iter().any(|tool| tool.retried) {
                Vec::new()
            } else {
                node.tool_events
                    .iter()
                    .map(|tool| (tool.class.as_str(), tool.latency_ms))
                    .collect()
            },
        })
        .collect::<Vec<_>>();
    render(dialect, &requests)
}
