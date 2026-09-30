// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use anyhow::{Context, Result};
use uuid::Uuid;

use super::artifacts::{SummaryIdentity, summarize, write_json, write_timeline};
use super::causal::{CausalWorkload, Chunk, ChunkNode, execute_causally};
use super::request::{
    build_http_client, normalize_target, prepare_open_file_limit, prepare_request, validate_options,
};
use super::{PreparedRequest, ReplayOptions, RequestExecution, RunSummary, TrafficKind};
use crate::headers::{HeaderInput, Headers, render};
use crate::token_shape::TokenDictionary;
use agent_loadgen_core::HeaderDialect;
use agent_loadgen_trace::AgenticTrace;

pub async fn run_agentic_replay(
    trace: AgenticTrace,
    dictionary: TokenDictionary,
    options: ReplayOptions,
) -> Result<RunSummary> {
    validate_options(&options)?;
    let headers = captured_headers(&trace, options.header_dialect)?;
    prepare_open_file_limit(options.max_in_flight)?;
    fs::create_dir_all(&options.output_dir).with_context(|| {
        format!(
            "failed to create output directory {}",
            options.output_dir.display()
        )
    })?;

    let client = build_http_client(&options)?;
    let target = normalize_target(&options.target);
    let run_id = Uuid::new_v4().to_string();
    let dictionary = dictionary.with_salt(options.token_salt, 0);
    let token_manifest = dictionary.manifest().clone();
    let options = Arc::new(options);
    let session_finals = session_final_flags(&trace);
    let mut workload = CapturedWorkload {
        trace: &trace,
        session_finals,
        headers,
        dictionary: &dictionary,
        client: client.clone(),
        target: target.clone(),
        run_id: run_id.clone(),
        options: Arc::clone(&options),
        scheduled: false,
    };
    let execution = execute_causally(&mut workload, client, &target, &options).await?;
    let summary = summarize(
        SummaryIdentity {
            traffic_kind: TrafficKind::CapturedTrace,
            run_id,
            target: &target,
        },
        &trace.manifest,
        &token_manifest,
        &options,
        &execution.accumulator,
        execution.elapsed,
        execution.stopped_early,
    );
    write_json(&options.output_dir.join("run.json"), &summary)?;
    write_timeline(
        &options.output_dir.join("timeline.csv"),
        &execution.accumulator,
    )?;
    Ok(summary)
}

/// The whole captured trace runs as one chunk that starts at time zero.
struct CapturedWorkload<'a> {
    trace: &'a AgenticTrace,
    session_finals: Vec<bool>,
    headers: Vec<Headers>,
    dictionary: &'a TokenDictionary,
    client: reqwest::Client,
    target: String,
    run_id: String,
    options: Arc<ReplayOptions>,
    scheduled: bool,
}

impl CausalWorkload for CapturedWorkload<'_> {
    type Payload = ();

    fn name(&self) -> &'static str {
        "captured agentic replay"
    }

    fn next_scheduled(&mut self) -> Result<Option<(u64, usize)>> {
        Ok((!std::mem::replace(&mut self.scheduled, true)).then_some((0, 0)))
    }

    fn successor(&mut self, _chunk: usize) -> Result<Option<(u64, usize)>> {
        Ok(None)
    }

    fn load(&mut self, _chunk: usize) -> Result<Chunk<()>> {
        Ok(Chunk {
            nodes: self
                .trace
                .turns
                .iter()
                .map(|turn| ChunkNode {
                    dependencies: turn.dependencies.clone(),
                    start_offset_ms: turn.root_arrival_ms,
                    delay_after_dependencies_ms: turn.delay_after_dependencies_ms,
                })
                .collect(),
            anchor: None,
            payload: (),
        })
    }

    fn prepare(&self, _payload: &(), node: usize, ordinal: usize) -> Result<PreparedRequest> {
        let turn = self
            .trace
            .turns
            .get(node)
            .with_context(|| format!("agentic replay turn {node} is missing"))?;
        let mut request = turn.request.clone();
        request.ordinal = ordinal;
        prepare_request(
            &self.client,
            &self.target,
            &self.run_id,
            &self.options,
            self.dictionary,
            request,
            RequestExecution {
                headers: self.headers[node].clone(),
                output_budget_tokens: Some(turn.request.output_tokens),
                compaction_attempt: None,
                session_final: self.session_finals[node],
            },
        )
    }
}

/// Render harness headers for every turn before dispatch.
///
/// Captured tool events carry profile-free classes without result collection
/// order, so only recorded Claude Code headers supply previous tool durations.
fn captured_headers(trace: &AgenticTrace, dialect: HeaderDialect) -> Result<Vec<Headers>> {
    let requests = trace
        .turns
        .iter()
        .map(|turn| {
            Ok(HeaderInput {
                context: turn
                    .request
                    .agent_context
                    .as_ref()
                    .context("captured replay requires agent_context")?,
                dependencies: &turn.dependencies,
                launched_tools: Vec::new(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    render(dialect, &requests)
}

/// Flag the last request of each captured session by ordinal.
///
/// Turn index equals request ordinal, and ordinals follow recorded arrival, so
/// the highest index per session ID is that session's final model request.
fn session_final_flags(trace: &AgenticTrace) -> Vec<bool> {
    let mut finals: HashMap<&str, usize> = HashMap::with_capacity(trace.manifest.session_count);
    for (ordinal, turn) in trace.turns.iter().enumerate() {
        if let Some(context) = &turn.request.agent_context {
            finals.insert(context.session_id.as_str(), ordinal);
        }
    }
    let mut flags = vec![false; trace.turns.len()];
    for ordinal in finals.into_values() {
        flags[ordinal] = true;
    }
    flags
}
