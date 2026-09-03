// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use anyhow::{Context, Result};
use uuid::Uuid;

use super::artifacts::{SummaryIdentity, summarize, write_json};
use super::causal::{CausalWorkload, execute_causally};
use super::request::{
    build_http_client, normalize_target, prepare_open_file_limit, prepare_request, validate_options,
};
use super::{PreparedRequest, ReplayOptions, RequestExecution, RunSummary, TrafficKind};
use crate::token_shape::TokenDictionary;
use agent_loadgen_trace::AgenticTrace;

pub async fn run_agentic_replay(
    trace: AgenticTrace,
    dictionary: TokenDictionary,
    options: ReplayOptions,
) -> Result<RunSummary> {
    validate_options(&options)?;
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
    let token_manifest = dictionary.manifest().clone();
    let options = Arc::new(options);
    let session_finals = session_final_flags(&trace);
    let workload = CapturedWorkload {
        request_factory: CapturedRequestFactory {
            trace: &trace,
            session_finals,
            dictionary: &dictionary,
            client: client.clone(),
            target: target.clone(),
            run_id: run_id.clone(),
            options: Arc::clone(&options),
        },
    };
    let execution = execute_causally(&workload, client, &target, &options).await?;
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
    );
    write_json(&options.output_dir.join("run.json"), &summary)?;
    Ok(summary)
}

struct CapturedWorkload<'a> {
    request_factory: CapturedRequestFactory<'a>,
}

impl CausalWorkload for CapturedWorkload<'_> {
    fn node_count(&self) -> usize {
        self.request_factory.trace.turns.len()
    }

    fn dependencies(&self, ordinal: usize) -> &[usize] {
        &self.request_factory.trace.turns[ordinal].dependencies
    }

    fn initial_arrival_ms(&self, ordinal: usize) -> Option<u64> {
        self.request_factory.trace.turns[ordinal].root_arrival_ms
    }

    fn delay_after_dependencies_ms(&self, ordinal: usize) -> u64 {
        self.request_factory.trace.turns[ordinal].delay_after_dependencies_ms
    }

    fn prepare(&self, ordinal: usize) -> Result<PreparedRequest> {
        self.request_factory.prepare(ordinal)
    }

    fn name(&self) -> &'static str {
        "captured agentic replay"
    }
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

struct CapturedRequestFactory<'a> {
    trace: &'a AgenticTrace,
    session_finals: Vec<bool>,
    dictionary: &'a TokenDictionary,
    client: reqwest::Client,
    target: String,
    run_id: String,
    options: Arc<ReplayOptions>,
}

impl CapturedRequestFactory<'_> {
    fn prepare(&self, ordinal: usize) -> Result<PreparedRequest> {
        let turn = self
            .trace
            .turns
            .get(ordinal)
            .with_context(|| format!("agentic replay turn {ordinal} is missing"))?;
        prepare_request(
            &self.client,
            &self.target,
            &self.run_id,
            &self.options,
            self.dictionary,
            turn.request.clone(),
            RequestExecution {
                output_budget_tokens: Some(turn.request.output_tokens),
                compaction_attempt: None,
                session_final: self.session_finals[ordinal],
            },
        )
    }
}
