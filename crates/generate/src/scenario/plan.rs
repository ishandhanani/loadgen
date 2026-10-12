// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Exp};
use sha2::{Digest, Sha256};

use super::config::{Interpolation, RateUnit, ResolvedGeneratorConfig, ResolvedLoad, ToolClass};
use super::model::{
    CompactionExpectedEffect, GeneratedCompactionAttempt, GeneratedCompactionOperation,
    GeneratedNode, GeneratedSession, GeneratedToolEvent, PlannedTree,
};
use agent_loadgen_core::{AgentContext, TraceRequest};

/// Trees planned to convert a request-rate schedule into session arrivals.
const RATE_CALIBRATION_TREES: usize = 1_000;

/// Plans top-level session trees independently.
///
/// Each tree samples from its own random stream derived from the seed and the
/// tree index, so a run can plan trees lazily, in any order, with the same
/// result as planning them all up front.
pub struct TreePlanner {
    config: ResolvedGeneratorConfig,
    profile_digest_sha256: String,
    shared_prefix: Vec<u64>,
    shared_label_owners: BTreeMap<u64, String>,
    arrivals: Option<ArrivalPlan>,
}

/// Sampled open-loop tree start times.
#[derive(Debug, Clone)]
pub struct ArrivalPlan {
    /// Start offset of each tree, in tree-index order.
    pub arrival_ms: Vec<u64>,
    /// Session rate per bucket, after converting a request-rate schedule.
    pub sessions_per_minute: Vec<f64>,
    /// Mean requests per tree used for that conversion.
    pub calibration_requests_per_tree: Option<f64>,
}

impl TreePlanner {
    pub fn new(config: ResolvedGeneratorConfig) -> Result<Self> {
        config.validate()?;
        let profile_digest_sha256 = hex::encode(Sha256::digest(serde_json::to_vec(&config)?));
        let mut planner = Self {
            config,
            profile_digest_sha256,
            shared_prefix: Vec::new(),
            shared_label_owners: BTreeMap::new(),
            arrivals: None,
        };
        let mut shared = TreeBuilder::new(
            &planner,
            usize::MAX,
            stream_rng(planner.config.seed, "shared"),
        );
        let system_tokens = shared.config.system_prefix_tokens.sample(&mut shared.rng)?;
        let tool_tokens = shared.config.tool_catalog_tokens.sample(&mut shared.rng)?;
        let mut prefix = shared.segment("shared:system", system_tokens)?;
        prefix.extend(shared.segment("shared:tools", tool_tokens)?);
        let owners = std::mem::take(&mut shared.label_owners);
        planner.shared_prefix = prefix;
        planner.shared_label_owners = owners;
        planner.arrivals = planner.sample_arrivals()?;
        Ok(planner)
    }

    pub fn config(&self) -> &ResolvedGeneratorConfig {
        &self.config
    }

    pub fn profile_digest_sha256(&self) -> &str {
        &self.profile_digest_sha256
    }

    /// Blocks every request starts with: the system prompt and tool catalog.
    pub fn shared_prefix(&self) -> &[u64] {
        &self.shared_prefix
    }

    pub fn shared_prefix_blocks(&self) -> usize {
        self.shared_prefix.len()
    }

    /// Open-loop arrivals, when the load is an arrival schedule.
    pub fn arrivals(&self) -> Option<&ArrivalPlan> {
        self.arrivals.as_ref()
    }

    /// Number of trees, or `None` for streams that run until a duration.
    pub fn tree_count(&self) -> Option<usize> {
        match (&self.config.load, &self.arrivals) {
            (_, Some(arrivals)) => Some(arrivals.arrival_ms.len()),
            (ResolvedLoad::Streams { num_sessions, .. }, None) => *num_sessions,
            (ResolvedLoad::Arrivals { .. }, None) => Some(0),
        }
    }

    /// Plan one top-level session tree with tree-local ordinals.
    ///
    /// Node 0 is the tree's first request and has no dependencies.
    pub fn plan_tree(&self, index: usize) -> Result<PlannedTree> {
        let slot = match &self.config.load {
            ResolvedLoad::Streams {
                concurrent_sessions,
                ..
            } => index % concurrent_sessions,
            ResolvedLoad::Arrivals { .. } => index,
        };
        let mut builder = TreeBuilder::new(self, index, tree_rng(self.config.seed, index));
        let restart_delay_ms = match &self.config.load {
            ResolvedLoad::Streams {
                restart_delay_ms, ..
            } => restart_delay_ms.sample(&mut builder.rng)?,
            ResolvedLoad::Arrivals { .. } => 0,
        };
        let root_final = builder
            .generate_session(SessionLaunch {
                parent_session_id: None,
                depth: 0,
                first_dependencies: Vec::new(),
                first_delay_ms: 0,
                initial_arrival_ms: Some(0),
                top_level_session_index: index,
                top_level_stream_slot: slot,
            })?
            .with_context(|| format!("generation limits prevented top-level session {index}"))?;
        builder.mark_session_finals()?;
        Ok(PlannedTree {
            index,
            top_level_arrival_ms: self
                .arrivals
                .as_ref()
                .and_then(|arrivals| arrivals.arrival_ms.get(index).copied()),
            restart_delay_ms,
            root_final,
            total_input_tokens: builder.total_input_tokens,
            sessions: builder.sessions,
            nodes: builder.nodes,
            compaction_operations: builder.compaction_operations,
        })
    }

    fn sample_arrivals(&self) -> Result<Option<ArrivalPlan>> {
        let ResolvedLoad::Arrivals {
            bucket_minutes,
            rate_unit,
            rates,
            interpolation,
        } = &self.config.load
        else {
            return Ok(None);
        };
        let (sessions_per_minute, calibration_requests_per_tree) = match rate_unit {
            RateUnit::SessionsPerMinute => (rates.clone(), None),
            RateUnit::RequestsPerSecond => {
                let mut requests = 0;
                for index in 0..RATE_CALIBRATION_TREES {
                    requests += self.plan_tree(index)?.nodes.len();
                }
                let per_tree = requests as f64 / RATE_CALIBRATION_TREES as f64;
                let rates = rates.iter().map(|rate| rate * 60.0 / per_tree).collect();
                (rates, Some(per_tree))
            }
        };
        let bucket_ms = bucket_minutes * 60_000.0;
        let duration_ms = bucket_ms * sessions_per_minute.len() as f64;
        let rate_at = |t: f64| -> f64 {
            let last = sessions_per_minute.len() - 1;
            match interpolation {
                Interpolation::Step => sessions_per_minute[((t / bucket_ms) as usize).min(last)],
                Interpolation::Linear => {
                    let position = (t / bucket_ms - 0.5).clamp(0.0, last as f64);
                    let lower = position.floor() as usize;
                    let upper = (lower + 1).min(last);
                    let fraction = position - lower as f64;
                    sessions_per_minute[lower] * (1.0 - fraction)
                        + sessions_per_minute[upper] * fraction
                }
            }
        };
        // Thinning: propose at the peak rate, accept in proportion to the rate.
        let peak_per_ms = sessions_per_minute.iter().copied().fold(0.0, f64::max) / 60_000.0;
        let gaps = Exp::new(peak_per_ms).context("invalid peak arrival rate")?;
        let mut rng = stream_rng(self.config.seed, "arrivals");
        let mut arrival_ms = Vec::new();
        let mut t = 0.0;
        loop {
            t += gaps.sample(&mut rng);
            if t >= duration_ms {
                break;
            }
            if rng.random::<f64>() * peak_per_ms * 60_000.0 < rate_at(t) {
                arrival_ms.push(t as u64);
            }
        }
        if arrival_ms.is_empty() {
            bail!("the arrival schedule produced no sessions; raise the rates or the duration");
        }
        Ok(Some(ArrivalPlan {
            arrival_ms,
            sessions_per_minute,
            calibration_requests_per_tree,
        }))
    }
}

fn stream_rng(seed: u64, stream: &str) -> StdRng {
    let mut digest = Sha256::new();
    digest.update(b"agent-loadgen/rng/v1");
    digest.update(seed.to_le_bytes());
    digest.update(stream.as_bytes());
    StdRng::from_seed(digest.finalize().into())
}

fn tree_rng(seed: u64, index: usize) -> StdRng {
    stream_rng(seed, &format!("tree:{index}"))
}

/// Samples one top-level session tree.
struct TreeBuilder<'a> {
    config: &'a ResolvedGeneratorConfig,
    shared_prefix: &'a [u64],
    shared_label_owners: &'a BTreeMap<u64, String>,
    index: usize,
    rng: StdRng,
    sessions: Vec<GeneratedSession>,
    nodes: Vec<GeneratedNode>,
    compaction_operations: Vec<GeneratedCompactionOperation>,
    labels: BTreeMap<String, u64>,
    label_owners: BTreeMap<u64, String>,
    total_input_tokens: u64,
}

impl<'a> TreeBuilder<'a> {
    fn new(planner: &'a TreePlanner, index: usize, rng: StdRng) -> Self {
        Self {
            config: &planner.config,
            shared_prefix: &planner.shared_prefix,
            shared_label_owners: &planner.shared_label_owners,
            index,
            rng,
            sessions: Vec::new(),
            nodes: Vec::new(),
            compaction_operations: Vec::new(),
            labels: BTreeMap::new(),
            label_owners: BTreeMap::new(),
            total_input_tokens: 0,
        }
    }
}

struct SessionState {
    session_id: String,
    parent_session_id: Option<String>,
    blocks: Vec<u64>,
    stable_blocks: usize,
    window_epoch: usize,
}

struct SessionLaunch {
    parent_session_id: Option<String>,
    depth: usize,
    first_dependencies: Vec<usize>,
    first_delay_ms: u64,
    initial_arrival_ms: Option<u64>,
    top_level_session_index: usize,
    top_level_stream_slot: usize,
}

struct NodeSpec<'a> {
    dependencies: Vec<usize>,
    delay_after_dependencies_ms: u64,
    initial_arrival_ms: Option<u64>,
    action: &'a str,
    input_trigger: &'a str,
    logical_output_tokens: u64,
    output_budget_tokens: u64,
    compaction: Option<serde_json::Value>,
    compaction_attempt: Option<GeneratedCompactionAttempt>,
}

#[derive(Debug, Clone, Copy)]
enum NextAction {
    Text,
    Tool,
    ParallelTools,
    Subagent,
    Swarm,
    Complete,
}

impl NextAction {
    fn name(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Tool => "tool",
            Self::ParallelTools => "parallel_tools",
            Self::Subagent => "subagent",
            Self::Swarm => "swarm",
            Self::Complete => "complete",
        }
    }
}

impl TreeBuilder<'_> {
    fn generate_session(&mut self, launch: SessionLaunch) -> Result<Option<usize>> {
        let SessionLaunch {
            parent_session_id,
            depth,
            first_dependencies,
            first_delay_ms,
            initial_arrival_ms,
            top_level_session_index,
            top_level_stream_slot,
        } = launch;
        if self.sessions.len() >= self.config.max_sessions
            || self.nodes.len().saturating_add(1) > self.config.max_nodes
        {
            return Ok(None);
        }
        let session_id = format!(
            "generated-{:016x}-t{}-s{}",
            self.config.seed,
            self.index,
            self.sessions.len()
        );
        self.sessions.push(GeneratedSession {
            session_id: session_id.clone(),
            parent_session_id: parent_session_id.clone(),
            depth,
            top_level_session_index,
            top_level_stream_slot,
        });
        let repository_tokens = self.config.repository_tokens.sample(&mut self.rng)?;
        let session_tokens = self.config.session_tokens.sample(&mut self.rng)?;
        let mut blocks = self.shared_prefix.to_vec();
        blocks.extend(self.segment(
            &format!("top-level:{top_level_session_index}:repository"),
            repository_tokens,
        )?);
        blocks.extend(self.segment(&format!("session:{session_id}:environment"), session_tokens)?);
        let stable_blocks = blocks.len();
        let mut state = SessionState {
            session_id,
            parent_session_id,
            blocks,
            stable_blocks,
            window_epoch: 0,
        };
        let user_tokens = self.config.user_tokens.sample(&mut self.rng)?;
        state
            .blocks
            .extend(self.segment(&format!("session:{}:user:0", state.session_id), user_tokens)?);

        let turns = if depth == 0 {
            self.config.turns.sample(&mut self.rng)?
        } else {
            self.config.subagent_turns.sample(&mut self.rng)?
        };
        let turns = usize::try_from(turns).context("turn count does not fit usize")?;
        let minimum_turns = turns.min(2);
        let mut dependencies = first_dependencies;
        let mut delay_ms = first_delay_ms;
        let mut next_trigger = "user_message";
        let mut final_node = None;

        for turn in 0..turns {
            if self.nodes.len() >= self.config.max_nodes {
                break;
            }
            if self.compaction_due(&state) {
                let original_blocks = state.blocks.len();
                let summary_prompt_tokens =
                    self.config.summary_input_tokens.sample(&mut self.rng)?;
                let summary_prompt = self.segment(
                    &format!("session:{}:compaction-prompt:{turn}", state.session_id),
                    summary_prompt_tokens,
                )?;
                state.blocks.extend(summary_prompt);
                let summary_tokens = self.config.summary_output_tokens.sample(&mut self.rng)?;
                let operation = self.push_compaction_operation(
                    &state,
                    turn,
                    dependencies,
                    delay_ms,
                    initial_arrival_ms.filter(|_| final_node.is_none()),
                    summary_tokens,
                )?;
                final_node = operation.last().copied();
                dependencies = operation
                    .last()
                    .copied()
                    .map(|node| vec![node])
                    .unwrap_or_default();
                delay_ms = 0;
                state.blocks.truncate(original_blocks);
                self.compact_state(&mut state, turn, summary_tokens)?;
                next_trigger = "other";
            }

            let output_tokens = self.config.output_tokens.sample(&mut self.rng)?;
            let action = if turn + 1 == turns {
                NextAction::Complete
            } else {
                self.sample_action(depth, turn + 1 >= minimum_turns)
            };
            let node = self.push_request(
                &state,
                NodeSpec {
                    dependencies,
                    delay_after_dependencies_ms: delay_ms,
                    initial_arrival_ms: initial_arrival_ms.filter(|_| final_node.is_none()),
                    action: action.name(),
                    input_trigger: next_trigger,
                    logical_output_tokens: output_tokens,
                    output_budget_tokens: output_tokens,
                    compaction: None,
                    compaction_attempt: None,
                },
            )?;
            final_node = Some(node);
            state.blocks.extend(self.segment(
                &format!("session:{}:assistant:{turn}", state.session_id),
                output_tokens,
            )?);

            match action {
                NextAction::Complete => break,
                NextAction::Text => {
                    let user_tokens = self.config.user_tokens.sample(&mut self.rng)?;
                    state.blocks.extend(self.segment(
                        &format!("session:{}:user:{}", state.session_id, turn + 1),
                        user_tokens,
                    )?);
                    dependencies = vec![node];
                    delay_ms = self.config.think_time_ms.sample(&mut self.rng)?;
                    next_trigger = "user_message";
                }
                NextAction::Tool | NextAction::ParallelTools => {
                    let count = if matches!(action, NextAction::ParallelTools) {
                        usize::try_from(self.config.parallel_count.sample(&mut self.rng)?)
                            .context("parallel tool count does not fit usize")?
                            .max(2)
                    } else {
                        1
                    };
                    let mut max_latency = 0;
                    let mut tool_events = Vec::with_capacity(count);
                    for tool_index in 0..count {
                        let class = self.sample_tool()?.clone();
                        let mut latency = class.latency_ms.sample(&mut self.rng)?;
                        let mut result_tokens = class.result_tokens.sample(&mut self.rng)?;
                        let failed = self.rng.random::<f64>() < class.error_probability;
                        let retried =
                            failed && self.rng.random::<f64>() < self.config.retry_probability;
                        if retried {
                            latency =
                                latency.saturating_add(class.latency_ms.sample(&mut self.rng)?);
                            result_tokens = result_tokens
                                .saturating_add(class.result_tokens.sample(&mut self.rng)?);
                        }
                        max_latency = max_latency.max(latency);
                        state.blocks.extend(self.segment(
                            &format!(
                                "session:{}:tool:{turn}:{tool_index}:{}:{failed}",
                                state.session_id, class.name
                            ),
                            result_tokens,
                        )?);
                        tool_events.push(GeneratedToolEvent {
                            class: class.name,
                            latency_ms: latency,
                            result_tokens,
                            failed,
                            retried,
                        });
                    }
                    self.nodes[node].tool_events = tool_events;
                    dependencies = vec![node];
                    delay_ms = max_latency;
                    next_trigger = "tool_result";
                }
                NextAction::Subagent | NextAction::Swarm => {
                    let requested = if matches!(action, NextAction::Swarm) {
                        usize::try_from(self.config.fanout.sample(&mut self.rng)?)
                            .context("subagent fanout does not fit usize")?
                            .max(2)
                    } else {
                        1
                    };
                    let available = self.config.max_sessions.saturating_sub(self.sessions.len());
                    let child_count = requested.min(available);
                    let blocking = self.rng.random::<f64>() < self.config.blocking_probability;
                    let mut child_finals = Vec::new();
                    let mut spawned_session_ids = Vec::new();
                    for _ in 0..child_count {
                        let spawn_delay = self.config.spawn_delay_ms.sample(&mut self.rng)?;
                        let child_session_index = self.sessions.len();
                        if let Some(child_final) = self.generate_session(SessionLaunch {
                            parent_session_id: Some(state.session_id.clone()),
                            depth: depth + 1,
                            first_dependencies: vec![node],
                            first_delay_ms: spawn_delay,
                            initial_arrival_ms: None,
                            top_level_session_index,
                            top_level_stream_slot,
                        })? {
                            child_finals.push(child_final);
                            spawned_session_ids
                                .push(self.sessions[child_session_index].session_id.clone());
                        }
                    }
                    self.nodes[node].spawned_session_ids = spawned_session_ids;
                    let join_tokens = self.config.tool_result_tokens.sample(&mut self.rng)?;
                    state.blocks.extend(self.segment(
                        &format!("session:{}:join:{turn}", state.session_id),
                        join_tokens,
                    )?);
                    dependencies = if blocking && !child_finals.is_empty() {
                        child_finals
                    } else {
                        vec![node]
                    };
                    delay_ms = self.config.spawn_delay_ms.sample(&mut self.rng)?;
                    next_trigger = "tool_result";
                }
            }
        }
        Ok(Some(
            final_node.context("generated session has no model turn")?,
        ))
    }

    fn sample_action(&mut self, depth: usize, allow_completion: bool) -> NextAction {
        let value = self.rng.random::<f64>();
        let mut boundary = self.config.tool_probability;
        if value < boundary {
            return NextAction::Tool;
        }
        boundary += self.config.parallel_tool_probability;
        if value < boundary {
            return NextAction::ParallelTools;
        }
        if depth < self.config.max_depth {
            boundary += self.config.subagent_probability;
            if value < boundary {
                return NextAction::Subagent;
            }
            boundary += self.config.swarm_probability;
            if value < boundary {
                return NextAction::Swarm;
            }
        }
        boundary += if allow_completion {
            self.config.completion_probability
        } else {
            0.0
        };
        if allow_completion && value < boundary {
            NextAction::Complete
        } else {
            NextAction::Text
        }
    }

    fn sample_tool(&mut self) -> Result<&ToolClass> {
        let total = self
            .config
            .tool_classes
            .iter()
            .map(|class| class.weight)
            .sum::<f64>();
        let mut value = self.rng.random::<f64>() * total;
        for class in &self.config.tool_classes {
            if value < class.weight {
                return Ok(class);
            }
            value -= class.weight;
        }
        self.config
            .tool_classes
            .last()
            .context("tool class list is empty")
    }

    fn compaction_due(&self, state: &SessionState) -> bool {
        self.config.compaction_enabled
            && state.blocks.len() * self.config.block_size
                >= (self.config.context_window_tokens as f64
                    * self.config.compaction_trigger_fraction) as usize
    }

    fn push_compaction_operation(
        &mut self,
        state: &SessionState,
        turn: usize,
        mut dependencies: Vec<usize>,
        delay_after_dependencies_ms: u64,
        initial_arrival_ms: Option<u64>,
        summary_tokens: u64,
    ) -> Result<Vec<usize>> {
        let phase = "pre_turn";
        let operation_id = format!(
            "compaction-{}-{}-{turn}",
            state.session_id, state.window_epoch
        );
        let available_attempts = self.config.max_nodes.saturating_sub(self.nodes.len());
        let max_attempts = self.config.compaction_max_attempts.min(available_attempts);
        if max_attempts == 0 {
            bail!("generated scenario has no room for a compaction attempt");
        }

        let mut effects = Vec::new();
        if max_attempts >= 2 && self.rng.random::<f64>() < self.config.compaction_abort_probability
        {
            effects.push(CompactionExpectedEffect::NoMutationAborted);
        }
        effects.push(CompactionExpectedEffect::ApplyOnce);
        if effects.len() < max_attempts
            && self.rng.random::<f64>() < self.config.compaction_retry_probability
        {
            effects.push(CompactionExpectedEffect::DuplicateNoop);
        }

        let mut nodes = Vec::with_capacity(effects.len());
        let mut applied_attempt = 0;
        for (index, expected_effect) in effects.into_iter().enumerate() {
            let attempt = index + 1;
            let abort_after_ms = if expected_effect == CompactionExpectedEffect::NoMutationAborted {
                Some(
                    self.config
                        .compaction_abort_after_ms
                        .sample(&mut self.rng)?,
                )
            } else {
                None
            };
            if expected_effect == CompactionExpectedEffect::ApplyOnce {
                applied_attempt = attempt;
            }
            let attempt_metadata = GeneratedCompactionAttempt {
                operation_id: operation_id.clone(),
                phase: phase.to_string(),
                attempt,
                expected_effect,
                abort_after_ms,
            };
            let compaction = serde_json::json!({
                "trigger": "auto",
                "reason": "context_limit",
                "implementation": "responses",
                "strategy": "memento",
                "operation_id": operation_id.clone(),
                "phase": phase,
                "attempt": attempt,
                "expected_effect": expected_effect,
                "window_epoch": state.window_epoch,
                "pre_tokens": state.blocks.len() * self.config.block_size,
            });
            let node = self.push_request(
                state,
                NodeSpec {
                    dependencies,
                    delay_after_dependencies_ms: if index == 0 {
                        delay_after_dependencies_ms
                    } else {
                        0
                    },
                    initial_arrival_ms: if index == 0 { initial_arrival_ms } else { None },
                    action: "compaction_attempt",
                    input_trigger: "other",
                    logical_output_tokens: if expected_effect == CompactionExpectedEffect::ApplyOnce
                    {
                        summary_tokens
                    } else {
                        0
                    },
                    output_budget_tokens: summary_tokens,
                    compaction: Some(compaction),
                    compaction_attempt: Some(attempt_metadata),
                },
            )?;
            dependencies = vec![node];
            nodes.push(node);
        }
        self.compaction_operations
            .push(GeneratedCompactionOperation {
                operation_id,
                session_id: state.session_id.clone(),
                phase: phase.to_string(),
                attempts: nodes.clone(),
                applied_attempt,
                expected_apply_count: 1,
            });
        Ok(nodes)
    }

    fn compact_state(
        &mut self,
        state: &mut SessionState,
        turn: usize,
        summary_tokens: u64,
    ) -> Result<()> {
        let retain_blocks = self
            .config
            .retained_recent_tokens
            .div_ceil(self.config.block_size)
            .min(state.blocks.len().saturating_sub(state.stable_blocks));
        let recent = state.blocks[state.blocks.len() - retain_blocks..].to_vec();
        state.blocks.truncate(state.stable_blocks);
        state.blocks.extend(self.segment(
            &format!("session:{}:summary:{turn}", state.session_id),
            summary_tokens,
        )?);
        state.blocks.extend(recent);
        state.window_epoch += 1;
        Ok(())
    }

    fn push_request(&mut self, state: &SessionState, spec: NodeSpec<'_>) -> Result<usize> {
        if self.nodes.len() >= self.config.max_nodes {
            bail!("generated scenario reached max_nodes");
        }
        if spec.dependencies.is_empty() != spec.initial_arrival_ms.is_some() {
            bail!("each generated request must have dependencies or one initial arrival");
        }
        if spec
            .dependencies
            .iter()
            .any(|dependency| *dependency >= self.nodes.len())
        {
            bail!("generated request has a forward or missing dependency");
        }
        let input_tokens = state
            .blocks
            .len()
            .checked_mul(self.config.block_size)
            .context("generated input length overflow")?;
        let input_sequence_hashes = state.blocks.clone();
        self.total_input_tokens = self
            .total_input_tokens
            .checked_add(input_tokens as u64)
            .context("generated total input length overflow")?;
        if self.total_input_tokens > self.config.max_total_input_tokens {
            bail!(
                "generated scenario exceeds max_total_input_tokens ({})",
                self.config.max_total_input_tokens
            );
        }
        let output_tokens = u32::try_from(spec.logical_output_tokens)
            .context("generated output token count does not fit u32")?;
        let output_budget_tokens = u32::try_from(spec.output_budget_tokens)
            .context("generated output budget does not fit u32")?;
        if output_tokens == 0 && spec.compaction_attempt.is_none() {
            bail!("generated request output must be greater than zero");
        }
        let ordinal = self.nodes.len();
        let node_id = format!("t{}-n{ordinal}", self.index);
        let request = TraceRequest {
            ordinal,
            source_request_id: format!(
                "generated-{:016x}-t{}-n{ordinal}",
                self.config.seed, self.index
            ),
            input_tokens,
            output_tokens,
            trace_block_size: self.config.block_size,
            input_sequence_hashes,
            agent_context: Some(AgentContext {
                session_id: state.session_id.clone(),
                parent_session_id: state.parent_session_id.clone(),
                compaction: spec.compaction,
                input_trigger: Some(spec.input_trigger.to_string()),
            }),
        };
        self.nodes.push(GeneratedNode {
            node_id,
            action: spec.action.to_string(),
            dependencies: spec.dependencies,
            delay_after_dependencies_ms: spec.delay_after_dependencies_ms,
            initial_arrival_ms: spec.initial_arrival_ms,
            window_epoch: state.window_epoch,
            tool_events: Vec::new(),
            spawned_session_ids: Vec::new(),
            output_budget_tokens,
            compaction_attempt: spec.compaction_attempt,
            session_final: false,
            request,
        });
        Ok(ordinal)
    }

    /// Mark the last request of every protocol session.
    ///
    /// A session's nodes are appended in causal order, so its highest ordinal
    /// is its final model request even when generation limits cut a session
    /// short of a `complete` action.
    fn mark_session_finals(&mut self) -> Result<()> {
        let mut finals = BTreeMap::new();
        for (ordinal, node) in self.nodes.iter().enumerate() {
            let session_id = &node
                .request
                .agent_context
                .as_ref()
                .with_context(|| format!("generated node {ordinal} is missing agent context"))?
                .session_id;
            finals.insert(session_id.clone(), ordinal);
        }
        for ordinal in finals.into_values() {
            self.nodes[ordinal].session_final = true;
        }
        Ok(())
    }

    fn segment(&mut self, scope: &str, tokens: u64) -> Result<Vec<u64>> {
        let tokens = usize::try_from(tokens).context("segment token count does not fit usize")?;
        let blocks = tokens.div_ceil(self.config.block_size);
        (0..blocks)
            .map(|block| self.label(&format!("{scope}:block:{block}")))
            .collect()
    }

    fn label(&mut self, owner: &str) -> Result<u64> {
        if let Some(label) = self.labels.get(owner) {
            return Ok(*label);
        }
        for nonce in 0_u64.. {
            let mut digest = Sha256::new();
            digest.update(b"agent-loadgen/generated-segment/v1");
            digest.update(self.config.seed.to_le_bytes());
            digest.update(owner.as_bytes());
            digest.update(nonce.to_le_bytes());
            let bytes: [u8; 8] = digest.finalize()[..8]
                .try_into()
                .expect("the digest prefix is eight bytes");
            let label = u64::from_le_bytes(bytes);
            match self
                .label_owners
                .get(&label)
                .or_else(|| self.shared_label_owners.get(&label))
            {
                None => {
                    self.labels.insert(owner.to_string(), label);
                    self.label_owners.insert(label, owner.to_string());
                    return Ok(label);
                }
                Some(existing) if existing == owner => return Ok(label),
                Some(_) => continue,
            }
        }
        unreachable!("u64 nonce space is not exhausted")
    }
}

#[cfg(test)]
#[path = "plan/tests.rs"]
mod tests;
