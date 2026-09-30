// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::scenario::config::{ResolvedLoad, UIntDistribution};

fn config() -> ResolvedGeneratorConfig {
    let mut config = ResolvedGeneratorConfig::defaults(42);
    config.load = ResolvedLoad::streams(3, 3);
    config.turns = UIntDistribution::fixed(4);
    config.subagent_turns = UIntDistribution::fixed(2);
    config.system_prefix_tokens = UIntDistribution::fixed(64);
    config.tool_catalog_tokens = UIntDistribution::fixed(64);
    config.repository_tokens = UIntDistribution::fixed(32);
    config.session_tokens = UIntDistribution::fixed(16);
    config.user_tokens = UIntDistribution::fixed(16);
    config.output_tokens = UIntDistribution::fixed(8);
    config.context_window_tokens = 512;
    config.compaction_trigger_fraction = 0.75;
    config.max_nodes = 500;
    config.max_sessions = 100;
    config.max_total_input_tokens = 1_000_000;
    config
}

#[test]
fn generation_is_seed_deterministic() {
    let first = GeneratedScenario::generate(config()).unwrap();
    let second = GeneratedScenario::generate(config()).unwrap();
    assert_eq!(first.scenario_digest_sha256, second.scenario_digest_sha256);
    assert_eq!(first.nodes.len(), second.nodes.len());
}

#[test]
fn graph_dependencies_only_point_backward() {
    let scenario = GeneratedScenario::generate(config()).unwrap();
    assert!(scenario.nodes.iter().enumerate().all(|(ordinal, node)| {
        node.dependencies
            .iter()
            .all(|dependency| *dependency < ordinal)
    }));
    assert!(scenario.nodes.iter().any(|node| node.action == "tool"));
    assert!(
        scenario
            .nodes
            .iter()
            .filter(|node| node.action == "tool")
            .all(|node| !node.tool_events.is_empty())
    );
}

#[test]
fn every_session_marks_exactly_its_last_request_final() {
    let scenario = GeneratedScenario::generate(config()).unwrap();
    assert!(scenario.sessions.len() > 1);
    for session in &scenario.sessions {
        let ordinals = scenario
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| {
                node.request.agent_context.as_ref().unwrap().session_id == session.session_id
            })
            .map(|(ordinal, _)| ordinal)
            .collect::<Vec<_>>();
        let finals = ordinals
            .iter()
            .filter(|ordinal| scenario.nodes[**ordinal].session_final)
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(finals, vec![*ordinals.last().unwrap()]);
        assert_eq!(scenario.nodes[finals[0]].action, "complete");
    }
}

#[test]
fn profile_preserves_global_prefix_sharing() {
    let scenario = GeneratedScenario::generate(config()).unwrap();
    let roots = scenario
        .nodes
        .iter()
        .filter(|node| node.initial_arrival_ms.is_some())
        .collect::<Vec<_>>();
    assert_eq!(roots.len(), 3);
    assert_eq!(
        &roots[0].request.input_sequence_hashes[..8],
        &roots[1].request.input_sequence_hashes[..8]
    );
}

#[test]
fn compaction_rewrites_a_window_and_emits_metadata() {
    let mut config = config();
    config.load = ResolvedLoad::streams(1, 1);
    config.context_window_tokens = 160;
    config.compaction_trigger_fraction = 0.75;
    let scenario = GeneratedScenario::generate(config).unwrap();
    let compactions = scenario
        .nodes
        .iter()
        .filter(|node| node.action == "compaction_attempt")
        .collect::<Vec<_>>();
    assert!(!compactions.is_empty());
    assert_eq!(scenario.compaction_operations.len(), compactions.len());
    assert!(compactions.iter().all(|node| {
        node.request
            .agent_context
            .as_ref()
            .and_then(|context| context.compaction.as_ref())
            .is_some()
    }));
    assert!(scenario.nodes.iter().any(|node| node.window_epoch > 0));
}

#[test]
fn compaction_attempts_share_identity_and_apply_once() {
    let mut config = config();
    config.load = ResolvedLoad::streams(1, 1);
    config.context_window_tokens = 160;
    config.compaction_trigger_fraction = 0.75;
    config.compaction_abort_probability = 1.0;
    config.compaction_retry_probability = 1.0;
    config.compaction_max_attempts = 3;

    let scenario = GeneratedScenario::generate(config).unwrap();
    let operation = scenario.compaction_operations.first().unwrap();
    assert_eq!(operation.attempts.len(), 3);
    assert_eq!(operation.applied_attempt, 2);
    assert_eq!(operation.expected_apply_count, 1);

    let attempts = operation
        .attempts
        .iter()
        .map(|ordinal| &scenario.nodes[*ordinal])
        .collect::<Vec<_>>();
    assert_eq!(
        attempts
            .iter()
            .map(|node| node.compaction_attempt.as_ref().unwrap().expected_effect)
            .collect::<Vec<_>>(),
        vec![
            CompactionExpectedEffect::NoMutationAborted,
            CompactionExpectedEffect::ApplyOnce,
            CompactionExpectedEffect::DuplicateNoop,
        ]
    );
    assert!(attempts.iter().enumerate().all(|(index, node)| {
        let attempt = node.compaction_attempt.as_ref().unwrap();
        attempt.operation_id == operation.operation_id
            && attempt.phase == operation.phase
            && attempt.attempt == index + 1
            && node.output_budget_tokens.is_some_and(|budget| budget > 0)
    }));
}

#[test]
fn blocking_swarm_joins_all_children() {
    let mut config = config();
    config.load = ResolvedLoad::streams(1, 1);
    config.turns = UIntDistribution::fixed(2);
    config.subagent_turns = UIntDistribution::fixed(1);
    config.tool_probability = 0.0;
    config.parallel_tool_probability = 0.0;
    config.subagent_probability = 0.0;
    config.swarm_probability = 1.0;
    config.completion_probability = 0.0;
    config.fanout = UIntDistribution::fixed(3);
    config.blocking_probability = 1.0;
    config.compaction_enabled = false;
    let scenario = GeneratedScenario::generate(config).unwrap();
    assert_eq!(scenario.sessions.len(), 4);
    assert_eq!(
        scenario.session_topology,
        GeneratedSessionTopology {
            configured_top_level_sessions: 1,
            configured_concurrent_sessions: Some(1),
            generated_subagent_sessions: 3,
            total_protocol_sessions: 4,
        }
    );
    let root_completion = scenario
        .nodes
        .iter()
        .find(|node| {
            node.action == "complete"
                && node
                    .request
                    .agent_context
                    .as_ref()
                    .unwrap()
                    .parent_session_id
                    .is_none()
        })
        .unwrap();
    assert_eq!(root_completion.dependencies.len(), 3);
    let swarm = scenario
        .nodes
        .iter()
        .find(|node| node.action == "swarm")
        .unwrap();
    assert_eq!(swarm.spawned_session_ids.len(), 3);
    assert!(scenario.sessions.iter().skip(1).all(|session| {
        session.parent_session_id.as_deref() == Some(scenario.sessions[0].session_id.as_str())
    }));
    assert!(scenario.sessions.iter().all(|session| {
        session.top_level_session_index == 0 && session.top_level_stream_slot == 0
    }));
}

#[test]
fn closed_loop_stream_restarts_after_its_previous_top_level_session_completes() {
    let mut config = config();
    config.load = ResolvedLoad::Streams {
        num_sessions: Some(3),
        concurrent_sessions: 1,
        startup_interval_ms: 0,
        restart_delay_ms: UIntDistribution::fixed(17),
        duration_minutes: None,
    };
    config.turns = UIntDistribution::fixed(1);
    config.tool_probability = 0.0;
    config.parallel_tool_probability = 0.0;
    config.subagent_probability = 0.0;
    config.swarm_probability = 0.0;
    config.completion_probability = 0.0;
    config.compaction_enabled = false;

    let scenario = GeneratedScenario::generate(config).unwrap();
    assert_eq!(scenario.nodes.len(), 3);
    assert_eq!(scenario.nodes[0].initial_arrival_ms, Some(0));
    assert!(scenario.nodes[0].dependencies.is_empty());
    assert_eq!(scenario.nodes[1].initial_arrival_ms, None);
    assert_eq!(scenario.nodes[1].dependencies, vec![0]);
    assert_eq!(scenario.nodes[1].delay_after_dependencies_ms, 17);
    assert_eq!(scenario.nodes[2].dependencies, vec![1]);
    assert_eq!(scenario.nodes[2].delay_after_dependencies_ms, 17);
    assert!(
        scenario
            .sessions
            .iter()
            .all(|session| session.top_level_stream_slot == 0)
    );
}

#[test]
fn reports_call_and_time_weighted_tool_parallelism() {
    let mut config = config();
    config.load = ResolvedLoad::streams(1, 1);
    config.turns = UIntDistribution::fixed(2);
    config.tool_probability = 0.0;
    config.parallel_tool_probability = 1.0;
    config.subagent_probability = 0.0;
    config.swarm_probability = 0.0;
    config.completion_probability = 0.0;
    config.compaction_enabled = false;
    config.parallel_count = UIntDistribution::fixed(3);
    for class in &mut config.tool_classes {
        class.latency_ms = UIntDistribution::fixed(10);
    }

    let scenario = GeneratedScenario::generate(config).unwrap();
    assert_eq!(scenario.tool_parallelism.tool_phases, 1);
    assert_eq!(scenario.tool_parallelism.parallel_tool_phases, 1);
    assert_eq!(scenario.tool_parallelism.tool_calls, 3);
    assert_eq!(scenario.tool_parallelism.parallel_tool_calls, 3);
    assert_eq!(scenario.tool_parallelism.parallel_call_fraction, 1.0);
    assert_eq!(scenario.tool_parallelism.tool_work_ms, 30);
    assert_eq!(scenario.tool_parallelism.tool_wall_ms, 10);
    assert_eq!(scenario.tool_parallelism.parallel_wall_ms, 10);
    assert_eq!(scenario.tool_parallelism.parallel_wall_time_fraction, 1.0);
    assert_eq!(scenario.tool_parallelism.work_to_wall_ratio, 3.0);
}

#[test]
fn trees_plan_the_same_alone_as_in_the_full_graph() {
    let planner = TreePlanner::new(config()).unwrap();
    let scenario = GeneratedScenario::generate(config()).unwrap();
    let mut offset = 0;
    for index in 0..3 {
        let tree = planner.plan_tree(index).unwrap();
        for (local, node) in tree.nodes.iter().enumerate() {
            let joined = &scenario.nodes[offset + local];
            assert_eq!(node.node_id, joined.node_id);
            assert_eq!(
                node.request.input_sequence_hashes,
                joined.request.input_sequence_hashes
            );
        }
        offset += tree.nodes.len();
    }
    // Planning out of order does not change a tree.
    let late = planner.plan_tree(2).unwrap();
    let early = TreePlanner::new(config()).unwrap().plan_tree(2).unwrap();
    assert_eq!(
        serde_json::to_value(&late).unwrap(),
        serde_json::to_value(&early).unwrap()
    );
}

fn arrivals(rate_unit: RateUnit, rates: Vec<f64>, interpolation: Interpolation) -> ResolvedLoad {
    ResolvedLoad::Arrivals {
        bucket_minutes: 1.0,
        rate_unit,
        rates,
        interpolation,
    }
}

#[test]
fn arrival_schedules_follow_each_bucket_rate() {
    let mut config = config();
    config.load = arrivals(
        RateUnit::SessionsPerMinute,
        vec![600.0, 0.0, 1200.0],
        Interpolation::Step,
    );
    let planner = TreePlanner::new(config.clone()).unwrap();
    let arrival_ms = &planner.arrivals().unwrap().arrival_ms;
    assert!(arrival_ms.windows(2).all(|pair| pair[0] <= pair[1]));
    let per_bucket = |bucket: u64| {
        arrival_ms
            .iter()
            .filter(|ms| **ms / 60_000 == bucket)
            .count() as f64
    };
    assert!((per_bucket(0) - 600.0).abs() < 100.0);
    assert_eq!(per_bucket(1), 0.0);
    assert!((per_bucket(2) - 1200.0).abs() < 150.0);
    assert_eq!(planner.tree_count(), Some(arrival_ms.len()));
    assert_eq!(
        TreePlanner::new(config)
            .unwrap()
            .arrivals()
            .unwrap()
            .arrival_ms,
        *arrival_ms
    );
}

#[test]
fn linear_arrivals_ramp_between_bucket_midpoints() {
    let mut config = config();
    config.load = arrivals(
        RateUnit::SessionsPerMinute,
        vec![0.0, 1200.0],
        Interpolation::Linear,
    );
    let planner = TreePlanner::new(config).unwrap();
    let arrival_ms = &planner.arrivals().unwrap().arrival_ms;
    let first_half = arrival_ms.iter().filter(|ms| **ms < 30_000).count();
    let second_half = arrival_ms
        .iter()
        .filter(|ms| (30_000..60_000).contains(*ms))
        .count();
    let last_half = arrival_ms.iter().filter(|ms| **ms >= 90_000).count();
    assert_eq!(first_half, 0);
    assert!(second_half > 0 && second_half < last_half);
}

#[test]
fn request_rates_convert_to_session_arrivals() {
    let mut config = config();
    config.load = arrivals(RateUnit::RequestsPerSecond, vec![20.0], Interpolation::Step);
    let planner = TreePlanner::new(config).unwrap();
    let plan = planner.arrivals().unwrap();
    let per_tree = plan.calibration_requests_per_tree.unwrap();
    assert!(per_tree >= 1.0);
    let expected_sessions = 20.0 * 60.0 / per_tree;
    assert!((plan.sessions_per_minute[0] - expected_sessions).abs() < 1e-9);
    let sessions = plan.arrival_ms.len() as f64;
    assert!((sessions - expected_sessions).abs() < expected_sessions * 0.3);
}

#[test]
fn unbounded_streams_plan_lazily_but_not_as_one_graph() {
    let mut config = config();
    config.load = ResolvedLoad::Streams {
        num_sessions: None,
        concurrent_sessions: 2,
        startup_interval_ms: 0,
        restart_delay_ms: UIntDistribution::fixed(0),
        duration_minutes: Some(5.0),
    };
    let planner = TreePlanner::new(config.clone()).unwrap();
    assert_eq!(planner.tree_count(), None);
    assert!(planner.plan_tree(1_000_000).is_ok());
    assert!(
        GeneratedScenario::generate(config)
            .unwrap_err()
            .to_string()
            .contains("duration_minutes")
    );
}
