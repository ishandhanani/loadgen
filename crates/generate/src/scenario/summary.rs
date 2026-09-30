// SPDX-License-Identifier: Apache-2.0

//! Traffic statistics for a plan of any size, from a sample of its trees.

use std::collections::HashSet;

use anyhow::Result;
use serde::Serialize;

use super::config::{RateUnit, ResolvedLoad};
use super::model::PlannedTree;
use super::plan::TreePlanner;

/// Trees are independent and identically distributed, so a prefix sample is fair.
pub const SUMMARY_SAMPLE_TREES: usize = 1_000;

/// What a plan will send, for comparison with a workload description.
#[derive(Debug, Clone, Serialize)]
pub struct PlanSummary {
    pub profile_digest_sha256: String,
    pub load: ResolvedLoad,
    /// Top-level session trees; absent for streams that run until a duration.
    pub top_level_sessions: Option<usize>,
    /// Arrival schedule or stream duration.
    pub duration_minutes: Option<f64>,
    pub sampled_trees: usize,
    pub per_tree: TreeStats,
    pub per_request: RequestStats,
    /// Totals over every tree, extrapolated from the sample.
    pub expected_totals: Option<Totals>,
    pub arrivals: Option<ArrivalSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TreeStats {
    pub requests: Stat,
    pub protocol_sessions: Stat,
    pub compaction_operations: Stat,
    /// Planned tool, think, spawn, and join delays in the root session, in
    /// seconds. A session lasts this long plus the model time of its requests.
    pub root_session_client_delay_s: Stat,
}

#[derive(Debug, Clone, Serialize)]
pub struct RequestStats {
    pub input_tokens: Stat,
    pub output_tokens: Stat,
    /// Share of input tokens in a prefix already sent earlier in the same tree
    /// or shared by every tree: the prefix-cache hit rate with unlimited cache
    /// and ideal routing.
    pub ideal_prefix_hit_rate: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Totals {
    pub requests: f64,
    pub input_tokens: f64,
    pub output_tokens: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArrivalSummary {
    pub bucket_minutes: f64,
    /// Mean requests per tree used to turn request rates into session rates.
    pub calibration_requests_per_tree: Option<f64>,
    pub buckets: Vec<ArrivalBucket>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArrivalBucket {
    pub start_minute: f64,
    /// Configured rate, in the schedule's own unit.
    pub configured_rate: f64,
    pub sessions_per_minute: f64,
    pub sampled_sessions: usize,
    /// Sessions started in the bucket times mean requests per tree. Requests
    /// trail their session start by the session's duration.
    pub expected_requests_per_second: f64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Stat {
    pub mean: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub max: f64,
}

impl Stat {
    fn of(mut values: Vec<f64>) -> Self {
        if values.is_empty() {
            return Self::default();
        }
        values.sort_by(f64::total_cmp);
        let rank = |quantile: f64| {
            let index = (values.len() as f64 * quantile).ceil() as usize;
            values[index.saturating_sub(1).min(values.len() - 1)]
        };
        Self {
            mean: values.iter().sum::<f64>() / values.len() as f64,
            p50: rank(0.50),
            p90: rank(0.90),
            p99: rank(0.99),
            max: *values.last().expect("values are not empty"),
        }
    }
}

impl PlanSummary {
    pub fn sample(planner: &TreePlanner) -> Result<Self> {
        let config = planner.config();
        let tree_count = planner.tree_count();
        let sampled = tree_count.map_or(SUMMARY_SAMPLE_TREES, |count| {
            count.min(SUMMARY_SAMPLE_TREES)
        });
        let mut requests = Vec::with_capacity(sampled);
        let mut sessions = Vec::with_capacity(sampled);
        let mut compactions = Vec::with_capacity(sampled);
        let mut client_delay = Vec::with_capacity(sampled);
        let mut input_tokens = Vec::new();
        let mut output_tokens = Vec::new();
        let (mut reused, mut total_input) = (0_u64, 0_u64);
        let shared: HashSet<u64> = prefix_chain(planner.shared_prefix()).into_iter().collect();
        for index in 0..sampled {
            let tree = planner.plan_tree(index)?;
            requests.push(tree.nodes.len() as f64);
            sessions.push(tree.sessions.len() as f64);
            compactions.push(tree.compaction_operations.len() as f64);
            client_delay.push(root_client_delay_ms(&tree) as f64 / 1000.0);
            let mut seen = shared.clone();
            for node in &tree.nodes {
                let request = &node.request;
                input_tokens.push(request.input_tokens as f64);
                output_tokens.push(f64::from(request.output_tokens));
                let chain = prefix_chain(&request.input_sequence_hashes);
                let hits = chain.iter().take_while(|hash| seen.contains(*hash)).count();
                seen.extend(chain);
                let hit_tokens = (hits * config.block_size).min(request.input_tokens);
                reused += hit_tokens as u64;
                total_input += request.input_tokens as u64;
            }
        }
        let per_tree = TreeStats {
            requests: Stat::of(requests),
            protocol_sessions: Stat::of(sessions),
            compaction_operations: Stat::of(compactions),
            root_session_client_delay_s: Stat::of(client_delay),
        };
        let per_request = RequestStats {
            input_tokens: Stat::of(input_tokens),
            output_tokens: Stat::of(output_tokens),
            ideal_prefix_hit_rate: reused as f64 / total_input.max(1) as f64,
        };
        let expected_totals = tree_count.map(|count| {
            let trees = count as f64;
            Totals {
                requests: per_tree.requests.mean * trees,
                input_tokens: per_request.input_tokens.mean * per_tree.requests.mean * trees,
                output_tokens: per_request.output_tokens.mean * per_tree.requests.mean * trees,
            }
        });
        let arrivals = match (&config.load, planner.arrivals()) {
            (
                ResolvedLoad::Arrivals {
                    bucket_minutes,
                    rate_unit,
                    rates,
                    ..
                },
                Some(plan),
            ) => {
                let bucket_ms = bucket_minutes * 60_000.0;
                let mut counts = vec![0_usize; rates.len()];
                for start in &plan.arrival_ms {
                    let bucket = ((*start as f64 / bucket_ms) as usize).min(rates.len() - 1);
                    counts[bucket] += 1;
                }
                let buckets = rates
                    .iter()
                    .zip(&plan.sessions_per_minute)
                    .zip(counts)
                    .enumerate()
                    .map(
                        |(index, ((configured_rate, sessions_per_minute), count))| ArrivalBucket {
                            start_minute: index as f64 * bucket_minutes,
                            configured_rate: *configured_rate,
                            sessions_per_minute: *sessions_per_minute,
                            sampled_sessions: count,
                            expected_requests_per_second: count as f64 * per_tree.requests.mean
                                / (bucket_ms / 1000.0),
                        },
                    )
                    .collect();
                Some(ArrivalSummary {
                    bucket_minutes: *bucket_minutes,
                    calibration_requests_per_tree: match rate_unit {
                        RateUnit::RequestsPerSecond => plan.calibration_requests_per_tree,
                        RateUnit::SessionsPerMinute => None,
                    },
                    buckets,
                })
            }
            _ => None,
        };
        Ok(Self {
            profile_digest_sha256: planner.profile_digest_sha256().to_string(),
            load: config.load.clone(),
            top_level_sessions: tree_count,
            duration_minutes: config.load.duration_ms().map(|ms| ms as f64 / 60_000.0),
            sampled_trees: sampled,
            per_tree,
            per_request,
            expected_totals,
            arrivals,
        })
    }
}

/// One hash per block that identifies the whole prefix ending at that block.
fn prefix_chain(blocks: &[u64]) -> Vec<u64> {
    let mut hash = 0x9e37_79b9_7f4a_7c15_u64;
    blocks
        .iter()
        .map(|block| {
            hash = (hash.rotate_left(17) ^ block).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            hash
        })
        .collect()
}

fn root_client_delay_ms(tree: &PlannedTree) -> u64 {
    let Some(root) = tree.sessions.first() else {
        return 0;
    };
    tree.nodes
        .iter()
        .skip(1)
        .filter(|node| {
            node.request
                .agent_context
                .as_ref()
                .is_some_and(|context| context.session_id == root.session_id)
        })
        .map(|node| node.delay_after_dependencies_ms)
        .sum()
}
