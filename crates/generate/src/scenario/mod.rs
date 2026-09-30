// SPDX-License-Identifier: Apache-2.0

//! Seeded coding-agent traffic plans.

mod config;
mod distribution;
mod graph;
mod model;
mod plan;
mod summary;
mod tool_parallelism;

pub use config::{GeneratorConfig, Interpolation, RateUnit, ResolvedGeneratorConfig, ResolvedLoad};
pub use graph::{PlanGraphArtifacts, render_plan_dot, write_plan_graph};
pub use model::{
    CompactionExpectedEffect, GeneratedCompactionAttempt, GeneratedCompactionOperation,
    GeneratedNode, GeneratedScenario, GeneratedSession, GeneratedSessionTopology,
    GeneratedToolEvent, GeneratedToolParallelism, PlannedTree,
};
pub use plan::{ArrivalPlan, TreePlanner};
pub use summary::{PlanSummary, SUMMARY_SAMPLE_TREES};
