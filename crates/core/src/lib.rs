// SPDX-License-Identifier: Apache-2.0

//! Shared workload contracts for agent-loadgen crates.

mod agent;
mod contracts;
pub mod scheduler;

pub use agent::HeaderDialect;
pub use contracts::{AgentContext, Percentiles, TraceRequest, percentiles};
