// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

pub(super) use super::distribution::UIntDistribution;

pub const GENERATOR_SCHEMA_VERSION: u32 = 5;

const AGENT_FIELD_REMOVED: &str = "schema 5 removed the `agent` field (shape is now fully defined by this file). Delete the `agent` line, set `schema_version = 5`, and choose the header dialect at run time with `--headers {claude-code|codex}`.";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratorConfig {
    pub schema_version: u32,
    #[serde(default)]
    pub seed: u64,
    #[serde(default)]
    pub load: LoadOverrides,
    #[serde(default)]
    pub trajectory: TrajectoryOverrides,
    #[serde(default)]
    pub tokens: TokenOverrides,
    #[serde(default)]
    pub behavior: BehaviorOverrides,
    #[serde(default)]
    pub compaction: CompactionOverrides,
    #[serde(default)]
    pub subagents: SubagentOverrides,
    #[serde(default)]
    pub tools: ToolOverrides,
    #[serde(default)]
    pub limits: LimitOverrides,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadOverrides {
    /// Total top-level agent session trees generated for the finite run.
    pub num_sessions: Option<usize>,
    /// Number of closed-loop top-level session streams kept active.
    pub concurrent_sessions: Option<usize>,
    /// Ramp-up spacing between the first task in each active top-level stream.
    pub startup_interval_ms: Option<u64>,
    /// Delay before a completed top-level stream starts its next session tree.
    pub restart_delay_ms: Option<UIntDistribution>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrajectoryOverrides {
    pub turns: Option<UIntDistribution>,
    pub think_time_ms: Option<UIntDistribution>,
    pub output_tokens: Option<UIntDistribution>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenOverrides {
    pub block_size: Option<usize>,
    pub system_prefix_tokens: Option<UIntDistribution>,
    pub tool_catalog_tokens: Option<UIntDistribution>,
    pub repository_tokens: Option<UIntDistribution>,
    pub session_tokens: Option<UIntDistribution>,
    pub user_tokens: Option<UIntDistribution>,
    pub tool_result_tokens: Option<UIntDistribution>,
    pub context_window_tokens: Option<usize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BehaviorOverrides {
    pub tool_probability: Option<f64>,
    pub parallel_tool_probability: Option<f64>,
    pub subagent_probability: Option<f64>,
    pub swarm_probability: Option<f64>,
    pub completion_probability: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionOverrides {
    pub enabled: Option<bool>,
    pub trigger_fraction: Option<f64>,
    pub summary_input_tokens: Option<UIntDistribution>,
    pub summary_output_tokens: Option<UIntDistribution>,
    pub retained_recent_tokens: Option<usize>,
    pub abort_probability: Option<f64>,
    pub retry_probability: Option<f64>,
    pub max_attempts: Option<usize>,
    pub abort_after_ms: Option<UIntDistribution>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubagentOverrides {
    pub max_depth: Option<usize>,
    pub turns: Option<UIntDistribution>,
    pub fanout: Option<UIntDistribution>,
    pub spawn_delay_ms: Option<UIntDistribution>,
    pub blocking_probability: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolOverrides {
    pub classes: Option<Vec<ToolClass>>,
    pub parallel_count: Option<UIntDistribution>,
    pub retry_probability: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitOverrides {
    pub max_nodes: Option<usize>,
    pub max_sessions: Option<usize>,
    pub max_total_input_tokens: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolClass {
    pub name: String,
    pub weight: f64,
    pub latency_ms: UIntDistribution,
    pub result_tokens: UIntDistribution,
    #[serde(default)]
    pub error_probability: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResolvedGeneratorConfig {
    pub seed: u64,
    pub num_sessions: usize,
    pub concurrent_sessions: usize,
    pub startup_interval_ms: u64,
    pub restart_delay_ms: UIntDistribution,
    pub turns: UIntDistribution,
    pub subagent_turns: UIntDistribution,
    pub think_time_ms: UIntDistribution,
    pub output_tokens: UIntDistribution,
    pub block_size: usize,
    pub system_prefix_tokens: UIntDistribution,
    pub tool_catalog_tokens: UIntDistribution,
    pub repository_tokens: UIntDistribution,
    pub session_tokens: UIntDistribution,
    pub user_tokens: UIntDistribution,
    pub tool_result_tokens: UIntDistribution,
    pub context_window_tokens: usize,
    pub tool_probability: f64,
    pub parallel_tool_probability: f64,
    pub subagent_probability: f64,
    pub swarm_probability: f64,
    pub completion_probability: f64,
    pub compaction_enabled: bool,
    pub compaction_trigger_fraction: f64,
    pub summary_input_tokens: UIntDistribution,
    pub summary_output_tokens: UIntDistribution,
    pub retained_recent_tokens: usize,
    pub compaction_abort_probability: f64,
    pub compaction_retry_probability: f64,
    pub compaction_max_attempts: usize,
    pub compaction_abort_after_ms: UIntDistribution,
    pub max_depth: usize,
    pub fanout: UIntDistribution,
    pub spawn_delay_ms: UIntDistribution,
    pub blocking_probability: f64,
    pub tool_classes: Vec<ToolClass>,
    pub parallel_count: UIntDistribution,
    pub retry_probability: f64,
    pub max_nodes: usize,
    pub max_sessions: usize,
    pub max_total_input_tokens: u64,
}

impl GeneratorConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read generator config {}", path.display()))?;
        Self::parse(&text).with_context(|| format!("invalid generator config {}", path.display()))
    }

    /// Parse strict profile TOML.
    ///
    /// The schema version is checked before the strict struct parse so a
    /// profile written for an older schema reports what changed instead of an
    /// unknown-field error. Nothing is ignored: the strict parse still rejects
    /// unknown fields and the version must match exactly.
    pub fn parse(text: &str) -> Result<Self> {
        let value: toml::Value =
            toml::from_str(text).context("generator config is not valid TOML")?;
        let table = value
            .as_table()
            .context("generator config must be a TOML table")?;
        let schema_version = table
            .get("schema_version")
            .context("generator config is missing `schema_version`")?
            .as_integer()
            .context("generator config `schema_version` must be an integer")?;
        let has_agent = table.contains_key("agent");
        match u32::try_from(schema_version) {
            Ok(GENERATOR_SCHEMA_VERSION) if has_agent => {
                bail!(
                    "generator schema {GENERATOR_SCHEMA_VERSION} has no `agent` field: {AGENT_FIELD_REMOVED}"
                )
            }
            Ok(GENERATOR_SCHEMA_VERSION) => {}
            Ok(4) => bail!("generator schema 4 is no longer supported: {AGENT_FIELD_REMOVED}"),
            _ => bail!(
                "unsupported generator schema version {schema_version}, expected {GENERATOR_SCHEMA_VERSION}"
            ),
        }
        let config: Self = toml::from_str(text)?;
        Ok(config)
    }

    pub fn resolve(self) -> Result<ResolvedGeneratorConfig> {
        if self.schema_version != GENERATOR_SCHEMA_VERSION {
            bail!(
                "unsupported generator schema version {}, expected {}",
                self.schema_version,
                GENERATOR_SCHEMA_VERSION
            );
        }
        let mut resolved = ResolvedGeneratorConfig::defaults(self.seed);
        replace(&mut resolved.num_sessions, self.load.num_sessions);
        replace(
            &mut resolved.concurrent_sessions,
            self.load.concurrent_sessions,
        );
        replace(
            &mut resolved.startup_interval_ms,
            self.load.startup_interval_ms,
        );
        replace(&mut resolved.restart_delay_ms, self.load.restart_delay_ms);
        replace(&mut resolved.turns, self.trajectory.turns);
        replace(&mut resolved.think_time_ms, self.trajectory.think_time_ms);
        replace(&mut resolved.output_tokens, self.trajectory.output_tokens);
        replace(&mut resolved.block_size, self.tokens.block_size);
        replace(
            &mut resolved.system_prefix_tokens,
            self.tokens.system_prefix_tokens,
        );
        replace(
            &mut resolved.tool_catalog_tokens,
            self.tokens.tool_catalog_tokens,
        );
        replace(
            &mut resolved.repository_tokens,
            self.tokens.repository_tokens,
        );
        replace(&mut resolved.session_tokens, self.tokens.session_tokens);
        replace(&mut resolved.user_tokens, self.tokens.user_tokens);
        replace(
            &mut resolved.tool_result_tokens,
            self.tokens.tool_result_tokens,
        );
        replace(
            &mut resolved.context_window_tokens,
            self.tokens.context_window_tokens,
        );
        replace(
            &mut resolved.tool_probability,
            self.behavior.tool_probability,
        );
        replace(
            &mut resolved.parallel_tool_probability,
            self.behavior.parallel_tool_probability,
        );
        replace(
            &mut resolved.subagent_probability,
            self.behavior.subagent_probability,
        );
        replace(
            &mut resolved.swarm_probability,
            self.behavior.swarm_probability,
        );
        replace(
            &mut resolved.completion_probability,
            self.behavior.completion_probability,
        );
        replace(&mut resolved.compaction_enabled, self.compaction.enabled);
        replace(
            &mut resolved.compaction_trigger_fraction,
            self.compaction.trigger_fraction,
        );
        replace(
            &mut resolved.summary_input_tokens,
            self.compaction.summary_input_tokens,
        );
        replace(
            &mut resolved.summary_output_tokens,
            self.compaction.summary_output_tokens,
        );
        replace(
            &mut resolved.retained_recent_tokens,
            self.compaction.retained_recent_tokens,
        );
        replace(
            &mut resolved.compaction_abort_probability,
            self.compaction.abort_probability,
        );
        replace(
            &mut resolved.compaction_retry_probability,
            self.compaction.retry_probability,
        );
        replace(
            &mut resolved.compaction_max_attempts,
            self.compaction.max_attempts,
        );
        replace(
            &mut resolved.compaction_abort_after_ms,
            self.compaction.abort_after_ms,
        );
        replace(&mut resolved.max_depth, self.subagents.max_depth);
        replace(&mut resolved.subagent_turns, self.subagents.turns);
        replace(&mut resolved.fanout, self.subagents.fanout);
        replace(&mut resolved.spawn_delay_ms, self.subagents.spawn_delay_ms);
        replace(
            &mut resolved.blocking_probability,
            self.subagents.blocking_probability,
        );
        replace(&mut resolved.tool_classes, self.tools.classes);
        replace(&mut resolved.parallel_count, self.tools.parallel_count);
        replace(
            &mut resolved.retry_probability,
            self.tools.retry_probability,
        );
        replace(&mut resolved.max_nodes, self.limits.max_nodes);
        replace(&mut resolved.max_sessions, self.limits.max_sessions);
        replace(
            &mut resolved.max_total_input_tokens,
            self.limits.max_total_input_tokens,
        );
        resolved.validate()?;
        Ok(resolved)
    }
}

fn replace<T>(target: &mut T, value: Option<T>) {
    if let Some(value) = value {
        *target = value;
    }
}

impl ResolvedGeneratorConfig {
    /// Neutral defaults for every field a profile omits.
    ///
    /// There is no per-harness preset: traffic shape comes only from the
    /// profile, and the wire header dialect is chosen at run time.
    pub(super) fn defaults(seed: u64) -> Self {
        Self {
            seed,
            num_sessions: 16,
            concurrent_sessions: 8,
            startup_interval_ms: 0,
            restart_delay_ms: UIntDistribution::fixed(0),
            turns: UIntDistribution::uniform(4, 10),
            subagent_turns: UIntDistribution::uniform(2, 5),
            think_time_ms: UIntDistribution::log_normal(750.0, 0.8, 25, 15_000),
            output_tokens: UIntDistribution::log_normal(450.0, 0.9, 16, 4096),
            block_size: 16,
            system_prefix_tokens: UIntDistribution::fixed(7_000),
            tool_catalog_tokens: UIntDistribution::fixed(11_000),
            repository_tokens: UIntDistribution::log_normal(2_000.0, 0.7, 256, 12_000),
            session_tokens: UIntDistribution::uniform(128, 1024),
            user_tokens: UIntDistribution::log_normal(180.0, 0.9, 16, 4096),
            tool_result_tokens: UIntDistribution::log_normal(700.0, 1.1, 16, 16_384),
            context_window_tokens: 128_000,
            tool_probability: 0.48,
            parallel_tool_probability: 0.08,
            subagent_probability: 0.10,
            swarm_probability: 0.025,
            completion_probability: 0.10,
            compaction_enabled: true,
            compaction_trigger_fraction: 0.78,
            summary_input_tokens: UIntDistribution::uniform(256, 1024),
            summary_output_tokens: UIntDistribution::uniform(512, 2048),
            retained_recent_tokens: 8_192,
            compaction_abort_probability: 0.0,
            compaction_retry_probability: 0.0,
            compaction_max_attempts: 3,
            compaction_abort_after_ms: UIntDistribution::fixed(10),
            max_depth: 2,
            fanout: UIntDistribution::uniform(2, 4),
            spawn_delay_ms: UIntDistribution::uniform(5, 100),
            blocking_probability: 0.72,
            tool_classes: default_tool_classes(),
            parallel_count: UIntDistribution::uniform(2, 4),
            retry_probability: 0.35,
            max_nodes: 10_000,
            max_sessions: 1_000,
            max_total_input_tokens: 250_000_000,
        }
    }

    pub(super) fn validate(&self) -> Result<()> {
        if self.num_sessions == 0 {
            bail!("num_sessions must be greater than zero");
        }
        if self.concurrent_sessions == 0 {
            bail!("concurrent_sessions must be greater than zero");
        }
        if self.concurrent_sessions > self.num_sessions {
            bail!("concurrent_sessions must not exceed num_sessions");
        }
        if self.num_sessions > self.max_sessions || self.num_sessions > self.max_nodes {
            bail!("generation limits cannot hold all configured top-level sessions");
        }
        if self.block_size == 0 {
            bail!("token block size must be greater than zero");
        }
        if self.context_window_tokens < self.block_size {
            bail!("context window must be at least one token block");
        }
        if self.max_nodes == 0 || self.max_sessions == 0 || self.max_total_input_tokens == 0 {
            bail!("generation limits must be greater than zero");
        }
        for (name, distribution) in [
            ("restart delay", &self.restart_delay_ms),
            ("turns", &self.turns),
            ("subagent turns", &self.subagent_turns),
            ("think time", &self.think_time_ms),
            ("output tokens", &self.output_tokens),
            ("system prefix tokens", &self.system_prefix_tokens),
            ("tool catalog tokens", &self.tool_catalog_tokens),
            ("repository tokens", &self.repository_tokens),
            ("session tokens", &self.session_tokens),
            ("user tokens", &self.user_tokens),
            ("tool result tokens", &self.tool_result_tokens),
            ("summary input tokens", &self.summary_input_tokens),
            ("summary output tokens", &self.summary_output_tokens),
            ("compaction abort delay", &self.compaction_abort_after_ms),
            ("subagent fanout", &self.fanout),
            ("spawn delay", &self.spawn_delay_ms),
            ("parallel tool count", &self.parallel_count),
        ] {
            distribution.validate(name)?;
        }
        for (name, distribution) in [
            ("turns", &self.turns),
            ("subagent turns", &self.subagent_turns),
            ("output tokens", &self.output_tokens),
            ("summary output tokens", &self.summary_output_tokens),
            ("subagent fanout", &self.fanout),
            ("parallel tool count", &self.parallel_count),
        ] {
            if distribution.bounds().0 == 0 {
                bail!("{name} must always be greater than zero");
            }
        }
        for (name, distribution) in [
            ("output tokens", &self.output_tokens),
            ("summary output tokens", &self.summary_output_tokens),
        ] {
            if distribution.bounds().1 > u32::MAX as u64 {
                bail!("{name} must fit u32");
            }
        }
        for (name, probability) in [
            ("tool_probability", self.tool_probability),
            ("parallel_tool_probability", self.parallel_tool_probability),
            ("subagent_probability", self.subagent_probability),
            ("swarm_probability", self.swarm_probability),
            ("completion_probability", self.completion_probability),
            ("blocking_probability", self.blocking_probability),
            ("retry_probability", self.retry_probability),
            (
                "compaction_abort_probability",
                self.compaction_abort_probability,
            ),
            (
                "compaction_retry_probability",
                self.compaction_retry_probability,
            ),
        ] {
            validate_probability(name, probability)?;
        }
        if self.compaction_max_attempts == 0 {
            bail!("compaction_max_attempts must be greater than zero");
        }
        if (self.compaction_abort_probability > 0.0 || self.compaction_retry_probability > 0.0)
            && self.compaction_max_attempts < 2
        {
            bail!("compaction_max_attempts must be at least two when retries are enabled");
        }
        let action_total = self.tool_probability
            + self.parallel_tool_probability
            + self.subagent_probability
            + self.swarm_probability
            + self.completion_probability;
        if action_total > 1.0 {
            bail!("behavior action probabilities sum to more than one");
        }
        if !self.compaction_trigger_fraction.is_finite()
            || !(0.0..=1.0).contains(&self.compaction_trigger_fraction)
        {
            bail!("compaction trigger fraction must be between zero and one");
        }
        if self.tool_classes.is_empty() {
            bail!("at least one tool class is required");
        }
        let mut names = BTreeSet::new();
        let mut weight = 0.0;
        for class in &self.tool_classes {
            if class.name.trim().is_empty() || !names.insert(&class.name) {
                bail!("tool class names must be non-empty and unique");
            }
            if !class.weight.is_finite() || class.weight <= 0.0 {
                bail!("tool class {:?} has an invalid weight", class.name);
            }
            validate_probability("tool error probability", class.error_probability)?;
            class.latency_ms.validate("tool latency")?;
            class.result_tokens.validate("tool result tokens")?;
            weight += class.weight;
        }
        if !weight.is_finite() || weight <= 0.0 {
            bail!("tool class weights are invalid");
        }
        Ok(())
    }
}

fn validate_probability(name: &str, value: f64) -> Result<()> {
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        bail!("{name} must be between zero and one");
    }
    Ok(())
}

fn default_tool_classes() -> Vec<ToolClass> {
    [
        ("read", 0.27, 40, 350, 128, 2_000, 0.01),
        ("search", 0.21, 80, 900, 128, 4_000, 0.03),
        ("shell", 0.21, 100, 2_500, 64, 8_000, 0.08),
        ("patch", 0.13, 120, 1_800, 64, 3_000, 0.05),
        ("network", 0.08, 300, 5_000, 128, 12_000, 0.10),
        ("orchestration", 0.10, 20, 250, 32, 512, 0.03),
    ]
    .into_iter()
    .map(
        |(name, weight, min_latency, max_latency, min_tokens, max_tokens, error_probability)| {
            ToolClass {
                name: name.to_string(),
                weight,
                latency_ms: UIntDistribution::log_normal(
                    (min_latency + max_latency) as f64 / 2.0,
                    0.9,
                    min_latency,
                    max_latency,
                ),
                result_tokens: UIntDistribution::log_normal(
                    (min_tokens + max_tokens) as f64 / 2.0,
                    1.0,
                    min_tokens,
                    max_tokens,
                ),
                error_probability,
            }
        },
    )
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_trajectories_that_can_have_zero_turns() {
        let config = GeneratorConfig::parse(
            r#"
                schema_version = 5

                [trajectory]
                turns = { kind = "uniform", min = 0, max = 2 }
            "#,
        )
        .unwrap();
        assert!(config.resolve().is_err());
    }

    #[test]
    fn rejects_removed_same_session_background_requests() {
        let config = GeneratorConfig::parse(
            r#"
                schema_version = 5

                [behavior]
                background_request_probability = 0.1
            "#,
        );
        assert!(config.is_err());
    }

    #[test]
    fn rejects_legacy_top_level_session_names() {
        let config = GeneratorConfig::parse(
            r#"
                schema_version = 5

                [load]
                root_sessions = 1
                concurrent_agents = 1
            "#,
        );
        assert!(config.is_err());
    }

    #[test]
    fn minimal_profile_resolves_to_neutral_defaults() {
        let resolved = GeneratorConfig::parse("schema_version = 5\nseed = 42\n")
            .unwrap()
            .resolve()
            .unwrap();
        assert_eq!(resolved.seed, 42);
        assert_eq!(
            resolved.system_prefix_tokens,
            UIntDistribution::fixed(7_000)
        );
        assert_eq!(
            resolved.tool_catalog_tokens,
            UIntDistribution::fixed(11_000)
        );
        assert_eq!(resolved.tool_probability, 0.48);
        assert_eq!(resolved.parallel_tool_probability, 0.08);
        assert_eq!(resolved.subagent_probability, 0.10);
        assert_eq!(resolved.swarm_probability, 0.025);
        assert_eq!(resolved.completion_probability, 0.10);
    }

    #[test]
    fn schema_four_profile_with_agent_reports_the_migration() {
        let error = GeneratorConfig::parse(
            r#"
                schema_version = 4
                agent = "codex"
                seed = 42
            "#,
        )
        .unwrap_err()
        .to_string();
        assert!(error.starts_with("generator schema 4 is no longer supported"));
        assert!(error.contains("removed the `agent` field"));
        assert!(error.contains("schema_version = 5"));
        assert!(error.contains("--headers {claude-code|codex}"));
    }

    #[test]
    fn current_schema_rejects_the_agent_field() {
        let error = GeneratorConfig::parse(
            r#"
                schema_version = 5
                agent = "codex"
            "#,
        )
        .unwrap_err()
        .to_string();
        assert!(error.starts_with("generator schema 5 has no `agent` field"));
        assert!(error.contains("--headers"));
    }

    #[test]
    fn unknown_schema_versions_are_rejected() {
        let error = GeneratorConfig::parse("schema_version = 6\n")
            .unwrap_err()
            .to_string();
        assert_eq!(error, "unsupported generator schema version 6, expected 5");
        assert!(GeneratorConfig::parse("seed = 1\n").is_err());
    }
}
