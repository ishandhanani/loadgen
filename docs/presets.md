# Generator defaults

This page records every default resolved by generator schema version 5. A profile only needs `schema_version`; omitted fields inherit these values. `scenario.json.config` is always the authoritative resolved configuration for a particular plan.

There is one neutral default set and no per-agent preset. Traffic shape comes only from the profile. The coding-agent harness is a wire concern: the required `--headers {claude-code|codex}` flag on `generate` and `replay` selects which harness's headers each request carries, and it never changes the plan. See [Header dialects](#header-dialects).

The defaults are starting points for repeatable simulation, not claims that any Claude Code or Codex deployment has these exact distributions. Values fall into three broad categories:

- Synthetic baseline: plausible traffic geometry chosen to exercise the load generator before workload-specific calibration.
- Stress-oriented: deliberately more aggressive than a native default so short runs cover compaction, recursion, or failures.
- Safety bound: protects planning memory and does not describe agent behavior.

Use trace replay when reproducing one captured workload. For generated traffic, override these defaults from a representative trace corpus whenever performance conclusions depend on the exact distribution. [Codex behavior and simulation controls](codex-behavior.md) records harness-specific calibration values that can be written into a profile.

## Distribution notation

- `fixed(x)` always returns `x`.
- `uniform(a, b)` samples the inclusive integer range.
- `log_normal(median, sigma, min, max)` samples a log-normal value, rounds it, and clamps it to the inclusive bounds.

All sampling is deterministic for the resolved seed. Token segments are rounded up to `tokens.block_size` when the KV-shape graph is built.

## Required identity and seed

| Field | Default | Why |
|---|---:|---|
| `schema_version` | Required: `5` | Prevents an older profile from silently acquiring new semantics. Schema 4 profiles fail with a migration message because they carried an `agent` field. |
| `seed` | `0` | Makes a minimal profile deterministic. Checked-in profiles explicitly use `42` for stable examples. |

## Load defaults

| Field | Default | Why |
|---|---:|---|
| `load.num_sessions` | `16` | Keeps the default finite plan small enough for a quick smoke while producing multiple top-level session trees. |
| `load.concurrent_sessions` | `8` | Exercises closed-loop top-level stream concurrency without treating the default as a production capacity target. |
| `load.startup_interval_ms` | `0` | Starts the initial top-level population together so concurrency is visible immediately. |
| `load.restart_delay_ms` | `fixed(0)` | Replaces a completed top-level session tree immediately; users should add measured user/task idle time when modeling an interactive fleet. |
| `load.duration_minutes` | unset | Streams run until every one of `num_sessions` trees completes. |
| `load.arrivals` | unset | Streams are the default load. An arrival schedule has no defaults: `bucket_minutes` and one rate list are required, and `interpolation` defaults to `step`. |

These are top-level session streams. Subagents are generated within those session trees and can push live session and request concurrency above `load.concurrent_sessions`.

## Trajectory defaults

| Field | Default | Why |
|---|---:|---|
| `trajectory.turns` | `uniform(4, 10)` | Produces multi-turn KV growth without making the smoke plan excessively large. This is a synthetic cap, not a native agent setting. |
| `trajectory.think_time_ms` | `log_normal(750, 0.8, 25, 15000)` | Represents non-tool continuation or later-user delay with a long tail. Native agents do not insert a fixed hidden-reasoning delay. |
| `trajectory.output_tokens` | `log_normal(450, 0.9, 16, 4096)` | Provides varied decode work while bounding each synthetic model response. Native harnesses normally do not force this exact output budget. |

## Token-shape defaults

| Field | Default | Why |
|---|---:|---|
| `tokens.block_size` | `16` | Matches the common Dynamo replay block granularity and keeps prefix topology compact. Override it to match the target trace or engine contract. |
| `tokens.system_prefix_tokens` | `fixed(7000)` | A neutral harness instruction surface. Measure the deployed harness version and override; Claude Code and Codex surfaces have been observed near `12000` and `11000`. |
| `tokens.tool_catalog_tokens` | `fixed(11000)` | A neutral tool-schema surface. Observed harness catalogs range from roughly `9000` (Codex) to `14000` (Claude Code). |
| `tokens.repository_tokens` | `log_normal(2000, 0.7, 256, 12000)` | Creates a root-tree prefix shared by a parent and all descendants, with a long tail for larger repository context. |
| `tokens.session_tokens` | `uniform(128, 1024)` | Adds session-specific environment and instruction state after the shared prefixes. |
| `tokens.user_tokens` | `log_normal(180, 0.9, 16, 4096)` | Models mostly short user turns with occasional large task descriptions. |
| `tokens.tool_result_tokens` | `log_normal(700, 1.1, 16, 16384)` | Models the parent-visible result appended after a subagent join; the wide tail covers large findings and logs. |
| `tokens.context_window_tokens` | `128000` | Supplies a broadly useful context-window basis for synthetic compaction. Set it to the deployed model's actual usable window for calibrated runs. |

## Behavior defaults

The action probabilities are sampled once per non-final model turn. They are not fractions of tool calls: one parallel action contributes every call sampled by `tools.parallel_count`.

| Field | Default | Why |
|---|---:|---|
| `behavior.tool_probability` | `0.48` | Serial tool phases dominate; a neutral split that keeps total tool-phase mass at `0.56`. |
| `behavior.parallel_tool_probability` | `0.08` | A small parallel-batch share. Observed harnesses differ widely: Claude Code batches almost never (`0.0`), Codex batches often (`0.19` of turns with the same total tool mass). Override from traces. |
| `behavior.subagent_probability` | `0.10` | Exercises single-child delegation regularly without dominating the plan. |
| `behavior.swarm_probability` | `0.025` | Rare multi-child fanout so joins and sibling concurrency appear in short runs. |
| `behavior.completion_probability` | `0.10` | Lets sessions end before the sampled turn cap once the minimum of two turns is reached. |

The remaining `0.215` becomes a text/user continuation. At the configured maximum subagent depth, delegation probability also falls through to text because another child cannot be created. The sum of the five probabilities must not exceed one.

## Tool defaults

| Field | Default | Why |
|---|---:|---|
| `tools.parallel_count` | `uniform(2, 4)` | Covers small batches centered on three calls, the scale seen in Codex command clusters. |
| `tools.retry_probability` | `0.35` after a failed call | Exercises retry-shaped delay and KV growth without retrying successful calls. Treat it as a synthetic stress value unless traces support it. |

The built-in tool list is a generic cross-harness baseline. Latency distributions use `sigma=0.9`; result-size distributions use `sigma=1.0`.

| Class | Weight | Latency milliseconds | Result tokens | Error probability | Rough role |
|---|---:|---|---|---:|---|
| `read` | `0.27` | `log_normal(195, 0.9, 40, 350)` | `log_normal(1064, 1.0, 128, 2000)` | `0.01` | Fast local file reads with moderate returned context. |
| `search` | `0.21` | `log_normal(490, 0.9, 80, 900)` | `log_normal(2064, 1.0, 128, 4000)` | `0.03` | Repository search with larger and more variable output. |
| `shell` | `0.21` | `log_normal(1300, 0.9, 100, 2500)` | `log_normal(4032, 1.0, 64, 8000)` | `0.08` | Commands, builds, and tests; an important source of parallel batches. |
| `patch` | `0.13` | `log_normal(960, 0.9, 120, 1800)` | `log_normal(1532, 1.0, 64, 3000)` | `0.05` | File-edit operations with bounded acknowledgements or diagnostics. |
| `network` | `0.08` | `log_normal(2650, 0.9, 300, 5000)` | `log_normal(6064, 1.0, 128, 12000)` | `0.10` | Slow, failure-prone remote operations with large results. |
| `orchestration` | `0.10` | `log_normal(135, 0.9, 20, 250)` | `log_normal(272, 1.0, 32, 512)` | `0.03` | Lightweight spawn, message, wait, and coordination actions. |

Tool class inclusion materially changes time-weighted parallelism. Fit weights and per-class latency from raw tool spans when matching a workload; do not infer time overlap from the action probabilities alone.

## Compaction defaults

| Field | Default | Why |
|---|---:|---|
| `compaction.enabled` | `true` | Makes long generated contexts exercise KV-window replacement automatically. |
| `compaction.trigger_fraction` | `0.78` | Stress-oriented threshold that makes compaction reachable in shorter synthetic runs. The studied Codex source uses a threshold no greater than `0.90`; use that value for a source-aligned Codex profile. |
| `compaction.summary_input_tokens` | `uniform(256, 1024)` | Models a bounded synthetic summarization instruction. |
| `compaction.summary_output_tokens` | `uniform(512, 2048)` | Creates a compact retained summary with varied decode and future-prefix cost. |
| `compaction.retained_recent_tokens` | `8192` | Keeps a meaningful recent tail beside the summary after old context is replaced. |
| `compaction.abort_probability` | `0.0` | Normal traffic has no injected client-aborted attempt. |
| `compaction.retry_probability` | `0.0` | Normal traffic has no injected duplicate attempt. |
| `compaction.max_attempts` | `3` | Bounds fault scenarios when abort or retry injection is explicitly enabled; dormant at the zero probabilities. |
| `compaction.abort_after_ms` | `fixed(10)` | Supplies a deterministic early-cancel delay only when abort injection is enabled. |

## Subagent defaults

| Field | Default | Why |
|---|---:|---|
| `subagents.max_depth` | `2` | Stress-oriented recursion coverage. The studied Codex default permits nesting depth one, so use `1` for that source-aligned workload. |
| `subagents.turns` | `uniform(2, 5)` | Keeps child trajectories shorter than roots while retaining multi-turn KV growth. |
| `subagents.fanout` | `uniform(2, 4)` | Models small sibling swarms without exploding the graph. |
| `subagents.spawn_delay_ms` | `uniform(5, 100)` | Adds lightweight spawn/coordination overhead after the parent model response. |
| `subagents.blocking_probability` | `0.72` | Makes most delegations feed a parent join while retaining non-blocking parent/child overlap. This is a synthetic coordination baseline, not a native harness knob. |

## Safety defaults

| Field | Default | Why |
|---|---:|---|
| `limits.max_nodes` | `10000` | Bounds the number of materialized model-request nodes. |
| `limits.max_sessions` | `1000` | Bounds roots plus recursively generated child sessions. |
| `limits.max_total_input_tokens` | `250000000` | Bounds aggregate plan size before HTTP payload construction. |

These are planner memory guards. Runtime HTTP concurrency is separately bounded by `generate --max-in-flight`.

## Header dialects

The dialect is a run-time flag, recorded in `run.json` as `header_dialect`, and is not part of `scenario.json` or the plan digest. `claude-code` sends Claude Code's session, subagent, and gateway hint headers; `codex` sends the Codex client's session, thread, window, and turn-metadata headers. Neither sends `x-dynamo-session-id`. See [Harness headers](headers.md) for every header and value.

## Checked-in profiles

`profiles/balanced.toml` sets `seed = 42` and nothing else; it is the neutral default set above.

`profiles/tools-heavy.toml` sets `seed = 42`, widens root turns to `uniform(4, 12)`, and replaces the generic tool list with a faster, smaller-result calibration of the same six classes. It is intentionally a useful load shape, not a native-harness calibration.

`profiles/claude-code.toml`, `profiles/codex.toml`, and `profiles/swe-bench.toml` write harness- and workload-shaped values into every section, `profiles/diurnal-8h.toml` puts the Claude Code shape under an eight-hour request-rate schedule, and `profiles/astra-fleet.toml` is fitted to the astra production fleet's agent sessions and daily start pattern. Each file's header comment separates measured values from assumptions; see [Describe a workload](workloads.md). They are shapes only: pair them with the matching `--headers` dialect to also match the harness on the wire.

Reasoning versus visible output, exact per-class parallel eligibility, history-fork modes, and model-specific compaction output still require trace evidence.
