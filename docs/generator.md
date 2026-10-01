# Generator configuration

Generator profiles are strict, versioned TOML. Unknown fields and unsupported schema versions fail during planning. Every omitted field resolves to a documented default, and `plan-summary.json` records the digest of the resolved profile.

Schema version 5 defines traffic shape entirely in the profile. There is no `agent` field and no per-agent preset: every omitted field resolves to the single neutral default set listed under [Defaults](#defaults). The coding-agent harness is a run-time wire choice, `--headers {claude-code|codex}`, and it never changes the plan or its digests.

Schema 4 profiles are rejected with a migration message: delete the `agent` line, set `schema_version = 5`, and pass `--headers` if a harness dialect is wanted. The five values the old presets varied (`tokens.system_prefix_tokens`, `tokens.tool_catalog_tokens`, `behavior.tool_probability`, `behavior.parallel_tool_probability`, `behavior.subagent_probability`) can be written into the profile explicitly to reproduce an old preset. Schema 3 names (`root_sessions`, `concurrent_agents`) remain rejected; there are no compatibility aliases.

The load controls count only top-level agent session trees; subagent sessions are generated descendants in those trees. Independent work uses subagents with separate session lineage and explicit dependencies. [Describe a workload](workloads.md) walks from a traffic description to a profile.

## Execution model

```text
arrival or stream -> session tree -> model request
                                  |
                                  +--text----------think delay------> next request
                                  +--tool----------tool latency-----> next request
                                  +--parallel tools-max latency-----> next request
                                  +--compaction attempt-compaction latency----> next request
                                  +--subagent------child completion-> parent join
                                  +--swarm---------all children-----> parent join
                                  +--complete response--restart delay------> next session tree in stream
```

Generated traffic is closed-loop inside each session tree. A tree starts at its scheduled arrival, or, for streams, during the ramp-up or `restart_delay_ms` after its stream's previous tree completes. Every later request becomes ready only after all graph dependencies complete, plus its sampled virtual delay. The final model response of the root session completes the tree. Target response latency therefore shapes the generated request rate. The scheduler can still run independent streams and sibling subagents concurrently. A blocking child or swarm joins the parent. A non-blocking child continues in its own session while the parent continues.

The generator does not execute tools or generate meaningful text. Tool classes sample delay, result-token geometry, failure, and retry behavior.

## Planning and execution

Each top-level session tree samples from its own random stream, derived from the seed and the tree's index, so any tree can be planned alone and in any order with the same result. `generate` plans each tree about two seconds before it starts and drops it when its last request completes, so a run of any length holds only live trees in memory.

`plan` and `generate` both write `plan-summary.json`: session-tree and request statistics from up to 1,000 sampled trees, expected totals, the ideal prefix-cache hit rate, and, for arrivals, the per-bucket session starts and expected request rate.

## Minimal profile

```toml
schema_version = 5
seed = 42
```

Every other field is an optional override of the neutral defaults; the sections below define the complete surface, and [Defaults](#defaults) lists every default with its rationale.

The surface is intentionally detailed. A few fields correspond to native agent settings, such as the compaction threshold and subagent depth. Most make runtime outcomes that a real agent's model chooses, such as whether a turn calls a tool, deterministic and tunable. Fit them from traces of the workload; the checked-in profiles record where their values come from.

## Distributions

Token sizes, turn counts, and delays accept one of these forms:

```toml
value = { kind = "fixed", value = 8 }
value = { kind = "uniform", min = 4, max = 12 }
value = { kind = "log_normal", median = 450.0, sigma = 0.9, min = 16, max = 4096 }
```

All sampling uses the profile seed. Log-normal samples are rounded and clamped to the configured inclusive bounds.

## Load

A profile uses either streams, the default, or an arrival schedule.

Streams are a fixed pool of closed-loop workers:

- `load.num_sessions`: total top-level agent session trees. Omit it with `duration_minutes` to run until the duration.
- `load.concurrent_sessions`: number of closed-loop top-level session streams. It must not exceed `num_sessions`.
- `load.startup_interval_ms`: spacing between the first session tree in each stream; zero starts the full population together.
- `load.restart_delay_ms`: sampled idle delay between a completed session tree and its stream's next tree.
- `load.duration_minutes`: no stream starts a new tree after this many minutes; trees already running finish.

For example, `num_sessions = 100` and `concurrent_sessions = 10` runs ten active top-level streams, each processing ten session trees. With a zero restart delay, every stream replaces a completed tree immediately. Subagents are generated inside those trees and are not counted by either setting, but can temporarily push live request concurrency above ten.

An arrival schedule starts top-level trees at Poisson times instead, independent of how fast earlier trees finish:

- `load.arrivals.bucket_minutes`: length of each schedule bucket; the schedule lasts this times the number of rates.
- `load.arrivals.sessions_per_minute` or `load.arrivals.requests_per_second`: exactly one, with one non-negative rate per bucket. Request rates become session rates through the mean requests per tree of 1,000 planned trees.
- `load.arrivals.interpolation`: `step` (default) holds each bucket's rate; `linear` ramps between bucket midpoints.

`[load.arrivals]` cannot be combined with the stream fields. Arrival times are sampled once from their own seeded random stream, so a profile and seed always produce the same start times.

## Trajectory and token shape

- `trajectory.turns`: maximum foreground model turns per top-level session.
- `trajectory.think_time_ms`: delay after a sampled text response.
- `trajectory.output_tokens`: requested output length.
- `tokens.block_size`: generated KV block size and token-segment quantization.
- `tokens.system_prefix_tokens`: global prefix shared by every session.
- `tokens.tool_catalog_tokens`: global tool-definition prefix shared by every session.
- `tokens.repository_tokens`: prefix shared by one top-level session tree and all of its descendants.
- `tokens.session_tokens`: environment/instruction prefix unique to a session.
- `tokens.user_tokens`: appended user-message size.
- `tokens.tool_result_tokens`: appended parent join-result size after child agents.
- `tokens.context_window_tokens`: compaction threshold basis.

The configured token sizes are rounded up to complete KV blocks. This keeps prefix construction simple and deterministic while preserving the intended cache geometry.

## Behavior

- `behavior.tool_probability`: one external tool call.
- `behavior.parallel_tool_probability`: two or more concurrent external tool calls represented by their maximum latency.
- `behavior.subagent_probability`: one child session.
- `behavior.swarm_probability`: sampled child fanout.
- `behavior.completion_probability`: early completion after the minimum trajectory prefix.

The first five probabilities must sum to at most 1.0. Remaining probability becomes a text/user turn. The last configured turn is always a completion so every non-truncated session terminates.

These are probabilities per model turn, not fractions of tool calls. A parallel phase contributes every sampled call in `tools.parallel_count`. For example, a serial probability of `0.39`, a parallel probability of `0.19`, and an average parallel count of three produce about 59% of tool calls in parallel phases even though only 19% of model turns select that action.

## Tool classes

`tools.classes` replaces the default list. Weights are relative and do not need to sum to one. Each selected class samples latency and result size, then independently samples `error_probability`. `tools.retry_probability` controls whether a failed call adds one more latency and result sample. `tools.parallel_count` controls tool fanout for a parallel action.

```toml
[tools]
parallel_count = { kind = "uniform", min = 2, max = 4 }
retry_probability = 0.35

[[tools.classes]]
name = "shell"
weight = 0.5
latency_ms = { kind = "log_normal", median = 600.0, sigma = 1.0, min = 100, max = 2500 }
result_tokens = { kind = "log_normal", median = 1200.0, sigma = 1.1, min = 64, max = 8000 }
error_probability = 0.08
```

## Compaction

- `compaction.enabled`: enables context-window compaction.
- `compaction.trigger_fraction`: fraction of `context_window_tokens` that triggers compaction before the next turn.
- `compaction.summary_input_tokens`: synthetic compaction-instruction tokens added to the compaction request.
- `compaction.summary_output_tokens`: summary output length and summary size retained in the next context window.
- `compaction.retained_recent_tokens`: recent non-stable context retained beside the summary.
- `compaction.abort_probability`: probability that a logical compaction starts with one client-aborted physical attempt.
- `compaction.retry_probability`: probability that a successful attempt is followed by a duplicate physical attempt.
- `compaction.max_attempts`: maximum physical attempts per logical operation.
- `compaction.abort_after_ms`: sampled delay before canceling an aborted attempt.

See [Compaction](compaction.md) for the logical operation, physical attempts, KV-window changes, and validation limits.

## Subagents and swarms

- `subagents.max_depth`: maximum recursive child depth.
- `subagents.turns`: foreground turns per child.
- `subagents.fanout`: child count for a swarm.
- `subagents.spawn_delay_ms`: delay between parent completion and child readiness, and the synthetic join delay.
- `subagents.blocking_probability`: probability that the parent next turn depends on every child completion.

See [Subagents and swarms](subagents.md) for graph shape, joins, KV sharing, recursion, and current limits.

## Safety limits

- `limits.max_nodes`: maximum requests in one top-level session tree.
- `limits.max_sessions`: maximum protocol sessions, including descendants, in one tree.
- `limits.max_total_input_tokens`: maximum summed input length of one tree.

A tree that reaches a limit is cut short. Trees are planned one at a time, so the limits apply per tree, not to the whole plan. These limits bound planner memory; they do not cap live HTTP concurrency, which comes from the load model and the target's latency.

## Reproducibility

The plan records a resolved-profile SHA-256 digest. Given the same binary behavior and config, the same seed produces the same arrival times, top-level session trees, descendant graph, token labels, and samples. Runtime request IDs and measured timing are intentionally run-specific.

## Defaults

The defaults are starting points for repeatable simulation, not claims that any Claude Code or Codex deployment has these exact distributions. Some are a plausible synthetic baseline, some are deliberately stress-oriented so short runs cover compaction and recursion, and the limits only protect planner memory. `plan-summary.json.load` records the resolved load of a plan. `fixed(x)`, `uniform(a, b)`, and `log_normal(median, sigma, min, max)` abbreviate the distributions above.

### Identity and seed

| Field | Default | Why |
|---|---:|---|
| `schema_version` | Required: `5` | Prevents an older profile from silently acquiring new semantics. Schema 4 profiles fail with a migration message because they carried an `agent` field. |
| `seed` | `0` | Makes a minimal profile deterministic. Checked-in profiles explicitly use `42` for stable examples. |

### Load defaults

| Field | Default | Why |
|---|---:|---|
| `load.num_sessions` | `16` | Keeps the default finite plan small enough for a quick smoke while producing multiple top-level session trees. |
| `load.concurrent_sessions` | `8` | Exercises closed-loop top-level stream concurrency without treating the default as a production capacity target. |
| `load.startup_interval_ms` | `0` | Starts the initial top-level population together so concurrency is visible immediately. |
| `load.restart_delay_ms` | `fixed(0)` | Replaces a completed top-level session tree immediately; users should add measured user/task idle time when modeling an interactive fleet. |
| `load.duration_minutes` | unset | Streams run until every one of `num_sessions` trees completes. |
| `load.arrivals` | unset | Streams are the default load. An arrival schedule has no defaults: `bucket_minutes` and one rate list are required, and `interpolation` defaults to `step`. |

These are top-level session streams. Subagents are generated within those session trees and can push live session and request concurrency above `load.concurrent_sessions`.

### Trajectory defaults

| Field | Default | Why |
|---|---:|---|
| `trajectory.turns` | `uniform(4, 10)` | Produces multi-turn KV growth without making the smoke plan excessively large. This is a synthetic cap, not a native agent setting. |
| `trajectory.think_time_ms` | `log_normal(750, 0.8, 25, 15000)` | Represents non-tool continuation or later-user delay with a long tail. Native agents do not insert a fixed hidden-reasoning delay. |
| `trajectory.output_tokens` | `log_normal(450, 0.9, 16, 4096)` | Provides varied decode work while bounding each synthetic model response. Native harnesses normally do not force this exact output budget. |

### Token-shape defaults

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

### Behavior defaults

The action probabilities are sampled once per non-final model turn. They are not fractions of tool calls: one parallel action contributes every call sampled by `tools.parallel_count`.

| Field | Default | Why |
|---|---:|---|
| `behavior.tool_probability` | `0.48` | Serial tool phases dominate; a neutral split that keeps total tool-phase mass at `0.56`. |
| `behavior.parallel_tool_probability` | `0.08` | A small parallel-batch share. Observed harnesses differ widely: Claude Code batches almost never (`0.0`), Codex batches often (`0.19` of turns with the same total tool mass). Override from traces. |
| `behavior.subagent_probability` | `0.10` | Exercises single-child delegation regularly without dominating the plan. |
| `behavior.swarm_probability` | `0.025` | Rare multi-child fanout so joins and sibling concurrency appear in short runs. |
| `behavior.completion_probability` | `0.10` | Lets sessions end before the sampled turn cap once the minimum of two turns is reached. |

The remaining `0.215` becomes a text/user continuation. At the configured maximum subagent depth, delegation probability also falls through to text because another child cannot be created. The sum of the five probabilities must not exceed one.

### Tool defaults

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

### Compaction defaults

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

### Subagent defaults

| Field | Default | Why |
|---|---:|---|
| `subagents.max_depth` | `2` | Stress-oriented recursion coverage. The studied Codex default permits nesting depth one, so use `1` for that source-aligned workload. |
| `subagents.turns` | `uniform(2, 5)` | Keeps child trajectories shorter than roots while retaining multi-turn KV growth. |
| `subagents.fanout` | `uniform(2, 4)` | Models small sibling swarms without exploding the graph. |
| `subagents.spawn_delay_ms` | `uniform(5, 100)` | Adds lightweight spawn/coordination overhead after the parent model response. |
| `subagents.blocking_probability` | `0.72` | Makes most delegations feed a parent join while retaining non-blocking parent/child overlap. This is a synthetic coordination baseline, not a native harness knob. |

### Safety defaults

| Field | Default | Why |
|---|---:|---|
| `limits.max_nodes` | `10000` | Bounds the number of model requests planned for one tree. |
| `limits.max_sessions` | `1000` | Bounds roots plus recursively generated child sessions. |
| `limits.max_total_input_tokens` | `250000000` | Bounds aggregate plan size before HTTP payload construction. |

These are planner memory guards. They do not bound live HTTP concurrency.
