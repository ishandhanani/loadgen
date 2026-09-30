# Describe a workload

This guide turns a description of coding-agent traffic, such as "eight hours of Claude Code users peaking at 95 requests per second", into a profile, checks the profile against the description, and runs it against a deployment. The profile describes shape: when sessions start, what each session does, and how large its requests are. `--headers` separately makes the requests look like Claude Code or Codex on the wire.

## 1. Start from a shape

Copy the checked-in profile closest to the traffic:

| Profile | Shape |
|---|---|
| `profiles/claude-code.toml` | Interactive Claude Code sessions: long serial tool loops, blocking Task subagents one level deep, human pauses between prompts. |
| `profiles/codex.toml` | Interactive Codex sessions: shell-heavy loops with parallel exec batches, asynchronous child threads, compaction at 90%. |
| `profiles/swe-bench.toml` | A SWE-bench Verified evaluation: 500 tasks on 32 concurrent workers, one prompt then about 36 tool turns, no human, no subagents. |
| `profiles/diurnal-8h.toml` | The Claude Code shape under an eight-hour hourly request-rate schedule. |
| `profiles/astra-fleet.toml` | The astra production fleet's agent traffic, fitted to 1.16M requests over 63 days, under its measured day of session starts. |

Each file states which values come from measured traffic and which are assumptions. [Generator configuration](generator.md) defines every field.

## 2. Describe when sessions start

There are two load models. Pick the one that matches how the traffic is produced.

**Arrivals** model a population of users. Top-level sessions start at Poisson times whose rate follows a schedule, and each session then runs closed-loop: its next request waits for the previous response and the sampled tool or human delay. Use this for interactive fleets.

```toml
[load.arrivals]
bucket_minutes = 60
requests_per_second = [20.0, 45.0, 80.0, 60.0, 70.0, 95.0, 65.0, 30.0]
interpolation = "linear"   # or "step" (the default)
```

One rate per consecutive bucket; the schedule lasts `bucket_minutes` times the number of rates. Give the rates as `requests_per_second`, when the description counts model requests, or `sessions_per_minute`, when it counts sessions or users. A request rate is converted to a session rate with the profile's mean requests per session, estimated from 1,000 planned sessions. `step` holds each rate for its bucket; `linear` ramps between bucket midpoints.

**Streams** model a fixed pool of workers, such as a benchmark harness with N parallel agents. Each stream runs one session tree after another.

```toml
[load]
num_sessions = 500          # total session trees; omit to run until duration_minutes
concurrent_sessions = 32    # parallel streams
restart_delay_ms = { kind = "uniform", min = 1000, max = 5000 }
duration_minutes = 120      # optional: start no new tree after this
```

Neither model fixes the request rate exactly. Requests follow model responses, so the achieved rate depends on target latency, and a session's requests trail its start by the session's length. When sessions last tens of minutes, begin the schedule with a ramp bucket, or read the steady-state buckets only.

## 3. Match the description

Map each fact in the description onto the profile:

| Description says | Profile field |
|---|---|
| Requests or sessions per hour | `[load.arrivals]` rates |
| Parallel agents in a batch job | `load.concurrent_sessions` |
| Requests per session, session length | `trajectory.turns`, `behavior.completion_probability` |
| Input length, context growth | `tokens.*` sizes and tool `result_tokens` |
| Output length | `trajectory.output_tokens` |
| Prefix-cache hit rate | shared `tokens.system_prefix_tokens` and `tool_catalog_tokens` against per-session content |
| Tool mix and tool time | `tools.classes` weights and `latency_ms` |
| Parallel tool calls | `behavior.parallel_tool_probability`, `tools.parallel_count` |
| Time between user prompts | `trajectory.think_time_ms` |
| Subagent or swarm use | `behavior.subagent_probability`, `swarm_probability`, `[subagents]` |
| Compaction frequency | `tokens.context_window_tokens`, `compaction.trigger_fraction` |

Then plan the profile and compare its summary with the description:

```bash
agent-loadgen plan --config customer.toml --output artifacts/customer-plan
```

`plan` writes `plan-summary.json` for a plan of any size. It samples up to 1,000 session trees and reports:

- `per_tree`: requests, protocol sessions, and compactions per top-level session, and the planned client-side delay in its root session.
- `per_request`: input and output token percentiles, and `ideal_prefix_hit_rate`, the share of input tokens in a prefix already sent earlier in the same session tree or shared by every session. That is the prefix-cache hit rate with unlimited cache and ideal routing; compare it with the deployment's observed hit rate.
- `expected_totals`: requests and tokens over the whole plan.
- `arrivals.buckets`: per bucket, the configured rate, the session rate, the sampled session starts, and the expected request rate.

Adjust fields and plan again until the summary matches. Plans that fit `limits.max_nodes` also write `scenario.json` and `plan.dot`, the full request graph.

## 4. Run it against a deployment

```bash
agent-loadgen generate --config customer.toml --headers claude-code \
  --model <served-model-name> --target http://<dynamo-frontend>:8000 \
  --tokenizer <hf-model-id-or-local-dir> --output artifacts/customer-run \
  --max-in-flight 4096
```

Generation plans each session tree shortly before it starts and drops it when it completes, so memory follows live sessions rather than the length of the schedule.

- `--headers claude-code` or `--headers codex` selects the harness headers. See [Harness headers](headers.md).
- `--stop-after-minutes N` stops sending after N minutes, lets in-flight requests finish, and reports the requests that were sent. Use it to cut the tail of sessions still running after a schedule ends, or to fit a run into a time slot.
- Each run mixes a fresh salt into session content after the shared prefix, so a second run of the same profile does not hit KV cached by the first; the synthetic system prompt and tool catalog stay identical across runs, as they would for a real harness.
- Generated session IDs on the wire are UUIDs unique to the run.
- The target can be `http://` or `https://`. Plain HTTP tries HTTP/2 first, which Dynamo's frontend speaks, and falls back to HTTP/1.1; `run.json` records the version used. Pass credentials with `--header Authorization="Bearer <token>"`.
- To see a whole day in less time at its real load level, shrink `bucket_minutes`.

## 5. Read the results

- `run.json` records the header dialect, request and error counts, output-length fidelity, dispatch lag, and `performance`: achieved requests and tokens per second, and TTFT, request latency, and time per output token percentiles.
- `timeline.csv` has one row per minute: requests, failures, root sessions started, request and token rates, and TTFT and latency percentiles. Compare it with `plan-summary.json`'s arrival buckets.
- `requests.jsonl` has one row per request. Its `replay_request_id` is the request's `x-request-id`, which joins it with Dynamo request traces and engine telemetry.
