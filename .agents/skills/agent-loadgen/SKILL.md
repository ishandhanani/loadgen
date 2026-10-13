---
name: agent-loadgen
description: Generate realistic LLM traffic with agent-loadgen and run it against an OpenAI-compatible or Dynamo endpoint. Use to turn a description of a company's or fleet's traffic (coding agents, batch evals, or plain API calls) into profiles, check them, run them, and read the results.
---

# agent-loadgen

agent-loadgen sends synthetic requests whose shape is real: token counts, prefix sharing, session structure, tool and human delays, and arrival times. Prompt text is meaningless. A profile (TOML) describes the traffic; `plan` checks it without sending anything; `export-plan` writes finite deterministic trees for offline analysis; `generate` sends them.

Read these when you need detail: `docs/workloads.md` (the procedure below, longer), `docs/generator.md` (every profile field and default), `docs/headers.md` (what each `--headers` dialect sends).

## Build

```bash
cargo build --release    # binary: target/release/agent-loadgen
```

If the build fails with `could not execute process 'sccache ...'`, prefix it with `RUSTC_WRAPPER=`.

## Workflow

1. **Split the description into kinds of traffic.** Each kind gets one profile and, at run time, one `generate` process; a company's mix runs as several processes at once. Typical kinds:
   - People using coding agents: start from `profiles/claude-code.toml` or `profiles/codex.toml`.
   - A batch evaluation with N parallel agents: start from `profiles/swe-bench.toml`.
   - Plain API or chat calls (not agentic): write the one-request profile below.
2. **Choose when sessions start** (the profile's `[load]`):
   - A population of users: `[load.arrivals]` with `bucket_minutes` and one rate per bucket, as `requests_per_second` (when the description counts requests) or `sessions_per_minute` (when it counts users or sessions). Hourly buckets describe a day; `interpolation = "linear"` smooths the steps.
   - A fixed pool of workers: `num_sessions`, `concurrent_sessions`, optional `restart_delay_ms` and `duration_minutes`.
3. **Map the rest of the description** onto fields: requests per session (`trajectory.turns`), output length (`trajectory.output_tokens`), shared prompt prefix (`tokens.system_prefix_tokens`, `tokens.tool_catalog_tokens`), per-session content (`tokens.repository_tokens`, `session_tokens`, `user_tokens`), tool mix and tool time (`[[tools.classes]]`), time between user prompts (`trajectory.think_time_ms`), subagents (`behavior.subagent_probability`, `[subagents]`), compaction (`tokens.context_window_tokens`, `[compaction]`). The mapping table is in `docs/workloads.md`. In each profile's header comment, say which values came from the description and which you assumed.
4. **Plan and compare**, then adjust the profile and repeat until it matches:

   ```bash
   target/release/agent-loadgen plan --config acme-claude.toml --output plans/acme-claude
   ```

   `plans/acme-claude/plan-summary.json` has, from up to 1,000 sampled sessions:
   - `per_tree.requests`: requests per top-level session (percentiles and mean).
   - `per_request.input_tokens`, `per_request.output_tokens`: request size percentiles.
   - `per_request.ideal_prefix_hit_rate`: prefix-cache hit rate with unlimited cache and ideal routing; a real deployment lands below it.
   - `arrivals.buckets[].expected_requests_per_second`: the request rate each bucket should produce.
   - `expected_totals`: requests and tokens over the whole plan.
   - `top_level_sessions`, `duration_minutes`.

5. **Run it** (only when asked to send traffic):

   ```bash
   target/release/agent-loadgen generate --config acme-claude.toml \
     --headers claude-code --model <served-model-name> \
     --target http://<frontend>:8000 --tokenizer <hf-model-id-or-local-dir> \
     --output runs/acme-claude --stop-after-minutes 120
   ```

   - `--headers claude-code` or `--headers codex` (required) chooses which harness the requests look like. Use `codex` for API traffic; see the limits below.
   - `--tokenizer` must be the served model's tokenizer.
   - `--target` is a base URL or a full `/v1/chat/completions` URL, `http` or `https`. Send credentials with `--header Authorization="Bearer <token>"`.
   - `--stop-after-minutes` stops sending at a deadline and lets in-flight requests finish. Arrival sessions keep running after their schedule ends, so set it for time-boxed runs.
   - Concurrency comes from the profile and the target's latency; there is no concurrency flag. The client only guards its own file descriptors: if `run.json` shows `peak_in_flight` equal to `in_flight_limit`, raise `ulimit -n` and rerun.
   - For a mix, start one `generate` per profile at the same time, each with its own `--output`.
   - On a SLURM cluster, run it as an srt-slurm custom benchmark: `examples/srt-slurm/` has a recipe and scripts.
6. **Export finite trees** when offline analysis needs exact request dependencies, block hashes, tool events, session lineage, or compaction metadata:

   ```bash
   target/release/agent-loadgen export-plan --config profiles/swe-bench.toml \
     --output plans/swe-bench-export
   ```

   This writes `trees.jsonl` and `plan-summary.json` without sending traffic. Duration-bounded streams must set `load.num_sessions` because execution latency otherwise determines how many trees start.
7. **Read the results** in the output directory:
   - `run.json`: `passed` (every sent request succeeded with its planned output length; the command exits non-zero otherwise), `failed`, `stopped_early`, `http_version`, `peak_in_flight` (the most requests in flight at once; under arrivals, the concurrency the load produced), and `performance` (requests and tokens per second; TTFT, request latency, and time-per-output-token percentiles).
   - `timeline.csv`: one row per minute with requests, failures, root sessions started, request and token rates, and TTFT and latency percentiles. Compare it with the plan's arrival buckets.
   - `requests.jsonl`: one row per request. `replay_request_id` is the request's `x-request-id`, which joins it with Dynamo request traces and engine logs.

## Plain API traffic

A session of exactly one request with no tools, subagents, or compaction is one independent API call. Put the shared system prompt in `system_prefix_tokens` and the request content in `user_tokens`:

```toml
schema_version = 5
seed = 42

[load.arrivals]
bucket_minutes = 60
requests_per_second = [5.0, 12.0, 20.0, 12.0]

[trajectory]
turns = { kind = "fixed", value = 1 }
output_tokens = { kind = "log_normal", median = 250.0, sigma = 1.0, min = 8, max = 4096 }

[tokens]
system_prefix_tokens = { kind = "fixed", value = 1500 }
tool_catalog_tokens = { kind = "fixed", value = 0 }
repository_tokens = { kind = "fixed", value = 0 }
session_tokens = { kind = "fixed", value = 0 }
user_tokens = { kind = "log_normal", median = 800.0, sigma = 1.2, min = 16, max = 32000 }

[compaction]
enabled = false

[subagents]
max_depth = 0
```

## Compressing time

To play a long schedule faster at the same load level, shrink `bucket_minutes`: 7.5-minute buckets play eight hourly buckets in one hour. Multiply the rates too if the run must send the whole period's requests. Say which you did.

## Limits

- Requests go to Chat Completions with synthetic token IDs in `nvext.token_data`. The endpoint must accept them unchanged, as Dynamo's frontend does. Other servers need that extension.
- Every request carries Claude Code or Codex session headers, even for plain API traffic, whose real clients usually send none.
- Output lengths are forced with `max_tokens` and `ignore_eos`.
- A session's timing is closed-loop: a slow target stretches sessions, which raises live concurrency under an arrival schedule and lowers the request rate of streams.
- Each run salts per-session content, so repeated runs do not reuse each other's KV; the shared system prompt and tool catalog stay identical across runs.
- Do not edit agent-loadgen itself to fit a description. If the description needs something profiles cannot express, report it.
