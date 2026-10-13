# agent-loadgen

[![A 90-second tour of a workload profile: each TOML section, what its knobs change in the plan, then plan and generate](docs/assets/agent-loadgen-intro.webp)](docs/assets/agent-loadgen-intro.mp4)

Click the animation for the full-resolution video with playback controls.

`agent-loadgen` load-tests a Dynamo OpenAI-compatible frontend with coding-agent traffic. A TOML profile describes the traffic, such as Claude Code or Codex users through a working day, a SWE-bench-style evaluation, or single-request API calls. `agent-loadgen` plans it deterministically from a seed and sends it with the session, subagent, and compaction headers of the harness you choose.

Prompt text is synthetic. What stays faithful is what a serving system sees: input and output lengths, KV-prefix reuse within and across sessions, agent session lineage, and causal timing, in which each request waits for the response, tool call, or human pause before it.

## Install

Every commit on `main` that passes CI is published as a [release](https://github.com/NVIDIA-dev/agent-loadgen/releases) named `v<major>.<minor>.<commits on main>`. Each release has two Linux binaries that need glibc 2.34 or newer, as in Ubuntu 22.04 and current SGLang and Dynamo containers: `agent-loadgen` for x86_64 and `agent-loadgen-aarch64` for Arm hosts such as Grace. The repository is internal, so download with an authenticated `gh`; without a tag it takes the latest release:

```bash
gh release download -R NVIDIA-dev/agent-loadgen --pattern agent-loadgen
# On Arm: gh release download -R NVIDIA-dev/agent-loadgen --pattern agent-loadgen-aarch64 --output agent-loadgen
chmod +x agent-loadgen
./agent-loadgen --version
```

To build from source with Rust 1.85 or newer, run `cargo build --release`; the binary is `target/release/agent-loadgen`.

## Quick start

Plan a profile to see what it would send, without sending anything:

```bash
agent-loadgen plan --config profiles/diurnal-8h.toml --output artifacts/diurnal-plan
```

Export every deterministic session tree from a finite profile for offline analysis:

```bash
agent-loadgen export-plan --config profiles/swe-bench.toml --output artifacts/swe-bench-plan
```

This writes `trees.jsonl` alongside `plan-summary.json`. Duration-bounded streams must set
`load.num_sessions` before they can be exported.

Then run it against a deployment, here with Claude Code's headers, and stop after two hours:

```bash
agent-loadgen generate --config profiles/diurnal-8h.toml --headers claude-code \
  --model zai-org/GLM-4.7-Flash --target http://127.0.0.1:8000 \
  --tokenizer zai-org/GLM-4.7-Flash --output artifacts/diurnal-run \
  --stop-after-minutes 120
```

To turn a description of real traffic into profiles, follow [Describe a workload](docs/workloads.md). Coding agents can follow the [agent-loadgen skill](.agents/skills/agent-loadgen/SKILL.md), which agents that read `.agents/skills/`, such as Codex, load automatically in this repository.

## Profiles

A profile is strict, versioned TOML: unknown fields fail, every omitted field takes a documented default, and the same profile and seed always plan the same traffic. A profile sets:

- **Load:** a fixed pool of closed-loop streams, such as 64 parallel evaluation agents, or sessions arriving on an hours-long schedule given in sessions per minute or requests per second.
- **Sessions:** turn counts, output lengths, human think time between prompts, and the token sizes of the system prompt, tool catalog, repository context, user messages, and tool results.
- **Tools:** the chance that a turn calls one tool or a parallel batch, and tool classes with their latency, result size, error, and retry behavior.
- **Subagents:** single children and swarms, recursion depth, and whether the parent waits for them.
- **Compaction:** the context-window threshold, the summary that replaces old context, and optional aborted or duplicate compaction attempts.

Start from a checked-in profile. Each states which of its values were measured and which are assumptions.

| Profile | Traffic |
|---|---|
| `profiles/claude-code.toml` | Interactive Claude Code sessions: long serial tool loops, blocking Task subagents, human pauses between prompts. |
| `profiles/codex.toml` | Interactive Codex sessions: parallel exec batches, asynchronous child threads, compaction at 90% of the context window. |
| `profiles/swe-bench.toml` | A SWE-bench Verified evaluation: 500 tasks on 32 parallel agents. |
| `profiles/diurnal-8h.toml` | The Claude Code shape under an eight-hour request-rate schedule. |

[Generator configuration](docs/generator.md) defines every field and default.

## Planning

`agent-loadgen plan` writes `plan-summary.json` for a plan of any size from up to 1,000 sampled session trees: requests, sessions, and client-side delay per tree; input and output token percentiles; the ideal prefix-cache hit rate, with unlimited cache and ideal routing; expected totals; and, for schedules, the sessions and requests expected in each bucket. Compare it with the traffic you are matching before you send any.

`agent-loadgen export-plan` additionally streams every tree from a finite plan to `trees.jsonl`.
Each row includes request dependencies, logical block hashes, output budgets, tool events, session
lineage, compaction attempts, and final-session markers. It does not execute requests or tools.
See the [DynoSim example](examples/dynosim/README.md) to convert this plan into an
`agentic_mooncake` replay trace.

## Running

`agent-loadgen generate` plans each session tree about two seconds before it starts and drops it when it finishes, so memory follows live sessions, not the length of the run. Each request waits for the responses, tool latencies, think time, and subagent joins it depends on, so a slower target stretches sessions: under a schedule that raises live concurrency, and with streams it lowers the request rate.

| Flag | Meaning |
|---|---|
| `--config` | The profile to run. |
| `--headers {claude-code,codex}` | The harness whose session, subagent, and compaction headers every request carries. See [Harness headers](docs/headers.md). |
| `--model`, `--target` | The served model name, and the frontend's base URL or full Chat Completions URL, over `http://` or `https://`. |
| `--tokenizer` | The served model's tokenizer: a Hugging Face model ID, or a local `tokenizer.json` or model directory. Synthetic prompts use only its ordinary token IDs. |
| `--output` | Directory for the plan summary and the run's results. |
| `--stop-after-minutes` | Stop sending after this long, let in-flight requests finish, and report what was sent. |
| `--timeout-seconds` | Per-request timeout; 600 by default. |
| `--header NAME=VALUE` | An extra request header, such as `Authorization`. Repeatable; harness headers cannot be overridden. |
| `--dynamo-session-final` | Mark each session's last request with `x-dynamo-session-final: true`, for deployments that consume it. |

Each request goes to Chat Completions with exact token IDs in `nvext.token_data`, a fixed `max_tokens`, and `ignore_eos`, so input and output lengths match the plan. Each run salts session content with a fresh value, so a repeated run does not reuse KV cached by the previous one; the shared system prompt and tool catalog stay identical across runs, as they would for a real harness. Session IDs on the wire are UUIDs unique to the run. Plain HTTP tries HTTP/2 first and falls back to HTTP/1.1. The client does not limit concurrency: it raises its open-file limit and holds a request back only when it would run out of descriptors.

To run inside an srt-slurm job on a cluster, see [the srt-slurm example](examples/srt-slurm/README.md).

## Results

| File | Contents |
|---|---|
| `plan-summary.json` | The plan summary, also written at the start of `generate`. |
| `run.json` | The header dialect and HTTP version, what the run planned, request and error counts, output-length fidelity, dispatch lag, peak requests in flight, and achieved throughput with TTFT, request latency, and time-per-output-token percentiles. |
| `requests.jsonl` | One row per request with its timing, lengths, and status. `replay_request_id` is the request's `x-request-id`, which joins it with Dynamo request traces and engine telemetry. |
| `timeline.csv` | Per minute: requests, failures, sessions started, request and token rates, and TTFT and latency percentiles. |

A run passes, and the command exits with status 0, when every request it sent succeeded with its planned output length.

## Documentation

- [Describe a workload](docs/workloads.md): from a description of traffic to profiles, a plan check, a run, and its results.
- [Generator configuration](docs/generator.md): every profile field, load model, distribution, limit, and default.
- [Harness headers](docs/headers.md): the Claude Code and Codex session, subagent, compaction, and gateway hint headers.
- [Subagents and swarms](docs/subagents.md): delegation, joins, lineage, and KV sharing between parent and child sessions.
- [Compaction](docs/compaction.md): trigger logic, physical attempts, and how compaction changes later KV shape.
- [How it works](docs/how-it-works.md): planning, causal scheduling, the request path, timing fields, and the crate layout.

## Limits

- The only request surface is Chat Completions with `nvext.token_data`, so the frontend must pass token IDs to the engine unchanged, as Dynamo's Rust preprocessor does.
- Prompt text is synthetic, and tools are not executed; their latency and result size are sampled.
- Routing and cache policy belong to Dynamo and the engine. The ideal hit rate in `plan-summary.json` is an upper bound, not a prediction.

## License

Apache-2.0. Files derived from NVIDIA Dynamo retain their NVIDIA copyright headers. See `NOTICE`.
