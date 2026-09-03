# agent-loadgen

`agent-loadgen` is a Rust load generator for coding-agent traffic through a Dynamo OpenAI-compatible frontend.

It supports two workload modes:

- `replay`: reproduce token counts, KV-prefix topology, agent headers, output lengths, root arrivals, turn delays, and subagent dependencies from `dynamo.request.trace.v1` traces.
- `generate`: create deterministic coding-agent traffic with tools, compaction, subagents, and swarms from a strict TOML profile.

Prompt text is synthetic. The fidelity contract is request shape, KV reuse, agent lineage, and timing.

Traffic shape and wire protocol are separate. The trace or profile defines shape. `--headers {dynamo|claude-code|codex|opencode}` on `replay` and `generate` selects the request header dialect: `dynamo` (the default) sends only the generic Dynamo session headers, and a harness dialect adds that harness's session, child, and compaction headers on top. See [Generator defaults](docs/presets.md#header-dialects).

## Build

```bash
cargo build --release
```

## Replay a trace

Inspect the trace and token dictionary first:

```bash
target/release/agent-loadgen inspect trace.jsonl.gz \
  --tokenizer /models/GLM-4.7-Flash
```

Replay it against Dynamo:

```bash
target/release/agent-loadgen replay trace.jsonl.gz \
  --headers codex \
  --model zai-org/GLM-4.7-Flash \
  --target http://127.0.0.1:8000 \
  --output artifacts/replay \
  --tokenizer /models/GLM-4.7-Flash \
  --token-path-verified \
  --engine-cache-mode ownership=session
```

Captured replay follows Dynamo's agentic policy. Independent roots keep their recorded arrival offsets. Dependent turns wait for actual replay completions plus the delay observed in the source trace.

## Generate agent traffic

Create a deterministic plan without sending traffic:

```bash
target/release/agent-loadgen plan \
  --config profiles/tools-heavy.toml \
  --output artifacts/tools-heavy-plan
```

Run the plan, here with Codex-style headers:

```bash
target/release/agent-loadgen generate \
  --config profiles/tools-heavy.toml \
  --headers codex \
  --model zai-org/GLM-4.7-Flash \
  --target http://127.0.0.1:8000 \
  --output artifacts/tools-heavy-run \
  --tokenizer /models/GLM-4.7-Flash
```

Generated traffic is closed-loop: model latency, tool time, compaction, and blocking subagent joins determine when the next request becomes ready. Every subagent is a separate child session with explicit parent lineage. Non-blocking children may continue after their parent advances.

## Serve the dashboard API

Run the HTTP service that turns dashboard sessions into replay bundles and TOML profiles into plan previews, all in process:

```bash
target/release/agent-loadgen serve \
  --root ~/dynamo-traces \
  --listen 127.0.0.1:8096
```

It reads dashboard data under `--root`, scans the request-trace object store with credentials from `~/.config/dynamo-traces/env`, and writes job artifacts to `<root>/loadgen-jobs/`. It never sends load itself. See [HTTP service](docs/serve.md).

## Outputs

- `scenario.json`: resolved generated profile and causal graph.
- `plan.dot`: Graphviz causal DAG grouped by top-level and child sessions, with spawn, join, and delay labels.
- `plan.svg`: Graphviz rendering of `plan.dot` when the `dot` executable is available.
- `requests.jsonl`: one result per completed request, including timing and output fidelity.
- `run.json`: run configuration including the header dialect, bounded timing summaries, fidelity gates, and pass/fail status.

## Documentation

- [How it works](docs/how-it-works.md): timing models, dependency scheduling, subagents, request construction, and module boundaries.
- [Workspace architecture](docs/workspace.md): crate ownership, dependency direction, and where to make a change.
- [Trace replay](docs/replay.md): trace contract, token synthesis, timing gates, comparison, and telemetry joins.
- [Generator configuration](docs/generator.md): profiles, distributions, tools, and safety limits.
- [Generator defaults](docs/presets.md): every resolved default, the header dialects, checked-in profile overrides, and why each baseline exists.
- [Codex behavior and simulation controls](docs/codex-behavior.md): source-derived agent-loop behavior, interpretation of generator knobs, and known fidelity gaps.
- [Subagents and swarms](docs/subagents.md): delegation graphs, joins, lineage, KV sharing, and current limits.
- [Compaction](docs/compaction.md): trigger logic, physical attempts, KV-window changes, and telemetry boundaries.
- [HTTP service](docs/serve.md): `agent-loadgen serve` endpoints, replay-bundle job model, caches and limits, and deployment behind Caddy.
- [Complex concurrent plan example](examples/complex-concurrency/README.md): a topology-rich TOML profile with its rendered causal graph.

## Current boundaries

- Chat Completions is the only supported protocol surface.
- Every captured request must carry `agent_context`; context-free and mixed traces are rejected.
- Captured requests must have positive input and output token counts.
- The tool does not generate meaningful prompt text or execute tools.
- Cache policy and routing behavior remain owned by Dynamo and the inference engine.
- Capacity or performance conclusions require a verified token path and declared engine cache mode.

## License

Apache-2.0. Files derived from NVIDIA Dynamo retain their NVIDIA copyright headers. See `NOTICE`.
