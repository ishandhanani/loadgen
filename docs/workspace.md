<!-- SPDX-License-Identifier: Apache-2.0 -->

# Workspace architecture

The repository is one Cargo workspace with five libraries and one thin CLI. The split is by ownership, not by a second execution path: a captured trace and a generated scenario retain their existing contracts and both use the same replay runtime.

```text
                 agent-loadgen CLI
                    /         \
                   v           v
      agent-loadgen-serve -> agent-loadgen-replay
                    \        /        \
    agent-loadgen-trace   agent-loadgen-generate
                    \        /
                 agent-loadgen-core
```

## Crate ownership

| Crate | Owns | Does not own |
|---|---|---|
| `agent-loadgen-core` | Header dialect enum, public trace/request contracts, percentile summaries, stable ready-time scheduling. | HTTP, wire-header rendering, Tokio, tokenizers, trace file parsing, profiles, or cache policy. |
| `agent-loadgen-trace` | `dynamo.request.trace.v1` parsing and validation, Dynamo-style causal lowering, trace comparison. | Request transport or synthetic planning. |
| `agent-loadgen-generate` | Neutral defaults, strict TOML profile schema, deterministic distributions, generated scenarios, tools, compaction, and subagent graphs. | Trace parsing, request transport, engine policy, or header dialects. |
| `agent-loadgen-replay` | Synthetic token payloads, Chat Completions transport, header-dialect rendering, SSE parsing, captured and generated causal execution, artifacts, and telemetry joins. | Agent workload policy or Dynamo cache policy. |
| `agent-loadgen-serve` | HTTP endpoints for the traces dashboard: in-process plan previews, replay-bundle export from the trace object store, job directories, caches, and limits. | Planning, trace compilation, or replay semantics; it calls the owning crates. |
| `agent-loadgen` | CLI parsing, command composition, and process exit status. | Workload semantics or runtime implementation. |

## Dependency rules

`core` is deliberately small and dependency-light. It is the only shared dependency between trace ingestion and generated planning.

- Among workspace crates, `trace` and `generate` may depend on `core`, but not on each other or on `replay`.
- Among workspace crates, `replay` may depend on `core`, `trace`, and `generate`; it owns execution-specific dependencies such as Tokio, Reqwest, and tokenizers.
- `serve` may depend on `trace`, `generate`, and `replay` (for the token dictionary used by inspect-style validation); it owns HTTP-service dependencies such as Axum and the S3 SDK. No other crate depends on `serve`.
- The CLI may depend on every internal crate, but should only translate flags into library calls.
- No crate implements Dynamo routing, cache policy, prefetch, demotion, or eviction. Those remain engine and frontend concerns.

## Where a change belongs

| Change | Crate |
|---|---|
| New captured trace field, validation rule, or lowering edge | `trace` (move the shared serialized contract to `core` only when both trace and replay need it) |
| New profile knob, distribution, generated tool behavior, compaction rule, or subagent topology | `generate` |
| Token encoding, HTTP headers, transport behavior, scheduler execution, result fields, run gates, or telemetry joins | `replay` |
| Header dialect names, shared agent-context IDs, request/manifest serialization, generic ready-time scheduling | `core` |
| HTTP header rendering and transport-specific static-header validation | `replay` |
| Dashboard endpoint, bundle job state, shard listing or scan, plan preview shape | `serve` |
| New subcommand or flag-to-library wiring | CLI |

## Compatibility boundary

The `agent-loadgen` binary name, commands, profile schema, trace schema, `scenario.json`, `requests.jsonl`, and `run.json` remain the external interfaces. A crate split alone must not change plan fingerprints or replay semantics. Validate checked-in profile plans after any generator change.
