<!-- SPDX-License-Identifier: Apache-2.0 -->

# How agent-loadgen works

[Watch a 78-second visual overview](assets/agent-loadgen-concepts.mp4) of the ideas on this page: growing agent contexts, the KV-prefix trie, causal timing with subagents, arrival schedules, and harness headers.

`agent-loadgen` has two graph producers and one causal timing contract:

```text
captured trace                         generated profile
      |                                      |
      v                                      v
Dynamo-style DAG lowering              seeded dependency graph
      |                                      |
      +------------------+-------------------+
                         |
                         v
               synthetic token request
                         |
                         v
                Dynamo Chat Completions
```

- Captured roots use recorded offsets. Captured dependent turns use actual replay completions plus recorded residual delays.
- Generated roots use configured offsets. Generated dependent turns use actual completions plus sampled delays.

## Shared request path

Both modes use the same request and result path:

1. Convert trace hashes or generated block labels into deterministic token IDs.
2. Send the IDs through `nvext.token_data` with valid dummy Chat Completions messages.
3. Add the harness session, subagent, and compaction headers of the `--headers` dialect, plus `x-dynamo-session-final` on a session's last request when `--dynamo-session-final` is set.
4. Stream the response and record TTFT, completion usage, status, and selected Dynamo headers.
5. Hand the result to a dedicated writer thread, which appends it to `requests.jsonl` and updates the run statistics, so a slow disk never delays scheduling.

Equal source block hashes always produce equal synthetic token blocks. Different hashes receive different codewords within the validated dictionary capacity. This preserves KV-prefix equality without preserving prompt text or production token IDs.

## Captured replay

```text
JSONL or gzip shards
        |
        v
request ends + terminal tool events
        |
        v
session and parent-session DAG lowering
        |
        v
roots: recorded offsets
dependents: completion + recorded gap
```

The trace loader validates all selected records before traffic starts. It rejects missing agent context, missing replay hashes, mixed block sizes, duplicate request IDs, zero-token input/output records, and cyclic dependencies.

Each session is sequential. Parent/child context adds subagent launch and join edges. Exact Claude tool metadata wins when present; otherwise Dynamo's timestamp association rules infer the parent request around the child lifetime. Ready requests are never dropped; if the client runs short of file descriptors, they wait for a free slot.

See [Trace replay](replay.md) for the exact trace and timing contract.

## Generated traffic

Generation has a planning phase and an execution phase.

### Planning

`plan` resolves a strict TOML profile over the neutral defaults. Each top-level session tree has its own seeded random stream, so trees are planned independently; arrival times come from another. The header dialect is not an input to planning. Each node records:

- Session and parent-session identity.
- KV block sequence and output budget.
- Dependencies and delay after dependency completion.
- Tool, subagent, swarm, or compaction metadata.

`plan-summary.json` summarizes a sample of trees for plans of any size. Runtime model text never changes the plan.

### Execution

```text
dependencies complete
        |
        +----> ready time = completion + sampled delay
        |
        +----> prepare request while the delay counts down
                         |
                         v
                  dispatch at ready time
                         |
                         v
                 response completes
                         |
                         v
                 release successors
```

The executor admits work one chunk at a time. A generated chunk is one top-level session tree; a captured trace is a single chunk. A chunk starts at a scheduled offset, such as a tree's arrival time, or a delay after its predecessor's anchor request completes, such as a stream's restart after the previous tree's root session ends. A chunk is planned about two seconds before it starts and dropped when its last request completes.

With streams, `load.concurrent_sessions` creates fixed top-level session streams. The first session tree in each stream follows the configured startup spacing. After its root session's last response completes, the stream starts its next tree after `restart_delay_ms`. With an arrival schedule, trees start at their sampled arrival times regardless of how fast earlier trees finish.

A slower target therefore lowers the request rate of streams, and raises the number of live sessions under an arrival schedule. Independent trees and child sessions can still overlap.

A chunk's root request bodies are prepared when the chunk is admitted. Successor bodies are prepared when their final dependency completes. Token-payload and plan memory therefore follow live session trees rather than the total planned load. `--stop-after-minutes` drops everything not yet dispatched at the deadline and ends the run once in-flight requests finish.

## Subagents and swarms

A subagent is a separate generated session, not another request inside the parent session.

```text
parent model response
        |
        +---- spawn delay ----> child session
        |                         parent_session_id = parent
        |                         independent turns and tools
        |
        +---- blocking ----------> parent waits for child's last response
        |
        +---- non-blocking ------> parent continues; child runs independently
```

Children share global and top-level-tree-scoped KV labels. Each child receives a unique session prefix and explicit parent lineage. Blocking children delay the parent. Non-blocking children continue independently.

See [Subagents and swarms](subagents.md) for the dependency rules, supported patterns, and current limits.

## Compaction

Compaction is one logical operation with one or more physical attempts:

```text
stable operation_id
  attempt 1  no_mutation_aborted   optional
  attempt 2  apply_once            exactly one
  attempt 3  duplicate_noop        optional
```

The successful attempt changes later KV shape once. All attempts keep the same operation ID. Cache-mutation claims require engine telemetry.

See [Compaction](compaction.md) for trigger logic, attempt semantics, retained context, and proof boundaries.

## Timing fields

`requests.jsonl` separates local timing stages:

- `scheduled_offset_ms`: intended causal release time.
- `scheduler_wake_lag_ms`: timer wakeup error.
- `local_admission_lag_ms`: time waiting for a free slot under the client's open-file guard; zero unless `peak_in_flight` reached `in_flight_limit`.
- `dispatch_lag_ms`: time from intended release until the HTTP client receives the request.
- `ttft_ms`: time from dispatch to first output.
- `total_time_ms`: time from dispatch to stream completion.

Frontend-observed arrival is different from client dispatch. A Dynamo request trace captured during the run records it; join the two by `x-request-id`.

## Artifacts and claims

`run.json` labels the protocol surface, traffic kind, and HTTP version, and reports achieved throughput with TTFT, request latency, and time-per-output-token percentiles. `timeline.csv` breaks the same measurements into one-minute buckets. A run passes when every sent request succeeded with its planned output length; client dispatch lag is reported, not gated.

Cache policy remains outside this project.

## Crate map

```text
                 agent-loadgen CLI
                        |
                        v
                      replay
                    /        \
                trace       generate
                    \        /
                      core
```

`core` has no I/O or workload policy. Trace ingestion and synthetic planning stay independent; replay is the runtime that consumes either planned workload.

| Crate | Responsibility |
|---|---|
| `agent-loadgen-core` | Public request and agent-context contracts, percentile summaries, and the stable ready-time queue. |
| `agent-loadgen-trace` | Trace v1 parsing and agentic causal lowering. |
| `agent-loadgen-generate` | Neutral defaults, strict TOML profile validation, seeded distributions, and generated scenario planning. |
| `agent-loadgen-replay` | Token shaping, request transport, harness headers, captured and generated causal schedulers, and run artifacts. |
| `agent-loadgen` | CLI flags and command wiring only. |

`AGENTS.md` records the dependency rules between the crates.
