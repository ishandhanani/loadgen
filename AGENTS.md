# Agent-loadgen guide

## Purpose

Agent-loadgen measures request shape and timing for coding-agent workloads. Request text is synthetic. Token identity, prefix reuse, request order, and timing are the important contracts.

Keep agent behavior separate from serving policy. Dynamo and the inference engine own routing, cache policy, prefetch, demotion, and eviction.

## Traffic models

All traffic is generated, agentic, and causal. A model response or sampled agent delay releases the next request. Target latency therefore changes the achieved request rate. Top-level session trees start either in closed-loop streams or at arrival times sampled from a rate schedule. Each tree has its own seeded random stream, so `generate` plans trees lazily and a run's memory follows live trees.

Ordinary generated turns have positive input and output token counts. Compaction attempts carry the planned summary length as their output budget. A session completes with its last model response. With `--dynamo-session-final`, that response carries `x-dynamo-session-final: true`; the header marks an existing request and never adds one. Do not add lifecycle-control requests or zero-output close nodes without a new design decision.

Traffic shape and wire protocol are separate. The profile defines shape; there is no per-agent preset. `--headers {claude-code|codex}` selects the harness header dialect at run time and must never influence planning or digests. A dialect sends only its harness's own headers; `x-dynamo-session-final` is opt-in through `--dynamo-session-final`.

## Subagents

Each child is a separate session with a parent session ID. A child inherits the global, tool-catalog, and root-repository prefixes. It also receives a unique session prefix.

The generator supports these patterns:

- One child spawned from a parent turn.
- A swarm that spawns multiple sibling children.
- Recursive delegation up to `subagents.max_depth`.
- A blocking join that waits for all direct children.
- Non-blocking delegation that lets the parent and children continue independently.

Child trajectories can include tools, compaction, and more delegation. `load.concurrent_sessions` limits top-level session streams only. Child sessions are generated descendants and can increase live concurrency.

The generator does not support child roles, per-child profiles, child-to-child messages, voting, work stealing, persistent pools, or runtime model decisions. Planning samples the full graph before execution.

## Repository map

| Path | Purpose |
|---|---|
| `crates/core/` | Header dialect enum, agent context, request contract, percentiles, and stable ready-time queue. No I/O or workload policy. |
| `crates/generate/` | Strict profile schema, deterministic sampling, serialized scenarios, and synthetic graph planning. |
| `crates/replay/` | Token synthesis, harness headers, HTTP/SSE transport, the causal scheduler, and run artifacts. |
| `src/` | Thin `agent-loadgen` CLI: flags, command composition, and exit behavior. |
| `profiles/` | Example profiles: harness- and workload-calibrated shapes and an arrival schedule; shape only, no header selection. |
| `docs/` | Architecture and detailed usage guides. |
| `.agents/skills/agent-loadgen/` | Skill that teaches an agent to use the tool. Keep it in step with CLI flags, profile fields, and output files. |

## Change rules

- Make the smallest change that satisfies the request.
- Preserve deterministic planning for a fixed profile and seed.
- Keep the profile schema strict. Reject unknown fields.
- Keep prompt semantics and tool execution out of scope.
- Keep cache-policy decisions out of this repository.
- Keep `core` free of HTTP, Tokio, tokenizers, and generator policy. Among this workspace's crates, `generate` may depend only on `core`; `replay` may consume both, and `generate` may not depend on `replay`.
- Add behavior details to `docs/`. Keep `README.md` as a short entry point.
- Keep compaction and subagent behavior in their dedicated guides.
- Update tests and docs when a timing, token-shape, or dependency contract changes.

## Validation

Run these commands before a commit:

```bash
cargo fmt --all
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo package --workspace --list --allow-dirty
```

The workspace crates are private and resolve each other by local path, so Cargo cannot prepare them for a crates.io upload. `cargo package --list` validates the files each package would include; the workspace test command is the compilation check.

Plan each checked-in profile after generator changes:

```bash
plan_root="$(mktemp -d)"
for profile in profiles/*.toml; do
  profile_name="${profile##*/}"
  cargo run --quiet -- plan --config "$profile" --output "$plan_root/${profile_name%.toml}"
done
```

## Git

Use `origin` for NVIDIA-dev/agent-loadgen. Do not push this repository to the old `cas` remote. Push only when the user asks. Sign commits with DCO and keep one logical change in each commit. Do not add generated attribution or coauthor lines.

Releases are automatic: every commit on `main` that passes CI is published by `.github/workflows/release.yml` as `v<major>.<minor>.<commits on main>` with x86_64 and aarch64 Linux binaries. Major and minor come from `[workspace.package]` in `Cargo.toml`; change them only when the user asks.
