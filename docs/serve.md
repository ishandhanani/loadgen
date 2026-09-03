# HTTP service (`agent-loadgen serve`)

`agent-loadgen serve` is the backend for the traces dashboard's "replay bundle" and "plan preview" panels. It replaces the earlier Python sidecar (`loadgen-api`) and keeps its JSON contract. Everything runs in process: planning uses `agent-loadgen-generate`, bundle validation uses `agent-loadgen-trace`, and shard scanning talks to the S3-compatible trace store directly.

The service produces artifacts and copy-paste commands only. It never sends load anywhere.

```bash
agent-loadgen serve \
  --root ~/dynamo-traces \
  --listen 127.0.0.1:8096 \
  --creds ~/.config/dynamo-traces/env \
  --bundle-concurrency 3 \
  --keep-jobs 50
```

| Flag | Default | Meaning |
|---|---|---|
| `--root` | `~/dynamo-traces` | Dashboard data root. Reads `web/data/m/<model>/global.json` and `web/data/m/<model>/session/<id>.json`; writes `loadgen-jobs/<job_id>/`. |
| `--listen` | `127.0.0.1:8096` | Bind address. Put a reverse proxy in front; the service trusts `X-Forwarded-For`. |
| `--creds` | `~/.config/dynamo-traces/env` | `export NAME=value` file with `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_ENDPOINT_URL` (optional `AWS_SESSION_TOKEN`, `AWS_REGION`). Startup fails if the file or a required key is missing. |
| `--bundle-concurrency` | `3` | Bundle jobs scanning shards at once. Additional jobs queue. |
| `--keep-jobs` | `50` | Newest job directories retained; older ones are pruned after each job. |

Logs go to stderr through `tracing`; `RUST_LOG` overrides the default `info` level.

## Endpoints

All responses are JSON except the two file downloads. Errors are `{"error": "..."}` with 400 (bad input), 404 (unknown session, job, or artifact), 422 (invalid profile), 429 (per-client job cap), or 500.

| Method and path | Purpose |
|---|---|
| `GET /health` | `{"ok":true,"loadgen":true,"s5cmd":false,"creds":true,"version":"..."}`. `loadgen` and `s5cmd` are kept for the dashboard; there is no external binary any more. |
| `POST /replay-bundle` | Body `{"model","session_id","include_tree":true}`. Returns `{"job_id":"r-<12 hex>"}` immediately and runs the export asynchronously. |
| `GET /jobs/{id}` | `state.json` fields plus `log` (last 4000 characters of `log.txt`). |
| `GET /jobs/{id}/bundle` | `bundle.jsonl.gz` as `application/gzip` with `Content-Disposition: attachment; filename="session-<sid>.jsonl.gz"`. |
| `POST /plan` | Body `{"toml": "<profile>"}` (at most 64 000 bytes). Plans synchronously and returns the preview described below. |
| `GET /jobs/{id}/scenario.json` | The planned `scenario.json`. Served with `Content-Encoding: gzip` when stored compressed; browsers and `curl --compressed` decode it transparently. |
| `GET /jobs/{id}/plan.svg` | Graphviz rendering when `dot` was available and the scenario had at most 500 nodes; otherwise 404. |

## Replay bundle jobs

A bundle job turns one dashboard session (optionally with its subagent tree) into a `dynamo.request.trace.v1` file that `agent-loadgen replay` accepts.

1. Resolve sessions. `session_id` plus, when `include_tree` is true, every descendant found by walking `global.json` `trees[].kids`. IDs must match `^[A-Za-z0-9_.:-]{1,128}$`; a missing root session is a 404.
2. Pick shard hours. Every `rows[].ts` of every included session maps to its UTC hour, widened by one hour on each side because shard hour keys lag request arrival. More than 12 hours is rejected (400) as too large for an interactive export.
3. Map the harness. `claude-code`, `codex`, and `opencode` become the same `--headers` dialect; anything else becomes `opencode` with `agent_inferred: true`.
4. List shards. `request-trace/v1/env=prd/` is listed once for `cluster=` partitions, then every `cluster=<C>/model=<M>/date=<D>/hour=<H>/` prefix is listed (eight listings at a time). Only `.jsonl.gz` keys are kept.
5. Scan. Up to eight shards per job stream concurrently. Each object is decompressed as multi-member gzip, split into lines, prefiltered with an Aho-Corasick automaton over the session IDs, and only hits are JSON-parsed. A line is kept when `agent_context.session_id` (from the record or its `event` envelope) is one of the sessions. `request_end` records with zero or missing `output_tokens` are dropped (`dropped_zero_output`); duplicates by `request.request_id` across overlapping listings are removed (`deduplicated`); `replay.trace_block_size` and `request.model` values are collected.
6. Write `bundle.jsonl` in deterministic order (session, request time, request ID, bytes), fail if nothing matched or more than one block size was seen, then validate it in process exactly like `agent-loadgen inspect --token-start 1000 --allow-unverified-token-range`. The result is `inspect: {root_requests, dependency_edges, fidelity}` or `inspect: {error}`; a failed inspect still delivers the bundle.
7. Gzip to `bundle.jsonl.gz`, delete the plain file, and record the copy-paste command.

`state.json` fields: `kind:"replay"`, `state` (`queued|running|done|error`), `step`, `error`, `model`, `session_id`, `sessions`, `agent`, `harness`, `agent_inferred`, `kept`, `dropped_zero_output`, `deduplicated`, `malformed_lines`, `scanned`, `shards`, `block_size`, `served_model` (lexicographically smallest `request.model`, else the dashboard model), `inspect`, `bundle_bytes`, `filename`, `command`, `queue_position` (0 once running), `progress: {shards_done, shards_total}`, `shard_etags`, `elapsed_ms`, `created_at`, `updated_at` (epoch seconds).

### Limits, joining, and caching

- A global semaphore of `--bundle-concurrency` permits bounds shard scanning; queued jobs report their live `queue_position`.
- Each client address (first `X-Forwarded-For` hop, else the peer) may have two queued or running bundle jobs; a third request is a 429.
- A job that runs longer than 15 minutes, including queue time, ends in `state: "error"`.
- Join-in-flight: a request whose key `(model, sorted session IDs)` matches a queued or running job returns that job's ID.
- Bundle cache: if a completed job has the same key, the service lists the shards again and, when every key, ETag, and size matches what that job recorded in `shard_etags`, returns the cached job ID instead of rescanning. A changed listing schedules a fresh job that reuses the new listing.
- On restart, jobs left `queued` or `running` are marked `error`, and caches are rebuilt from completed jobs on disk.

## Plan previews

`POST /plan` parses the TOML with the same strict path as `agent-loadgen plan` (`GeneratorConfig::parse` -> `resolve` -> `GeneratedScenario::generate`). Invalid profiles return 422 `{"job_id","error"}` with the generator's message, including the schema 4 migration hint.

Response:

```json
{
  "job_id": "p-<12 hex>",
  "summary": {
    "sessions": 0, "nodes": 0, "top_level_sessions": 0, "compaction_operations": 0,
    "total_input_tokens": 0, "total_output_budget_tokens": 0,
    "tool_parallelism": {"tool_phases": 0, "...": 0}, "scenario_digest": "<sha256>"
  },
  "timeline": {
    "sessions": [{"id": "...", "parent": null, "depth": 0}],
    "nodes": [{"s": 0, "o": 0, "a": "tool", "in": 0, "out": 0, "tools": 0, "wait": 0, "spawn": [], "compaction": false}]
  },
  "has_svg": false,
  "scenario_bytes": 0,
  "command": "agent-loadgen generate --config scenario.toml \\\n  --headers dynamo ...",
  "elapsed_ms": 0
}
```

`timeline.nodes[].a` uses the action names from `scenario.json` (`text`, `tool`, `parallel_tools`, `subagent`, `swarm`, `complete`); `out` is the output budget when present, else the request's output tokens; `wait` is the summed tool latency; `spawn` lists spawned session indices.

Artifacts under the job directory: `config.toml`, `plan.dot` (always), `plan.svg` (Graphviz on `PATH` and at most 500 nodes), `scenario.json.gz` (when the pretty-printed scenario is at most 64 MiB; larger scenarios are regenerated from `config.toml` on the first `GET`), and `plan.json` (the response, used by the cache).

Plan cache: identical TOML text (by SHA-256) returns the stored response with the original `job_id`.

## Deployment

systemd user unit (`~/.config/systemd/user/agent-loadgen-serve.service`):

```ini
[Unit]
Description=agent-loadgen serve (plan previews and replay bundles)
After=network-online.target

[Service]
ExecStart=%h/agent-loadgen/target/release/agent-loadgen serve --root %h/dynamo-traces --listen 127.0.0.1:8096
Restart=on-failure
Environment=RUST_LOG=info

[Install]
WantedBy=default.target
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now agent-loadgen-serve
curl -s 127.0.0.1:8096/health
```

Caddy, in the dashboard site block (the prefix is stripped before proxying):

```caddyfile
handle_path /api/loadgen/* {
    reverse_proxy localhost:8096
}
```

## Module map

| File | Owns |
|---|---|
| `crates/serve/src/api.rs` | Routes, request validation, JSON errors, file streaming. |
| `crates/serve/src/bundle.rs` | Session resolution, shard scan, merge and dedupe, inspect, gzip, job runner. |
| `crates/serve/src/plan.rs` | In-process planning, summary and timeline, artifact storage. |
| `crates/serve/src/shards.rs` | `ShardSource` trait with the S3 and local-directory implementations, shard listing. |
| `crates/serve/src/jobs.rs` | `state.json` schema, atomic writes, job index (in-flight, caches, queue, per-client counts), pruning. |
| `crates/serve/src/hours.rs` | Shard-hour expansion and the 12-hour limit. |
| `crates/serve/src/creds.rs` | `export`-file credential parsing. |
