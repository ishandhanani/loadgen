# HTTP service (`agent-loadgen serve`)

`agent-loadgen serve` is the backend for the traces dashboard's "Generate Load" plan preview. It plans in process with `agent-loadgen-generate` and returns JSON. It never sends load anywhere.

```bash
agent-loadgen serve --listen 127.0.0.1:8096
```

`--listen` defaults to `127.0.0.1:8096`. Put a reverse proxy in front; the service trusts `X-Forwarded-For` for its per-client usage metrics. Logs go to stderr through `tracing`; `RUST_LOG` overrides the default `info` level.

## Endpoints

All responses are JSON except `/metrics`. Errors are `{"error": "..."}` with 400 (bad input), 422 (invalid profile), or 500.

| Method and path | Purpose |
|---|---|
| `GET /health` | `{"ok":true,"version":"..."}`. |
| `POST /plan` | Body `{"toml": "<profile>"}` (at most 64 000 bytes). Plans synchronously and returns the preview described below. |
| `GET /metrics` | Prometheus text exposition (`text/plain; version=0.0.4`). See [Metrics](#metrics). |

## Plan previews

`POST /plan` parses the TOML with the same strict path as `agent-loadgen plan` and returns its `plan-summary.json` with swimlane data for the first top-level trees. Invalid profiles return 422 `{"error"}` with the generator's message, including the schema 4 migration hint.

Response:

```json
{
  "summary": {"top_level_sessions": 4, "per_tree": {"...": {}}, "per_request": {"...": {}}, "...": null},
  "timeline": {
    "trees": 4,
    "sessions": [{"id": "...", "parent": null, "depth": 0}],
    "nodes": [{"s": 0, "o": 0, "a": "tool", "in": 0, "out": 0, "tools": 0, "wait": 0, "spawn": [], "compaction": false}]
  },
  "command": "agent-loadgen generate --config scenario.toml \\\n  --headers claude-code ...",
  "elapsed_ms": 0
}
```

`summary` is exactly the `plan-summary.json` that `agent-loadgen plan` writes; see [Describe a workload](workloads.md). `timeline` draws whole trees, from the first, until it holds at least 2,000 requests. `timeline.nodes[].a` is the planned action (`text`, `tool`, `parallel_tools`, `subagent`, `swarm`, `compaction_attempt`, `complete`); `out` is the output budget; `wait` is the summed tool latency; `spawn` lists spawned session indices.

## Metrics

`GET /metrics` serves a process-local Prometheus registry. Names and labels are a contract with the Grafana dashboard. Durations are seconds.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `loadgen_serve_http_requests_total` | counter | `route`, `method`, `status` | Every request. `route` is the matched axum pattern, or `unmatched` for 404 fallbacks. |
| `loadgen_serve_jobs_total` | counter | `kind` (`plan`), `result` (`done`, `error`) | Plan outcomes. |
| `loadgen_serve_client_jobs_total` | counter | `client`, `kind` | Accepted `POST /plan` requests per client. `client` is the first `X-Forwarded-For` hop, else the peer IP; at most 500 distinct values are tracked, then `other`. |
| `loadgen_serve_rejected_total` | counter | `reason` (`bad_request`) | Refused requests. |
| `loadgen_serve_jobs_inflight` | gauge | `kind` | Plans running now. |
| `loadgen_serve_active_clients` | gauge | `window` (`15m`, `24h`) | Distinct clients that requested a plan within the window, computed at scrape time. |
| `loadgen_serve_build_info` | gauge | `version` | Always 1. |
| `loadgen_serve_http_request_duration_seconds` | histogram | `route` | 1 ms to 10 s. |
| `loadgen_serve_plan_duration_seconds` | histogram | | 10 ms to 30 s. |

Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: loadgen-serve
    static_configs:
      - targets: ["localhost:8096"]
```

## Deployment

systemd user unit (`~/.config/systemd/user/agent-loadgen-serve.service`):

```ini
[Unit]
Description=agent-loadgen serve (plan previews)
After=network-online.target

[Service]
ExecStart=%h/agent-loadgen/target/release/agent-loadgen serve --listen 127.0.0.1:8096
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
| `crates/serve/src/api.rs` | Routes, request validation, JSON errors. |
| `crates/serve/src/plan.rs` | In-process planning, plan summary, and timeline. |
| `crates/serve/src/metrics.rs` | Prometheus registry, HTTP middleware, client activity tracking, `/metrics` handler. |
