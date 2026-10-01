// SPDX-License-Identifier: Apache-2.0

//! End-to-end checks against a live listener with fixture dashboard data and a
//! directory-backed shard store.

use std::fs;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use agent_loadgen_serve::hours::ShardHour;
use agent_loadgen_serve::shards::{BASE_PREFIX, LocalShardSource};
use agent_loadgen_serve::{AppState, ServeLimits, router};
use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use serde_json::{Value, json};

const MODEL: &str = "org.model-x";
const ROOT_SESSION: &str = "sess-root-0123456789abcdef";
const CHILD_SESSION: &str = "sess-child-0123456789abcdef";
/// 2026-01-02T03:20:00Z
const T0: u64 = 1_767_324_000_000;
/// Large enough for the unverified 1024-token alphabet to encode u64 hashes.
const BLOCK_SIZE: usize = 8;

struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    store: PathBuf,
}

fn request_end(
    request_id: &str,
    session_id: &str,
    parent: Option<&str>,
    received_ms: u64,
    output: u64,
    hashes: &[u64],
) -> Value {
    let mut context = json!({"session_id": session_id});
    if let Some(parent) = parent {
        context["parent_session_id"] = json!(parent);
    }
    json!({
        "schema": "dynamo.request.trace.v1",
        "event_type": "request_end",
        "event_time_unix_ms": received_ms + 250,
        "agent_context": context,
        "request": {
            "request_id": request_id,
            "model": "served/model-x",
            "input_tokens": hashes.len() * BLOCK_SIZE,
            "output_tokens": output,
            "request_received_ms": received_ms,
            "total_time_ms": 250.0,
            "replay": {"trace_block_size": BLOCK_SIZE, "input_length": hashes.len() * BLOCK_SIZE, "input_sequence_hashes": hashes}
        }
    })
}

fn write_gz(path: &Path, members: &[Vec<Value>]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut bytes = Vec::new();
    for member in members {
        let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::fast());
        for line in member {
            writeln!(encoder, "{line}").unwrap();
        }
        bytes.extend(encoder.finish().unwrap());
    }
    fs::write(path, bytes).unwrap();
}

fn shard_path(store: &Path, cluster: &str, hour: ShardHour, name: &str) -> PathBuf {
    store
        .join(BASE_PREFIX.trim_end_matches('/'))
        .join(format!("cluster={cluster}"))
        .join(format!("model={MODEL}"))
        .join(format!("date={}", hour.date()))
        .join(format!("hour={}", hour.hour()))
        .join(name)
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    let store = dir.path().join("store");
    let model_dir = root.join("web/data/m").join(MODEL);
    fs::create_dir_all(model_dir.join("session")).unwrap();
    fs::write(
        model_dir.join("global.json"),
        json!({"trees": [{"id": ROOT_SESSION, "kids": [{"id": CHILD_SESSION}]}]}).to_string(),
    )
    .unwrap();
    fs::write(
        model_dir.join("session").join(format!("{ROOT_SESSION}.json")),
        json!({"id": ROOT_SESSION, "turns": 2, "parent": null, "sub": 1, "harness": "codex",
               "rows": [{"t": 0, "ts": T0, "in": 4, "out": 2}, {"t": 1, "ts": T0 + 30_000, "in": 6, "out": 2}]})
        .to_string(),
    )
    .unwrap();
    fs::write(
        model_dir.join("session").join(format!("{CHILD_SESSION}.json")),
        json!({"id": CHILD_SESSION, "turns": 1, "parent": ROOT_SESSION, "sub": 0, "harness": "codex",
               "rows": [{"t": 0, "ts": T0 + 10_000, "in": 4, "out": 2}]})
        .to_string(),
    )
    .unwrap();

    // Shards land one hour after the request hour; the other hours stay empty.
    let lagged = ShardHour(ShardHour::from_epoch_ms(T0 as i64).0 + 1);
    let root_1 = request_end("req-1", ROOT_SESSION, None, T0, 2, &[11, 12]);
    let root_zero = request_end("req-zero", ROOT_SESSION, None, T0 + 5_000, 0, &[11]);
    let root_2 = request_end("req-2", ROOT_SESSION, None, T0 + 30_000, 3, &[11, 12, 13]);
    let child = json!({"timestamp": 1, "event": request_end("req-child", CHILD_SESSION, Some(ROOT_SESSION), T0 + 10_000, 2, &[11, 21])});
    let unrelated = request_end(
        &format!("{ROOT_SESSION}-decoy"),
        "sess-unrelated",
        None,
        T0,
        2,
        &[99],
    );
    write_gz(
        &shard_path(&store, "alpha", lagged, "trace-0.jsonl.gz"),
        &[vec![root_1.clone(), root_zero, unrelated], vec![child]],
    );
    write_gz(
        &shard_path(&store, "beta", lagged, "trace-1.jsonl.gz"),
        &[vec![root_1, root_2]],
    );
    fs::write(shard_path(&store, "alpha", lagged, "ignored.txt"), b"x").unwrap();
    Fixture {
        _dir: dir,
        root,
        store,
    }
}

async fn start(fixture: &Fixture) -> (String, Arc<AppState>) {
    let shards = Arc::new(LocalShardSource::new(&fixture.store));
    let limits = ServeLimits {
        bundle_concurrency: 2,
        keep_jobs: 20,
        ..ServeLimits::default()
    };
    let state = AppState::new(&fixture.root, shards, &limits, false).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(Arc::clone(&state));
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (format!("http://{addr}"), state)
}

async fn wait_for_job(client: &reqwest::Client, base: &str, id: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let job: Value = client
            .get(format!("{base}/jobs/{id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        match job["state"].as_str() {
            Some("done") | Some("error") => return job,
            _ if Instant::now() > deadline => panic!("job did not finish: {job}"),
            _ => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
}

fn gunzip(bytes: &[u8]) -> String {
    let mut text = String::new();
    MultiGzDecoder::new(bytes)
        .read_to_string(&mut text)
        .unwrap();
    text
}

#[tokio::test]
async fn health_plan_and_replay_bundle_flow() {
    let fixture = fixture();
    let (base, _state) = start(&fixture).await;
    let client = reqwest::Client::new();

    // /health keeps the legacy keys.
    let health: Value = client
        .get(format!("{base}/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["ok"], json!(true));
    assert_eq!(health["loadgen"], json!(true));
    assert_eq!(health["s5cmd"], json!(false));
    assert_eq!(health["creds"], json!(false));
    assert!(health["version"].is_string());

    // /plan with a small profile.
    let toml = "schema_version = 5\nseed = 42\n[load]\nnum_sessions = 4\nconcurrent_sessions = 2\n";
    let started = Instant::now();
    let response = client
        .post(format!("{base}/plan"))
        .json(&json!({"toml": toml}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let plan: Value = response.json().await.unwrap();
    eprintln!(
        "plan latency: {} ms (server elapsed_ms={})",
        started.elapsed().as_millis(),
        plan["elapsed_ms"]
    );
    assert_eq!(plan["summary"]["top_level_sessions"], json!(4));
    assert_eq!(plan["timeline"]["trees"], json!(4));
    assert!(plan["summary"]["per_request"]["ideal_prefix_hit_rate"].is_number());
    assert!(!plan["timeline"]["nodes"].as_array().unwrap().is_empty());
    assert_eq!(
        plan["command"],
        json!(
            "agent-loadgen generate --config scenario.toml \\\n  --headers claude-code --model <served-model-name> \\\n  --target http://<your-dynamo-frontend>:8000 \\\n  --tokenizer <hf-model-id-or-local-dir> \\\n  --output ./generate-run"
        )
    );
    let first_node = &plan["timeline"]["nodes"][0];
    for key in [
        "s",
        "o",
        "a",
        "in",
        "out",
        "tools",
        "wait",
        "spawn",
        "compaction",
    ] {
        assert!(!first_node[key].is_null(), "timeline node missing {key}");
    }

    // schema-4 profiles get the actionable migration message.
    let response = client
        .post(format!("{base}/plan"))
        .json(&json!({"toml": "schema_version = 4\nseed = 1\nagent = \"codex\"\n"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 422);
    let error: Value = response.json().await.unwrap();
    assert!(
        error["error"]
            .as_str()
            .unwrap()
            .contains("schema 4 is no longer supported"),
        "{error}"
    );
    let response = client
        .post(format!("{base}/plan"))
        .body("not json")
        .header("content-type", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    let response = client
        .post(format!("{base}/plan"))
        .json(&json!({"toml": "x".repeat(70_000)}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);

    // /replay-bundle validation.
    let response = client
        .post(format!("{base}/replay-bundle"))
        .json(&json!({"model": MODEL, "session_id": "nope"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
    let response = client
        .post(format!("{base}/replay-bundle"))
        .json(&json!({"model": "../x", "session_id": ROOT_SESSION}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    let response = client
        .get(format!("{base}/jobs/r-missing000000"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);

    // Full bundle job.
    let started: Value = client
        .post(format!("{base}/replay-bundle"))
        .json(&json!({"model": MODEL, "session_id": ROOT_SESSION, "include_tree": true}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let job_id = started["job_id"].as_str().unwrap().to_string();
    assert!(job_id.starts_with("r-") && job_id.len() == 14);
    let job = wait_for_job(&client, &base, &job_id).await;
    assert_eq!(job["state"], json!("done"), "{job}");
    assert_eq!(job["kind"], json!("replay"));
    assert_eq!(job["sessions"], json!([CHILD_SESSION, ROOT_SESSION]));
    assert_eq!(job["agent"], json!("codex"));
    assert_eq!(job["harness"], json!("codex"));
    assert_eq!(job["agent_inferred"], json!(false));
    assert_eq!(job["kept"], json!(3));
    assert_eq!(job["dropped_zero_output"], json!(1));
    assert_eq!(job["deduplicated"], json!(1));
    assert_eq!(job["scanned"], json!(6));
    assert_eq!(job["shards"], json!(2));
    assert_eq!(job["block_size"], json!(BLOCK_SIZE));
    assert_eq!(job["served_model"], json!("served/model-x"));
    assert_eq!(job["queue_position"], json!(0));
    assert_eq!(
        job["progress"],
        json!({"shards_done": 2, "shards_total": 2})
    );
    assert_eq!(
        job["filename"],
        json!(format!("session-{ROOT_SESSION}.jsonl.gz"))
    );
    assert_eq!(
        job["inspect"]["fidelity"],
        json!("agentic-causal"),
        "{}",
        job["inspect"]
    );
    assert_eq!(job["inspect"]["root_requests"], json!(1));
    assert!(job["inspect"]["dependency_edges"].as_u64().unwrap() >= 2);
    assert_eq!(
        job["command"],
        json!(format!(
            "agent-loadgen replay session-{ROOT_SESSION}.jsonl.gz \\\n  --headers codex --model served/model-x \\\n  --target http://<your-dynamo-frontend>:8000 \\\n  --tokenizer <hf-model-id-or-local-dir> \\\n  --output ./replay-sess-root-01"
        ))
    );
    assert!(job["log"].as_str().unwrap().contains("inspect ok"));
    assert!(job["created_at"].is_number() && job["updated_at"].is_number());

    let response = client
        .get(format!("{base}/jobs/{job_id}/bundle"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["content-type"], "application/gzip");
    assert_eq!(
        response.headers()["content-disposition"],
        format!("attachment; filename=\"session-{ROOT_SESSION}.jsonl.gz\"")
    );
    let bytes = response.bytes().await.unwrap();
    assert_eq!(bytes.len() as u64, job["bundle_bytes"].as_u64().unwrap());
    let text = gunzip(&bytes);
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 3);
    let ids: Vec<String> = lines
        .iter()
        .map(|line| {
            let value: Value = serde_json::from_str(line).unwrap();
            let event = if value["event"].is_object() {
                &value["event"]
            } else {
                &value
            };
            event["request"]["request_id"].as_str().unwrap().to_string()
        })
        .collect();
    assert_eq!(ids, vec!["req-child", "req-1", "req-2"]);

    // Same key with unchanged shards: cached job id.
    let cached: Value = client
        .post(format!("{base}/replay-bundle"))
        .json(&json!({"model": MODEL, "session_id": ROOT_SESSION}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cached["job_id"], json!(job_id));

    // Root only (no tree) is a different key and a different job.
    let solo: Value = client
        .post(format!("{base}/replay-bundle"))
        .json(&json!({"model": MODEL, "session_id": ROOT_SESSION, "include_tree": false}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_ne!(solo["job_id"], json!(job_id));
    let solo_job = wait_for_job(&client, &base, solo["job_id"].as_str().unwrap()).await;
    assert_eq!(solo_job["state"], json!("done"), "{solo_job}");
    assert_eq!(solo_job["kept"], json!(2));
    assert_eq!(solo_job["sessions"], json!([ROOT_SESSION]));

    // A new shard invalidates the cache and a fresh job is scheduled.
    let lagged = ShardHour(ShardHour::from_epoch_ms(T0 as i64).0 + 1);
    write_gz(
        &shard_path(&fixture.store, "alpha", lagged, "trace-9.jsonl.gz"),
        &[vec![request_end(
            "req-3",
            ROOT_SESSION,
            None,
            T0 + 40_000,
            1,
            &[11, 12, 13, 14],
        )]],
    );
    let fresh: Value = client
        .post(format!("{base}/replay-bundle"))
        .json(&json!({"model": MODEL, "session_id": ROOT_SESSION}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_ne!(fresh["job_id"], json!(job_id));
    let fresh_job = wait_for_job(&client, &base, fresh["job_id"].as_str().unwrap()).await;
    assert_eq!(fresh_job["kept"], json!(4), "{fresh_job}");
    assert_eq!(fresh_job["shards"], json!(3));

    // /metrics reflects the plan, cache hits, bundle jobs, and rejections above.
    let response = client.get(format!("{base}/metrics")).send().await.unwrap();
    assert_eq!(response.status(), 200);
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/plain")
    );
    let metrics = response.text().await.unwrap();
    let has_line = |line: &str| metrics.lines().any(|candidate| candidate == line);
    for line in [
        // One real plan and one 422 (schema 4).
        "loadgen_serve_jobs_total{kind=\"plan\",result=\"done\"} 1",
        "loadgen_serve_jobs_total{kind=\"plan\",result=\"error\"} 1",
        // Tree, solo, and post-invalidation bundle jobs; one unchanged-shard cache hit.
        "loadgen_serve_jobs_total{kind=\"replay\",result=\"done\"} 3",
        "loadgen_serve_jobs_total{kind=\"replay\",result=\"cached\"} 1",
        "loadgen_serve_jobs_total{kind=\"replay\",result=\"error\"} 0",
        "loadgen_serve_jobs_inflight{kind=\"plan\"} 0",
        "loadgen_serve_jobs_inflight{kind=\"replay\"} 0",
        "loadgen_serve_jobs_queued 0",
        "loadgen_serve_http_requests_total{method=\"POST\",route=\"/plan\",status=\"200\"} 1",
        "loadgen_serve_http_requests_total{method=\"POST\",route=\"/plan\",status=\"400\"} 2",
        "loadgen_serve_http_requests_total{method=\"POST\",route=\"/plan\",status=\"422\"} 1",
        "loadgen_serve_http_requests_total{method=\"POST\",route=\"/replay-bundle\",status=\"404\"} 1",
        "loadgen_serve_http_requests_total{method=\"GET\",route=\"/jobs/{id}/bundle\",status=\"200\"} 1",
        "loadgen_serve_rejected_total{reason=\"bad_request\"} 3",
        // Unknown session and unknown job.
        "loadgen_serve_rejected_total{reason=\"not_found\"} 2",
        "loadgen_serve_rejected_total{reason=\"per_ip_cap\"} 0",
        "loadgen_serve_rejected_total{reason=\"too_many_hours\"} 0",
        // 2 + 2 + 3 shards across the three completed jobs.
        "loadgen_serve_shards_scanned_total 7",
        // 6 + 6 + 7 lines; 3 + 2 + 4 kept records; one zero-output drop per job.
        "loadgen_serve_lines_scanned_total 19",
        "loadgen_serve_records_kept_total 9",
        "loadgen_serve_zero_output_dropped_total 3",
        "loadgen_serve_s3_errors_total 0",
        "loadgen_serve_active_clients{window=\"15m\"} 1",
        "loadgen_serve_active_clients{window=\"24h\"} 1",
        // The real plan and the 422; the 400s never reach acceptance.
        "loadgen_serve_client_jobs_total{client=\"127.0.0.1\",kind=\"plan\"} 2",
        "loadgen_serve_client_jobs_total{client=\"127.0.0.1\",kind=\"replay\"} 4",
        &format!(
            "loadgen_serve_build_info{{version=\"{}\"}} 1",
            env!("CARGO_PKG_VERSION")
        ),
    ] {
        assert!(
            has_line(line),
            "missing metric line {line:?} in:\n{metrics}"
        );
    }
    let shard_bytes: u64 = metrics
        .lines()
        .find_map(|line| line.strip_prefix("loadgen_serve_shard_bytes_total "))
        .and_then(|value| value.parse().ok())
        .unwrap();
    assert!(shard_bytes > 0, "{metrics}");
    for histogram in [
        "loadgen_serve_http_request_duration_seconds_count{route=\"/plan\"} 4",
        "loadgen_serve_plan_duration_seconds_count 2",
        "loadgen_serve_bundle_job_duration_seconds_count{result=\"done\"} 3",
    ] {
        assert!(has_line(histogram), "missing {histogram:?} in:\n{metrics}");
    }
}

#[tokio::test]
async fn bundle_job_reports_no_matching_records() {
    let fixture = fixture();
    // Point the store at an empty directory tree with a cluster but no shards.
    fs::remove_dir_all(&fixture.store).unwrap();
    fs::create_dir_all(fixture.store.join(BASE_PREFIX).join("cluster=alpha")).unwrap();
    let (base, _state) = start(&fixture).await;
    let client = reqwest::Client::new();
    let started: Value = client
        .post(format!("{base}/replay-bundle"))
        .json(&json!({"model": MODEL, "session_id": ROOT_SESSION}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let job = wait_for_job(&client, &base, started["job_id"].as_str().unwrap()).await;
    assert_eq!(job["state"], json!("error"));
    assert!(
        job["error"]
            .as_str()
            .unwrap()
            .contains("no S3 shards found"),
        "{job}"
    );
}
