// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Response, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::json;
use tempfile::tempdir;

use super::*;
use crate::token_shape::{SafeTokenAlphabet, TokenDictionary};
use agent_loadgen_core::percentiles;
use agent_loadgen_generate::scenario::{GeneratorConfig, TreePlanner};

#[test]
fn target_normalization_accepts_base_or_endpoint() {
    assert_eq!(
        normalize_target("http://localhost:8000"),
        "http://localhost:8000/v1/chat/completions"
    );
    assert_eq!(
        normalize_target("http://localhost:8000/v1/chat/completions"),
        "http://localhost:8000/v1/chat/completions"
    );
}

#[test]
fn calculates_nearest_rank_percentiles() {
    let values = (1..=100).map(|value| value as f64).collect();
    let result = percentiles(values);
    assert_eq!(result.p50, 50.0);
    assert_eq!(result.p95, 95.0);
    assert_eq!(result.p99, 99.0);
}

#[test]
fn detects_output_chunks() {
    assert!(!chunk_contains_output(
        &json!({"choices":[{"delta":{"role":"assistant"}}]})
    ));
    assert!(chunk_contains_output(
        &json!({"choices":[{"delta":{"content":"x"}}]})
    ));
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Option<(HeaderMap, serde_json::Value)>>>);

async fn shape_endpoint(
    State(capture): State<Capture>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response<Body> {
    *capture.0.lock().unwrap() = Some((headers, body));
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from(concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n",
            "data: {\"choices\":[],\"usage\":{\"completion_tokens\":2}}\n\n",
            "data: [DONE]\n\n"
        )))
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_graph_releases_tool_successor_after_completion() {
    let capture = Capture::default();
    let app = Router::new()
        .route("/v1/chat/completions", post(shape_endpoint))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config = GeneratorConfig::parse(
        r#"
                schema_version = 5
                seed = 9

                [load]
                num_sessions = 1
                concurrent_sessions = 1

                [trajectory]
                turns = { kind = "fixed", value = 2 }
                output_tokens = { kind = "fixed", value = 2 }

                [tokens]
                system_prefix_tokens = { kind = "fixed", value = 16 }
                tool_catalog_tokens = { kind = "fixed", value = 16 }
                repository_tokens = { kind = "fixed", value = 16 }
                session_tokens = { kind = "fixed", value = 16 }
                user_tokens = { kind = "fixed", value = 16 }

                [behavior]
                tool_probability = 1.0
                parallel_tool_probability = 0.0
                subagent_probability = 0.0
                swarm_probability = 0.0
                completion_probability = 0.0

                [compaction]
                enabled = false

                [subagents]
                max_depth = 0

                [tools]
                classes = [{ name = "shell", weight = 1.0, latency_ms = { kind = "fixed", value = 5 }, result_tokens = { kind = "fixed", value = 16 } }]
            "#,
    )
    .unwrap();
    let planner = TreePlanner::new(config.resolve().unwrap()).unwrap();
    let tree = planner.plan_tree(0).unwrap();
    assert_eq!(tree.nodes.len(), 2);
    assert_eq!(tree.nodes[1].dependencies, vec![0]);
    assert!(!tree.nodes[0].session_final);
    assert!(tree.nodes[1].session_final);
    let dictionary = TokenDictionary::new(
        planner.config().block_size,
        SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
    )
    .unwrap();
    let output = tempdir().unwrap();
    let summary = run_generated(
        &planner,
        dictionary,
        ReplayOptions {
            header_dialect: HeaderDialect::Codex,
            dynamo_session_final: true,
            model: "test-model".to_string(),
            target: format!("http://{address}"),
            output_dir: output.path().to_path_buf(),
            timeout: Duration::from_secs(5),
            static_headers: Vec::new(),
            stop_after: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(summary.succeeded, 2);
    assert_eq!(summary.budgeted_requests, 2);
    assert_eq!(summary.output_length_matches, 2);
    assert_eq!(summary.plan.top_level_sessions, 1);
    assert_eq!(summary.plan.subagent_sessions, 0);
    assert_eq!(summary.plan.requests, 2);
    assert!(summary.passed);
    assert!(output.path().join("run.json").is_file());
    assert!(output.path().join("requests.jsonl").is_file());
    assert!(output.path().join("timeline.csv").is_file());
    // The capture holds the last request sent: the session's final turn,
    // which carries the tool result.
    let (headers, body) = capture.0.lock().unwrap().clone().unwrap();
    assert_eq!(headers.get("x-dynamo-session-final").unwrap(), "true");
    assert!(headers.get("x-dynamo-session-id").is_none());
    assert_eq!(body["max_tokens"], 2);
    assert!(body.get("min_tokens").is_none());
    assert_eq!(body["ignore_eos"], true);
    assert_eq!(body["messages"][0]["role"], "assistant");
    assert_eq!(body["messages"][1]["role"], "tool");
    assert_eq!(
        body["messages"][1]["tool_call_id"],
        "agent-loadgen-shape-tool"
    );
    assert_eq!(
        body["nvext"]["token_data"].as_array().unwrap().len(),
        tree.nodes[1].request.input_tokens
    );
    // Wire session IDs are run-unique UUIDs, not planned session IDs.
    let thread = headers.get("thread-id").unwrap().to_str().unwrap();
    assert!(uuid::Uuid::parse_str(thread).is_ok());
    assert_ne!(thread, tree.sessions[0].session_id);
    assert_eq!(headers.get("session-id").unwrap(), thread);
    server.abort();
}

#[derive(Clone, Default)]
struct HeaderCapture(Arc<Mutex<Vec<HeaderMap>>>);

async fn header_endpoint(
    State(capture): State<HeaderCapture>,
    headers: HeaderMap,
) -> Response<Body> {
    capture.0.lock().unwrap().push(headers);
    Response::builder()
        .header("content-type", "text/event-stream")
        .body(Body::from(
            "data: {\"choices\":[],\"usage\":{\"completion_tokens\":2}}\n\ndata: [DONE]\n\n",
        ))
        .unwrap()
}

#[tokio::test]
async fn generated_claude_headers_reach_the_wire() {
    let capture = HeaderCapture::default();
    let app = Router::new()
        .route("/v1/chat/completions", post(header_endpoint))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config = GeneratorConfig::parse(
        r#"
            schema_version = 5
            seed = 9
            [load]
            num_sessions = 1
            concurrent_sessions = 1
            [trajectory]
            turns = { kind = "fixed", value = 3 }
            output_tokens = { kind = "fixed", value = 2 }
            [behavior]
            tool_probability = 0.0
            parallel_tool_probability = 1.0
            subagent_probability = 0.0
            swarm_probability = 0.0
            completion_probability = 0.0
            [tools]
            parallel_count = { kind = "fixed", value = 2 }
            classes = [{ name = "shell", weight = 1.0, latency_ms = { kind = "fixed", value = 17 }, result_tokens = { kind = "fixed", value = 16 } }]
            [compaction]
            enabled = false
        "#,
    )
    .unwrap();
    let planner = TreePlanner::new(config.resolve().unwrap()).unwrap();
    let dictionary = TokenDictionary::new(
        planner.config().block_size,
        SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
    )
    .unwrap();
    let output = tempdir().unwrap();
    let summary = run_generated(
        &planner,
        dictionary,
        ReplayOptions {
            header_dialect: HeaderDialect::ClaudeCode,
            dynamo_session_final: false,
            model: "test-model".to_string(),
            target,
            output_dir: output.path().to_path_buf(),
            timeout: Duration::from_secs(5),
            static_headers: Vec::new(),
            stop_after: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(summary.succeeded, 3);
    let headers = capture.0.lock().unwrap();
    let session = headers[0]["x-claude-code-session-id"].clone();
    let prompt = headers[0]["x-claude-code-prompt-id"].clone();
    for (index, headers) in headers.iter().enumerate() {
        assert_eq!(headers["x-claude-code-session-id"], session);
        assert_eq!(headers["x-claude-code-request-class"], "main");
        assert_eq!(headers["x-claude-code-prompt-id"], prompt);
        assert!(!headers.contains_key("x-claude-code-agent-id"));
        assert!(
            headers
                .keys()
                .all(|name| !name.as_str().starts_with("x-dynamo-"))
        );
        assert_eq!(
            headers
                .get("x-claude-code-prev-tool-durations")
                .map(|value| value.to_str().unwrap()),
            (index > 0).then_some("Bash=17;Bash=17")
        );
    }
    server.abort();
}

#[test]
fn harness_headers_cannot_be_overridden_with_static_headers() {
    let mut options = ReplayOptions {
        header_dialect: HeaderDialect::ClaudeCode,
        dynamo_session_final: false,
        model: "test-model".to_string(),
        target: "http://localhost:8000".to_string(),
        output_dir: std::path::PathBuf::from("unused"),
        timeout: Duration::from_secs(1),
        static_headers: Vec::new(),
        stop_after: None,
    };
    for name in [
        "X-Claude-Code-Request-Class",
        "Session-Id",
        "x-codex-window-id",
    ] {
        options.static_headers = vec![(name.to_string(), "override".to_string())];
        assert!(
            super::request::validate_options(&options)
                .unwrap_err()
                .to_string()
                .contains("owned by agent-loadgen")
        );
    }
    options.static_headers = vec![("anthropic-beta".to_string(), "future".to_string())];
    super::request::validate_options(&options).unwrap();
}

fn quick_options(target: String, output: &std::path::Path) -> ReplayOptions {
    ReplayOptions {
        header_dialect: HeaderDialect::Codex,
        dynamo_session_final: false,
        model: "test-model".to_string(),
        target,
        output_dir: output.to_path_buf(),
        timeout: Duration::from_secs(5),
        static_headers: Vec::new(),
        stop_after: None,
    }
}

const ONE_TURN: &str = r#"
    [trajectory]
    turns = { kind = "fixed", value = 1 }
    output_tokens = { kind = "fixed", value = 2 }
    [tokens]
    system_prefix_tokens = { kind = "fixed", value = 16 }
    tool_catalog_tokens = { kind = "fixed", value = 16 }
    repository_tokens = { kind = "fixed", value = 16 }
    session_tokens = { kind = "fixed", value = 16 }
    user_tokens = { kind = "fixed", value = 16 }
    [compaction]
    enabled = false
"#;

async fn header_server() -> (String, HeaderCapture, tokio::task::JoinHandle<()>) {
    let capture = HeaderCapture::default();
    let app = Router::new()
        .route("/v1/chat/completions", post(header_endpoint))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (target, capture, server)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arrival_schedule_starts_every_planned_tree() {
    let (target, capture, server) = header_server().await;
    let config = GeneratorConfig::parse(&format!(
        "schema_version = 5\nseed = 3\n[load.arrivals]\nbucket_minutes = 0.01\nsessions_per_minute = [3000, 0, 3000]\n{ONE_TURN}"
    ))
    .unwrap();
    let planner = TreePlanner::new(config.resolve().unwrap()).unwrap();
    let trees = planner.tree_count().unwrap();
    assert!(trees > 10);
    let dictionary = TokenDictionary::new(
        16,
        SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
    )
    .unwrap();
    let output = tempdir().unwrap();
    let summary = run_generated(&planner, dictionary, quick_options(target, output.path()))
        .await
        .unwrap();
    assert_eq!(summary.request_count, trees);
    assert_eq!(summary.succeeded, trees);
    assert_eq!(summary.plan.top_level_sessions, trees);
    assert_eq!(capture.0.lock().unwrap().len(), trees);
    // The schedule lasts 1.8 s, with a quiet middle bucket.
    assert!(summary.total_time_ms >= 1_000.0);
    let timeline = std::fs::read_to_string(output.path().join("timeline.csv")).unwrap();
    assert!(timeline.starts_with("start_s,requests,"));
    assert_eq!(timeline.lines().count(), 2);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_with_a_duration_stop_starting_trees() {
    let (target, _capture, server) = header_server().await;
    let config = GeneratorConfig::parse(&format!(
        "schema_version = 5\nseed = 3\n[load]\nconcurrent_sessions = 2\nduration_minutes = 0.01\nrestart_delay_ms = {{ kind = \"fixed\", value = 100 }}\n{ONE_TURN}"
    ))
    .unwrap();
    let planner = TreePlanner::new(config.resolve().unwrap()).unwrap();
    assert_eq!(planner.tree_count(), None);
    let dictionary = TokenDictionary::new(
        16,
        SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
    )
    .unwrap();
    let output = tempdir().unwrap();
    let summary = run_generated(&planner, dictionary, quick_options(target, output.path()))
        .await
        .unwrap();
    // Two streams restart every ~100 ms for 600 ms.
    let trees = summary.plan.top_level_sessions;
    assert!((6..=14).contains(&trees), "{trees} trees");
    assert_eq!(summary.request_count, trees);
    assert!(summary.passed);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_after_ends_a_long_run_with_the_requests_it_sent() {
    let (target, capture, server) = header_server().await;
    let config = GeneratorConfig::parse(&format!(
        "schema_version = 5\nseed = 3\n[load.arrivals]\nbucket_minutes = 1\nsessions_per_minute = [600]\n{ONE_TURN}"
    ))
    .unwrap();
    let planner = TreePlanner::new(config.resolve().unwrap()).unwrap();
    let dictionary = TokenDictionary::new(
        16,
        SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
    )
    .unwrap();
    let output = tempdir().unwrap();
    let mut options = quick_options(target, output.path());
    options.stop_after = Some(Duration::from_millis(400));
    let summary = run_generated(&planner, dictionary, options).await.unwrap();
    assert!(summary.stopped_early);
    assert!(summary.request_count > 0);
    assert!(summary.request_count < planner.tree_count().unwrap());
    assert_eq!(summary.request_count, capture.0.lock().unwrap().len());
    assert!(summary.passed);
    assert!(summary.total_time_ms < 5_000.0);
    server.abort();
}

#[tokio::test]
async fn connect_prefers_h2c_and_falls_back_to_http1() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // axum speaks HTTP/2 without TLS, as Dynamo's frontend does.
    let app = Router::new().route("/v1/models", axum::routing::get(|| async { "{}" }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let output = tempdir().unwrap();
    let options = quick_options(target.clone(), output.path());
    let (_, version) = super::request::connect(&options, &normalize_target(&target), 16)
        .await
        .unwrap();
    assert_eq!(version, "h2c");
    server.abort();

    // A server that speaks only HTTP/1.1 drops the HTTP/2 preface.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buffer = vec![0; 4096];
                let read = socket.read(&mut buffer).await.unwrap_or(0);
                if !buffer[..read].starts_with(b"PRI * HTTP/2.0") {
                    socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}",
                        )
                        .await
                        .ok();
                }
            });
        }
    });
    let options = quick_options(target.clone(), output.path());
    let (_, version) = super::request::connect(&options, &normalize_target(&target), 16)
        .await
        .unwrap();
    assert_eq!(version, "http/1.1");
    server.abort();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blocked_result_file_does_not_delay_dispatch() {
    use std::io::Read;

    let (target, capture, server) = header_server().await;
    // About 300 one-request sessions in 1.2 s, all released on time only if
    // the scheduler never waits on the results file.
    let config = GeneratorConfig::parse(&format!(
        "schema_version = 5\nseed = 3\n[load.arrivals]\nbucket_minutes = 0.02\nsessions_per_minute = [15000]\n{ONE_TURN}"
    ))
    .unwrap();
    let planner = TreePlanner::new(config.resolve().unwrap()).unwrap();
    let requests = planner.tree_count().unwrap();
    let dictionary = TokenDictionary::new(
        16,
        SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
    )
    .unwrap();
    let output = tempdir().unwrap();
    // requests.jsonl is a pipe whose reader waits before draining it, so the
    // writer blocks once the pipe buffer fills, like a stalled file system.
    let fifo = output.path().join("requests.jsonl");
    let path = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
    let reader = std::thread::spawn(move || {
        let mut file = std::fs::File::open(fifo).unwrap();
        std::thread::sleep(Duration::from_millis(1500));
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        contents.lines().count()
    });
    let summary = run_generated(&planner, dictionary, quick_options(target, output.path()))
        .await
        .unwrap();
    assert_eq!(summary.request_count, requests);
    assert_eq!(capture.0.lock().unwrap().len(), requests);
    assert!(
        summary.dispatch_lag_ms.max < 500.0,
        "dispatch waited on the result file: {:?}",
        summary.dispatch_lag_ms
    );
    assert_eq!(reader.join().unwrap(), requests);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_reports_peak_requests_in_flight() {
    async fn slow_endpoint() -> Response<Body> {
        tokio::time::sleep(Duration::from_millis(300)).await;
        Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from(
                "data: {\"choices\":[],\"usage\":{\"completion_tokens\":2}}\n\ndata: [DONE]\n\n",
            ))
            .unwrap()
    }
    let app = Router::new().route("/v1/chat/completions", post(slow_endpoint));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    // Four workers, two one-request sessions each: at most four requests at once.
    let config = GeneratorConfig::parse(&format!(
        "schema_version = 5\nseed = 3\n[load]\nnum_sessions = 8\nconcurrent_sessions = 4\n{ONE_TURN}"
    ))
    .unwrap();
    let planner = TreePlanner::new(config.resolve().unwrap()).unwrap();
    let dictionary = TokenDictionary::new(
        16,
        SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
    )
    .unwrap();
    let output = tempdir().unwrap();
    let summary = run_generated(&planner, dictionary, quick_options(target, output.path()))
        .await
        .unwrap();
    assert_eq!(summary.request_count, 8);
    assert_eq!(summary.peak_in_flight, 4);
    assert!(summary.in_flight_limit > 4);
    server.abort();
}
