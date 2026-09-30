// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Response, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::json;
use tempfile::tempdir;

use super::request::scaled_offset_ns;
use super::*;
use crate::token_shape::{SafeTokenAlphabet, TokenDictionary};
use agent_loadgen_core::{AgentContext, TraceRequest, percentiles};
use agent_loadgen_generate::scenario::{GeneratorConfig, TreePlanner};
use agent_loadgen_trace::{AgenticTrace, AgenticTurn};

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

#[derive(Clone, Default)]
struct DelayedCapture(Arc<Mutex<Vec<Instant>>>);

async fn delayed_shape_endpoint(State(capture): State<DelayedCapture>) -> Response<Body> {
    let call_index = {
        let mut calls = capture.0.lock().unwrap();
        calls.push(Instant::now());
        calls.len()
    };
    if call_index == 1 {
        tokio::time::sleep(Duration::from_millis(75)).await;
    }
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

#[tokio::test]
async fn replay_sends_shape_and_agent_headers() {
    let capture = Capture::default();
    let app = Router::new()
        .route("/v1/chat/completions", post(shape_endpoint))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let request = TraceRequest {
        ordinal: 0,
        source_request_id: "source-1".to_string(),
        source_x_request_id: None,
        source_model: None,
        input_tokens: 3,
        output_tokens: 2,
        request_received_ms: 1000,
        trace_block_size: 16,
        input_sequence_hashes: vec![11],
        agent_context: Some(AgentContext {
            session_id: "thread-1".to_string(),
            parent_session_id: None,
            compaction: Some(serde_json::json!({"phase": "mid_turn"})),
            agent_headers: Default::default(),
            input_trigger: Some("tool_result".to_string()),
        }),
    };
    let trace = AgenticTrace {
        turns: vec![AgenticTurn {
            request: request.clone(),
            dependencies: Vec::new(),
            root_arrival_ms: Some(0),
            delay_after_dependencies_ms: 0,
            non_tool_delay_ms: 0,
            tool_wait_ms: 0,
            tool_events: Vec::new(),
        }],
        manifest: TraceManifest {
            request_count: 1,
            session_count: 1,
            requests_with_agent_context: 1,
            first_request_received_ms: 1000,
            last_request_received_ms: 1000,
            duration_ms: 0,
            input_tokens: 3,
            output_tokens: 2,
            distinct_sequence_hashes: 1,
            trace_block_size: 16,
            source_digest_sha256: "source".to_string(),
        },
    };
    let dictionary = TokenDictionary::build(
        &[request],
        SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
    )
    .unwrap();
    let output = tempdir().unwrap();
    let summary = run_agentic_replay(
        trace,
        dictionary,
        ReplayOptions {
            header_dialect: HeaderDialect::Codex,
            dynamo_session_final: true,
            model: "test-model".to_string(),
            target: format!("http://{address}"),
            output_dir: output.path().to_path_buf(),
            max_in_flight: 1,
            timeout: Duration::from_secs(5),
            time_scale: 1.0,
            static_headers: Vec::new(),
            stop_after: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(summary.succeeded, 1);
    assert_eq!(summary.output_length_matches, 1);
    assert!(summary.passed);
    assert!(summary.total_time_ms >= 5.0);
    let (headers, body) = capture.0.lock().unwrap().clone().unwrap();
    assert_eq!(headers.get("thread-id").unwrap(), "thread-1");
    assert!(headers.get("x-dynamo-session-id").is_none());
    assert_eq!(headers.get("x-dynamo-session-final").unwrap(), "true");
    assert_eq!(headers.get("session-id").unwrap(), "thread-1");
    assert_eq!(headers.get("x-codex-window-id").unwrap(), "thread-1:0");
    let metadata = serde_json::from_str::<serde_json::Value>(
        headers
            .get("x-codex-turn-metadata")
            .unwrap()
            .to_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(metadata["request_kind"], "compaction");
    assert_eq!(
        metadata["compaction"],
        serde_json::json!({"phase": "mid_turn"})
    );
    assert_eq!(metadata["thread_id"], "thread-1");
    assert_eq!(body["max_tokens"], 2);
    assert!(body.get("min_tokens").is_none());
    assert_eq!(body["ignore_eos"], true);
    assert_eq!(body["messages"][0]["role"], "assistant");
    assert_eq!(body["messages"][1]["role"], "tool");
    assert_eq!(
        body["messages"][1]["tool_call_id"],
        "agent-loadgen-shape-tool"
    );
    assert_eq!(body["nvext"]["token_data"].as_array().unwrap().len(), 3);
    assert!(output.path().join("run.json").is_file());
    assert!(output.path().join("requests.jsonl").is_file());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recorded_scheduler_handles_tied_millisecond_arrivals() {
    let capture = Capture::default();
    let app = Router::new()
        .route("/v1/chat/completions", post(shape_endpoint))
        .with_state(capture);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let requests = (0..96)
        .map(|ordinal| TraceRequest {
            ordinal,
            source_request_id: format!("source-{ordinal}"),
            source_x_request_id: None,
            source_model: None,
            input_tokens: 3,
            output_tokens: 2,
            request_received_ms: 1_000 + (ordinal / 3) as u64,
            trace_block_size: 16,
            input_sequence_hashes: vec![11],
            agent_context: Some(AgentContext {
                session_id: format!("thread-{ordinal}"),
                parent_session_id: None,
                compaction: None,
                agent_headers: Default::default(),
                input_trigger: Some("user_message".to_string()),
            }),
        })
        .collect::<Vec<_>>();
    let dictionary = TokenDictionary::build(
        &requests,
        SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
    )
    .unwrap();
    let trace = AgenticTrace {
        turns: requests
            .iter()
            .cloned()
            .enumerate()
            .map(|(ordinal, request)| AgenticTurn {
                request,
                dependencies: Vec::new(),
                root_arrival_ms: Some((ordinal / 3) as u64),
                delay_after_dependencies_ms: 0,
                non_tool_delay_ms: 0,
                tool_wait_ms: 0,
                tool_events: Vec::new(),
            })
            .collect(),
        manifest: TraceManifest {
            request_count: 96,
            session_count: 96,
            requests_with_agent_context: 96,
            first_request_received_ms: 1_000,
            last_request_received_ms: 1_031,
            duration_ms: 31,
            input_tokens: 288,
            output_tokens: 192,
            distinct_sequence_hashes: 1,
            trace_block_size: 16,
            source_digest_sha256: "source".to_string(),
        },
    };
    let output = tempdir().unwrap();
    let summary = run_agentic_replay(
        trace,
        dictionary,
        ReplayOptions {
            header_dialect: HeaderDialect::Codex,
            dynamo_session_final: true,
            model: "test-model".to_string(),
            target: format!("http://{address}"),
            output_dir: output.path().to_path_buf(),
            max_in_flight: 96,
            timeout: Duration::from_secs(5),
            time_scale: 1.0,
            static_headers: Vec::new(),
            stop_after: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(summary.succeeded, 96);
    assert_eq!(summary.output_length_matches, 96);
    assert!(summary.scheduler_wake_lag_ms.p99 < 100.0);
    assert!(summary.dispatch_lag_ms.p99 < 100.0);
    server.abort();
}

#[tokio::test]
async fn captured_successor_waits_for_actual_completion_plus_recorded_gap() {
    let capture = DelayedCapture::default();
    let app = Router::new()
        .route("/v1/chat/completions", post(delayed_shape_endpoint))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let requests = (0..2)
        .map(|ordinal| TraceRequest {
            ordinal,
            source_request_id: format!("source-{ordinal}"),
            source_x_request_id: None,
            source_model: None,
            input_tokens: 3,
            output_tokens: 2,
            request_received_ms: 1_000 + ordinal as u64 * 150,
            trace_block_size: 16,
            input_sequence_hashes: vec![11],
            agent_context: Some(AgentContext {
                session_id: "thread".to_string(),
                parent_session_id: None,
                compaction: None,
                agent_headers: Default::default(),
                input_trigger: Some(if ordinal == 0 {
                    "user_message".to_string()
                } else {
                    "tool_result".to_string()
                }),
            }),
        })
        .collect::<Vec<_>>();
    let dictionary = TokenDictionary::build(
        &requests,
        SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
    )
    .unwrap();
    let trace = AgenticTrace {
        turns: vec![
            AgenticTurn {
                request: requests[0].clone(),
                dependencies: Vec::new(),
                root_arrival_ms: Some(0),
                delay_after_dependencies_ms: 0,
                non_tool_delay_ms: 0,
                tool_wait_ms: 0,
                tool_events: Vec::new(),
            },
            AgenticTurn {
                request: requests[1].clone(),
                dependencies: vec![0],
                root_arrival_ms: None,
                delay_after_dependencies_ms: 50,
                non_tool_delay_ms: 50,
                tool_wait_ms: 0,
                tool_events: Vec::new(),
            },
        ],
        manifest: TraceManifest {
            request_count: 2,
            session_count: 1,
            requests_with_agent_context: 2,
            first_request_received_ms: 1_000,
            last_request_received_ms: 1_150,
            duration_ms: 150,
            input_tokens: 6,
            output_tokens: 4,
            distinct_sequence_hashes: 1,
            trace_block_size: 16,
            source_digest_sha256: "source".to_string(),
        },
    };
    let output = tempdir().unwrap();
    let summary = run_agentic_replay(
        trace,
        dictionary,
        ReplayOptions {
            header_dialect: HeaderDialect::Codex,
            dynamo_session_final: true,
            model: "test-model".to_string(),
            target: format!("http://{address}"),
            output_dir: output.path().to_path_buf(),
            max_in_flight: 2,
            timeout: Duration::from_secs(5),
            time_scale: 1.0,
            static_headers: Vec::new(),
            stop_after: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(summary.succeeded, 2);
    let calls = capture.0.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert!(calls[1].duration_since(calls[0]) >= Duration::from_millis(120));
    server.abort();
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
        0,
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
            max_in_flight: 2,
            timeout: Duration::from_secs(5),
            time_scale: 100.0,
            static_headers: Vec::new(),
            stop_after: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(summary.succeeded, 2);
    assert_eq!(summary.budgeted_requests, 2);
    let topology = summary.session_topology.unwrap();
    assert_eq!(topology.configured_top_level_sessions, 1);
    assert_eq!(topology.generated_subagent_sessions, 0);
    assert_eq!(topology.total_protocol_sessions, 1);
    assert!(summary.passed);
    assert!(output.path().join("timeline.csv").is_file());
    // The capture holds the last request sent, which is the session's final turn.
    let (headers, _) = capture.0.lock().unwrap().clone().unwrap();
    assert_eq!(headers.get("x-dynamo-session-final").unwrap(), "true");
    // Wire session IDs are run-unique UUIDs, not planned session IDs.
    let thread = headers.get("thread-id").unwrap().to_str().unwrap();
    assert!(uuid::Uuid::parse_str(thread).is_ok());
    assert_ne!(thread, tree.sessions[0].session_id);
    assert_eq!(headers.get("session-id").unwrap(), thread);
    server.abort();
}

#[test]
fn scaled_offsets_use_integer_nanoseconds() {
    assert_eq!(scaled_offset_ns(1, 1.0).unwrap(), 1_000_000);
    assert_eq!(scaled_offset_ns(1, 2.0).unwrap(), 500_000);
    assert_eq!(scaled_offset_ns(1, 3.0).unwrap(), 333_333);
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
        0,
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
            max_in_flight: 2,
            timeout: Duration::from_secs(5),
            time_scale: 100.0,
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
        max_in_flight: 1,
        timeout: Duration::from_secs(1),
        time_scale: 1.0,
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

fn quick_options(target: String, output: &std::path::Path, time_scale: f64) -> ReplayOptions {
    ReplayOptions {
        header_dialect: HeaderDialect::Codex,
        dynamo_session_final: false,
        model: "test-model".to_string(),
        target,
        output_dir: output.to_path_buf(),
        max_in_flight: 64,
        timeout: Duration::from_secs(5),
        time_scale,
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
        0,
        SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
    )
    .unwrap();
    let output = tempdir().unwrap();
    let summary = run_generated(
        &planner,
        dictionary,
        quick_options(target, output.path(), 1.0),
    )
    .await
    .unwrap();
    assert_eq!(summary.request_count, trees);
    assert_eq!(summary.succeeded, trees);
    assert_eq!(
        summary
            .session_topology
            .unwrap()
            .configured_top_level_sessions,
        trees
    );
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
        0,
        SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
    )
    .unwrap();
    let output = tempdir().unwrap();
    let summary = run_generated(
        &planner,
        dictionary,
        quick_options(target, output.path(), 1.0),
    )
    .await
    .unwrap();
    // Two streams restart every ~100 ms for 600 ms.
    let trees = summary
        .session_topology
        .unwrap()
        .configured_top_level_sessions;
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
        0,
        SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
    )
    .unwrap();
    let output = tempdir().unwrap();
    let mut options = quick_options(target, output.path(), 1.0);
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
    let options = quick_options(target.clone(), output.path(), 1.0);
    let (_, version) = super::request::connect(&options, &normalize_target(&target))
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
    let options = quick_options(target.clone(), output.path(), 1.0);
    let (_, version) = super::request::connect(&options, &normalize_target(&target))
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
        0,
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
    let summary = run_generated(
        &planner,
        dictionary,
        quick_options(target, output.path(), 1.0),
    )
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
