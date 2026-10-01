// SPDX-License-Identifier: Apache-2.0

//! End-to-end checks against a live listener.

use std::net::SocketAddr;
use std::sync::Arc;

use agent_loadgen_serve::{AppState, router};
use serde_json::{Value, json};

async fn start() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(Arc::new(AppState::default()));
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn health_plan_and_metrics() {
    let base = start().await;
    let client = reqwest::Client::new();

    let health: Value = client
        .get(format!("{base}/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["ok"], json!(true));
    assert!(health["version"].is_string());

    // /plan with a small profile returns the CLI's plan summary and a swimlane.
    let toml = "schema_version = 5\nseed = 42\n[load]\nnum_sessions = 4\nconcurrent_sessions = 2\n";
    let response = client
        .post(format!("{base}/plan"))
        .json(&json!({"toml": toml}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let plan: Value = response.json().await.unwrap();
    assert_eq!(plan["summary"]["top_level_sessions"], json!(4));
    assert!(plan["summary"]["per_request"]["ideal_prefix_hit_rate"].is_number());
    assert_eq!(plan["timeline"]["trees"], json!(4));
    assert!(plan["elapsed_ms"].is_number());
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
        "loadgen_serve_jobs_total{kind=\"plan\",result=\"done\"} 1",
        "loadgen_serve_jobs_total{kind=\"plan\",result=\"error\"} 1",
        "loadgen_serve_jobs_inflight{kind=\"plan\"} 0",
        "loadgen_serve_http_requests_total{method=\"POST\",route=\"/plan\",status=\"200\"} 1",
        "loadgen_serve_http_requests_total{method=\"POST\",route=\"/plan\",status=\"400\"} 2",
        "loadgen_serve_http_requests_total{method=\"POST\",route=\"/plan\",status=\"422\"} 1",
        "loadgen_serve_rejected_total{reason=\"bad_request\"} 2",
        "loadgen_serve_active_clients{window=\"15m\"} 1",
        "loadgen_serve_active_clients{window=\"24h\"} 1",
        // The real plan and the 422; the 400s never reach acceptance.
        "loadgen_serve_client_jobs_total{client=\"127.0.0.1\",kind=\"plan\"} 2",
        "loadgen_serve_http_request_duration_seconds_count{route=\"/plan\"} 4",
        "loadgen_serve_plan_duration_seconds_count 2",
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
}
