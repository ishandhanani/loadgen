// SPDX-License-Identifier: Apache-2.0

//! Replay bundle export: resolve a dashboard session tree, scan the shard hours
//! that can hold its records, keep only matching trace lines, validate the
//! result with the trace compiler, and gzip the bundle.

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use agent_loadgen_replay::token_shape::{SafeTokenAlphabet, TokenAlphabetSource, TokenDictionary};
use agent_loadgen_trace::load_agentic_trace;
use aho_corasick::AhoCorasick;
use anyhow::{Context, Result, bail};
use flate2::bufread::MultiGzDecoder;
use flate2::write::GzEncoder;
use futures_util::StreamExt;
use serde::Deserialize;
use tokio_util::io::SyncIoBridge;

use crate::AppState;
use crate::hours::{ShardHour, expand_hours};
use crate::jobs::{BundleKey, JobDir, JobState, Progress};
use crate::shards::{ShardRef, ShardSource, list_model_shards};

/// Concurrent shard scans within one bundle job.
const SHARD_FANOUT: usize = 8;
/// Synthetic token range used to validate a bundle without a tokenizer
/// (`--token-start 1000 --allow-unverified-token-range`).
const UNVERIFIED_TOKEN_START: u32 = 1000;
const UNVERIFIED_TOKEN_ALPHABET: u32 = 1024;

/// `POST /replay-bundle` body.
#[derive(Debug, Clone, Deserialize)]
pub struct BundleRequest {
    pub model: String,
    pub session_id: String,
    #[serde(default = "default_true")]
    pub include_tree: bool,
}

fn default_true() -> bool {
    true
}

/// Dashboard harness label -> `--headers` dialect. Other harnesses fall back to
/// `codex`, whose flat thread and parent-thread headers carry any session tree,
/// and the job reports `agent_inferred: true`.
pub fn agent_for_harness(harness: &str) -> (&'static str, bool) {
    match harness {
        "claude-code" => ("claude-code", false),
        "codex" => ("codex", false),
        _ => ("codex", true),
    }
}

/// Sessions, hours, and dialect derived from the dashboard data for one request.
#[derive(Debug, Clone)]
pub struct ResolvedBundle {
    /// Sorted session IDs (root plus descendants when `include_tree`).
    pub sessions: Vec<String>,
    /// Session IDs from the tree with no `session/<id>.json`.
    pub missing_sessions: Vec<String>,
    pub harness: String,
    pub agent: String,
    pub agent_inferred: bool,
    pub hours: Vec<ShardHour>,
}

impl ResolvedBundle {
    pub fn key(&self, model: &str) -> BundleKey {
        (model.to_string(), self.sessions.clone())
    }
}

#[derive(Deserialize)]
struct SessionFile {
    #[serde(default)]
    harness: Option<String>,
    #[serde(default)]
    rows: Vec<SessionRow>,
}

#[derive(Deserialize)]
struct SessionRow {
    ts: f64,
}

#[derive(Deserialize)]
struct GlobalFile {
    #[serde(default)]
    trees: Vec<Tree>,
}

#[derive(Deserialize)]
struct Tree {
    id: String,
    #[serde(default)]
    kids: Vec<TreeKid>,
}

#[derive(Deserialize)]
struct TreeKid {
    id: String,
}

fn session_path(web_dir: &Path, model: &str, session_id: &str) -> PathBuf {
    web_dir
        .join("m")
        .join(model)
        .join("session")
        .join(format!("{session_id}.json"))
}

fn load_session(web_dir: &Path, model: &str, session_id: &str) -> Result<Option<SessionFile>> {
    let path = session_path(web_dir, model, session_id);
    match fs::read(&path) {
        Ok(bytes) => {
            Ok(Some(serde_json::from_slice(&bytes).with_context(|| {
                format!("invalid session file {}", path.display())
            })?))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("failed to read {}", path.display())),
    }
}

/// Resolve the session set and shard hours. `Ok(None)` means the root session
/// does not exist for that model.
pub fn resolve_bundle(web_dir: &Path, request: &BundleRequest) -> Result<Option<ResolvedBundle>> {
    let Some(root) = load_session(web_dir, &request.model, &request.session_id)? else {
        return Ok(None);
    };
    let mut ids: BTreeSet<String> = BTreeSet::from([request.session_id.clone()]);
    if request.include_tree {
        let global_path = web_dir.join("m").join(&request.model).join("global.json");
        let global: GlobalFile = serde_json::from_slice(
            &fs::read(&global_path)
                .with_context(|| format!("failed to read {}", global_path.display()))?,
        )
        .with_context(|| format!("invalid {}", global_path.display()))?;
        let mut todo = vec![request.session_id.clone()];
        while let Some(current) = todo.pop() {
            for tree in global.trees.iter().filter(|tree| tree.id == current) {
                for kid in &tree.kids {
                    if ids.insert(kid.id.clone()) {
                        todo.push(kid.id.clone());
                    }
                }
            }
        }
    }
    let mut timestamps = Vec::new();
    let mut missing = Vec::new();
    for id in &ids {
        let session = if *id == request.session_id {
            Some(SessionFile {
                harness: root.harness.clone(),
                rows: root
                    .rows
                    .iter()
                    .map(|row| SessionRow { ts: row.ts })
                    .collect(),
            })
        } else {
            load_session(web_dir, &request.model, id)?
        };
        match session {
            Some(session) => timestamps.extend(session.rows.iter().map(|row| row.ts as i64)),
            None => missing.push(id.clone()),
        }
    }
    let hours = expand_hours(timestamps)?;
    let harness = root.harness.unwrap_or_else(|| "unknown".to_string());
    let (agent, agent_inferred) = agent_for_harness(&harness);
    Ok(Some(ResolvedBundle {
        sessions: ids.into_iter().collect(),
        missing_sessions: missing,
        harness,
        agent: agent.to_string(),
        agent_inferred,
        hours,
    }))
}

/// One kept trace line with the fields needed to order and deduplicate it.
#[derive(Debug, Clone)]
pub struct KeptLine {
    pub session_id: String,
    pub time_ms: u64,
    pub tiebreak: String,
    /// `request.request_id` for `request_end` records; used for deduplication.
    pub request_id: Option<String>,
    pub block_size: Option<u64>,
    pub model: Option<String>,
    pub raw: Vec<u8>,
}

/// Result of scanning one shard.
#[derive(Debug, Default)]
pub struct ShardScan {
    pub scanned: u64,
    pub dropped_zero_output: u64,
    pub malformed: u64,
    /// Compressed bytes read from the shard source.
    pub compressed_bytes: u64,
    pub kept: Vec<KeptLine>,
}

/// `Read` adapter counting the bytes pulled from the underlying reader.
struct CountingReader<R> {
    inner: R,
    bytes: u64,
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.bytes += read as u64;
        Ok(read)
    }
}

#[derive(Deserialize)]
struct LineEnvelope {
    #[serde(default)]
    event: Option<serde_json::Value>,
    #[serde(flatten)]
    direct: EventFields,
}

#[derive(Deserialize, Default)]
struct EventFields {
    #[serde(default)]
    event_type: Option<String>,
    #[serde(default)]
    event_time_unix_ms: Option<u64>,
    #[serde(default)]
    agent_context: Option<AgentContextFields>,
    #[serde(default)]
    request: Option<RequestFields>,
    #[serde(default)]
    tool: Option<ToolFields>,
}

#[derive(Deserialize)]
struct AgentContextFields {
    #[serde(default)]
    session_id: Option<String>,
}

#[derive(Deserialize)]
struct RequestFields {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    request_received_ms: Option<u64>,
    #[serde(default)]
    replay: Option<ReplayFields>,
}

#[derive(Deserialize)]
struct ReplayFields {
    #[serde(default)]
    trace_block_size: Option<u64>,
}

#[derive(Deserialize)]
struct ToolFields {
    #[serde(default)]
    tool_call_id: Option<String>,
}

/// Build the session-ID prefilter.
pub fn needle_matcher(sessions: &[String]) -> Result<AhoCorasick> {
    AhoCorasick::new(sessions).context("failed to build session-id matcher")
}

/// Scan one gzip (possibly multi-member) JSONL stream on the current thread.
pub fn scan_reader(
    reader: impl Read,
    matcher: &AhoCorasick,
    sessions: &HashSet<String>,
) -> Result<ShardScan> {
    let mut lines = BufReader::new(MultiGzDecoder::new(BufReader::new(CountingReader {
        inner: reader,
        bytes: 0,
    })));
    let mut scan = ShardScan::default();
    let mut buffer = Vec::new();
    loop {
        buffer.clear();
        let read = lines
            .read_until(b'\n', &mut buffer)
            .context("failed to read shard stream")?;
        if read == 0 {
            scan.compressed_bytes = lines.get_ref().get_ref().get_ref().bytes;
            break;
        }
        while matches!(buffer.last(), Some(b'\n' | b'\r')) {
            buffer.pop();
        }
        if buffer.is_empty() {
            continue;
        }
        scan.scanned += 1;
        if !matcher.is_match(&buffer) {
            continue;
        }
        let Ok(envelope) = serde_json::from_slice::<LineEnvelope>(&buffer) else {
            scan.malformed += 1;
            continue;
        };
        let event = match envelope.event {
            Some(serde_json::Value::Object(map)) => {
                match serde_json::from_value::<EventFields>(serde_json::Value::Object(map)) {
                    Ok(event) => event,
                    Err(_) => {
                        scan.malformed += 1;
                        continue;
                    }
                }
            }
            Some(_) => {
                scan.malformed += 1;
                continue;
            }
            None => envelope.direct,
        };
        let Some(session_id) = event
            .agent_context
            .as_ref()
            .and_then(|context| context.session_id.as_deref())
            .filter(|session_id| sessions.contains(*session_id))
        else {
            continue;
        };
        let mut kept = KeptLine {
            session_id: session_id.to_string(),
            time_ms: event.event_time_unix_ms.unwrap_or(0),
            tiebreak: String::new(),
            request_id: None,
            block_size: None,
            model: None,
            raw: buffer.clone(),
        };
        if event.event_type.as_deref() == Some("request_end") {
            let request = event.request.unwrap_or(RequestFields {
                request_id: None,
                model: None,
                output_tokens: None,
                request_received_ms: None,
                replay: None,
            });
            if request.output_tokens.unwrap_or(0) == 0 {
                scan.dropped_zero_output += 1;
                continue;
            }
            if let Some(received) = request.request_received_ms {
                kept.time_ms = received;
            }
            kept.tiebreak = request.request_id.clone().unwrap_or_default();
            kept.request_id = Some(request.request_id.unwrap_or_default());
            kept.block_size = request.replay.and_then(|replay| replay.trace_block_size);
            kept.model = request.model.filter(|model| !model.is_empty());
        } else if let Some(tool) = event.tool {
            kept.tiebreak = tool.tool_call_id.unwrap_or_default();
        }
        scan.kept.push(kept);
    }
    Ok(scan)
}

/// Stream one shard from the source and scan it on a blocking thread.
pub async fn scan_shard(
    source: &dyn ShardSource,
    shard: &ShardRef,
    matcher: Arc<AhoCorasick>,
    sessions: Arc<HashSet<String>>,
) -> Result<ShardScan> {
    let reader = source.open(&shard.key).await?;
    let key = shard.key.clone();
    tokio::task::spawn_blocking(move || {
        scan_reader(SyncIoBridge::new(reader), &matcher, &sessions)
            .with_context(|| format!("failed to scan shard {key}"))
    })
    .await
    .context("shard scan task panicked")?
}

/// Sorted, deduplicated bundle content.
#[derive(Debug, Default)]
pub struct MergedBundle {
    pub lines: Vec<KeptLine>,
    pub scanned: u64,
    pub dropped_zero_output: u64,
    pub deduplicated: u64,
    pub malformed: u64,
    pub block_sizes: BTreeSet<u64>,
    pub models: BTreeSet<String>,
}

/// Merge per-shard scans: order by (session, time, id, bytes) and drop repeated
/// `request_end` records (the same request can appear in overlapping listings).
pub fn merge_scans(scans: Vec<ShardScan>) -> MergedBundle {
    let mut merged = MergedBundle::default();
    let mut lines = Vec::new();
    for scan in scans {
        merged.scanned += scan.scanned;
        merged.dropped_zero_output += scan.dropped_zero_output;
        merged.malformed += scan.malformed;
        lines.extend(scan.kept);
    }
    lines.sort_by(|left, right| {
        (&left.session_id, left.time_ms, &left.tiebreak, &left.raw).cmp(&(
            &right.session_id,
            right.time_ms,
            &right.tiebreak,
            &right.raw,
        ))
    });
    let mut seen = HashSet::new();
    for line in lines {
        if let Some(request_id) = &line.request_id {
            if !seen.insert(request_id.clone()) {
                merged.deduplicated += 1;
                continue;
            }
            if let Some(block_size) = line.block_size {
                merged.block_sizes.insert(block_size);
            }
            if let Some(model) = &line.model {
                merged.models.insert(model.clone());
            }
        }
        merged.lines.push(line);
    }
    merged
}

/// Compile the bundle the way `agent-loadgen inspect` does, with an unverified
/// synthetic token range so no tokenizer is needed.
pub fn inspect_bundle(path: &Path) -> Result<serde_json::Value> {
    let trace = load_agentic_trace(&[path.to_path_buf()], None)?;
    let alphabet = SafeTokenAlphabet::load(
        TokenAlphabetSource::UnverifiedRange {
            start: UNVERIFIED_TOKEN_START,
            size: UNVERIFIED_TOKEN_ALPHABET,
        },
        UNVERIFIED_TOKEN_ALPHABET as usize,
        &[],
    )?;
    TokenDictionary::new(
        trace.manifest.trace_block_size,
        trace.manifest.distinct_sequence_hashes,
        alphabet,
    )?;
    Ok(serde_json::json!({
        "root_requests": trace.root_request_count(),
        "dependency_edges": trace.dependency_edge_count(),
        "fidelity": "agentic-causal",
    }))
}

fn gzip_file(source: &Path, target: &Path) -> Result<u64> {
    let mut input =
        fs::File::open(source).with_context(|| format!("failed to open {}", source.display()))?;
    let output = fs::File::create(target)
        .with_context(|| format!("failed to create {}", target.display()))?;
    let mut encoder = GzEncoder::new(output, flate2::Compression::default());
    std::io::copy(&mut input, &mut encoder).context("failed to gzip bundle")?;
    encoder.finish().context("failed to finish gzip bundle")?;
    Ok(fs::metadata(target)?.len())
}

/// The copy-paste replay command shown by the dashboard.
pub fn replay_command(filename: &str, agent: &str, served_model: &str, session_id: &str) -> String {
    let short: String = session_id.chars().take(12).collect();
    format!(
        "agent-loadgen replay {filename} \\\n  --headers {agent} --model {served_model} \\\n  --target http://<your-dynamo-frontend>:8000 \\\n  --tokenizer <hf-model-id-or-local-dir> \\\n  --output ./replay-{short}"
    )
}

/// Everything the spawned job needs.
pub struct BundleJob {
    pub job: JobDir,
    pub request: BundleRequest,
    pub resolved: ResolvedBundle,
    /// Shard listing already fetched by the request handler (cache check), if any.
    pub prelisted: Option<Vec<ShardRef>>,
}

/// Run a bundle job to completion, recording state and log lines in its directory.
pub async fn run_bundle_job(app: Arc<AppState>, work: BundleJob) {
    let job = work.job.clone();
    let mut state = match job.read_state() {
        Ok(Some(state)) => state,
        Ok(None) => JobState::new("replay", "queued"),
        Err(error) => {
            tracing::error!(job = %job.id, %error, "cannot read job state");
            app.store.finish(&job.id);
            return;
        }
    };
    let started = Instant::now();
    let outcome =
        tokio::time::timeout(app.job_timeout, execute_bundle_job(&app, &work, &mut state)).await;
    let result = if matches!(outcome, Ok(Ok(()))) {
        "done"
    } else {
        "error"
    };
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let message = format!("{error:#}");
            job.append_log(&format!("ERROR {message}"));
            state.state = "error".to_string();
            state.error = Some(message);
        }
        Err(_) => {
            let message = format!("job timed out after {}s", app.job_timeout.as_secs());
            job.append_log(&format!("ERROR {message}"));
            state.state = "error".to_string();
            state.error = Some(message);
        }
    }
    state.queue_position = Some(0);
    let elapsed = started.elapsed();
    state.elapsed_ms = Some(elapsed.as_millis() as u64);
    if let Err(error) = job.write_state(&mut state) {
        tracing::error!(job = %job.id, %error, "failed to persist final job state");
    }
    // A job still in the permit queue never became "running".
    if app.store.dequeue(&job.id) {
        app.metrics.jobs_queued.dec();
    } else {
        app.metrics
            .jobs_inflight
            .with_label_values(&["replay"])
            .dec();
    }
    app.metrics.job_finished("replay", result);
    app.metrics
        .bundle_job_duration
        .with_label_values(&[result])
        .observe(elapsed.as_secs_f64());
    app.store.finish(&job.id);
    app.store.prune();
}

async fn execute_bundle_job(
    app: &Arc<AppState>,
    work: &BundleJob,
    state: &mut JobState,
) -> Result<()> {
    let job = &work.job;
    let request = &work.request;
    let resolved = &work.resolved;

    state.queue_position = Some(app.store.enqueue(&job.id));
    app.metrics.jobs_queued.inc();
    state.sessions = Some(resolved.sessions.clone());
    state.agent = Some(resolved.agent.clone());
    state.harness = Some(resolved.harness.clone());
    state.agent_inferred = Some(resolved.agent_inferred);
    job.write_state(state)?;

    let _permit = app
        .bundle_semaphore
        .clone()
        .acquire_owned()
        .await
        .context("bundle semaphore closed")?;
    if app.store.dequeue(&job.id) {
        app.metrics.jobs_queued.dec();
    }
    app.metrics
        .jobs_inflight
        .with_label_values(&["replay"])
        .inc();
    state.state = "running".to_string();
    state.queue_position = Some(0);
    state.step = Some("resolving sessions".to_string());
    job.write_state(state)?;
    job.append_log(&format!(
        "sessions={} agent={} hours={:?}",
        resolved.sessions.len(),
        resolved.agent,
        resolved
            .hours
            .iter()
            .map(|hour| hour.label())
            .collect::<Vec<_>>()
    ));
    if !resolved.missing_sessions.is_empty() {
        job.append_log(&format!(
            "warning: {} tree session(s) have no session file: {:?}",
            resolved.missing_sessions.len(),
            resolved.missing_sessions
        ));
    }

    state.step = Some("listing shards".to_string());
    job.write_state(state)?;
    let shards = match &work.prelisted {
        Some(shards) => shards.clone(),
        None => list_model_shards(app.shards.as_ref(), &request.model, &resolved.hours)
            .await
            .inspect_err(|_| app.metrics.s3_errors.inc())?,
    };
    job.append_log(&format!("{} shard(s) to scan", shards.len()));
    if shards.is_empty() {
        bail!("no S3 shards found for those hours (outside S3 retention?)");
    }
    state.shards = Some(shards.len());
    state.shard_etags = Some(shards.clone());
    state.progress = Some(Progress {
        shards_done: 0,
        shards_total: shards.len(),
    });
    state.step = Some(format!("scanning shards (0/{})", shards.len()));
    job.write_state(state)?;

    let matcher = Arc::new(needle_matcher(&resolved.sessions)?);
    let sessions: Arc<HashSet<String>> = Arc::new(resolved.sessions.iter().cloned().collect());
    let scan_started = Instant::now();
    let mut scans = Vec::with_capacity(shards.len());
    let mut running_kept = 0_u64;
    {
        let source = app.shards.as_ref();
        let scans_pending: Vec<_> = shards
            .iter()
            .map(|shard| {
                let matcher = Arc::clone(&matcher);
                let sessions = Arc::clone(&sessions);
                async move { scan_shard(source, shard, matcher, sessions).await }
            })
            .collect();
        let mut stream = futures_util::stream::iter(scans_pending)
            .buffer_unordered(SHARD_FANOUT.min(shards.len()));
        while let Some(scan) = stream.next().await {
            let scan = scan.inspect_err(|_| app.metrics.s3_errors.inc())?;
            app.metrics.shards_scanned.inc();
            app.metrics.shard_bytes.inc_by(scan.compressed_bytes);
            app.metrics.lines_scanned.inc_by(scan.scanned);
            running_kept += scan.kept.len() as u64;
            scans.push(scan);
            if let Some(progress) = state.progress.as_mut() {
                progress.shards_done = scans.len();
            }
            state.kept = Some(running_kept);
            state.step = Some(format!(
                "scanning shards ({}/{})",
                scans.len(),
                shards.len()
            ));
            job.write_state(state)?;
        }
    }
    let merged = merge_scans(scans);
    app.metrics.records_kept.inc_by(merged.lines.len() as u64);
    app.metrics
        .zero_output_dropped
        .inc_by(merged.dropped_zero_output);
    job.append_log(&format!(
        "scanned={} kept={} dropped_zero_output={} deduplicated={} malformed={} block_sizes={:?} in {} ms",
        merged.scanned,
        merged.lines.len(),
        merged.dropped_zero_output,
        merged.deduplicated,
        merged.malformed,
        merged.block_sizes,
        scan_started.elapsed().as_millis()
    ));
    state.scanned = Some(merged.scanned);
    state.kept = Some(merged.lines.len() as u64);
    state.dropped_zero_output = Some(merged.dropped_zero_output);
    state.deduplicated = Some(merged.deduplicated);
    state.malformed_lines = Some(merged.malformed);
    if merged.lines.is_empty() {
        bail!("no records matched -- session may be older than S3 retention");
    }
    if merged.block_sizes.len() > 1 {
        bail!(
            "mixed trace_block_size in one session: {:?} (loadgen needs exactly one)",
            merged.block_sizes
        );
    }
    state.block_size = merged.block_sizes.iter().next().copied();

    let plain_path = job.path.join("bundle.jsonl");
    {
        let mut writer = std::io::BufWriter::new(
            fs::File::create(&plain_path)
                .with_context(|| format!("failed to create {}", plain_path.display()))?,
        );
        for line in &merged.lines {
            writer.write_all(&line.raw)?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
    }

    state.step = Some("validating with agent-loadgen inspect".to_string());
    job.write_state(state)?;
    let inspect_path = plain_path.clone();
    let inspect = tokio::task::spawn_blocking(move || inspect_bundle(&inspect_path))
        .await
        .context("inspect task panicked")?;
    state.inspect = Some(match inspect {
        Ok(report) => {
            job.append_log(&format!("inspect ok: {report}"));
            report
        }
        Err(error) => {
            let message = format!("{error:#}");
            job.append_log(&format!("inspect FAILED: {message}"));
            serde_json::json!({ "error": message })
        }
    });

    state.step = Some("compressing bundle".to_string());
    job.write_state(state)?;
    let gz_path = job.path.join("bundle.jsonl.gz");
    let (plain, gz) = (plain_path.clone(), gz_path.clone());
    let bundle_bytes = tokio::task::spawn_blocking(move || gzip_file(&plain, &gz))
        .await
        .context("gzip task panicked")??;
    fs::remove_file(&plain_path)
        .with_context(|| format!("failed to remove {}", plain_path.display()))?;

    let served_model = merged
        .models
        .iter()
        .next()
        .cloned()
        .unwrap_or_else(|| request.model.clone());
    let filename = format!("session-{}.jsonl.gz", request.session_id);
    state.command = Some(replay_command(
        &filename,
        &resolved.agent,
        &served_model,
        &request.session_id,
    ));
    state.served_model = Some(served_model);
    state.filename = Some(filename);
    state.bundle_bytes = Some(bundle_bytes);
    state.state = "done".to_string();
    state.step = Some("done".to_string());
    job.write_state(state)?;
    app.store
        .record_bundle(resolved.key(&request.model), &job.id);
    job.append_log(&format!("done: bundle_bytes={bundle_bytes}"));
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::write::GzEncoder;

    use super::*;

    fn request_end(request_id: &str, session_id: &str, received_ms: u64, output: u64) -> String {
        serde_json::json!({
            "schema": "dynamo.request.trace.v1",
            "event_type": "request_end",
            "event_time_unix_ms": received_ms + 100,
            "agent_context": {"session_id": session_id},
            "request": {
                "request_id": request_id,
                "model": "served/model",
                "input_tokens": 4,
                "output_tokens": output,
                "request_received_ms": received_ms,
                "replay": {"trace_block_size": 2, "input_length": 4, "input_sequence_hashes": [1, 2]}
            }
        })
        .to_string()
    }

    fn gzip_members(members: &[&[String]]) -> Vec<u8> {
        let mut out = Vec::new();
        for member in members {
            let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::fast());
            for line in member.iter() {
                writeln!(encoder, "{line}").unwrap();
            }
            out.extend(encoder.finish().unwrap());
        }
        out
    }

    fn matcher_for(sessions: &[&str]) -> (AhoCorasick, HashSet<String>) {
        let sessions: Vec<String> = sessions.iter().map(|s| s.to_string()).collect();
        (
            needle_matcher(&sessions).unwrap(),
            sessions.into_iter().collect(),
        )
    }

    #[test]
    fn scan_filters_by_agent_context_and_drops_zero_output() {
        let wrapped = serde_json::json!({
            "timestamp": 1,
            "event": serde_json::from_str::<serde_json::Value>(&request_end("r-wrapped", "sess-a", 2_000, 3)).unwrap()
        })
        .to_string();
        // Mentions the session id in another field only: prefilter hit, then rejected.
        let decoy = serde_json::json!({
            "event_type": "request_end",
            "agent_context": {"session_id": "sess-other"},
            "request": {"request_id": "sess-a-lookalike", "output_tokens": 3}
        })
        .to_string();
        let tool = serde_json::json!({
            "schema": "dynamo.request.trace.v1",
            "event_type": "tool_end",
            "event_time_unix_ms": 1_500,
            "agent_context": {"session_id": "sess-a"},
            "tool": {"tool_call_id": "t1", "tool_class": "shell"}
        })
        .to_string();
        let member_one = [
            request_end("r-1", "sess-a", 1_000, 2),
            request_end("r-zero", "sess-a", 1_200, 0),
            decoy,
            "not json {".to_string(),
            request_end("r-b", "sess-b", 900, 1),
        ];
        let member_two = [wrapped, tool, "sess-a garbage".to_string()];
        let bytes = gzip_members(&[&member_one, &member_two]);
        let (matcher, sessions) = matcher_for(&["sess-a"]);
        let scan = scan_reader(&bytes[..], &matcher, &sessions).unwrap();
        assert_eq!(scan.scanned, 8);
        assert_eq!(scan.compressed_bytes, bytes.len() as u64);
        assert_eq!(scan.dropped_zero_output, 1);
        assert_eq!(
            scan.malformed, 1,
            "only the line mentioning the session but not JSON"
        );
        let ids: Vec<Option<&str>> = scan.kept.iter().map(|k| k.request_id.as_deref()).collect();
        assert_eq!(ids, vec![Some("r-1"), Some("r-wrapped"), None]);
        assert_eq!(scan.kept[0].block_size, Some(2));
        assert_eq!(scan.kept[0].model.as_deref(), Some("served/model"));
    }

    #[test]
    fn merge_sorts_deterministically_and_dedupes_request_ids() {
        let (matcher, sessions) = matcher_for(&["sess-a", "sess-b"]);
        let shard_one = gzip_members(&[&[
            request_end("r-2", "sess-a", 2_000, 1),
            request_end("r-1", "sess-a", 1_000, 1),
        ]]);
        let shard_two = gzip_members(&[&[
            request_end("r-1", "sess-a", 1_000, 1),
            request_end("r-3", "sess-b", 500, 1),
        ]]);
        let scans = vec![
            scan_reader(&shard_two[..], &matcher, &sessions).unwrap(),
            scan_reader(&shard_one[..], &matcher, &sessions).unwrap(),
        ];
        let merged = merge_scans(scans);
        assert_eq!(merged.deduplicated, 1);
        assert_eq!(merged.scanned, 4);
        let order: Vec<&str> = merged
            .lines
            .iter()
            .map(|line| line.request_id.as_deref().unwrap())
            .collect();
        assert_eq!(order, vec!["r-1", "r-2", "r-3"]);
        assert_eq!(merged.block_sizes.into_iter().collect::<Vec<_>>(), vec![2]);
        assert_eq!(
            merged.models.into_iter().collect::<Vec<_>>(),
            vec!["served/model"]
        );
    }

    #[test]
    fn replay_command_matches_dashboard_contract() {
        let command = replay_command(
            "session-abcdefghijklmnop.jsonl.gz",
            "codex",
            "served/model",
            "abcdefghijklmnop",
        );
        assert_eq!(
            command,
            "agent-loadgen replay session-abcdefghijklmnop.jsonl.gz \\\n  --headers codex --model served/model \\\n  --target http://<your-dynamo-frontend>:8000 \\\n  --tokenizer <hf-model-id-or-local-dir> \\\n  --output ./replay-abcdefghijkl"
        );
    }

    #[test]
    fn harness_mapping() {
        assert_eq!(agent_for_harness("claude-code"), ("claude-code", false));
        assert_eq!(agent_for_harness("codex"), ("codex", false));
        assert_eq!(agent_for_harness("opencode"), ("codex", true));
        assert_eq!(agent_for_harness("unknown"), ("codex", true));
    }
}
