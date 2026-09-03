// SPDX-License-Identifier: Apache-2.0

//! On-disk job directories plus the in-memory index used for join-in-flight,
//! bundle and plan caches, queue positions, and per-client limits.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::shards::ShardRef;

/// Persisted job record (`state.json`). Field names are the dashboard contract.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobState {
    pub kind: String,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sessions: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_inferred: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kept: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dropped_zero_output: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deduplicated: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub malformed_lines: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scanned: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shards: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub served_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inspect: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_position: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<Progress>,
    /// Shard listing the bundle was built from; used to validate cache hits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_etags: Option<Vec<ShardRef>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toml_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
    pub created_at: f64,
    pub updated_at: f64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Progress {
    pub shards_done: usize,
    pub shards_total: usize,
}

impl JobState {
    pub fn new(kind: &str, state: &str) -> Self {
        let now = epoch_seconds();
        Self {
            kind: kind.to_string(),
            state: state.to_string(),
            created_at: now,
            updated_at: now,
            ..Self::default()
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(self.state.as_str(), "queued" | "running")
    }
}

pub fn epoch_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0)
}

/// `^[A-Za-z0-9_.:-]{1,128}$`
pub fn is_safe_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b':' | b'-'))
}

/// One job's directory under `<root>/loadgen-jobs/`.
#[derive(Debug, Clone)]
pub struct JobDir {
    pub id: String,
    pub path: PathBuf,
}

impl JobDir {
    pub fn state_path(&self) -> PathBuf {
        self.path.join("state.json")
    }

    /// Atomically replace `state.json` (write to a temp file, then rename).
    pub fn write_state(&self, state: &mut JobState) -> Result<()> {
        state.updated_at = epoch_seconds();
        let tmp = self.path.join("state.json.tmp");
        let body = serde_json::to_vec(state)?;
        fs::write(&tmp, body).with_context(|| format!("failed to write {}", tmp.display()))?;
        fs::rename(&tmp, self.state_path())
            .with_context(|| format!("failed to replace {}", self.state_path().display()))
    }

    pub fn read_state(&self) -> Result<Option<JobState>> {
        match fs::read(self.state_path()) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes).with_context(|| {
                format!("corrupt job state {}", self.state_path().display())
            })?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error)
                .with_context(|| format!("failed to read {}", self.state_path().display())),
        }
    }

    pub fn append_log(&self, message: &str) {
        use std::io::Write;
        let stamp = chrono::DateTime::from_timestamp(epoch_seconds() as i64, 0)
            .map(|time| time.format("%H:%M:%S").to_string())
            .unwrap_or_default();
        let line = format!("[{stamp}] {message}\n");
        let appended = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.path.join("log.txt"))
            .and_then(|mut file| file.write_all(line.as_bytes()));
        if let Err(error) = appended {
            warn!(job = %self.id, %error, "failed to append job log");
        }
        info!(job = %self.id, "{message}");
    }

    /// Last `max_chars` characters of `log.txt` (empty when there is no log yet).
    pub fn log_tail(&self, max_chars: usize) -> String {
        let text = fs::read_to_string(self.path.join("log.txt")).unwrap_or_default();
        let count = text.chars().count();
        if count <= max_chars {
            text
        } else {
            text.chars().skip(count - max_chars).collect()
        }
    }
}

/// Identity of a replay bundle: dashboard model plus the sorted session set.
pub type BundleKey = (String, Vec<String>);

#[derive(Debug)]
struct ActiveJob {
    key: BundleKey,
    client: String,
}

#[derive(Debug, Default)]
struct Index {
    active: HashMap<String, ActiveJob>,
    /// Jobs waiting for a bundle permit, in arrival order.
    queue: Vec<String>,
    bundle_cache: HashMap<BundleKey, String>,
    plan_cache: HashMap<String, String>,
    per_client: HashMap<String, usize>,
}

/// Job directory manager with the in-memory index.
#[derive(Debug)]
pub struct JobStore {
    jobs_dir: PathBuf,
    keep_jobs: usize,
    index: Mutex<Index>,
}

impl JobStore {
    /// Open (creating if needed) `jobs_dir`, rebuild caches from completed jobs,
    /// and mark jobs left queued/running by a previous process as errors.
    pub fn open(jobs_dir: impl Into<PathBuf>, keep_jobs: usize) -> Result<Self> {
        let jobs_dir = jobs_dir.into();
        fs::create_dir_all(&jobs_dir)
            .with_context(|| format!("failed to create {}", jobs_dir.display()))?;
        let store = Self {
            jobs_dir,
            keep_jobs,
            index: Mutex::new(Index::default()),
        };
        store.rebuild_index()?;
        Ok(store)
    }

    pub fn jobs_dir(&self) -> &Path {
        &self.jobs_dir
    }

    fn rebuild_index(&self) -> Result<()> {
        let mut dirs = Vec::new();
        for entry in fs::read_dir(&self.jobs_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                dirs.push(entry.path());
            }
        }
        // Oldest first so the newest completed job wins a cache slot.
        dirs.sort_by_key(|path| fs::metadata(path).and_then(|meta| meta.modified()).ok());
        let mut index = self.index.lock().expect("job index poisoned");
        for path in dirs {
            let Some(id) = path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
            else {
                continue;
            };
            let job = JobDir {
                id: id.clone(),
                path: path.clone(),
            };
            let Some(mut state) = job.read_state().unwrap_or_else(|error| {
                warn!(%error, "skipping unreadable job");
                None
            }) else {
                continue;
            };
            if state.is_active() {
                state.state = "error".to_string();
                state.error = Some("service restarted before the job finished".to_string());
                state.queue_position = None;
                job.write_state(&mut state)?;
                continue;
            }
            if state.state != "done" {
                continue;
            }
            match state.kind.as_str() {
                "replay" => {
                    if let (Some(model), Some(sessions)) = (state.model, state.sessions) {
                        index.bundle_cache.insert((model, sessions), id);
                    }
                }
                "plan" => {
                    if let Some(sha) = state.toml_sha256 {
                        if path.join("plan.json").is_file() {
                            index.plan_cache.insert(sha, id);
                        }
                    }
                }
                _ => {}
            }
        }
        info!(
            bundles = index.bundle_cache.len(),
            plans = index.plan_cache.len(),
            "rebuilt job caches"
        );
        Ok(())
    }

    /// Resolve an existing job directory by ID (validated, not required to exist).
    pub fn job_dir(&self, id: &str) -> Result<JobDir> {
        if !is_safe_id(id) {
            bail!("bad job id");
        }
        Ok(JobDir {
            id: id.to_string(),
            path: self.jobs_dir.join(id),
        })
    }

    /// Create a fresh job directory named `<prefix>-<12 hex>`.
    pub fn create(&self, prefix: &str) -> Result<JobDir> {
        let id = format!(
            "{prefix}-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        );
        let path = self.jobs_dir.join(&id);
        fs::create_dir(&path).with_context(|| format!("failed to create {}", path.display()))?;
        Ok(JobDir { id, path })
    }

    pub fn in_flight(&self, key: &BundleKey) -> Option<String> {
        let index = self.index.lock().expect("job index poisoned");
        index
            .active
            .iter()
            .find(|(_, job)| &job.key == key)
            .map(|(id, _)| id.clone())
    }

    /// Completed bundle for `key`, with the shard listing it was built from.
    pub fn cached_bundle(&self, key: &BundleKey) -> Option<(String, Vec<ShardRef>)> {
        let id = {
            let index = self.index.lock().expect("job index poisoned");
            index.bundle_cache.get(key)?.clone()
        };
        let job = self.job_dir(&id).ok()?;
        let state = job.read_state().ok().flatten()?;
        if state.state != "done" || !job.path.join("bundle.jsonl.gz").is_file() {
            self.forget(&id);
            return None;
        }
        Some((id, state.shard_etags.unwrap_or_default()))
    }

    pub fn record_bundle(&self, key: BundleKey, id: &str) {
        let mut index = self.index.lock().expect("job index poisoned");
        index.bundle_cache.insert(key, id.to_string());
    }

    pub fn cached_plan(&self, toml_sha256: &str) -> Option<String> {
        let id = {
            let index = self.index.lock().expect("job index poisoned");
            index.plan_cache.get(toml_sha256)?.clone()
        };
        if self
            .job_dir(&id)
            .is_ok_and(|job| job.path.join("plan.json").is_file())
        {
            Some(id)
        } else {
            self.forget(&id);
            None
        }
    }

    pub fn record_plan(&self, toml_sha256: &str, id: &str) {
        let mut index = self.index.lock().expect("job index poisoned");
        index
            .plan_cache
            .insert(toml_sha256.to_string(), id.to_string());
    }

    /// Register a bundle job as active for `client`. Fails with the current count
    /// when the client already has `cap` queued or running jobs.
    pub fn start(&self, id: &str, key: BundleKey, client: &str, cap: usize) -> Result<(), usize> {
        let mut index = self.index.lock().expect("job index poisoned");
        let count = index.per_client.entry(client.to_string()).or_insert(0);
        if *count >= cap {
            return Err(*count);
        }
        *count += 1;
        index.active.insert(
            id.to_string(),
            ActiveJob {
                key,
                client: client.to_string(),
            },
        );
        Ok(())
    }

    /// Remove a job from the active set (idempotent).
    pub fn finish(&self, id: &str) {
        let mut index = self.index.lock().expect("job index poisoned");
        index.queue.retain(|queued| queued != id);
        if let Some(job) = index.active.remove(id) {
            if let Some(count) = index.per_client.get_mut(&job.client) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    index.per_client.remove(&job.client);
                }
            }
        }
    }

    /// Append to the permit queue; returns the 1-based position.
    pub fn enqueue(&self, id: &str) -> usize {
        let mut index = self.index.lock().expect("job index poisoned");
        index.queue.push(id.to_string());
        index.queue.len()
    }

    pub fn dequeue(&self, id: &str) {
        let mut index = self.index.lock().expect("job index poisoned");
        index.queue.retain(|queued| queued != id);
    }

    /// Live 1-based queue position for a waiting job, or `None` if it is not waiting.
    pub fn queue_position(&self, id: &str) -> Option<usize> {
        let index = self.index.lock().expect("job index poisoned");
        index
            .queue
            .iter()
            .position(|queued| queued == id)
            .map(|position| position + 1)
    }

    fn forget(&self, id: &str) {
        let mut index = self.index.lock().expect("job index poisoned");
        index.bundle_cache.retain(|_, cached| cached != id);
        index.plan_cache.retain(|_, cached| cached != id);
    }

    /// Delete the oldest job directories beyond `keep_jobs`, never touching active jobs.
    pub fn prune(&self) {
        let Ok(entries) = fs::read_dir(&self.jobs_dir) else {
            return;
        };
        let mut dirs: Vec<(SystemTime, PathBuf)> = entries
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .filter_map(|entry| {
                let modified = entry.metadata().ok()?.modified().ok()?;
                Some((modified, entry.path()))
            })
            .collect();
        if dirs.len() <= self.keep_jobs {
            return;
        }
        dirs.sort();
        let excess = dirs.len() - self.keep_jobs;
        let active: Vec<String> = {
            let index = self.index.lock().expect("job index poisoned");
            index.active.keys().cloned().collect()
        };
        for (_, path) in dirs.into_iter().take(excess) {
            let id = path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned());
            let Some(id) = id else {
                continue;
            };
            if active.contains(&id) {
                continue;
            }
            match fs::remove_dir_all(&path) {
                Ok(()) => {
                    self.forget(&id);
                    info!(job = %id, "pruned job directory");
                }
                Err(error) => warn!(job = %id, %error, "failed to prune job directory"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_ids() {
        assert!(is_safe_id("r-0123456789ab"));
        assert!(is_safe_id("a.b:c_d-E"));
        assert!(!is_safe_id(""));
        assert!(!is_safe_id("../x"));
        assert!(!is_safe_id("a b"));
        assert!(!is_safe_id(&"x".repeat(129)));
    }

    #[test]
    fn per_client_cap_and_queue_positions() {
        let dir = tempfile::tempdir().unwrap();
        let store = JobStore::open(dir.path().join("jobs"), 10).unwrap();
        let key = ("m".to_string(), vec!["s".to_string()]);
        store.start("j1", key.clone(), "c", 2).unwrap();
        store.start("j2", key.clone(), "c", 2).unwrap();
        assert_eq!(store.start("j3", key.clone(), "c", 2), Err(2));
        let in_flight = store.in_flight(&key).unwrap();
        assert!(in_flight == "j1" || in_flight == "j2");
        assert_eq!(store.enqueue("j1"), 1);
        assert_eq!(store.enqueue("j2"), 2);
        store.dequeue("j1");
        assert_eq!(store.queue_position("j2"), Some(1));
        store.finish("j1");
        store.finish("j2");
        store.start("j3", key, "c", 2).unwrap();
    }

    #[test]
    fn restart_marks_active_jobs_as_errors_and_rebuilds_caches() {
        let dir = tempfile::tempdir().unwrap();
        let jobs = dir.path().join("jobs");
        {
            let store = JobStore::open(&jobs, 10).unwrap();
            let running = store.create("r").unwrap();
            running
                .write_state(&mut JobState::new("replay", "running"))
                .unwrap();
            let done = store.create("r").unwrap();
            let mut state = JobState::new("replay", "done");
            state.model = Some("m".to_string());
            state.sessions = Some(vec!["s".to_string()]);
            done.write_state(&mut state).unwrap();
            fs::write(done.path.join("bundle.jsonl.gz"), b"x").unwrap();
            let plan = store.create("p").unwrap();
            let mut state = JobState::new("plan", "done");
            state.toml_sha256 = Some("abc".to_string());
            plan.write_state(&mut state).unwrap();
            fs::write(plan.path.join("plan.json"), b"{}").unwrap();
        }
        let store = JobStore::open(&jobs, 10).unwrap();
        let key = ("m".to_string(), vec!["s".to_string()]);
        assert!(store.cached_bundle(&key).is_some());
        assert!(store.cached_plan("abc").is_some());
        let errored = fs::read_dir(&jobs)
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let job = store.job_dir(&entry.file_name().to_string_lossy()).ok()?;
                job.read_state().ok().flatten()
            })
            .filter(|state| state.state == "error")
            .count();
        assert_eq!(errored, 1);
    }
}
