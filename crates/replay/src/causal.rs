// SPDX-License-Identifier: Apache-2.0

//! Closed-loop execution of generated agent graphs.
//!
//! A workload is a sequence of chunks. Each chunk is a request DAG with
//! chunk-local dependencies. A chunk starts either at a scheduled offset or a
//! delay after its predecessor's anchor request completes. Chunks are loaded
//! shortly before they start and dropped when their last request completes,
//! so a long run holds only the chunks that are live.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::artifacts::{ResultSink, RunAccumulator};
use super::request::{ms_to_ns, send_request};
use super::{PreparedRequest, ReplayContext, ReplayOptions, RequestResult, duration_ns};
use crate::clock::sleep_until;
use agent_loadgen_core::scheduler::ReadyQueue;

/// Load a chunk this long before it starts, so planning never delays dispatch.
const LOAD_AHEAD: Duration = Duration::from_secs(2);

/// Time between starting the loop and the first scheduled request.
const START_DELAY: Duration = Duration::from_millis(100);

/// One request DAG admitted as a unit.
pub(crate) struct Chunk<P> {
    pub(crate) nodes: Vec<ChunkNode>,
    /// Completing this node starts the chunk's successor, if the workload has one.
    pub(crate) anchor: Option<usize>,
    pub(crate) payload: P,
}

pub(crate) struct ChunkNode {
    pub(crate) dependencies: Vec<usize>,
    /// Offset from the chunk start; set exactly for nodes without dependencies.
    pub(crate) start_offset_ms: Option<u64>,
    pub(crate) delay_after_dependencies_ms: u64,
}

pub(crate) trait CausalWorkload {
    type Payload;

    fn name(&self) -> &'static str;
    /// The next chunk with a known start offset, in nondecreasing start order.
    fn next_scheduled(&mut self) -> Result<Option<(u64, usize)>>;
    /// The chunk that starts after `chunk`'s anchor completes, with its delay.
    fn successor(&mut self, chunk: usize) -> Result<Option<(u64, usize)>>;
    fn load(&mut self, chunk: usize) -> Result<Chunk<Self::Payload>>;
    /// Build one node's request. `ordinal` is unique within the run.
    fn prepare(
        &self,
        payload: &Self::Payload,
        node: usize,
        ordinal: usize,
    ) -> Result<PreparedRequest>;
    /// No chunk starts at or after this offset.
    fn start_deadline_ms(&self) -> Option<u64> {
        None
    }
}

pub(crate) struct CausalExecution {
    pub(crate) accumulator: RunAccumulator,
    pub(crate) elapsed: Duration,
    pub(crate) peak_in_flight: usize,
    /// `stop_after` ended the run with planned requests unsent.
    pub(crate) stopped_early: bool,
}

/// HTTP requests currently sent and not yet finished, and the most at once.
#[derive(Default)]
struct InFlight {
    current: AtomicUsize,
    peak: AtomicUsize,
}

impl InFlight {
    fn enter(self: &Arc<Self>) -> InFlightGuard {
        let current = self.current.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(current, Ordering::Relaxed);
        InFlightGuard(Arc::clone(self))
    }
}

struct InFlightGuard(Arc<InFlight>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.current.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(crate) async fn execute_causally<W: CausalWorkload>(
    workload: &mut W,
    client: reqwest::Client,
    options: &ReplayOptions,
    in_flight_limit: usize,
) -> Result<CausalExecution> {
    let wall_started = Instant::now();
    let base = wall_started
        .checked_add(START_DELAY)
        .context("causal replay start delay exceeds the monotonic clock range")?;
    let context = ReplayContext { client, base };
    let semaphore = Arc::new(Semaphore::new(in_flight_limit));
    let in_flight = Arc::new(InFlight::default());
    let mut tasks = JoinSet::new();
    let mut scheduler = Scheduler {
        workload,
        live: HashMap::new(),
        loaded_nodes: 0,
        completed: 0,
        next_scheduled: None,
        scheduled_exhausted: false,
        pending_starts: ReadyQueue::with_capacity(0),
        ready: ReadyQueue::with_capacity(0),
        sink: ResultSink::create(&options.output_dir.join("requests.jsonl"))?,
        base,
        start_deadline_ns: None,
        stopped: false,
    };
    scheduler.start_deadline_ns = scheduler
        .workload
        .start_deadline_ms()
        .map(ms_to_ns)
        .transpose()?;
    let load_ahead_ns = duration_ns(LOAD_AHEAD);
    let stop_ns = options.stop_after.map(duration_ns);
    let mut stopped_early = false;

    loop {
        let now = Instant::now();
        let now_ns = duration_ns(now.saturating_duration_since(base));
        if !stopped_early && stop_ns.is_some_and(|stop_ns| now_ns >= stop_ns) {
            // Send nothing more; the loop ends once in-flight requests finish.
            scheduler.stop();
            stopped_early = true;
        }
        if !stopped_early {
            scheduler.admit(now_ns.saturating_add(load_ahead_ns))?;
        }
        // Nothing is due before the start delay has elapsed.
        let due = if now >= base {
            scheduler.ready.pop_due(now_ns, usize::MAX)
        } else {
            Vec::new()
        };
        if !due.is_empty() {
            for item in due {
                let (chunk, node) = item.value;
                let request = scheduler.take_request(chunk, node)?;
                let scheduled = base
                    .checked_add(Duration::from_nanos(item.ready_at_ns))
                    .context("a causal replay timestamp exceeds the monotonic clock range")?;
                let scheduler_wake = Instant::now();
                let context = context.clone();
                let semaphore = Arc::clone(&semaphore);
                let in_flight = Arc::clone(&in_flight);
                tasks.spawn(async move {
                    let permit = semaphore
                        .acquire_owned()
                        .await
                        .context("the causal replay request semaphore closed")?;
                    let _in_flight = in_flight.enter();
                    let result = send_request(
                        &context,
                        scheduled,
                        scheduler_wake,
                        item.ready_at_ns,
                        request,
                        permit,
                    )
                    .await?;
                    Ok::<_, anyhow::Error>((chunk, node, Instant::now(), result))
                });
            }
            continue;
        }
        let next_wake_ns = [
            stop_ns.filter(|_| !stopped_early),
            scheduler.ready.next_ready_at_ns(),
            scheduler
                .pending_starts
                .next_ready_at_ns()
                .map(|ns| ns.saturating_sub(load_ahead_ns)),
            scheduler
                .next_scheduled
                .map(|(ns, _)| ns.saturating_sub(load_ahead_ns)),
        ]
        .into_iter()
        .flatten()
        .min();
        match next_wake_ns {
            None if tasks.is_empty() => break,
            None => {
                let result = tasks.join_next().await.expect("tasks are not empty");
                scheduler.release(result.context("a causal replay request task failed")??)?;
            }
            Some(wake_ns) => {
                let deadline = base
                    .checked_add(Duration::from_nanos(wake_ns))
                    .context("a causal replay timestamp exceeds the monotonic clock range")?;
                if tasks.is_empty() {
                    sleep_until(deadline).await;
                } else {
                    tokio::select! {
                        () = sleep_until(deadline) => {},
                        result = tasks.join_next() => {
                            if let Some(result) = result {
                                scheduler.release(
                                    result.context("a causal replay request task failed")??,
                                )?;
                            }
                        }
                    }
                }
            }
        }
    }

    let elapsed = wall_started.elapsed();
    if !stopped_early && !scheduler.live.is_empty() {
        bail!(
            "{} graph stalled after {} of {} loaded requests",
            scheduler.workload.name(),
            scheduler.completed,
            scheduler.loaded_nodes,
        );
    }
    Ok(CausalExecution {
        accumulator: scheduler.sink.finish()?,
        elapsed,
        peak_in_flight: in_flight.peak.load(Ordering::Relaxed),
        stopped_early,
    })
}

struct LiveChunk<P> {
    chunk: Chunk<P>,
    base_ordinal: usize,
    remaining_dependencies: Vec<usize>,
    dependency_completion_ns: Vec<u64>,
    successors: Vec<Vec<usize>>,
    prepared: Vec<Option<PreparedRequest>>,
    outstanding: usize,
}

struct Scheduler<'a, W: CausalWorkload> {
    workload: &'a mut W,
    live: HashMap<usize, LiveChunk<W::Payload>>,
    loaded_nodes: usize,
    completed: usize,
    /// The workload's next scheduled chunk, peeked before it is due.
    next_scheduled: Option<(u64, usize)>,
    scheduled_exhausted: bool,
    /// Successor chunks with a known start that are not loaded yet.
    pending_starts: ReadyQueue<usize>,
    ready: ReadyQueue<(usize, usize)>,
    sink: ResultSink,
    base: Instant,
    start_deadline_ns: Option<u64>,
    /// After `stop_after`, completions record results but release nothing.
    stopped: bool,
}

impl<W: CausalWorkload> Scheduler<'_, W> {
    /// Drop every request not yet dispatched; in-flight requests still finish.
    fn stop(&mut self) {
        self.ready = ReadyQueue::with_capacity(0);
        self.pending_starts = ReadyQueue::with_capacity(0);
        self.next_scheduled = None;
        self.scheduled_exhausted = true;
        self.stopped = true;
    }

    /// Load every chunk that starts at or before `horizon_ns`.
    fn admit(&mut self, horizon_ns: u64) -> Result<()> {
        loop {
            if self.next_scheduled.is_none() && !self.scheduled_exhausted {
                match self.workload.next_scheduled()? {
                    Some((start_ms, chunk)) => {
                        let start_ns = ms_to_ns(start_ms)?;
                        if self
                            .start_deadline_ns
                            .is_some_and(|deadline| start_ns >= deadline)
                        {
                            self.scheduled_exhausted = true;
                        } else {
                            self.next_scheduled = Some((start_ns, chunk));
                        }
                    }
                    None => self.scheduled_exhausted = true,
                }
            }
            match self.next_scheduled {
                Some((start_ns, chunk)) if start_ns <= horizon_ns => {
                    self.next_scheduled = None;
                    self.start_chunk(chunk, start_ns)?;
                }
                _ => break,
            }
        }
        for item in self.pending_starts.pop_due(horizon_ns, usize::MAX) {
            self.start_chunk(item.value, item.ready_at_ns)?;
        }
        Ok(())
    }

    fn start_chunk(&mut self, id: usize, start_ns: u64) -> Result<()> {
        let chunk = self.workload.load(id)?;
        let count = chunk.nodes.len();
        if count == 0 {
            bail!("{} chunk {id} has no requests", self.workload.name());
        }
        let base_ordinal = self.loaded_nodes;
        self.loaded_nodes += count;
        let mut successors = vec![Vec::new(); count];
        let mut roots = Vec::new();
        for (node, spec) in chunk.nodes.iter().enumerate() {
            for dependency in &spec.dependencies {
                if *dependency >= node {
                    bail!(
                        "{} chunk {id} node {node} depends on a later node {dependency}",
                        self.workload.name()
                    );
                }
                successors[*dependency].push(node);
            }
            match (spec.dependencies.is_empty(), spec.start_offset_ms) {
                (true, Some(offset_ms)) => roots.push((node, offset_ms)),
                (true, None) => bail!(
                    "{} chunk {id} root node {node} has no start offset",
                    self.workload.name()
                ),
                (false, Some(_)) => bail!(
                    "{} chunk {id} dependent node {node} also has a start offset",
                    self.workload.name()
                ),
                (false, None) => {}
            }
        }
        let mut live = LiveChunk {
            remaining_dependencies: chunk
                .nodes
                .iter()
                .map(|node| node.dependencies.len())
                .collect(),
            dependency_completion_ns: vec![0; count],
            successors,
            prepared: std::iter::repeat_with(|| None).take(count).collect(),
            outstanding: count,
            base_ordinal,
            chunk,
        };
        for (node, offset_ms) in roots {
            live.prepared[node] = Some(self.workload.prepare(
                &live.chunk.payload,
                node,
                base_ordinal + node,
            )?);
            self.ready.push(
                start_ns
                    .checked_add(ms_to_ns(offset_ms)?)
                    .context("causal chunk start timestamp overflow")?,
                base_ordinal + node,
                (id, node),
            );
        }
        self.live.insert(id, live);
        Ok(())
    }

    fn take_request(&mut self, chunk: usize, node: usize) -> Result<PreparedRequest> {
        self.live
            .get_mut(&chunk)
            .and_then(|live| live.prepared.get_mut(node))
            .and_then(Option::take)
            .with_context(|| {
                format!(
                    "{} chunk {chunk} node {node} was released twice",
                    self.workload.name()
                )
            })
    }

    fn release(
        &mut self,
        (id, node, completed_at, result): (usize, usize, Instant, RequestResult),
    ) -> Result<()> {
        let completed_ns = duration_ns(completed_at.saturating_duration_since(self.base));
        self.sink.record(result)?;
        self.completed += 1;
        if self.stopped {
            return Ok(());
        }
        let name = self.workload.name();
        let live = self
            .live
            .get_mut(&id)
            .with_context(|| format!("{name} chunk {id} completed after it was dropped"))?;
        for successor in live.successors[node].clone() {
            live.remaining_dependencies[successor] = live.remaining_dependencies[successor]
                .checked_sub(1)
                .with_context(|| format!("{name} chunk {id} node {successor} released twice"))?;
            let completion = &mut live.dependency_completion_ns[successor];
            *completion = (*completion).max(completed_ns);
            if live.remaining_dependencies[successor] == 0 {
                let delay_ns = ms_to_ns(live.chunk.nodes[successor].delay_after_dependencies_ms)?;
                if live.prepared[successor].is_some() {
                    bail!("{name} chunk {id} node {successor} was prepared twice");
                }
                live.prepared[successor] = Some(self.workload.prepare(
                    &live.chunk.payload,
                    successor,
                    live.base_ordinal + successor,
                )?);
                self.ready.push(
                    live.dependency_completion_ns[successor]
                        .checked_add(delay_ns)
                        .context("causal successor timestamp overflow")?,
                    live.base_ordinal + successor,
                    (id, successor),
                );
            }
        }
        let anchor = live.chunk.anchor == Some(node);
        live.outstanding -= 1;
        if live.outstanding == 0 {
            self.live.remove(&id);
        }
        if anchor && let Some((delay_ms, next)) = self.workload.successor(id)? {
            let start_ns = completed_ns
                .checked_add(ms_to_ns(delay_ms)?)
                .context("causal successor chunk timestamp overflow")?;
            if !self
                .start_deadline_ns
                .is_some_and(|deadline| start_ns >= deadline)
            {
                self.pending_starts.push(start_ns, next, next);
            }
        }
        Ok(())
    }
}
