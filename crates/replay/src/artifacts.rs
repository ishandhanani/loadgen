// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use hdrhistogram::Histogram;
use serde::Serialize;

use super::{
    Percentiles, Performance, ReplayOptions, RequestResult, RunSummary, TrafficKind, millis,
};
use crate::clock::TIMER_BACKEND;
use crate::token_shape::TokenDictionaryManifest;
use agent_loadgen_core::TraceManifest;
use agent_loadgen_generate::scenario::CompactionExpectedEffect;

/// Width of each `timeline.csv` row.
const TIMELINE_BUCKET_MS: f64 = 60_000.0;

pub(super) struct ResultSink {
    writer: BufWriter<File>,
    flush_interval: usize,
    records_since_flush: usize,
    pub(super) accumulator: RunAccumulator,
}

impl ResultSink {
    pub(super) fn create(
        path: &Path,
        flush_interval: usize,
        dispatch_p99_limit_ms: f64,
    ) -> Result<Self> {
        let writer = BufWriter::new(
            File::create(path).with_context(|| format!("failed to create {}", path.display()))?,
        );
        Ok(Self {
            writer,
            flush_interval,
            records_since_flush: 0,
            accumulator: RunAccumulator::new(dispatch_p99_limit_ms)?,
        })
    }

    pub(super) fn record(&mut self, result: RequestResult) -> Result<()> {
        self.accumulator.record(&result)?;
        serde_json::to_writer(&mut self.writer, &result)
            .context("failed to write request result")?;
        self.writer.write_all(b"\n")?;
        self.records_since_flush += 1;
        if self.records_since_flush >= self.flush_interval {
            self.flush()?;
        }
        Ok(())
    }

    pub(super) fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.records_since_flush = 0;
        Ok(())
    }
}

pub(super) struct RunAccumulator {
    pub(super) request_count: usize,
    succeeded: usize,
    budgeted_requests: usize,
    unbudgeted_requests: usize,
    planned_aborts: usize,
    output_length_matches: usize,
    output_length_mismatches: usize,
    missing_output_usage: usize,
    planned_abort_matches: usize,
    planned_abort_mismatches: usize,
    scheduler_wake_lag_us: Histogram<u64>,
    dispatch_lag_us: Histogram<u64>,
    local_admission_lag_us: Histogram<u64>,
    dispatch_p99_limit_ms: f64,
    pub(super) dispatch_at_or_below_p99_limit: usize,
    pub(super) dispatch_max_ms: f64,
    ttft_us: Histogram<u64>,
    latency_us: Histogram<u64>,
    tpot_us: Histogram<u64>,
    input_tokens: u64,
    output_tokens: u64,
    timeline: Vec<TimelineBucket>,
    /// Root sessions already counted in the timeline.
    sessions_seen: HashSet<String>,
}

/// Requests dispatched during one timeline bucket.
#[derive(Default)]
struct TimelineBucket {
    requests: usize,
    failed: usize,
    input_tokens: u64,
    output_tokens: u64,
    root_sessions_started: usize,
    ttft_ms: Vec<f32>,
    latency_ms: Vec<f32>,
}

impl RunAccumulator {
    pub(super) fn new(dispatch_p99_limit_ms: f64) -> Result<Self> {
        Ok(Self {
            request_count: 0,
            succeeded: 0,
            budgeted_requests: 0,
            unbudgeted_requests: 0,
            planned_aborts: 0,
            output_length_matches: 0,
            output_length_mismatches: 0,
            missing_output_usage: 0,
            planned_abort_matches: 0,
            planned_abort_mismatches: 0,
            scheduler_wake_lag_us: Histogram::new(3)?,
            dispatch_lag_us: Histogram::new(3)?,
            local_admission_lag_us: Histogram::new(3)?,
            dispatch_p99_limit_ms,
            dispatch_at_or_below_p99_limit: 0,
            dispatch_max_ms: 0.0,
            ttft_us: Histogram::new(3)?,
            latency_us: Histogram::new(3)?,
            tpot_us: Histogram::new(3)?,
            input_tokens: 0,
            output_tokens: 0,
            timeline: Vec::new(),
            sessions_seen: HashSet::new(),
        })
    }

    fn record(&mut self, result: &RequestResult) -> Result<()> {
        self.request_count += 1;
        let planned_abort_succeeded = result.planned_abort_match == Some(true);
        let succeeded = planned_abort_succeeded
            || (result.error.is_none() && result.status_code.is_some_and(|status| status < 400));
        if succeeded {
            self.succeeded += 1;
        }
        self.record_performance(result, succeeded)?;
        if result.expected_output_tokens.is_some() {
            self.budgeted_requests += 1;
            match result.output_length_match {
                Some(true) => self.output_length_matches += 1,
                Some(false) => self.output_length_mismatches += 1,
                None => self.missing_output_usage += 1,
            }
        } else {
            self.unbudgeted_requests += 1;
        }
        if result.compaction_expected_effect == Some(CompactionExpectedEffect::NoMutationAborted) {
            self.planned_aborts += 1;
            match result.planned_abort_match {
                Some(true) => self.planned_abort_matches += 1,
                Some(false) | None => self.planned_abort_mismatches += 1,
            }
        }
        record_milliseconds(
            &mut self.scheduler_wake_lag_us,
            result.scheduler_wake_lag_ms,
        )?;
        record_milliseconds(&mut self.dispatch_lag_us, result.dispatch_lag_ms)?;
        record_milliseconds(
            &mut self.local_admission_lag_us,
            result.local_admission_lag_ms,
        )?;
        if result.dispatch_lag_ms <= self.dispatch_p99_limit_ms {
            self.dispatch_at_or_below_p99_limit += 1;
        }
        self.dispatch_max_ms = self.dispatch_max_ms.max(result.dispatch_lag_ms);
        Ok(())
    }
}

impl RunAccumulator {
    fn record_performance(&mut self, result: &RequestResult, succeeded: bool) -> Result<()> {
        let output_tokens = result.observed_output_tokens.unwrap_or(0);
        self.input_tokens += result.expected_input_tokens as u64;
        self.output_tokens += output_tokens;
        let bucket = (result.dispatch_offset_ms / TIMELINE_BUCKET_MS) as usize;
        if self.timeline.len() <= bucket {
            self.timeline
                .resize_with(bucket + 1, TimelineBucket::default);
        }
        let entry = &mut self.timeline[bucket];
        entry.requests += 1;
        entry.input_tokens += result.expected_input_tokens as u64;
        entry.output_tokens += output_tokens;
        if let Some(session) = &result.session_id
            && result.parent_session_id.is_none()
            && !self.sessions_seen.contains(session)
        {
            self.sessions_seen.insert(session.clone());
            entry.root_sessions_started += 1;
        }
        if !succeeded {
            entry.failed += 1;
            return Ok(());
        }
        if result.planned_abort_match.is_some() {
            return Ok(());
        }
        record_milliseconds(&mut self.latency_us, result.total_time_ms)?;
        entry.latency_ms.push(result.total_time_ms as f32);
        if let Some(ttft_ms) = result.ttft_ms {
            record_milliseconds(&mut self.ttft_us, ttft_ms)?;
            entry.ttft_ms.push(ttft_ms as f32);
            if output_tokens > 1 {
                let tpot_ms =
                    (result.total_time_ms - ttft_ms).max(0.0) / (output_tokens - 1) as f64;
                record_milliseconds(&mut self.tpot_us, tpot_ms)?;
            }
        }
        Ok(())
    }

    fn performance(&self, elapsed: Duration) -> Performance {
        let seconds = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
        Performance {
            requests_per_second: self.request_count as f64 / seconds,
            input_tokens_per_second: self.input_tokens as f64 / seconds,
            output_tokens_per_second: self.output_tokens as f64 / seconds,
            ttft_ms: histogram_percentiles(&self.ttft_us),
            request_latency_ms: histogram_percentiles(&self.latency_us),
            tpot_ms: histogram_percentiles(&self.tpot_us),
        }
    }
}

/// Write one CSV row per timeline bucket of dispatched requests.
pub(super) fn write_timeline(path: &Path, accumulator: &RunAccumulator) -> Result<()> {
    let mut writer = BufWriter::new(
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?,
    );
    writeln!(
        writer,
        "start_s,requests,failed,root_sessions_started,requests_per_s,input_tokens_per_s,output_tokens_per_s,ttft_p50_ms,ttft_p99_ms,latency_p50_ms,latency_p99_ms"
    )?;
    let seconds = TIMELINE_BUCKET_MS / 1000.0;
    for (index, bucket) in accumulator.timeline.iter().enumerate() {
        let ttft = sorted(&bucket.ttft_ms);
        let latency = sorted(&bucket.latency_ms);
        writeln!(
            writer,
            "{},{},{},{},{:.3},{:.1},{:.1},{},{},{},{}",
            index as f64 * seconds,
            bucket.requests,
            bucket.failed,
            bucket.root_sessions_started,
            bucket.requests as f64 / seconds,
            bucket.input_tokens as f64 / seconds,
            bucket.output_tokens as f64 / seconds,
            quantile(&ttft, 0.50),
            quantile(&ttft, 0.99),
            quantile(&latency, 0.50),
            quantile(&latency, 0.99),
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn sorted(values: &[f32]) -> Vec<f32> {
    let mut values = values.to_vec();
    values.sort_by(f32::total_cmp);
    values
}

/// Nearest-rank quantile, or an empty CSV field when there are no values.
fn quantile(sorted: &[f32], quantile: f64) -> String {
    if sorted.is_empty() {
        return String::new();
    }
    let rank = (sorted.len() as f64 * quantile).ceil() as usize;
    format!(
        "{:.1}",
        sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
    )
}

fn record_milliseconds(histogram: &mut Histogram<u64>, value: f64) -> Result<()> {
    if !value.is_finite() || value < 0.0 {
        bail!("a request timing value is not a non-negative finite number");
    }
    let microseconds = (value * 1000.0).round_ties_even();
    if microseconds > u64::MAX as f64 {
        bail!("a request timing value exceeds the histogram range");
    }
    histogram.record(microseconds as u64)?;
    Ok(())
}

fn histogram_percentiles(histogram: &Histogram<u64>) -> Percentiles {
    if histogram.is_empty() {
        return Percentiles::default();
    }
    Percentiles {
        min: histogram.min() as f64 / 1000.0,
        p50: histogram.value_at_quantile(0.50) as f64 / 1000.0,
        p95: histogram.value_at_quantile(0.95) as f64 / 1000.0,
        p99: histogram.value_at_quantile(0.99) as f64 / 1000.0,
        max: histogram.max() as f64 / 1000.0,
    }
}

pub(super) struct SummaryIdentity<'a> {
    pub(super) traffic_kind: TrafficKind,
    pub(super) run_id: String,
    pub(super) target: &'a str,
}

pub(super) fn summarize(
    identity: SummaryIdentity<'_>,
    source: &TraceManifest,
    dictionary: &TokenDictionaryManifest,
    options: &ReplayOptions,
    accumulator: &RunAccumulator,
    elapsed: Duration,
    stopped_early: bool,
) -> RunSummary {
    let scheduler_wake_lag_ms = histogram_percentiles(&accumulator.scheduler_wake_lag_us);
    let mut dispatch_lag_ms = histogram_percentiles(&accumulator.dispatch_lag_us);
    // Histograms are microsecond-quantized. Preserve the exact maximum so a
    // value just over the configured hard limit cannot round down and pass.
    dispatch_lag_ms.max = accumulator.dispatch_max_ms;
    let local_admission_lag_ms = histogram_percentiles(&accumulator.local_admission_lag_us);
    // A run stopped by `stop_after` is judged on the requests it sent.
    let request_fidelity_matches = (stopped_early
        || accumulator.request_count == source.request_count)
        && accumulator.succeeded == accumulator.request_count
        && accumulator.output_length_matches == accumulator.budgeted_requests
        && accumulator.missing_output_usage == 0
        && accumulator.planned_abort_matches == accumulator.planned_aborts;
    let dispatch_timing_matches = dispatch_timing_matches(accumulator, options.max_dispatch_max_ms);
    let passed = request_fidelity_matches && dispatch_timing_matches;
    let mut conclusion_blockers = Vec::new();
    if !options.token_path_verified {
        conclusion_blockers.push("token_path_unverified".to_string());
    }
    if options.engine_cache_mode.is_empty() {
        conclusion_blockers.push("engine_cache_mode_undeclared".to_string());
    }
    RunSummary {
        run_id: identity.run_id,
        header_dialect: options.header_dialect,
        dynamo_session_final: options.dynamo_session_final,
        model: options.model.clone(),
        target: identity.target.to_string(),
        time_scale: options.time_scale,
        max_in_flight: options.max_in_flight,
        warmup_connections: options.warmup_connections,
        http_transport: options.http_transport,
        result_flush_interval: options.result_flush_interval,
        max_dispatch_p99_ms: options.max_dispatch_p99_ms,
        max_dispatch_max_ms: options.max_dispatch_max_ms,
        timer_backend: TIMER_BACKEND,
        static_header_names: options
            .static_headers
            .iter()
            .map(|(name, _)| name.clone())
            .collect(),
        protocol_surface: "chat_completions",
        traffic_kind: identity.traffic_kind,
        scheduling_model: identity.traffic_kind.scheduling_model(),
        token_path_verified: options.token_path_verified,
        engine_cache_mode: options.engine_cache_mode.clone(),
        capacity_performance_conclusions_allowed: conclusion_blockers.is_empty(),
        conclusion_blockers,
        source: source.clone(),
        session_topology: None,
        token_dictionary: dictionary.clone(),
        request_count: accumulator.request_count,
        budgeted_requests: accumulator.budgeted_requests,
        unbudgeted_requests: accumulator.unbudgeted_requests,
        planned_aborts: accumulator.planned_aborts,
        succeeded: accumulator.succeeded,
        failed: accumulator.request_count - accumulator.succeeded,
        output_length_matches: accumulator.output_length_matches,
        output_length_mismatches: accumulator.output_length_mismatches,
        missing_output_usage: accumulator.missing_output_usage,
        planned_abort_matches: accumulator.planned_abort_matches,
        planned_abort_mismatches: accumulator.planned_abort_mismatches,
        scheduler_wake_lag_ms,
        dispatch_lag_ms,
        local_admission_lag_ms,
        performance: accumulator.performance(elapsed),
        stopped_early,
        request_fidelity_matches,
        dispatch_timing_matches,
        passed,
        total_time_ms: millis(elapsed),
    }
}

pub(super) fn dispatch_timing_matches(accumulator: &RunAccumulator, max_limit_ms: f64) -> bool {
    let p99_rank = usize::try_from((accumulator.request_count as u128 * 99).div_ceil(100))
        .unwrap_or(usize::MAX);
    accumulator.dispatch_at_or_below_p99_limit >= p99_rank
        && accumulator.dispatch_max_ms <= max_limit_ms
}

pub(super) fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let writer = BufWriter::new(
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?,
    );
    serde_json::to_writer_pretty(writer, value)
        .with_context(|| format!("failed to write {}", path.display()))
}
