//! Machine-readable benchmark output, alongside the human-readable tables.
//!
//! Set `BENCH_JSON` to a path and every measurement is appended there as one
//! JSON object per line:
//!
//! ```text
//! BENCH_JSON=target/bench/acquire.jsonl cargo bench --bench acquire
//! ```
//!
//! A file rather than stdout, because stdout is the table a person reads and
//! cargo owns stderr. Line-delimited rather than one document, because appending
//! needs no parsing and a diff between two runs stays readable.
//!
//! # Every record carries a distribution, not a number
//!
//! `docs/performance.md` says of the multi-thread figures that they swing
//! "between 362 and 837 ns/op across runs of a *single* binary". A chart makes
//! small differences look meaningful in a way a text table does not, so emitting
//! one number per case would invite exactly the comparisons that caveat warns
//! against. Each record therefore reports `n`, `min`, `p50`, `p95`, `p99` and
//! `max`, plus `p999` where there are enough samples for it to mean anything, over
//! repeated measurements, and anything plotting it can show the
//! spread it actually has.
//!
//! Percentiles rather than every sample: a run of the Redis command example
//! collects tens of thousands of samples, and a chart needs the shape, not the
//! raw vector.
#![allow(dead_code)]

use std::{fs::OpenOptions, io::Write, path::PathBuf};

/// Where records go, if anywhere.
pub fn sink() -> Option<PathBuf> {
    std::env::var_os("BENCH_JSON")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

/// True when `BENCH_JSON` is set, for callers that would rather not do the work.
pub fn enabled() -> bool {
    sink().is_some()
}

/// Summary of repeated measurements of one thing.
#[derive(Debug, Clone, Copy)]
pub struct Stats {
    pub n: usize,
    pub min: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
    pub mean: f64,
}

impl Stats {
    /// Summarises `samples`, which need not be sorted.
    ///
    /// # Panics
    ///
    /// If `samples` is empty; a measurement with no samples is a bug in the
    /// bench, not a case to paper over with zeroes.
    pub fn of(samples: &[f64]) -> Self {
        assert!(!samples.is_empty(), "a measurement needs at least one sample");
        let mut sorted = samples.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).expect("no NaN in a duration"));
        Self {
            n: sorted.len(),
            min: sorted[0],
            p50: percentile(&sorted, 0.50),
            p95: percentile(&sorted, 0.95),
            p99: percentile(&sorted, 0.99),
            p999: percentile(&sorted, 0.999),
            max: sorted[sorted.len() - 1],
            mean: sorted.iter().sum::<f64>() / sorted.len() as f64,
        }
    }
}

/// Nearest-rank percentile of an already-sorted slice.
pub fn percentile(sorted: &[f64], p: f64) -> f64 {
    sorted[(((sorted.len() - 1) as f64) * p).round() as usize]
}

/// Appends one distribution record.
///
/// `suite` is the binary, `panel` groups cases that belong on one chart, `case`
/// is the series within it, and `threads` is set only where the case is a point
/// on a concurrency curve.
pub fn stat(
    suite: &str,
    panel: &str,
    case: &str,
    threads: Option<usize>,
    unit: &str,
    s: &Stats,
) {
    let threads = match threads {
        Some(t) => format!(r#","threads":{t}"#),
        None => String::new(),
    };
    line(&format!(
        r#"{{"kind":"stat","suite":"{}","panel":"{}","case":"{}"{threads},"unit":"{}",
"n":{},"min":{:.4},"p50":{:.4},"p95":{:.4},"p99":{:.4},"p999":{:.4},"max":{:.4},"mean":{:.4}}}"#,
        escape(suite),
        escape(panel),
        escape(case),
        escape(unit),
        s.n,
        s.min,
        s.p50,
        s.p95,
        s.p99,
        s.p999,
        s.max,
        s.mean,
    ));
}

/// Appends one scalar record, for things that are counts rather than timings.
pub fn value(suite: &str, panel: &str, case: &str, threads: Option<usize>, unit: &str, v: f64) {
    let threads = match threads {
        Some(t) => format!(r#","threads":{t}"#),
        None => String::new(),
    };
    line(&format!(
        r#"{{"kind":"value","suite":"{}","panel":"{}","case":"{}"{threads},"unit":"{}","value":{:.4}}}"#,
        escape(suite),
        escape(panel),
        escape(case),
        escape(unit),
        v,
    ));
}

/// Appends a record describing the run itself, so a chart can label it.
pub fn meta(suite: &str) {
    line(&format!(
        r#"{{"kind":"meta","suite":"{}","profile":"{}","os":"{}","arch":"{}","cores":{}}}"#,
        escape(suite),
        if cfg!(debug_assertions) { "debug" } else { "release" },
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
    ));
}

/// Writes one line, with the newlines that keep `stat!`'s literal readable
/// squeezed back out.
fn line(record: &str) {
    let Some(path) = sink() else { return };
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        let _ = std::fs::create_dir_all(parent);
    }
    let record: String = record.split('\n').collect();
    match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(mut file) => {
            if let Err(e) = writeln!(file, "{record}") {
                eprintln!("BENCH_JSON: cannot write {}: {e}", path.display());
            }
        }
        Err(e) => eprintln!("BENCH_JSON: cannot open {}: {e}", path.display()),
    }
}

/// The only characters a bench case name could plausibly contain that JSON
/// forbids bare.
fn escape(s: &str) -> String {
    s.replace('\\', r"\\").replace('"', r#"\""#)
}
