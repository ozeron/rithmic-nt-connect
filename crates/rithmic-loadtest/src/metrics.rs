//! Latency histograms, RSS sampling and the JSON report.

use std::sync::OnceLock;
use std::time::Instant;

use hdrhistogram::Histogram;
use serde::Serialize;

static EPOCH: OnceLock<Instant> = OnceLock::new();

/// Monotonic nanoseconds since the first call in this process. Events carry it
/// in `ts_event_ns`, so a consumer measures end-to-end latency with one clock.
pub fn now_ns() -> u64 {
    EPOCH.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

/// Microsecond latency histogram (1 us .. 1 h, 3 significant digits).
pub struct Lat(Histogram<u64>);

impl Default for Lat {
    fn default() -> Self {
        Self::new()
    }
}

impl Lat {
    pub fn new() -> Self {
        Self(Histogram::new_with_bounds(1, 3_600_000_000, 3).expect("histogram bounds"))
    }

    pub fn record_ns(&mut self, ns: u64) {
        let us = (ns / 1_000).max(1);
        let _ = self.0.record(us.min(3_600_000_000));
    }

    pub fn merge(&mut self, other: &Lat) {
        let _ = self.0.add(&other.0);
    }

    pub fn summary(&self) -> LatSummary {
        LatSummary {
            count: self.0.len(),
            p50_us: self.0.value_at_quantile(0.50),
            p99_us: self.0.value_at_quantile(0.99),
            p999_us: self.0.value_at_quantile(0.999),
            max_us: self.0.max(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LatSummary {
    pub count: u64,
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
}

/// Per-consumer outcome.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ConnReport {
    pub kind: String,
    pub received: u64,
    /// Events skipped inside the observed sequence (a gap the consumer saw).
    pub missing_in_stream: u64,
    pub reordered: u64,
    /// Gap events delivered, and the events they reported as skipped.
    pub gap_events: u64,
    pub gap_skipped: u64,
    /// Times the parent told this connection its plants were rebuilt.
    pub resumed: u32,
    pub reattaches: u32,
    /// Longest time spent re-attaching after a drop, in milliseconds.
    pub max_outage_ms: u64,
    pub dropped_by_parent: bool,
    pub latency: LatSummary,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Report {
    pub scenario: String,
    pub params: serde_json::Value,
    pub duration_s: f64,
    pub sent: u64,
    pub conns: Vec<ConnReport>,
    /// Smallest received/sent ratio over the reading consumers.
    pub min_delivered_ratio: f64,
    /// Events per second delivered to the slowest reading consumer.
    pub min_throughput_eps: f64,
    pub latency: LatSummary,
    pub order_latency: Option<LatSummary>,
    pub order_idle_latency: Option<LatSummary>,
    pub peak_rss_kb: u64,
    pub rss_samples_kb: Vec<u64>,
    /// Events missing from a consumer's sequence that no gap accounts for.
    pub unsignalled_loss: u64,
    pub allocs_per_event: Option<f64>,
    pub alloc_bytes_per_event: Option<f64>,
    pub notes: Vec<String>,
}

/// Peak resident set size of this process in KiB.
pub fn peak_rss_kb() -> u64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let raw = ru.ru_maxrss as u64;
    if cfg!(target_os = "macos") {
        raw / 1024
    } else {
        raw
    }
}

/// Current resident set size in KiB (best effort; 0 when unavailable).
pub fn current_rss_kb() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(s) = std::fs::read_to_string("/proc/self/statm") {
            if let Some(pages) = s
                .split_whitespace()
                .nth(1)
                .and_then(|p| p.parse::<u64>().ok())
            {
                return pages * 4;
            }
        }
        0
    }
    #[cfg(not(target_os = "linux"))]
    {
        std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0)
    }
}
