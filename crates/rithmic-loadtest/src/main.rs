//! `rithmic-load`: synthetic load harness for the gateway and session layers.
//!
//! Everything runs in this process against the real server code over a fake
//! plant, so no Rithmic credentials are needed. See
//! `docs/references/ops-runbook.md` ("Load harness") for how to read a report.
//!
//! ```text
//! rithmic-load suite [--full] [--out report.json]
//! rithmic-load run <scenario> [--out report.json]
//! rithmic-load smoke [--out report.json]
//! rithmic-load list
//! ```

mod alloc;
mod metrics;
mod rig;
mod scen;

use metrics::Report;

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1).cloned())
}

fn ms(us: u64) -> f64 {
    us as f64 / 1000.0
}

fn print_summary(r: &Report) {
    if let Some(a) = r.allocs_per_event {
        eprintln!(
            "{:<28} allocs/event {:.2}  bytes/event {:.0}",
            r.scenario,
            a,
            r.alloc_bytes_per_event.unwrap_or(0.0)
        );
        return;
    }
    eprintln!(
        "{:<28} sent {:>9}  delivered>= {:>6.2}%  {:>9.0} ev/s  lat p50 {:>8.2} p99 {:>9.2} p99.9 {:>9.2} ms  rss {:>6} MB",
        r.scenario,
        r.sent,
        r.min_delivered_ratio * 100.0,
        r.min_throughput_eps,
        ms(r.latency.p50_us),
        ms(r.latency.p99_us),
        ms(r.latency.p999_us),
        r.peak_rss_kb / 1024
    );
    if let (Some(idle), Some(loaded)) = (&r.order_idle_latency, &r.order_latency) {
        eprintln!(
            "{:<28} order p99 idle {:.2} ms, loaded {:.2} ms; loaded max {:.2} ms ({} samples)",
            "",
            ms(idle.p99_us),
            ms(loaded.p99_us),
            ms(loaded.max_us),
            loaded.count
        );
    }
    if r.scenario.starts_with("rpc_isolation") {
        for n in &r.notes {
            eprintln!("{:<28} {n}", r.scenario);
        }
        return;
    }
    let (gap_events, gap_skipped) = r.conns.iter().fold((0u64, 0u64), |a, c| {
        (a.0 + c.gap_events, a.1 + c.gap_skipped)
    });
    if gap_events > 0 {
        eprintln!(
            "{:<28} gaps delivered: {gap_events} (reporting {gap_skipped} skipped events)",
            ""
        );
    }
    for n in &r.notes {
        eprintln!("{:<28} note: {n}", "");
    }
    let resumed: u32 = r.conns.iter().map(|c| c.resumed).sum();
    if resumed > 0 {
        eprintln!(
            "{:<28} told of a plant rebuild (TransportResumed): {resumed}",
            ""
        );
    }
    let drops: u32 = r.conns.iter().map(|c| c.reattaches).sum();
    if drops > 0 {
        let outage = r.conns.iter().map(|c| c.max_outage_ms).max().unwrap_or(0);
        eprintln!("{:<28} reattaches {drops}, max outage {outage} ms", "");
    }
}

fn write_out(args: &[String], reports: &[Report]) {
    if let Some(path) = arg_value(args, "--out") {
        let json = serde_json::to_string_pretty(reports).expect("serialize report");
        std::fs::write(&path, json).unwrap_or_else(|e| panic!("write {path}: {e}"));
        eprintln!("report written to {path}");
    }
}

/// Loose floors for CI. They guard the invariants (no loss without a gap, no
/// reordering, orders not held up by a slow read, healthy peers unaffected by a
/// stalled one) and a throughput floor far below what the code does, so a
/// loaded shared runner cannot fail it on latency noise alone.
fn smoke_failures(r: &Report) -> Vec<String> {
    let mut bad = Vec::new();
    let mut check = |ok: bool, what: String| {
        if !ok {
            bad.push(format!("{}: {what}", r.scenario));
        }
    };
    if r.scenario == "rpc_isolation_same_connection" {
        let slow_ms = r.params["slow_secs"].as_f64().unwrap_or(0.0) * 1000.0;
        let place_ms = r.order_latency.as_ref().map_or(f64::MAX, |l| ms(l.max_us));
        check(
            place_ms < slow_ms / 2.0,
            format!("place waited {place_ms:.0} ms behind a {slow_ms:.0} ms read"),
        );
        return bad;
    }
    check(
        r.unsignalled_loss == 0,
        format!("{} events lost without a gap", r.unsignalled_loss),
    );
    check(
        r.conns.iter().all(|c| c.reordered == 0),
        "events delivered out of order".into(),
    );
    check(
        r.min_delivered_ratio >= 0.9,
        format!(
            "slowest reader got {:.1}% of the events",
            r.min_delivered_ratio * 100.0
        ),
    );
    if r.scenario.starts_with("flood_") {
        check(
            r.min_throughput_eps >= 5_000.0,
            format!(
                "throughput {:.0} ev/s below the 5000 floor",
                r.min_throughput_eps
            ),
        );
    }
    bad
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = args.first().map(String::as_str).unwrap_or("help");
    match cmd {
        "list" => {
            for p in scen::suite(false) {
                println!("{}", p.name);
            }
            println!("rpc_isolation_same_connection");
            println!("alloc_publish_1sub");
            println!("alloc_publish_4sub");
        }
        "suite" => {
            let full = args.iter().any(|a| a == "--full");
            let mut reports = Vec::new();
            for p in scen::suite(full) {
                let r = scen::run(p).await;
                print_summary(&r);
                reports.push(r);
            }
            let r = scen::run_rpc_isolation(if full { 8.0 } else { 3.0 }).await;
            print_summary(&r);
            reports.push(r);
            for n in [1, 4] {
                let r = scen::run_alloc(n).await;
                print_summary(&r);
                reports.push(r);
            }
            write_out(&args, &reports);
        }
        "smoke" => {
            // S1 at 10k events/s for 5 s, S2 isolation, S4 stalled consumer.
            let mut reports = Vec::new();
            let mut failures = Vec::new();
            for mut p in scen::suite(false).into_iter().filter(|p| {
                matches!(
                    p.name.as_str(),
                    "flood_10k_4conn_1session" | "stalled_consumer"
                )
            }) {
                if p.name == "stalled_consumer" {
                    p.secs = 4.0;
                    p.drain_secs = 5.0;
                }
                let r = scen::run(p).await;
                print_summary(&r);
                failures.extend(smoke_failures(&r));
                reports.push(r);
            }
            let r = scen::run_rpc_isolation(2.0).await;
            print_summary(&r);
            failures.extend(smoke_failures(&r));
            reports.push(r);
            write_out(&args, &reports);
            if !failures.is_empty() {
                for f in &failures {
                    eprintln!("SMOKE FAILED: {f}");
                }
                std::process::exit(1);
            }
            eprintln!("smoke ok");
        }
        "run" => {
            let name = args.get(1).expect("scenario name (see `list`)");
            let full = args.iter().any(|a| a == "--full");
            let report = if name == "rpc_isolation_same_connection" {
                scen::run_rpc_isolation(3.0).await
            } else if let Some(n) = name.strip_prefix("alloc_publish_") {
                let receivers: usize = n.trim_end_matches("sub").parse().expect("receiver count");
                scen::run_alloc(receivers).await
            } else {
                let mut p = scen::suite(full)
                    .into_iter()
                    .find(|p| &p.name == name)
                    .unwrap_or_else(|| panic!("unknown scenario {name}"));
                if let Some(v) = arg_value(&args, "--rate") {
                    p.rate = v.parse().expect("--rate");
                }
                if let Some(v) = arg_value(&args, "--secs") {
                    p.secs = v.parse().expect("--secs");
                }
                if let Some(v) = arg_value(&args, "--conns") {
                    p.raw = v.parse().expect("--conns");
                }
                scen::run(p).await
            };
            print_summary(&report);
            write_out(&args, &[report]);
        }
        _ => {
            eprintln!("usage: rithmic-load suite [--full] [--out f] | smoke [--out f] | run <scenario> [--rate N] [--secs S] [--conns N] [--out f] | list");
        }
    }
}
