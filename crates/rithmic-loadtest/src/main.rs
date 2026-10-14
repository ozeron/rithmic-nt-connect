//! `rithmic-load`: synthetic load harness for the gateway and session layers.
//!
//! Everything runs in this process against the real server code over a fake
//! plant, so no Rithmic credentials are needed. See
//! `docs/references/ops-runbook.md` ("Load harness") for how to read a report.
//!
//! ```text
//! rithmic-load suite [--full] [--out report.json]
//! rithmic-load run <scenario> [--out report.json]
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
            eprintln!("usage: rithmic-load suite [--full] [--out f] | run <scenario> [--rate N] [--secs S] [--conns N] [--out f] | list");
        }
    }
}
