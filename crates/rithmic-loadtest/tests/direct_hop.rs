//! Both routes must drain more than a stream capacity without loss or reordering.
#[test]
fn direct_hop_paths_deliver_every_scripted_event() {
    for path in ["venue", "host"] {
        let dir = tempfile::tempdir().unwrap();
        let report = dir.path().join("report.json");
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_rithmic-load"))
            .args([
                "run",
                &format!("direct_hop_{path}"),
                "--events",
                "70001",
                "--out",
            ])
            .arg(&report)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let reports: serde_json::Value =
            serde_json::from_slice(&std::fs::read(report).unwrap()).unwrap();
        let r = &reports[0];
        assert_eq!(r["scenario"], format!("direct_hop_{path}"));
        assert_eq!(r["sent"], 70_001);
        assert_eq!(r["conns"][0]["received"], 70_001);
        assert_eq!(r["latency"]["count"], 70_001);
        assert_eq!(r["min_delivered_ratio"], 1.0);
        for field in [
            "missing_in_stream",
            "reordered",
            "gap_events",
            "gap_skipped",
        ] {
            assert_eq!(r["conns"][0][field], 0, "{path}: {field}");
        }
        assert_eq!(r["unsignalled_loss"], 0);
        assert!(r["min_throughput_eps"].as_f64().unwrap() > 0.0);
        assert!(r["allocs_per_event"].as_f64().unwrap() > 0.0);
        assert!(r["peak_rss_kb"].as_u64().unwrap() > 0);
        let latency = &r["latency"];
        assert!(latency["p50_us"].as_u64() <= latency["p99_us"].as_u64());
        assert!(latency["p99_us"].as_u64() <= latency["p999_us"].as_u64());
    }
}
