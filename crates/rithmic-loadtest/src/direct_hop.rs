//! Matched direct-delivery proxy: the same fake plant, with/without the host.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rithmic_host::fake::FakeVenue;
use rithmic_host::{HistoryPolicy, ParentGates, PlantEvent, Session, SessionHost, Stream, Venue};

use crate::alloc;
use crate::metrics::{now_ns, peak_rss_kb, ConnReport, Lat, Report};
use crate::rig::{tick, EXCHANGE, SYMBOL};

const BATCH: usize = 1024;
const TIMEOUT: Duration = Duration::from_secs(10);
const WARMUP: u64 = 8192;

/// Publish at most one batch ahead of the reader. This keeps both paths below
/// every bounded queue's capacity while measuring unpaced completion throughput.
/// The shared driver also gives both paths identical timestamp/accounting cost.
async fn drive<F, Fut>(fake: &FakeVenue, events: u64, lat: &mut Lat, mut take: F) -> ConnReport
where
    F: FnMut() -> Fut,
    Fut: Future<Output = rithmic_host::Result<Vec<PlantEvent>>>,
{
    let mut rep = ConnReport::default();
    for start in (0..events).step_by(BATCH) {
        let end = (start + BATCH as u64).min(events);
        for seq in start..end {
            fake.push_event(Stream::Ticker, tick(seq));
        }
        let deadline = Instant::now() + TIMEOUT;
        while rep.received < end {
            assert!(Instant::now() < deadline, "direct-hop drain timed out");
            let batch = take().await.expect("direct-hop next_events");
            // Take time, before histogram/sequence processing, shared by all
            // events in this returned batch. The clock is monotonic throughout.
            let taken_ns = now_ns();
            assert!(!batch.is_empty(), "direct-hop next_events timed out");
            for event in batch {
                let PlantEvent::LastTrade(lt) = event else {
                    panic!("unexpected direct-hop event: {event:?}");
                };
                assert_eq!(
                    lt.trade_size,
                    Some(rep.received as i32),
                    "direct-hop loss, duplication or reordering"
                );
                lat.record_ns(taken_ns.saturating_sub(lt.ts_event_ns.expect("publish timestamp")));
                rep.received += 1;
            }
        }
    }
    assert_eq!(rep.received, events);
    rep
}

async fn measure<F, Fut>(fake: &FakeVenue, events: u64, take: F) -> Report
where
    F: FnMut() -> Fut,
    Fut: Future<Output = rithmic_host::Result<Vec<PlantEvent>>>,
{
    let mut lat = Lat::new();
    let before = alloc::snapshot();
    let started = Instant::now();
    let mut rep = drive(fake, events, &mut lat, take).await;
    let duration_s = started.elapsed().as_secs_f64();
    let after = alloc::snapshot();
    rep.latency = lat.summary();
    Report {
        params: serde_json::json!({
            "events": events, "batch": BATCH, "max_in_flight": BATCH,
            "timeout_s": TIMEOUT.as_secs(), "warmup_events": WARMUP,
        }),
        duration_s,
        sent: events,
        conns: vec![rep],
        min_delivered_ratio: 1.0,
        min_throughput_eps: events as f64 / duration_s,
        latency: lat.summary(),
        peak_rss_kb: peak_rss_kb(),
        allocs_per_event: Some((after.allocs - before.allocs) as f64 / events as f64),
        alloc_bytes_per_event: Some((after.bytes - before.bytes) as f64 / events as f64),
        notes: vec![
            "Rust-only direct-hop proxy; no Python/FFI, socket, credentials or live plants".into(),
            "unpaced 1024-event bursts, each fully drained before the next; setup and warmup excluded".into(),
            "throughput and process-global allocations include fixture creation, publish, delivery and histogram/sequence accounting".into(),
            "latency: fixture publish timestamp to next_events return, histogram resolution 1 us; RSS is process peak including setup/warmup".into(),
        ],
        ..Default::default()
    }
}

pub async fn run(host_path: bool, events: u64) -> Report {
    assert!(
        events > 0 && events <= i32::MAX as u64,
        "--events must be in 1..=2147483647"
    );
    let fake = Arc::new(FakeVenue::new().with_gates(false, false));
    let mut report = if host_path {
        let host = SessionHost::with_handle(
            Some(rithmic_host::VenueHandle::new(fake.clone())),
            ParentGates {
                trading_enabled: false,
                cancel_all_enabled: false,
            },
            Default::default(),
            HistoryPolicy::RefuseUnderLiveMd,
            false,
            rithmic_host::fanout::DEFAULT_QUEUE_CAP,
        );
        let session = host.attach_owned().expect("attach");
        session.connect().await.expect("host connect");
        session
            .subscribe(SYMBOL, EXCHANGE)
            .await
            .expect("host subscribe");
        drive(&fake, WARMUP, &mut Lat::new(), || {
            session.next_events(Stream::Ticker, BATCH, TIMEOUT)
        })
        .await;
        let report = measure(&fake, events, || {
            session.next_events(Stream::Ticker, BATCH, TIMEOUT)
        })
        .await;
        session.disconnect().await.expect("detach");
        host.shutdown().await;
        report
    } else {
        Venue::connect(fake.as_ref()).await.expect("venue connect");
        Venue::subscribe(fake.as_ref(), SYMBOL, EXCHANGE)
            .await
            .expect("venue subscribe");
        // Wave 5 removed the blanket Session-for-Venue impl. Its next_events
        // implementation simply delegated here; do not resurrect it in the API.
        drive(&fake, WARMUP, &mut Lat::new(), || {
            Venue::next_events(fake.as_ref(), Stream::Ticker, BATCH, TIMEOUT)
        })
        .await;
        let report = measure(&fake, events, || {
            Venue::next_events(fake.as_ref(), Stream::Ticker, BATCH, TIMEOUT)
        })
        .await;
        Venue::disconnect(fake.as_ref())
            .await
            .expect("venue disconnect");
        report
    };
    let path = if host_path { "host" } else { "venue" };
    report.scenario = format!("direct_hop_{path}");
    report.params["path"] = path.into();
    report.conns[0].kind = path.into();
    report
}
