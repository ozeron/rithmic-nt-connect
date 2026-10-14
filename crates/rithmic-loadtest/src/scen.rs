//! Scenario runner: one flexible driver parameterised by [`Params`], plus the
//! allocation microbench and the preset suites.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rithmic_gateway::server::publish_plant_event;
use rithmic_gateway::subscriptions::{FanoutHub, SubKey};
use rithmic_plants::dto::PlantEvent;
use rithmic_plants::{PlaceOrder, Session};
use serde::Serialize;
use tokio::task::JoinHandle;

use crate::alloc;
use crate::metrics::{current_rss_kb, now_ns, peak_rss_kb, ConnReport, Lat, Report};
use crate::rig::{tick, wait_until, Item, RawClient, Rig, EXCHANGE, SYMBOL};

#[derive(Clone, Debug, Serialize)]
pub struct Params {
    pub name: String,
    /// Ticks per second published into the fake plant (0 = none).
    pub rate: u64,
    pub secs: f64,
    /// Independent raw connections that read and measure.
    pub raw: usize,
    /// Library facades (share one socket per process) that poll and measure.
    pub sessions: usize,
    /// Raw connections that subscribe and then never read.
    pub stalled: usize,
    /// Round-trip place/cancel orders on a library facade during the run.
    pub orders: bool,
    /// Make a read RPC on the same socket take this long (0 = none).
    pub slow_rpc_secs: f64,
    /// Drop the plants this many seconds into the flood.
    pub drop_plants_at: Option<f64>,
    pub sample_rss: bool,
    /// Idle period before the flood (order idle baseline).
    pub warmup_secs: f64,
    /// How long to wait for consumers to catch up after the producer stops.
    pub drain_secs: f64,
}

impl Params {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            rate: 0,
            secs: 5.0,
            raw: 1,
            sessions: 0,
            stalled: 0,
            orders: false,
            slow_rpc_secs: 0.0,
            drop_plants_at: None,
            sample_rss: false,
            warmup_secs: 0.0,
            drain_secs: 3.0,
        }
    }
}

struct Counter {
    received: AtomicU64,
}

fn account(rep: &mut ConnReport, lat: &mut Lat, expected: &mut Option<i64>, seq: i32, ts: u64) {
    let s = seq as u32 as i64;
    rep.received += 1;
    match *expected {
        Some(e) if s > e => {
            rep.missing_in_stream += (s - e) as u64;
            *expected = Some(s + 1);
        }
        Some(e) if s < e => rep.reordered += 1,
        _ => *expected = Some(s + 1),
    }
    lat.record_ns(now_ns().saturating_sub(ts));
}

async fn raw_consumer(
    rig: Arc<Rig>,
    stop: Arc<AtomicBool>,
    ready: Arc<AtomicUsize>,
    counter: Arc<Counter>,
) -> (ConnReport, Lat) {
    let mut rep = ConnReport {
        kind: "raw".into(),
        ..Default::default()
    };
    let mut lat = Lat::new();
    let mut expected = None;
    let mut first = true;
    let mut dropped_at: Option<Instant> = None;
    'outer: loop {
        let mut client = loop {
            if stop.load(Ordering::Relaxed) {
                break 'outer;
            }
            if let Ok(mut c) = RawClient::connect(&rig).await {
                if c.subscribe().await.is_ok() {
                    break c;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        if first {
            ready.fetch_add(1, Ordering::SeqCst);
            first = false;
        } else {
            rep.reattaches += 1;
            if let Some(t) = dropped_at.take() {
                rep.max_outage_ms = rep.max_outage_ms.max(t.elapsed().as_millis() as u64);
            }
        }
        loop {
            if stop.load(Ordering::Relaxed) {
                break 'outer;
            }
            match tokio::time::timeout(Duration::from_millis(100), client.next_item()).await {
                Ok(Ok(Some(Item::Tick { seq, created_ns }))) => {
                    account(&mut rep, &mut lat, &mut expected, seq, created_ns);
                    counter.received.store(rep.received, Ordering::Relaxed);
                }
                Ok(Ok(Some(Item::Gap { skipped }))) => {
                    rep.gap_events += 1;
                    rep.gap_skipped += skipped;
                }
                Ok(Ok(None)) | Ok(Err(_)) => {
                    dropped_at = Some(Instant::now());
                    continue 'outer;
                }
                Err(_) => {
                    if stop.load(Ordering::Relaxed) {
                        break 'outer;
                    }
                }
            }
        }
    }
    (rep, lat)
}

async fn session_consumer(
    rig: Arc<Rig>,
    stop: Arc<AtomicBool>,
    ready: Arc<AtomicUsize>,
    counter: Arc<Counter>,
) -> (ConnReport, Lat) {
    let mut rep = ConnReport {
        kind: "session".into(),
        ..Default::default()
    };
    let mut lat = Lat::new();
    let mut expected = None;
    let session = rig.session().await;
    session
        .subscribe(SYMBOL, EXCHANGE)
        .await
        .expect("subscribe");
    ready.fetch_add(1, Ordering::SeqCst);
    while !stop.load(Ordering::Relaxed) {
        match session.poll_event(Duration::from_millis(50)).await {
            Ok(Some(PlantEvent::LastTrade(lt))) => {
                account(
                    &mut rep,
                    &mut lat,
                    &mut expected,
                    lt.trade_size.unwrap_or(0),
                    lt.ts_event_ns.unwrap_or(0),
                );
                counter.received.store(rep.received, Ordering::Relaxed);
            }
            Ok(_) => {}
            Err(rithmic_plants::Error::ChannelLagged { skipped, .. }) => {
                rep.gap_events += 1;
                rep.gap_skipped += skipped;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
        }
    }
    (rep, lat)
}

async fn producer(rig: Arc<Rig>, rate: u64, secs: f64, sent: Arc<AtomicU64>) {
    let start = Instant::now();
    let mut iv = tokio::time::interval(Duration::from_millis(1));
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    loop {
        iv.tick().await;
        let el = start.elapsed().as_secs_f64();
        let target = if el >= secs {
            (rate as f64 * secs) as u64
        } else {
            (rate as f64 * el) as u64
        };
        let done = sent.load(Ordering::Relaxed);
        for seq in done..target {
            rig.push_tick(seq);
        }
        sent.store(target, Ordering::Relaxed);
        if el >= secs {
            return;
        }
    }
}

fn order(tag: &str) -> PlaceOrder {
    PlaceOrder {
        symbol: SYMBOL.into(),
        exchange: EXCHANGE.into(),
        side: "BUY".into(),
        price_type: "LIMIT".into(),
        quantity: 1,
        user_tag: tag.into(),
        price: Some(100.0),
        trigger_price: None,
        duration: "DAY".into(),
        trail_by_ticks: None,
        trail_by_price_id: None,
    }
}

/// Open-loop order flow: a place+cancel pair starts every 20 ms regardless of
/// whether earlier pairs finished, so a stalled socket shows up as many slow
/// operations (a sequential loop would hide it behind one stuck call).
async fn order_task(
    rig: Arc<Rig>,
    stop: Arc<AtomicBool>,
    phase: Arc<AtomicU8>,
    idle: Arc<Mutex<Lat>>,
    loaded: Arc<Mutex<Lat>>,
) {
    let session = Arc::new(rig.session().await);
    let mut n = 0u64;
    let mut inflight = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        n += 1;
        let (session, idle, loaded) = (session.clone(), idle.clone(), loaded.clone());
        let started_in_idle = phase.load(Ordering::Relaxed) == 0;
        inflight.push(tokio::spawn(async move {
            let t = Instant::now();
            let placed = session.place_order(&order(&format!("t{n}"))).await;
            let place_ns = t.elapsed().as_nanos() as u64;
            let t = Instant::now();
            let cancelled = session.cancel_order("B1").await;
            let cancel_ns = t.elapsed().as_nanos() as u64;
            if placed.is_ok() && cancelled.is_ok() {
                let target = if started_in_idle { &idle } else { &loaded };
                let mut h = target.lock().unwrap();
                h.record_ns(place_ns);
                h.record_ns(cancel_ns);
            }
        }));
        inflight.retain(|h| !h.is_finished());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for h in inflight {
        let _ = tokio::time::timeout(Duration::from_secs(15), h).await;
    }
}

pub async fn run(p: Params) -> Report {
    let rig = Arc::new(Rig::start(true).await);
    let stop = Arc::new(AtomicBool::new(false));
    let ready = Arc::new(AtomicUsize::new(0));
    let sent = Arc::new(AtomicU64::new(0));

    let mut counters = Vec::new();
    let mut readers: Vec<JoinHandle<(ConnReport, Lat)>> = Vec::new();
    for _ in 0..p.raw {
        let c = Arc::new(Counter {
            received: AtomicU64::new(0),
        });
        counters.push(c.clone());
        readers.push(tokio::spawn(raw_consumer(
            rig.clone(),
            stop.clone(),
            ready.clone(),
            c,
        )));
    }
    for _ in 0..p.sessions {
        let c = Arc::new(Counter {
            received: AtomicU64::new(0),
        });
        counters.push(c.clone());
        readers.push(tokio::spawn(session_consumer(
            rig.clone(),
            stop.clone(),
            ready.clone(),
            c,
        )));
    }
    let mut stalled = Vec::new();
    for _ in 0..p.stalled {
        let mut c = RawClient::connect(&rig).await.expect("stalled connect");
        c.subscribe().await.expect("stalled subscribe");
        stalled.push(c);
    }
    let want = p.raw + p.sessions;
    wait_until(Duration::from_secs(10), || {
        ready.load(Ordering::SeqCst) >= want
    })
    .await;

    let started = Instant::now();
    let phase = Arc::new(AtomicU8::new(0));
    let idle = Arc::new(Mutex::new(Lat::new()));
    let loaded = Arc::new(Mutex::new(Lat::new()));
    let order_handle = p.orders.then(|| {
        tokio::spawn(order_task(
            rig.clone(),
            stop.clone(),
            phase.clone(),
            idle.clone(),
            loaded.clone(),
        ))
    });

    let rss = Arc::new(Mutex::new(Vec::<u64>::new()));
    let rss_stop = Arc::new(AtomicBool::new(false));
    let rss_handle = p.sample_rss.then(|| {
        let (rss, rss_stop) = (rss.clone(), rss_stop.clone());
        tokio::spawn(async move {
            while !rss_stop.load(Ordering::Relaxed) {
                rss.lock().unwrap().push(current_rss_kb());
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    });

    if p.warmup_secs > 0.0 {
        tokio::time::sleep(Duration::from_secs_f64(p.warmup_secs)).await;
    }
    phase.store(1, Ordering::Relaxed);

    let slow_note = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut aux = Vec::new();
    if p.slow_rpc_secs > 0.0 {
        rig.fake.set_delay(
            "load_fill_history",
            Duration::from_secs_f64(p.slow_rpc_secs),
        );
        let rig2 = rig.clone();
        let slow_note = slow_note.clone();
        aux.push(tokio::spawn(async move {
            let s = rig2.session().await;
            let t = Instant::now();
            let res = s.load_fill_history(0, 0).await;
            slow_note.lock().unwrap().push(format!(
                "slow RPC load_fill_history finished in {:.2}s: {}",
                t.elapsed().as_secs_f64(),
                match res {
                    Ok(rows) => format!("ok ({} rows)", rows.len()),
                    Err(e) => format!("error: {e}"),
                }
            ));
        }));
    }
    if let Some(at) = p.drop_plants_at {
        let rig2 = rig.clone();
        aux.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs_f64(at)).await;
            rig2.fake.drop_plants();
        }));
    }

    if p.rate > 0 {
        producer(rig.clone(), p.rate, p.secs, sent.clone()).await;
    } else {
        tokio::time::sleep(Duration::from_secs_f64(p.secs)).await;
    }
    let total = sent.load(Ordering::Relaxed);
    wait_until(Duration::from_secs_f64(p.drain_secs), || {
        counters
            .iter()
            .all(|c| c.received.load(Ordering::Relaxed) >= total)
    })
    .await;

    let mut dropped = 0usize;
    for mut c in stalled {
        let end = Instant::now() + Duration::from_secs(2);
        let mut gone = false;
        while Instant::now() < end {
            match tokio::time::timeout(Duration::from_millis(200), c.next_item()).await {
                Ok(Ok(Some(_))) => {}
                Ok(_) => {
                    gone = true;
                    break;
                }
                Err(_) => {}
            }
        }
        dropped += usize::from(gone);
    }

    stop.store(true, Ordering::Relaxed);
    rss_stop.store(true, Ordering::Relaxed);
    let mut conns = Vec::new();
    let mut lat = Lat::new();
    for h in readers {
        let (rep, l) = h.await.expect("consumer");
        lat.merge(&l);
        conns.push(ConnReport {
            latency: l.summary(),
            ..rep
        });
    }
    if let Some(h) = order_handle {
        let _ = h.await;
    }
    if let Some(h) = rss_handle {
        let _ = h.await;
    }
    for h in aux {
        h.abort();
    }

    let mut notes = slow_note.lock().unwrap().clone();
    if p.stalled > 0 {
        notes.push(format!(
            "{dropped}/{} stalled connections were dropped by the parent",
            p.stalled
        ));
    }
    let ratios: Vec<f64> = conns
        .iter()
        .map(|c| {
            if total == 0 {
                1.0
            } else {
                c.received as f64 / total as f64
            }
        })
        .collect();
    let min_ratio = ratios.iter().copied().fold(f64::INFINITY, f64::min);
    let min_eps = conns
        .iter()
        .map(|c| c.received as f64 / p.secs.max(0.001))
        .fold(f64::INFINITY, f64::min);
    // Loss that nothing told the consumer about: a hole in the sequence that no
    // gap accounts for, on a connection that never had to reattach (a reattach
    // is itself the signal for events missed while disconnected).
    let unsignalled: u64 = conns
        .iter()
        .filter(|c| c.reattaches == 0)
        .map(|c| c.missing_in_stream.saturating_sub(c.gap_skipped))
        .sum();
    if unsignalled > 0 {
        notes.push(format!(
            "UNSIGNALLED LOSS: {unsignalled} events missing with no gap"
        ));
    }
    let rss_samples = rss.lock().unwrap().clone();
    Report {
        scenario: p.name.clone(),
        params: serde_json::to_value(&p).unwrap_or_default(),
        duration_s: started.elapsed().as_secs_f64(),
        sent: total,
        conns,
        min_delivered_ratio: if min_ratio.is_finite() {
            min_ratio
        } else {
            0.0
        },
        min_throughput_eps: if min_eps.is_finite() { min_eps } else { 0.0 },
        latency: lat.summary(),
        order_latency: p.orders.then(|| loaded.lock().unwrap().summary()),
        order_idle_latency: p.orders.then(|| idle.lock().unwrap().summary()),
        peak_rss_kb: peak_rss_kb(),
        rss_samples_kb: rss_samples,
        allocs_per_event: None,
        alloc_bytes_per_event: None,
        unsignalled_loss: unsignalled,
        notes,
    }
}

/// Head-of-line probe on ONE raw connection: a slow read RPC followed by a
/// place_order. If the server dispatches serially, the place reply waits for the
/// slow reply.
pub async fn run_rpc_isolation(slow_secs: f64) -> Report {
    use rithmic_gateway::pb::{frame::Body, LoadFillHistoryRequest, PlaceOrderRequest};
    let rig = Rig::start(true).await;
    rig.fake
        .set_delay("load_fill_history", Duration::from_secs_f64(slow_secs));
    let mut c = RawClient::connect(&rig).await.expect("connect");
    let t0 = Instant::now();
    c.send(
        10,
        Body::LoadFillHistory(LoadFillHistoryRequest {
            start_date: 0,
            finish_date: 0,
        }),
    )
    .await
    .expect("send slow");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let t_place = Instant::now();
    c.send(
        11,
        Body::PlaceOrder(PlaceOrderRequest {
            symbol: SYMBOL.into(),
            exchange: EXCHANGE.into(),
            side: "BUY".into(),
            price_type: "LIMIT".into(),
            quantity: 1,
            user_tag: "probe".into(),
            price: Some(100.0),
            duration: "DAY".into(),
            ..Default::default()
        }),
    )
    .await
    .expect("send place");
    let replies = c.replies(&[10, 11]).await.expect("replies");
    let at = |id: u64| {
        replies
            .iter()
            .find(|(i, _, _)| *i == id)
            .map(|(_, f, t)| (f.clone(), t.duration_since(t0).as_secs_f64()))
            .expect("reply")
    };
    let (place, place_at_s) = at(11);
    let (slow, slow_at_s) = at(10);
    let place_ms =
        t_place.elapsed().as_secs_f64().min(place_at_s) * 0.0 + (place_at_s - 0.1) * 1000.0;
    let place_ok = matches!(place.body, Some(Body::Ack(_)));
    let slow_s = slow_at_s;
    let order_of_arrival: Vec<u64> = replies.iter().map(|(i, _, _)| *i).collect();
    let mut lat = Lat::new();
    lat.record_ns((place_ms * 1_000_000.0) as u64);
    Report {
        scenario: "rpc_isolation_same_connection".into(),
        params: serde_json::json!({ "slow_secs": slow_secs }),
        duration_s: slow_s,
        order_latency: Some(lat.summary()),
        peak_rss_kb: peak_rss_kb(),
        notes: vec![
            format!(
                "place_order behind a {slow_secs}s read RPC on one connection replied in {place_ms:.1} ms (ack: {place_ok})"
            ),
            format!(
                "arrival order {order_of_arrival:?} (10 = slow read RPC, 11 = place); slow reply at {slow_s:.2}s: {:?}",
                slow.body.map(|b| format!("{b:?}").chars().take(30).collect::<String>())
            ),
        ],
        ..Default::default()
    }
}

/// Allocations per published event on the server path (convert + encode + hub
/// publish), with `receivers` attached but not reading.
pub async fn run_alloc(receivers: usize) -> Report {
    let hub = Arc::new(FanoutHub::new(1024));
    let key = SubKey {
        symbol: SYMBOL.into(),
        exchange: EXCHANGE.into(),
    };
    hub.add_interest(key.clone()).await;
    let mut rxs = Vec::new();
    for _ in 0..receivers {
        rxs.push(hub.subscribe_receiver(&key).await.expect("receiver"));
    }
    let (warm, n) = (1_000usize, 100_000usize);
    let mut events: Vec<PlantEvent> = (0..(warm + n) as u64).map(tick).collect();
    for _ in 0..warm {
        publish_plant_event(&hub, events.pop().unwrap()).await;
    }
    let before = alloc::snapshot();
    for _ in 0..n {
        publish_plant_event(&hub, events.pop().unwrap()).await;
    }
    let after = alloc::snapshot();
    Report {
        scenario: format!("alloc_publish_{receivers}sub"),
        params: serde_json::json!({ "receivers": receivers, "events": n }),
        sent: n as u64,
        peak_rss_kb: peak_rss_kb(),
        allocs_per_event: Some((after.allocs - before.allocs) as f64 / n as f64),
        alloc_bytes_per_event: Some((after.bytes - before.bytes) as f64 / n as f64),
        ..Default::default()
    }
}

/// Preset suites. `quick` is the CI smoke and the default baseline; `full`
/// uses the durations from the spec (30 s sustained, 10 minute soak).
pub fn suite(full: bool) -> Vec<Params> {
    let sustained = if full { 30.0 } else { 8.0 };
    let soak = if full { 600.0 } else { 45.0 };
    let mut v = Vec::new();
    let mut add = |mut p: Params, f: &dyn Fn(&mut Params)| {
        f(&mut p);
        v.push(p);
    };
    add(Params::new("idle_wake"), &|p| {
        p.rate = 10;
        p.secs = 6.0;
        p.raw = 1;
        p.drain_secs = 1.0;
    });
    add(Params::new("flood_1k_1conn"), &|p| {
        p.rate = 1_000;
        p.secs = 5.0;
    });
    add(Params::new("flood_10k_4conn_1session"), &|p| {
        p.rate = 10_000;
        p.secs = 5.0;
        p.raw = 4;
        p.sessions = 1;
    });
    add(Params::new("flood_50k_4conn"), &|p| {
        p.rate = 50_000;
        p.secs = sustained;
        p.raw = 4;
        p.drain_secs = 10.0;
    });
    add(Params::new("burst_200k_1s"), &|p| {
        p.rate = 200_000;
        p.secs = 1.0;
        p.raw = 4;
        p.drain_secs = 15.0;
    });
    add(Params::new("latency_20k"), &|p| {
        p.rate = 20_000;
        p.secs = sustained;
        p.raw = 1;
        p.sessions = 1;
        p.drain_secs = 10.0;
    });
    add(Params::new("orders_flood_slow_rpc"), &|p| {
        p.rate = 50_000;
        p.secs = 12.0;
        p.raw = 2;
        p.sessions = 1;
        p.orders = true;
        p.slow_rpc_secs = 8.0;
        p.warmup_secs = 2.0;
        p.drain_secs = 10.0;
    });
    add(Params::new("stalled_consumer"), &|p| {
        p.rate = 20_000;
        p.secs = 8.0;
        p.raw = 3;
        p.stalled = 1;
        p.drain_secs = 8.0;
    });
    add(Params::new("reconnect_during_flood"), &|p| {
        p.rate = 10_000;
        p.secs = 8.0;
        p.raw = 2;
        p.drop_plants_at = Some(3.0);
        p.drain_secs = 8.0;
    });
    add(Params::new("soak"), &|p| {
        p.rate = 10_000;
        p.secs = soak;
        p.raw = 1;
        p.sample_rss = true;
        p.drain_secs = 10.0;
    });
    v
}
