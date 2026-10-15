use super::*;
use crate::fake::{FakeFailure, FakeVenue};
use crate::{Session, SessionEvent, Stream};
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn exhaustion_tears_down_the_host_and_releases_its_registry_and_flock() {
    let dir = tempfile::tempdir().unwrap();
    let registry = HostRegistry::<FakeVenue>::default();
    let key = HostFingerprint::new("exhausted", "Fake", "wss://fake", "Test");
    let fake = Arc::new(FakeVenue::new());
    let old = registry
        .get_or_start(
            key.clone(),
            || fake.clone(),
            dir.path(),
            HostOptions::default(),
        )
        .await
        .unwrap();
    let session = old.attach_owned().unwrap();
    session.connect().await.unwrap();
    session.subscribe("NQU6", "CME").await.unwrap();
    let mut events = session.session_events();
    for _ in 0..3 {
        fake.fail_next("reset_ticker", FakeFailure::Config("offline".into()));
    }
    fake.fail_stream(Stream::Ticker, "ticker failed; history still connected");
    assert!(matches!(
        events.recv().await.unwrap(),
        SessionEvent::Interrupted { .. }
    ));
    assert!(matches!(
        events.recv().await.unwrap(),
        SessionEvent::RecoveryFailed { attempts: 3, .. }
    ));
    // Bound scheduler turns: a missing background cleanup must fail, not hang
    // under paused time while these facades deliberately keep the host alive.
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        fake.count("disconnect"),
        1,
        "terminal host kept sibling plants connected"
    );
    let lock = crate::SessionLock::try_acquire_in(dir.path(), &key)
        .expect("terminal host kept the credential flock");
    assert!(
        !registry.hosts.lock().await.contains_key(&key),
        "terminal host remains registered"
    );
    assert!(session.connect().await.is_err());
    assert!(session
        .request_plants(crate::PlantSet::EXECUTION)
        .await
        .is_err());
    assert!(session.subscribe("NQU6", "CME").await.is_err());
    assert!(old.attach_owned().is_err());
    drop(lock);
    let fresh = Arc::new(FakeVenue::new());
    let new = registry
        .get_or_start(
            key.clone(),
            || fresh.clone(),
            dir.path(),
            HostOptions::default(),
        )
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&old, &new));
    let replacement = new.attach_owned().unwrap();
    replacement.connect().await.unwrap();
    assert_eq!(fresh.count("connect"), 1);
    assert!(session.connect().await.is_err());
    // An old attachment's late detach must not evict the new incarnation.
    session.disconnect().await.unwrap();
    replacement.subscribe("NQU6", "CME").await.unwrap();
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert!(registry
        .hosts
        .lock()
        .await
        .get(&key)
        .unwrap()
        .ptr_eq(&Arc::downgrade(&new)));
    replacement.disconnect().await.unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
}

#[tokio::test(start_paused = true)]
async fn terminal_connect_rejects_without_waiting_for_slow_venue_teardown() {
    let dir = tempfile::tempdir().unwrap();
    let registry = HostRegistry::<FakeVenue>::default();
    let key = HostFingerprint::new("slow-terminal", "Fake", "wss://fake", "Test");
    let fake = Arc::new(FakeVenue::new());
    let host = registry
        .get_or_start(
            key.clone(),
            || fake.clone(),
            dir.path(),
            HostOptions::default(),
        )
        .await
        .unwrap();
    let session = host.attach_owned().unwrap();
    session.connect().await.unwrap();
    fake.set_delay("disconnect", Duration::from_secs(10));
    let mut events = session.session_events();
    for _ in 0..3 {
        fake.fail_next("reset_ticker", FakeFailure::Config("offline".into()));
    }
    fake.fail_stream(Stream::Ticker, "lost");
    assert!(matches!(
        events.recv().await.unwrap(),
        SessionEvent::Interrupted { .. }
    ));
    assert!(matches!(
        events.recv().await.unwrap(),
        SessionEvent::RecoveryFailed { .. }
    ));
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        fake.count("disconnect"),
        0,
        "teardown is still waiting for the venue"
    );
    assert!(crate::SessionLock::try_acquire_in(dir.path(), &key).is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(1), session.connect())
            .await
            .expect("terminal connect waited for venue teardown")
            .is_err()
    );
    tokio::time::advance(Duration::from_secs(10)).await;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(fake.count("disconnect"), 1);
    let _lock = crate::SessionLock::try_acquire_in(dir.path(), &key).unwrap();
    assert!(!registry.hosts.lock().await.contains_key(&key));
}
