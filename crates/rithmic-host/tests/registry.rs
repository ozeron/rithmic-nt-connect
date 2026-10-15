use std::sync::Arc;

use rithmic_host::{
    fake::FakeVenue, HostFingerprint, HostOptions, HostRegistry, Session, SessionLock,
};

#[tokio::test]
async fn registry_shares_login_but_not_intents_and_releases_the_last_lock() {
    let dir = tempfile::tempdir().unwrap();
    let registry = HostRegistry::<FakeVenue>::default();
    let key = HostFingerprint::new("registry-test", "system", "wss://fake", "Test");
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
    let a = host.attach_owned().unwrap();
    let same = registry
        .get_or_start(
            key.clone(),
            || panic!("second venue"),
            dir.path(),
            HostOptions::default(),
        )
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&host, &same));
    let b = same.attach_owned().unwrap();
    let (x, y) = tokio::join!(a.connect(), b.connect());
    x.unwrap();
    y.unwrap();
    assert_eq!(fake.count("connect"), 1);
    a.subscribe("NQ", "CME").await.unwrap();
    b.subscribe("NQ", "CME").await.unwrap();
    a.disconnect().await.unwrap();
    assert_eq!(fake.count("unsubscribe NQ CME"), 0);
    assert!(SessionLock::try_acquire_in(dir.path(), &key).is_err());
    b.disconnect().await.unwrap();
    assert_eq!(fake.count("disconnect"), 1);
    let _lock = SessionLock::try_acquire_in(dir.path(), &key).unwrap();
}

#[tokio::test]
async fn flock_is_taken_before_constructing_the_venue_and_gates_are_root_owned() {
    let dir = tempfile::tempdir().unwrap();
    let key = HostFingerprint::new("held", "system", "wss://fake", "Test");
    let held = SessionLock::try_acquire_in(dir.path(), &key).unwrap();
    let registry = HostRegistry::<FakeVenue>::default();
    assert!(registry
        .get_or_start(
            key.clone(),
            || panic!("must not construct a venue"),
            dir.path(),
            HostOptions::default()
        )
        .await
        .is_err());
    drop(held);
    let host = registry
        .get_or_start(
            key,
            || Arc::new(FakeVenue::new()),
            dir.path(),
            HostOptions::default(),
        )
        .await
        .unwrap();
    let direct = host.attach_owned().unwrap();
    let server = host.attach();
    assert!(!server.open_gates(true, true).await.trading);
    assert!(!server.open_gates(false, false).await.cancel_all);
    let gates = direct.open_gates(false, true).await;
    assert!(!gates.trading && gates.cancel_all);
    let gates = direct.open_gates(true, false).await;
    assert!(gates.trading && gates.cancel_all);
    assert_eq!(direct.open_gates(false, false).await, gates);
    assert_eq!(server.open_gates(false, false).await, gates);
    direct.disconnect().await.unwrap();
    server.disconnect().await.unwrap();
}

#[tokio::test]
async fn exhausted_hosts_are_replaced_even_while_old_attachments_are_alive() {
    let dir = tempfile::tempdir().unwrap();
    let key = HostFingerprint::new("terminal", "system", "wss://fake", "Test");
    let registry = HostRegistry::<FakeVenue>::default();
    let old = registry
        .get_or_start(
            key.clone(),
            || Arc::new(FakeVenue::new()),
            dir.path(),
            HostOptions::default(),
        )
        .await
        .unwrap();
    let old_session = old.attach_owned().unwrap();
    old_session.connect().await.unwrap();
    // The recovery suite proves exhaustion sets this terminal state. Here the
    // registry must replace it without waiting for the old attachment's Drop.
    old.state
        .failed
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let new = registry
        .get_or_start(
            key,
            || Arc::new(FakeVenue::new()),
            dir.path(),
            HostOptions::default(),
        )
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&old, &new));
    assert!(old_session.connect().await.is_err());
    let new_session = new.attach_owned().unwrap();
    new_session.connect().await.unwrap();
    old_session.disconnect().await.unwrap();
    new_session.disconnect().await.unwrap();
}

#[tokio::test]
async fn attaching_to_a_closed_incarnation_refuses_and_the_registry_starts_a_new_one() {
    let dir = tempfile::tempdir().unwrap();
    let key = HostFingerprint::new("closing", "system", "wss://fake", "Test");
    let registry = HostRegistry::<FakeVenue>::default();
    let old = registry
        .get_or_start(
            key.clone(),
            || Arc::new(FakeVenue::new()),
            dir.path(),
            HostOptions::default(),
        )
        .await
        .unwrap();
    let session = old.attach_owned().unwrap();
    session.connect().await.unwrap();
    session.disconnect().await.unwrap();
    assert!(old.attach_owned().is_err());
    let new = registry
        .get_or_start(
            key,
            || Arc::new(FakeVenue::new()),
            dir.path(),
            HostOptions::default(),
        )
        .await
        .unwrap();
    let session = new.attach_owned().unwrap();
    session.connect().await.unwrap();
    session.disconnect().await.unwrap();
}

#[tokio::test]
async fn execution_plant_union_is_effective_before_and_after_connect_in_the_fake() {
    use rithmic_host::{AccountTriple, PlantEvent, PlantSet, Stream, Venue};
    use std::time::Duration;
    for before_connect in [true, false] {
        let fake = FakeVenue::new();
        fake.set_account(AccountTriple {
            account_id: "A1".into(),
            fcm_id: "F1".into(),
            ib_id: "I1".into(),
        });
        if before_connect {
            fake.request_plants(PlantSet::EXECUTION).await.unwrap();
        }
        fake.connect().await.unwrap();
        if !before_connect {
            fake.request_plants(PlantSet::EXECUTION).await.unwrap();
        }
        // A smaller later request must not remove the PnL plant.
        fake.request_plants(PlantSet::MARKET_DATA).await.unwrap();
        let event = PlantEvent::Other {
            type_name: "pnl-proof".into(),
            source: "pnl".into(),
        };
        fake.push_event(Stream::Pnl, event.clone());
        assert_eq!(
            fake.poll_pnl_event(Duration::ZERO).await.unwrap(),
            Some(event)
        );
        assert_eq!(fake.count("connect"), 1);
        fake.disconnect().await.unwrap();
    }
}
