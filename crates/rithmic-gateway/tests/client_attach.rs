//! Rust gateway client: attach-only dialing, handshake, no password on the wire.
//! (Ports of the Python `test_gateway_attach` / client handshake behaviors.)

mod common;

use std::time::{Duration, Instant};

use common::*;
use rithmic_gateway::client::{GatewayConfig, GatewaySession};
use rithmic_gateway::pb::frame::Body;
use rithmic_gateway::singleton::SessionLock;
use rithmic_host::{Error, Session};

fn config_at(user: &str, path: &std::path::Path) -> GatewayConfig {
    GatewayConfig::new(user, SYSTEM, URL, "Live")
        .with_listen(&format!("unix://{}", path.display()))
        .expect("listen")
}

fn gateway_code(err: &Error) -> &str {
    match err {
        Error::Gateway { code, .. } => code,
        other => panic!("expected a gateway error, got {other:?}"),
    }
}

#[tokio::test]
async fn no_parent_and_no_flock_is_parent_not_running_and_starts_nothing() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let session = GatewaySession::new(config_at(&unique_user("none"), &path));

    let err = session.connect().await.expect_err("no parent");
    assert_eq!(gateway_code(&err), "parent_not_running");
    let message = err.to_string();
    assert!(message.contains(path.to_str().unwrap()), "{message}");
    assert!(message.contains("python -m rithmic_gateway"), "{message}");
    assert!(!path.exists(), "the client must not create the socket");
}

#[tokio::test]
async fn flock_held_and_socket_appears_attaches() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let user = unique_user("late");
    let _lock = SessionLock::try_acquire(&user, SYSTEM, URL, "Live").expect("flock");

    let late_path = path.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let mock =
            MockParent::spawn(&late_path, mock_ready(false, false), 0, |_, _| Action::Ack).await;
        tokio::time::sleep(Duration::from_secs(30)).await;
        drop(mock);
    });

    let session = GatewaySession::new(config_at(&user, &path));
    session
        .connect()
        .await
        .expect("attach to the starting parent");
    assert!(session.transport_connected());
    session.disconnect().await.unwrap();
}

#[tokio::test]
async fn flock_released_while_waiting_fails_fast_without_the_full_timeout() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let user = unique_user("gone");
    let lock = SessionLock::try_acquire(&user, SYSTEM, URL, "Live").expect("flock");
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(lock);
    });

    let mut cfg = config_at(&user, &path);
    cfg.attach_timeout = Duration::from_secs(20);
    let started = Instant::now();
    let err = GatewaySession::new(cfg)
        .connect()
        .await
        .expect_err("no socket");
    assert_eq!(gateway_code(&err), "parent_flock_held_no_socket");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "did not wait the full timeout"
    );
}

#[tokio::test]
async fn flock_held_but_wrong_listen_path_is_listen_path_mismatch() {
    let dir = short_dir();
    let path = dir.path().join("nothing-listens-here.sock");
    let user = unique_user("wrongpath");
    let _lock = SessionLock::try_acquire(&user, SYSTEM, URL, "Live").expect("flock");

    let mut cfg = config_at(&user, &path);
    cfg.attach_timeout = Duration::from_millis(300);
    let err = GatewaySession::new(cfg)
        .connect()
        .await
        .expect_err("mismatch");
    assert_eq!(gateway_code(&err), "listen_path_mismatch");
    assert!(err.to_string().contains(path.to_str().unwrap()));
}

#[tokio::test]
async fn a_dialable_socket_without_the_credential_flock_is_refused_as_an_impostor() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let _mock = MockParent::spawn(&path, mock_ready(true, true), 0, |_, _| Action::Ack).await;

    // attest_flock stays on (the default): nobody holds the flock for this user.
    let session = GatewaySession::new(config_at(&unique_user("impostor"), &path));
    let err = session.connect().await.expect_err("impostor");
    assert_eq!(gateway_code(&err), "parent_unattested");
}

#[tokio::test]
async fn tcp_and_tls_listen_urls_are_not_implemented() {
    for url in ["tcp://127.0.0.1:7600", "tls://gw.example:7600"] {
        let err = GatewayConfig::new("u", SYSTEM, URL, "Live")
            .with_listen(url)
            .expect_err("remote transport");
        assert!(err.to_string().contains("not implemented"), "{err}");
    }
}

#[tokio::test]
async fn handshake_carries_identity_and_never_a_password() {
    // SAFETY: tests in this binary do not read this variable concurrently.
    std::env::set_var("RITHMIC_PASSWORD", "sentinel-pw-7f3a91");
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let mock = MockParent::spawn(&path, mock_ready(false, false), 0, |_, _| Action::Ack).await;
    let cfg = mock_config(&path).with_auth_token("tok-123");
    let user = cfg.user.clone();
    let session = GatewaySession::new(cfg);
    session.connect().await.expect("connect");
    session.subscribe(NQ, CME).await.expect("rpc");

    let hello = mock.handshakes.lock().unwrap()[0].clone();
    let frame = {
        use prost::Message;
        rithmic_gateway::pb::Frame::decode(hello.as_slice()).expect("decode")
    };
    let Some(Body::Handshake(hs)) = frame.body else {
        panic!("first frame must be a handshake");
    };
    assert_eq!(hs.user, user);
    assert_eq!(hs.system_name, SYSTEM);
    assert_eq!(hs.auth_token, "tok-123");

    let needle = b"sentinel-pw-7f3a91";
    let all_frames: Vec<Vec<u8>> = {
        use prost::Message;
        let mut v = vec![hello];
        v.extend(
            mock.requests
                .lock()
                .unwrap()
                .iter()
                .map(|f| f.encode_to_vec()),
        );
        v
    };
    for raw in all_frames {
        assert!(
            !raw.windows(needle.len()).any(|w| w == needle),
            "password on the wire"
        );
    }
    assert!(!format!("{:?}", config_at("u", &path)).contains("pw"));
    session.disconnect().await.unwrap();
}

#[tokio::test]
async fn a_config_never_prints_its_auth_token() {
    let cfg = GatewayConfig::new("u", SYSTEM, URL, "Live").with_auth_token("super-secret-token");
    assert!(!format!("{cfg:?}").contains("super-secret-token"));
}

#[tokio::test]
async fn ready_scopes_and_gates_are_exposed_read_only() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let _mock = MockParent::spawn(&path, mock_ready(true, false), 0, |_, _| Action::Ack).await;
    let session = GatewaySession::new(mock_config(&path));
    session.connect().await.expect("connect");
    assert!(session.trading_enabled());
    assert!(!session.cancel_all_enabled());
    assert_eq!(session.scopes(), ["md", "history"]);
    assert_eq!(session.gateway_instance_id(), 7);
    assert_eq!(session.transport_generation(), 1);
    session.disconnect().await.unwrap();
}

#[tokio::test]
async fn not_ready_is_retried_until_the_parent_is_ready() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let mock = MockParent::spawn(&path, mock_ready(false, false), 2, |_, _| Action::Ack).await;
    let session = GatewaySession::new(mock_config(&path));
    session.connect().await.expect("ready after retries");
    assert_eq!(
        mock.connections.load(std::sync::atomic::Ordering::SeqCst),
        3
    );
    session.disconnect().await.unwrap();
}

#[tokio::test]
async fn rpcs_before_connect_are_not_connected_errors() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let session = GatewaySession::new(config_at(&unique_user("early"), &path));
    let err = session.subscribe(NQ, CME).await.expect_err("not connected");
    assert!(matches!(err, Error::NotConnected { .. }), "{err:?}");
    let err = session
        .poll_event(Duration::ZERO)
        .await
        .expect_err("not connected");
    assert!(matches!(err, Error::NotConnected { .. }), "{err:?}");
}

#[tokio::test]
async fn a_failed_connect_leaves_the_session_reusable() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let user = unique_user("retry");
    let session = GatewaySession::new(config_at(&user, &path));
    assert!(session.connect().await.is_err());
    assert!(matches!(
        session.subscribe(NQ, CME).await,
        Err(Error::NotConnected { .. })
    ));

    // A parent comes up (holding the flock for this fingerprint): the same
    // session connects. Nothing from the failed attempt is left half-open.
    let _lock = SessionLock::try_acquire(&user, SYSTEM, URL, "Live").expect("flock");
    let _mock = MockParent::spawn(&path, mock_ready(false, false), 0, |_, _| Action::Ack).await;
    session
        .connect()
        .await
        .expect("connect after the parent came up");
    session.disconnect().await.unwrap();
}

#[tokio::test]
async fn connecting_twice_is_an_already_connected_error() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let _mock = MockParent::spawn(&path, mock_ready(false, false), 0, |_, _| Action::Ack).await;
    let session = GatewaySession::new(mock_config(&path));
    session.connect().await.expect("first connect");
    assert!(matches!(
        session.connect().await,
        Err(Error::AlreadyConnected)
    ));
    session.disconnect().await.unwrap();
}

#[test]
fn the_client_library_has_no_spawn_or_binary_resolution_path() {
    // The client only attaches. Nothing under src/client may start a process or
    // look for a gateway binary (the launcher in the Python package owns that).
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/client");
    // The client reads no environment at all (no password, no binary path) and
    // has no password field. Message text may *mention* the parent's password.
    let forbidden = [
        "std::process",
        "tokio::process",
        "Command::new",
        ".spawn()",
        "env::var",
        "password:",
        ".password",
    ];
    for entry in std::fs::read_dir(&dir).expect("src/client") {
        let path = entry.expect("entry").path();
        if path.extension().is_none_or(|e| e != "rs") {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("read");
        // Comments may mention these words; code may not.
        for line in text.lines().filter(|l| !l.trim_start().starts_with("//")) {
            for word in forbidden {
                assert!(
                    !line.contains(word),
                    "{} must not contain `{word}`: {line}",
                    path.display()
                );
            }
        }
    }
}
