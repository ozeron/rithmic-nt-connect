use super::*;
use rithmic_plants::fake_server::{FakeRithmicServer, Plant};

#[test]
fn direct_constructor_honors_execution_plants_without_constructor_io() {
    Python::initialize();
    for existing_host in [false, true] {
        let server = runtime().block_on(FakeRithmicServer::start());
        server.front_month("ES", "CME", "ESZ6");
        let construct = |plants| {
            Python::attach(|py| {
                PySession::new(
                    py,
                    "fake_user".into(),
                    Some("fake_password".into()),
                    "Rithmic Test",
                    &server.url(),
                    "rithmic-nt-connect",
                    "0.1.0",
                    "Test",
                    Some("ACCOUNT".into()),
                    Some("FCM".into()),
                    Some("IB".into()),
                    Some(server.url()),
                    plants,
                    "direct",
                    None,
                    None,
                )
                .unwrap()
            })
        };
        let data = existing_host.then(|| construct("market_data"));
        if let Some(data) = &data {
            Python::attach(|py| data.connect(py)).unwrap();
        }
        let execution = construct("execution");
        assert_eq!(
            server.connections(Plant::Pnl),
            0,
            "constructor did plant I/O"
        );
        Python::attach(|py| execution.connect(py)).unwrap();
        assert_eq!(server.connections(Plant::Pnl), 1, "execution omitted PnL");
        assert_eq!(
            server.connections(Plant::Ticker),
            1,
            "registry shared login"
        );
        Python::attach(|py| execution.disconnect(py)).unwrap();
        Python::attach(|py| execution.connect(py)).unwrap();
        assert_eq!(
            server.connections(Plant::Pnl),
            1,
            "reconnect reapplies constructor plants"
        );
        Python::attach(|py| execution.disconnect(py)).unwrap();
        if let Some(data) = data {
            Python::attach(|py| data.disconnect(py)).unwrap();
        }
    }
}
