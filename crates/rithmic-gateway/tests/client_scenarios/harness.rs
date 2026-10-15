//! Attachment seam for the shared scenarios; no venue/network credentials.
use super::{common::*, Duration};
use async_trait::async_trait;
use rithmic_gateway::client::{GatewayConfig, RemoteSession};
use rithmic_host::Venue;
use rithmic_host::{
    fake::FakeVenue, HistoryPolicy, ParentGates, Session, SessionHost, Timing, VenueHandle,
};
use std::sync::Arc;

/// Script and observe the very venue the attachment's host owns.
///
/// Recovery extension point for A2/B2: call `venue().fail_stream(stream, cause)`
/// (or `drop_plants()`), and subscribe to `session.session_events()` BEFORE the
/// failure. No down/up policy or reconnect assertions are baked into this seam.
/// `attach` always creates an independent attachment to the SAME host, including
/// a separate duplex connection remotely. Shared-socket facade ownership remains
/// in `remote_only`, because it is a separate client-side refcount layer.
#[async_trait]
pub trait Harness: Sized + Send + Sync {
    async fn start(opts: Opts) -> Self;
    async fn attach(&self) -> Arc<dyn Session>;
    fn venue(&self) -> &FakeVenue;

    async fn command(&self, command: &str) -> Vec<String> {
        let reply = rithmic_gateway::testing::control_command(self.venue(), command);
        assert!(!reply.starts_with("err"), "{command}: {reply}");
        reply.lines().map(str::to_owned).collect()
    }
    async fn calls(&self) -> Vec<String> {
        self.venue().calls()
    }
    async fn count(&self, prefix: &str) -> usize {
        self.venue().count(prefix)
    }
    async fn clear_calls(&self) {
        self.venue().clear_calls();
    }
    async fn wait_for_call(&self, prefix: &str, timeout: Duration) -> bool {
        wait_until(timeout, || self.venue().count(prefix) > 0).await
    }
    async fn push_last_trades(&self, symbol: &str, n: usize) {
        for i in 0..n {
            self.venue().push_event(
                rithmic_host::Stream::Ticker,
                rithmic_gateway::testing::last_trade(symbol, CME, 100.25 + i as f64, 1),
            );
        }
        tokio::task::yield_now().await;
    }
}

pub struct LocalHarness {
    venue: Arc<FakeVenue>,
    host: Arc<SessionHost<FakeVenue>>,
}
#[async_trait]
impl Harness for LocalHarness {
    async fn start(opts: Opts) -> Self {
        let venue = Arc::new(FakeVenue::new());
        venue.connect().await.unwrap();
        let host = SessionHost::from_connected(
            VenueHandle::new(venue.clone()),
            ParentGates {
                trading_enabled: opts.trading,
                cancel_all_enabled: opts.cancel_all,
            },
            Timing {
                restore_backoff: Duration::from_millis(50),
                ..Default::default()
            },
            // Match the remote host's ROOT setting, not a mode-specific policy.
            HistoryPolicy::RefuseUnderLiveMd,
        );
        Self { venue, host }
    }
    async fn attach(&self) -> Arc<dyn Session> {
        Arc::new(self.host.attach())
    }
    fn venue(&self) -> &FakeVenue {
        &self.venue
    }
}

pub struct RemoteHarness(FakeParent);
#[async_trait]
impl Harness for RemoteHarness {
    async fn start(opts: Opts) -> Self {
        Self(FakeParent::start(opts).await)
    }
    async fn attach(&self) -> Arc<dyn Session> {
        // GatewayState currently erases V to dyn Venue. Its venue is still this
        // FakeVenue, using the same host implementation/options as LocalHarness.
        // No typed-host server overload exists at this base commit.
        Arc::new(RemoteSession::over(
            GatewayConfig::new(&self.0.user, SYSTEM, URL, "Live").with_auth_token(AUTH_TOKEN),
            self.0.stream(),
        ))
    }
    fn venue(&self) -> &FakeVenue {
        &self.0.fake
    }
}
