//! `DirectSession`: in-process plants behind the [`Session`] trait.
//!
//! Wraps one [`RithmicSession`] and owns the order-operation safety that used
//! to live in the PyO3 class: an in-flight order op blocks `disconnect`, an
//! order op started while a disconnect is in progress is rejected, and order
//! I/O runs on a cloned plant handle outside the session lock so a blocking
//! poll never waits behind a place / cancel / modify.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::{Mutex, MutexGuard};

use crate::backend::{AccountTriple, ModifyOrder, PlaceBracketOrder, PlaceOrder, Session};
use crate::config::SessionConfig;
use crate::dto::{
    AccountRmsInfoDto, FillHistoryDto, FrontMonthDto, HistoryBarDto, HistoryTickDto,
    OrderNotificationDto, PlantEvent, ProductRmsInfoDto, ReferenceDataDto, TimeBarProbeRow,
};
use crate::error::{Error, Result};
use crate::history::parse_time_bar_type;
use crate::plants::PlantSet;
use crate::session::{
    adjust_bracket_stop_on, adjust_bracket_target_on, cancel_all_orders_on, cancel_order_on,
    load_account_rms_info_on, load_fill_history_on, load_order_history_dates_on,
    load_order_history_on, load_orders_on, load_product_rms_info_on, modify_order_on,
    place_bracket_order_on, place_order_on, RithmicSession,
};

/// How long `disconnect` waits for in-flight order operations to finish.
const DISCONNECT_WAIT: Duration = Duration::from_secs(30);
/// Sleep between re-polls while a blocking `poll_*` waits for an event.
const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// In-process plants for one credential set.
pub struct DirectSession {
    inner: Mutex<RithmicSession>,
    /// Held for the full duration of place/cancel/modify so disconnect cannot interleave.
    session_ops: Mutex<()>,
    disconnecting: AtomicBool,
}

/// Marks a disconnect in progress for its lifetime (cleared on every exit path).
struct DisconnectGate<'a>(&'a AtomicBool);

impl<'a> DisconnectGate<'a> {
    fn new(flag: &'a AtomicBool) -> Self {
        flag.store(true, Ordering::SeqCst);
        Self(flag)
    }
}

impl Drop for DisconnectGate<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

impl DirectSession {
    pub fn new(config: SessionConfig, plants: PlantSet) -> Self {
        Self {
            inner: Mutex::new(RithmicSession::with_plants(config, plants)),
            session_ops: Mutex::new(()),
            disconnecting: AtomicBool::new(false),
        }
    }

    /// Begin a place / cancel / modify: rejected while a disconnect is in progress.
    async fn begin_order_op(&self) -> Result<MutexGuard<'_, ()>> {
        if self.disconnecting.load(Ordering::SeqCst) {
            return Err(Error::Session(
                "session disconnect in progress; order operation rejected".into(),
            ));
        }
        Ok(self.session_ops.lock().await)
    }

    /// Wait for in-flight order operations, bounded by [`DISCONNECT_WAIT`].
    async fn wait_for_order_ops(&self) -> Result<MutexGuard<'_, ()>> {
        tokio::time::timeout(DISCONNECT_WAIT, self.session_ops.lock())
            .await
            .map_err(|_| Error::Session("timed out waiting for in-flight order operations".into()))
    }

    /// Ensure the order plant under a short lock, then clone its handle so
    /// network I/O runs without blocking `poll_order_event`.
    async fn order_handle(&self) -> Result<rithmic_rs::RithmicOrderPlantHandle> {
        let mut inner = self.inner.lock().await;
        inner.ensure_order_plant().await?;
        inner.clone_order_handle()
    }

    /// Poll once under the lock; with a timeout, re-poll (releasing the lock
    /// between attempts) until an event arrives or the deadline passes.
    async fn poll_with<F>(&self, timeout: Duration, mut poll: F) -> Result<Option<PlantEvent>>
    where
        F: FnMut(&mut RithmicSession) -> Result<Option<PlantEvent>> + Send,
    {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(event) = poll(&mut *self.inner.lock().await)? {
                return Ok(Some(event));
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            tokio::time::sleep(POLL_INTERVAL.min(deadline - now)).await;
        }
    }
}

#[async_trait]
impl Session for DirectSession {
    async fn connect(&self) -> Result<()> {
        self.inner.lock().await.connect().await
    }

    async fn disconnect(&self) -> Result<()> {
        let _gate = DisconnectGate::new(&self.disconnecting);
        let _ops = self.wait_for_order_ops().await?;
        self.inner.lock().await.disconnect().await
    }

    async fn request_plants(&self, extra: PlantSet) -> Result<()> {
        self.inner.lock().await.request_plants(extra).await
    }

    async fn ensure_order_plant(&self) -> Result<()> {
        self.inner.lock().await.ensure_order_plant().await
    }

    async fn ensure_pnl_plant(&self) -> Result<()> {
        self.inner.lock().await.ensure_pnl_plant().await
    }

    async fn disconnect_order_plant(&self) -> Result<()> {
        let _gate = DisconnectGate::new(&self.disconnecting);
        let _ops = self.wait_for_order_ops().await?;
        self.inner.lock().await.disconnect_order_plant().await
    }

    async fn disconnect_pnl_plant(&self) -> Result<()> {
        self.inner.lock().await.disconnect_pnl_plant().await
    }

    async fn reset_ticker(&self) -> Result<()> {
        // Refcount-blind on purpose: the data client's channel-error resync must
        // actually recreate the ticker plant even when the session is shared.
        self.inner.lock().await.reset_ticker_plant().await
    }

    async fn resolved_account(&self) -> Option<AccountTriple> {
        self.inner
            .lock()
            .await
            .resolved_account()
            .map(|a| AccountTriple {
                account_id: a.account_id.clone(),
                fcm_id: a.fcm_id.clone(),
                ib_id: a.ib_id.clone(),
            })
    }

    async fn subscribe(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.inner.lock().await.subscribe(symbol, exchange).await
    }

    async fn unsubscribe(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.inner.lock().await.unsubscribe(symbol, exchange).await
    }

    async fn subscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.inner
            .lock()
            .await
            .subscribe_order_book_summary(symbol, exchange)
            .await
    }

    async fn unsubscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.inner
            .lock()
            .await
            .unsubscribe_order_book_summary(symbol, exchange)
            .await
    }

    async fn subscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> Result<()> {
        self.inner
            .lock()
            .await
            .subscribe_time_bars(symbol, exchange, bar_type, period)
            .await
    }

    async fn unsubscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> Result<()> {
        self.inner
            .lock()
            .await
            .unsubscribe_time_bars(symbol, exchange, bar_type, period)
            .await
    }

    async fn subscribe_pnl(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.ensure_pnl_plant().await?;
        inner.subscribe_pnl().await
    }

    async fn subscribe_order_updates(&self) -> Result<()> {
        // Must use the session's own handle (the one poll_order_event reads).
        self.inner.lock().await.subscribe_order_updates().await
    }

    async fn subscribe_bracket_updates(&self) -> Result<()> {
        self.inner.lock().await.subscribe_bracket_updates().await
    }

    async fn get_front_month(&self, symbol: &str, exchange: &str) -> Result<FrontMonthDto> {
        self.inner
            .lock()
            .await
            .get_front_month(symbol, exchange)
            .await
    }

    async fn get_reference_data(&self, symbol: &str, exchange: &str) -> Result<ReferenceDataDto> {
        self.inner
            .lock()
            .await
            .get_reference_data(symbol, exchange)
            .await
    }

    async fn load_ticks(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<HistoryTickDto>> {
        self.inner
            .lock()
            .await
            .load_ticks_all(symbol, exchange, start_time_sec, end_time_sec)
            .await
    }

    async fn load_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
        bar_type: i32,
        period: i32,
    ) -> Result<Vec<HistoryBarDto>> {
        let bar_type = parse_time_bar_type(bar_type)?;
        self.inner
            .lock()
            .await
            .load_time_bars_all(
                symbol,
                exchange,
                bar_type,
                period.max(1),
                start_time_sec,
                end_time_sec,
            )
            .await
    }

    async fn probe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
        bar_type: i32,
        period: i32,
    ) -> Result<Vec<TimeBarProbeRow>> {
        let bar_type = parse_time_bar_type(bar_type)?;
        self.inner
            .lock()
            .await
            .probe_time_bars(
                symbol,
                exchange,
                bar_type,
                period.max(1),
                start_time_sec,
                end_time_sec,
            )
            .await
    }

    async fn load_orders(
        &self,
        _start_sec: i32,
        _end_sec: i32,
    ) -> Result<Vec<OrderNotificationDto>> {
        // Detached handle: the drain can run for seconds and must not block the
        // event pump / pollers on the session lock. The window is ignored: only
        // the current working set is requested.
        let handle = self.order_handle().await?;
        load_orders_on(&handle).await
    }

    async fn load_product_rms_info(&self) -> Result<Vec<ProductRmsInfoDto>> {
        let handle = self.order_handle().await?;
        load_product_rms_info_on(&handle).await
    }

    async fn load_account_rms_info(&self) -> Result<Vec<AccountRmsInfoDto>> {
        let handle = self.order_handle().await?;
        load_account_rms_info_on(&handle).await
    }

    async fn load_fill_history(&self, start: i32, finish: i32) -> Result<Vec<FillHistoryDto>> {
        let handle = self.order_handle().await?;
        load_fill_history_on(&handle, start, finish).await
    }

    async fn load_order_history(&self, date: &str) -> Result<Vec<OrderNotificationDto>> {
        let handle = self.order_handle().await?;
        load_order_history_on(&handle, date).await
    }

    async fn load_order_history_dates(&self) -> Result<Vec<String>> {
        let handle = self.order_handle().await?;
        load_order_history_dates_on(&handle).await
    }

    async fn place_order(&self, o: &PlaceOrder) -> Result<()> {
        let _op = self.begin_order_op().await?;
        let handle = self.order_handle().await?;
        place_order_on(
            &handle,
            &o.symbol,
            &o.exchange,
            &o.side,
            &o.price_type,
            o.quantity,
            &o.user_tag,
            o.price,
            o.trigger_price,
            &o.duration,
            o.trail_by_ticks,
            o.trail_by_price_id,
        )
        .await
    }

    async fn place_bracket_order(&self, o: &PlaceBracketOrder) -> Result<()> {
        let _op = self.begin_order_op().await?;
        let handle = self.order_handle().await?;
        place_bracket_order_on(
            &handle,
            &o.symbol,
            &o.exchange,
            &o.side,
            &o.price_type,
            o.quantity,
            &o.localid,
            o.price,
            o.trigger_price,
            &o.duration,
            o.stop_ticks,
            o.target_ticks,
        )
        .await
    }

    async fn adjust_bracket_stop(
        &self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()> {
        let _op = self.begin_order_op().await?;
        let handle = self.order_handle().await?;
        adjust_bracket_stop_on(&handle, basket_id, ticks, level).await
    }

    async fn adjust_bracket_target(
        &self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()> {
        let _op = self.begin_order_op().await?;
        let handle = self.order_handle().await?;
        adjust_bracket_target_on(&handle, basket_id, ticks, level).await
    }

    async fn cancel_order(&self, basket_id: &str) -> Result<()> {
        let _op = self.begin_order_op().await?;
        let handle = self.order_handle().await?;
        cancel_order_on(&handle, basket_id).await
    }

    async fn modify_order(&self, o: &ModifyOrder) -> Result<()> {
        let _op = self.begin_order_op().await?;
        let handle = self.order_handle().await?;
        modify_order_on(
            &handle,
            &o.basket_id,
            &o.symbol,
            &o.exchange,
            o.quantity,
            &o.price_type,
            o.price,
            o.trigger_price,
            o.trail_by_ticks,
        )
        .await
    }

    async fn cancel_all_orders(&self) -> Result<()> {
        let _op = self.begin_order_op().await?;
        let handle = self.order_handle().await?;
        cancel_all_orders_on(&handle).await
    }

    async fn poll_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.poll_with(timeout, RithmicSession::poll_event).await
    }

    async fn poll_history_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.poll_with(timeout, RithmicSession::poll_history_event)
            .await
    }

    async fn poll_pnl_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.poll_with(timeout, RithmicSession::poll_pnl_event)
            .await
    }

    async fn poll_order_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.poll_with(timeout, RithmicSession::poll_order_event)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use super::*;

    fn offline() -> DirectSession {
        let cfg = SessionConfig::builder()
            .user("direct-test")
            .password("pw")
            .system_name("LucidTrading")
            .url("wss://rprotocol.rithmic.com:443")
            .env(rithmic_rs::RithmicEnv::from_str("live").expect("env"))
            .build()
            .expect("config");
        DirectSession::new(cfg, PlantSet::MARKET_DATA)
    }

    #[tokio::test]
    async fn order_op_is_rejected_while_a_disconnect_is_in_progress() {
        let s = offline();
        s.disconnecting.store(true, Ordering::SeqCst);
        let err = s.begin_order_op().await.expect_err("must reject");
        assert!(
            err.to_string()
                .contains("session disconnect in progress; order operation rejected"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn disconnect_waits_for_an_inflight_order_op_then_clears_the_gate() {
        let s = Arc::new(offline());
        let op = s.begin_order_op().await.expect("order op");

        let disconnecting = Arc::clone(&s);
        let task = tokio::spawn(async move { disconnecting.disconnect().await });

        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(!task.is_finished(), "disconnect must wait for the order op");
        assert!(s.disconnecting.load(Ordering::SeqCst), "gate is raised");
        // A new order op started during the disconnect is refused outright.
        assert!(s.begin_order_op().await.is_err());

        drop(op);
        task.await.expect("join").expect("disconnect ok");
        assert!(!s.disconnecting.load(Ordering::SeqCst), "gate is cleared");
        assert!(s.begin_order_op().await.is_ok(), "orders allowed again");
    }

    #[tokio::test]
    async fn disconnect_gate_is_cleared_when_the_wait_fails() {
        let s = offline();
        let _op = s.begin_order_op().await.expect("order op");
        // Simulate the bounded wait expiring without sleeping 30s.
        let timed_out = tokio::time::timeout(Duration::from_millis(20), s.disconnect()).await;
        assert!(timed_out.is_err(), "disconnect is still waiting");
        // Dropping the cancelled future dropped its DisconnectGate.
        assert!(!s.disconnecting.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn zero_timeout_poll_is_a_single_non_blocking_drain() {
        let s = offline();
        let started = Instant::now();
        let got = s.poll_pnl_event(Duration::ZERO).await.expect("poll");
        assert!(got.is_none());
        assert!(started.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn positive_timeout_poll_waits_then_returns_none() {
        let s = offline();
        let started = Instant::now();
        let got = s
            .poll_pnl_event(Duration::from_millis(40))
            .await
            .expect("poll");
        assert!(got.is_none());
        assert!(started.elapsed() >= Duration::from_millis(40));
    }

    #[tokio::test]
    async fn polling_an_unconnected_ticker_is_a_typed_error() {
        let s = offline();
        let err = s
            .poll_event(Duration::ZERO)
            .await
            .expect_err("not connected");
        assert!(
            matches!(err, Error::NotConnected { plant: "ticker" }),
            "{err}"
        );
    }

    #[tokio::test]
    async fn resolved_account_is_none_until_resolved() {
        assert!(offline().resolved_account().await.is_none());
    }
}
