//! `RithmicVenue`: in-process plants behind the [`Venue`] trait.
//!
//! Wraps one [`Plants`] and owns the order-operation safety that used
//! to live in the PyO3 class: an in-flight order op blocks `disconnect`, an
//! order op started while a disconnect is in progress is rejected, and order
//! I/O runs on a cloned plant handle outside the session lock so a blocking
//! poll never waits behind a place / cancel / modify.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::config::SessionConfig;
use crate::history::parse_time_bar_type;
use crate::session::{
    adjust_bracket_on, cancel_all_orders_on, cancel_order_on, load_account_rms_info_on,
    load_fill_history_on, load_order_history_dates_on, load_order_history_on, load_orders_on,
    load_product_rms_info_on, modify_order_on, place_bracket_order_on, place_order_on, BracketLeg,
    Handles, Plants,
};
use rithmic_host::backend::{AccountTriple, ModifyOrder, PlaceBracketOrder, PlaceOrder, Venue};
use rithmic_host::dto::{
    AccountRmsInfoDto, FillHistoryDto, FrontMonthDto, HistoryBarDto, HistoryTickDto,
    OrderNotificationDto, PlantEvent, ProductRmsInfoDto, ReferenceDataDto, TimeBarProbeRow,
};
use rithmic_host::plants::PlantSet;
use rithmic_host::streams::{Stream, StreamHub};
use rithmic_host::{Error, Result};

/// How long `disconnect` waits for in-flight order operations to finish.
const DISCONNECT_WAIT: Duration = Duration::from_secs(30);

/// In-process plants for one credential set.
pub struct RithmicVenue {
    inner: Mutex<Plants>,
    /// Event lanes: consumers read them without taking `inner`.
    streams: Arc<StreamHub>,
    /// Order operations hold it shared for their whole duration (they run
    /// concurrently); `disconnect` takes it exclusively, so it waits for them and
    /// none starts underneath it.
    session_ops: RwLock<()>,
    /// Disconnects in progress (`disconnect`, `disconnect_order_plant`); a
    /// count, so overlapping ones keep order operations refused until the last
    /// finishes.
    disconnecting: AtomicUsize,
    /// The order plant was disconnected on request: commands are refused (not
    /// sent) until `ensure_order_plant` or an order subscribe opens it again,
    /// instead of silently re-opening a plant with no subscriptions.
    order_closed: AtomicBool,
    /// Trading gate (place, cancel, modify, brackets, order subscriptions and the
    /// order-plant loads). Off by default: nothing reaches the order plant until
    /// the owner opens it.
    trading_enabled: AtomicBool,
    /// Plant-wide cancel-all gate, independent of trading.
    cancel_all_enabled: AtomicBool,
    /// The live order plant's handle, mirrored from `inner` whenever a call
    /// that can change it returns, so place / cancel / modify on a live plant
    /// never wait for `inner` (a ticker reset or a reconnect may hold it).
    order_cache: StdMutex<Option<rithmic_rs::RithmicOrderPlantHandle>>,
    /// The resolved account, mirrored the same way.
    account_cache: StdMutex<Option<AccountTriple>>,
    /// Ticker resets run one at a time, and ticker calls wait for one in flight
    /// instead of finding the ticker missing.
    ticker_reset: Mutex<()>,
    /// `load_orders` and `load_order_history` both collect replayed rows off the
    /// shared order stream with no request correlation; run one at a time so
    /// neither collects the other's replay.
    order_drain: Mutex<()>,
}

use rithmic_host::backend::{DISCONNECT_IN_PROGRESS, ORDER_PLANT_CLOSED};

/// Marks a disconnect in progress for its lifetime (cleared on every exit path).
struct DisconnectGate<'a>(&'a AtomicUsize);

impl<'a> DisconnectGate<'a> {
    fn new(count: &'a AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(count)
    }
}

impl Drop for DisconnectGate<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl RithmicVenue {
    pub fn new(config: SessionConfig, plants: PlantSet) -> Self {
        let session = Plants::with_plants(config, plants);
        let account = session.resolved_account().map(account_triple);
        Self {
            streams: session.streams(),
            order_cache: StdMutex::new(session.current_order_handle()),
            account_cache: StdMutex::new(account),
            ticker_reset: Mutex::new(()),
            order_drain: Mutex::new(()),
            inner: Mutex::new(session),
            session_ops: RwLock::new(()),
            disconnecting: AtomicUsize::new(0),
            order_closed: AtomicBool::new(false),
            trading_enabled: AtomicBool::new(false),
            cancel_all_enabled: AtomicBool::new(false),
        }
    }

    /// Open the trading and/or cancel-all gate at construction.
    pub fn with_gates(self, trading_enabled: bool, cancel_all_enabled: bool) -> Self {
        self.allow(trading_enabled, cancel_all_enabled);
        self
    }

    /// Open (never close) a gate. A direct session is shared by every client in
    /// the process, so the gate is open when any holder asked for it; closing one
    /// would let one client take trading away from another mid-flight.
    pub fn allow(&self, trading_enabled: bool, cancel_all_enabled: bool) {
        if trading_enabled {
            self.trading_enabled.store(true, Ordering::SeqCst);
        }
        if cancel_all_enabled {
            self.cancel_all_enabled.store(true, Ordering::SeqCst);
        }
    }

    pub fn trading_enabled(&self) -> bool {
        self.trading_enabled.load(Ordering::SeqCst)
    }

    pub fn cancel_all_enabled(&self) -> bool {
        self.cancel_all_enabled.load(Ordering::SeqCst)
    }

    /// Deny a trading RPC while the gate is closed. Same error code the gateway
    /// parent answers, so both modes fail identically and before anything is sent.
    fn require_trading(&self, what: &str) -> Result<()> {
        if self.trading_enabled() {
            Ok(())
        } else {
            Err(Error::Gateway {
                code: "trading_disabled".into(),
                message: format!("{what} denied: trading disabled"),
            })
        }
    }

    fn require_cancel_all(&self) -> Result<()> {
        if self.cancel_all_enabled() {
            Ok(())
        } else {
            Err(Error::Gateway {
                code: "cancel_all_denied".into(),
                message: "cancel_all_orders denied: cancel_all disabled".into(),
            })
        }
    }

    /// Begin a place / cancel / modify: rejected while a disconnect is in progress.
    async fn begin_order_op(&self) -> Result<RwLockReadGuard<'_, ()>> {
        let refused = || Error::NotSent(DISCONNECT_IN_PROGRESS.into());
        if self.disconnecting.load(Ordering::SeqCst) > 0 {
            return Err(refused());
        }
        let guard = self.session_ops.read().await;
        // A disconnect may have started (and finished) while this waited behind its
        // queued write lock: re-check, or the order would re-open the plant it closed.
        if self.disconnecting.load(Ordering::SeqCst) > 0 {
            return Err(refused());
        }
        Ok(guard)
    }

    /// Wait for in-flight order operations, bounded by [`DISCONNECT_WAIT`].
    async fn wait_for_order_ops(&self) -> Result<RwLockWriteGuard<'_, ()>> {
        tokio::time::timeout(DISCONNECT_WAIT, self.session_ops.write())
            .await
            .map_err(|_| Error::Session("timed out waiting for in-flight order operations".into()))
    }

    /// The order handle for a command or an order-plant read. Nothing has been
    /// sent when this fails, so a missing or unreachable plant is
    /// [`Error::NotSent`], never an unknown outcome. After an explicit
    /// `disconnect_order_plant` it refuses instead of silently re-opening a plant
    /// with no subscriptions.
    async fn command_handle(&self) -> Result<rithmic_rs::RithmicOrderPlantHandle> {
        if self.order_closed.load(Ordering::SeqCst) {
            return Err(Error::NotSent(ORDER_PLANT_CLOSED.into()));
        }
        self.order_handle()
            .await
            .map_err(|e| Error::NotSent(e.to_string()))
    }

    /// The session for a bring-up (connect, ensure, request plants, subscribe on
    /// the order plant): waits out a ticker reset in flight first, because the
    /// reset takes the ticker out of the session for its venue round trips and a
    /// bring-up would otherwise see no ticker. Lock order: `ticker_reset`, then
    /// `inner` (the same order `reset_ticker` uses).
    async fn bring_up(&self) -> (MutexGuard<'_, ()>, MutexGuard<'_, Plants>) {
        let reset = self.ticker_reset.lock().await;
        (reset, self.inner.lock().await)
    }

    /// Clone the plant handles under an instant's lock; the I/O that follows runs
    /// without it.
    async fn handles(&self) -> Handles {
        self.inner.lock().await.handles()
    }

    /// The handles for a ticker call: waits out a ticker reset in flight, so a
    /// subscribe during a resync lands on the new plant instead of failing.
    async fn ticker_handles(&self) -> Handles {
        let _reset = self.ticker_reset.lock().await;
        self.handles().await
    }

    /// Mirror the order handle and account from `inner` after a call that can
    /// change them (connect, disconnect, ensure, subscribe, rollback).
    fn sync_caches(&self, inner: &Plants) {
        *self.order_cache.lock().expect("order cache") = inner.current_order_handle();
        if let Some(account) = inner.resolved_account() {
            *self.account_cache.lock().expect("account cache") = Some(account_triple(account));
        }
    }

    /// The order handle: the mirrored one when the plant is up, otherwise ensure
    /// the plant under the session lock. Network I/O runs on the clone, never
    /// under the lock.
    async fn order_handle(&self) -> Result<rithmic_rs::RithmicOrderPlantHandle> {
        if let Some(handle) = self.order_cache.lock().expect("order cache").clone() {
            return Ok(handle);
        }
        let (_reset, mut inner) = self.bring_up().await;
        let ensured = inner.ensure_order_plant().await;
        self.sync_caches(&inner);
        ensured?;
        inner.clone_order_handle()
    }
}

fn account_triple(a: &rithmic_rs::RithmicAccount) -> AccountTriple {
    AccountTriple {
        account_id: a.account_id.clone(),
        fcm_id: a.fcm_id.clone(),
        ib_id: a.ib_id.clone(),
    }
}

#[async_trait]
impl Venue for RithmicVenue {
    async fn connect(&self) -> Result<()> {
        let (_reset, mut inner) = self.bring_up().await;
        let result = inner.connect().await;
        self.sync_caches(&inner);
        if result.is_ok() {
            self.order_closed.store(false, Ordering::SeqCst);
        }
        result
    }

    async fn disconnect(&self) -> Result<()> {
        let _gate = DisconnectGate::new(&self.disconnecting);
        let _ops = self.wait_for_order_ops().await?;
        let mut inner = self.inner.lock().await;
        let result = inner.disconnect().await;
        self.sync_caches(&inner);
        result
    }

    async fn request_plants(&self, extra: PlantSet) -> Result<()> {
        let (_reset, mut inner) = self.bring_up().await;
        let result = inner.request_plants(extra).await;
        self.sync_caches(&inner);
        result
    }

    async fn ensure_order_plant(&self) -> Result<()> {
        self.require_trading("ensure_order_plant")?;
        let (_reset, mut inner) = self.bring_up().await;
        let result = inner.ensure_order_plant().await;
        self.sync_caches(&inner);
        if result.is_ok() {
            self.order_closed.store(false, Ordering::SeqCst);
        }
        result
    }

    async fn ensure_pnl_plant(&self) -> Result<()> {
        let (_reset, mut inner) = self.bring_up().await;
        let result = inner.ensure_pnl_plant().await;
        self.sync_caches(&inner);
        result
    }

    async fn disconnect_order_plant(&self) -> Result<()> {
        let _gate = DisconnectGate::new(&self.disconnecting);
        let _ops = self.wait_for_order_ops().await?;
        let mut inner = self.inner.lock().await;
        let result = inner.disconnect_order_plant().await;
        self.sync_caches(&inner);
        self.order_closed.store(true, Ordering::SeqCst);
        result
    }

    async fn disconnect_pnl_plant(&self) -> Result<()> {
        self.inner.lock().await.disconnect_pnl_plant().await
    }

    async fn reset_ticker(&self) -> Result<()> {
        // Refcount-blind on purpose: the data client's channel-error resync must
        // actually recreate the ticker plant even when the session is shared.
        // The venue round trips run without the session lock.
        let _serial = self.ticker_reset.lock().await;
        let reset = self.inner.lock().await.begin_ticker_reset()?;
        let reconnected = reset.reconnect().await?;
        self.inner
            .lock()
            .await
            .finish_ticker_reset(reconnected)
            .await;
        Ok(())
    }

    async fn reset_history(&self) -> Result<()> {
        self.inner.lock().await.reset_history().await
    }

    async fn pnl_snapshot(&self) -> Result<()> {
        self.handles().await.pnl_snapshot().await
    }

    async fn refresh_book(&self, symbol: &str, exchange: &str) -> Result<()> {
        let handles = self.ticker_handles().await;
        handles
            .unsubscribe_order_book_summary(symbol, exchange)
            .await?;
        handles.subscribe_order_book_summary(symbol, exchange).await
    }

    async fn resolved_account(&self) -> Option<AccountTriple> {
        self.account_cache.lock().expect("account cache").clone()
    }

    async fn subscribe(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ticker_handles()
            .await
            .subscribe(symbol, exchange)
            .await
    }

    async fn unsubscribe(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ticker_handles()
            .await
            .unsubscribe(symbol, exchange)
            .await
    }

    async fn subscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ticker_handles()
            .await
            .subscribe_order_book_summary(symbol, exchange)
            .await
    }

    async fn unsubscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.ticker_handles()
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
        self.handles()
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
        self.handles()
            .await
            .unsubscribe_time_bars(symbol, exchange, bar_type, period)
            .await
    }

    async fn subscribe_pnl(&self) -> Result<()> {
        self.ensure_pnl_plant().await?;
        self.handles().await.subscribe_pnl().await
    }

    async fn subscribe_order_updates(&self) -> Result<()> {
        self.require_trading("subscribe_order_updates")?;
        // Must use the session's own handle (the one poll_order_event reads).
        let (_reset, mut inner) = self.bring_up().await;
        let result = inner.subscribe_order_updates().await;
        self.sync_caches(&inner);
        if result.is_ok() {
            self.order_closed.store(false, Ordering::SeqCst);
        }
        result
    }

    async fn subscribe_bracket_updates(&self) -> Result<()> {
        self.require_trading("subscribe_bracket_updates")?;
        let (_reset, mut inner) = self.bring_up().await;
        let result = inner.subscribe_bracket_updates().await;
        self.sync_caches(&inner);
        if result.is_ok() {
            self.order_closed.store(false, Ordering::SeqCst);
        }
        result
    }

    async fn get_front_month(&self, symbol: &str, exchange: &str) -> Result<FrontMonthDto> {
        self.ticker_handles()
            .await
            .get_front_month(symbol, exchange)
            .await
    }

    async fn get_reference_data(&self, symbol: &str, exchange: &str) -> Result<ReferenceDataDto> {
        self.ticker_handles()
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
        self.handles()
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
        self.handles()
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
        self.handles()
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
        self.require_trading("load_orders")?;
        // Detached handle: the drain can run for seconds and must not block the
        // event pump / pollers on the session lock. The window is ignored: only
        // the current working set is requested.
        let handle = self.command_handle().await?;
        let _drain = self.order_drain.lock().await;
        load_orders_on(&handle).await
    }

    async fn load_product_rms_info(&self) -> Result<Vec<ProductRmsInfoDto>> {
        self.require_trading("load_product_rms_info")?;
        let handle = self.command_handle().await?;
        load_product_rms_info_on(&handle).await
    }

    async fn load_account_rms_info(&self) -> Result<Vec<AccountRmsInfoDto>> {
        self.require_trading("load_account_rms_info")?;
        let handle = self.command_handle().await?;
        load_account_rms_info_on(&handle).await
    }

    async fn load_fill_history(&self, start: i32, finish: i32) -> Result<Vec<FillHistoryDto>> {
        self.require_trading("load_fill_history")?;
        let handle = self.command_handle().await?;
        load_fill_history_on(&handle, start, finish).await
    }

    async fn load_order_history(&self, date: &str) -> Result<Vec<OrderNotificationDto>> {
        self.require_trading("load_order_history")?;
        let handle = self.command_handle().await?;
        let _drain = self.order_drain.lock().await;
        load_order_history_on(&handle, date).await
    }

    async fn load_order_history_dates(&self) -> Result<Vec<String>> {
        self.require_trading("load_order_history_dates")?;
        let handle = self.command_handle().await?;
        load_order_history_dates_on(&handle).await
    }

    async fn place_order(&self, o: &PlaceOrder) -> Result<()> {
        self.require_trading("place_order")?;
        let _op = self.begin_order_op().await?;
        let handle = self.command_handle().await?;
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
        self.require_trading("place_bracket_order")?;
        let _op = self.begin_order_op().await?;
        let handle = self.command_handle().await?;
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
        self.require_trading("adjust_bracket_stop")?;
        let _op = self.begin_order_op().await?;
        let handle = self.command_handle().await?;
        adjust_bracket_on(&handle, BracketLeg::Stop, basket_id, ticks, level).await
    }

    async fn adjust_bracket_target(
        &self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()> {
        self.require_trading("adjust_bracket_target")?;
        let _op = self.begin_order_op().await?;
        let handle = self.command_handle().await?;
        adjust_bracket_on(&handle, BracketLeg::Target, basket_id, ticks, level).await
    }

    async fn cancel_order(&self, basket_id: &str) -> Result<()> {
        self.require_trading("cancel_order")?;
        let _op = self.begin_order_op().await?;
        let handle = self.command_handle().await?;
        cancel_order_on(&handle, basket_id).await
    }

    async fn modify_order(&self, o: &ModifyOrder) -> Result<()> {
        self.require_trading("modify_order")?;
        let _op = self.begin_order_op().await?;
        let handle = self.command_handle().await?;
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
        self.require_cancel_all()?;
        let _op = self.begin_order_op().await?;
        let handle = self.command_handle().await?;
        cancel_all_orders_on(&handle).await
    }

    async fn poll_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.streams.poll(Stream::Ticker, timeout).await
    }

    async fn poll_history_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.streams.poll(Stream::History, timeout).await
    }

    async fn poll_pnl_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.streams.poll(Stream::Pnl, timeout).await
    }

    async fn poll_order_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.streams.poll(Stream::Order, timeout).await
    }

    async fn next_events(
        &self,
        stream: Stream,
        max: usize,
        timeout: Duration,
    ) -> Result<Vec<PlantEvent>> {
        self.streams.next_events(stream, max, timeout).await
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Instant;

    use super::*;

    fn offline() -> RithmicVenue {
        let cfg = SessionConfig::builder()
            .user("direct-test")
            .password("pw")
            .system_name("LucidTrading")
            .url("wss://rprotocol.rithmic.com:443")
            .env(rithmic_host::Env::from_str("live").expect("env"))
            .build()
            .expect("config");
        RithmicVenue::new(cfg, PlantSet::MARKET_DATA)
    }

    #[tokio::test]
    async fn order_op_is_rejected_while_a_disconnect_is_in_progress() {
        let s = offline();
        s.disconnecting.store(1, Ordering::SeqCst);
        let err = s.begin_order_op().await.expect_err("must reject");
        assert!(
            err.to_string()
                .contains("session disconnect in progress; order operation rejected"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn overlapping_disconnects_keep_orders_refused_until_the_last_ends() {
        let s = offline();
        let first = DisconnectGate::new(&s.disconnecting);
        let second = DisconnectGate::new(&s.disconnecting);
        drop(first);
        assert!(
            s.begin_order_op().await.is_err(),
            "one disconnect still in progress"
        );
        drop(second);
        assert!(s.begin_order_op().await.is_ok());
    }

    fn place() -> rithmic_host::backend::PlaceOrder {
        rithmic_host::backend::PlaceOrder {
            symbol: "NQU6".into(),
            exchange: "CME".into(),
            side: "BUY".into(),
            price_type: "LIMIT".into(),
            quantity: 1,
            user_tag: "t".into(),
            price: Some(1.0),
            trigger_price: None,
            duration: "DAY".into(),
            trail_by_ticks: None,
            trail_by_price_id: None,
        }
    }

    fn code(err: &Error) -> &str {
        match err {
            Error::Gateway { code, .. } => code,
            other => panic!("expected a gateway-coded denial, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn every_trading_rpc_is_denied_while_the_gate_is_closed_and_nothing_is_sent() {
        let s = offline();
        assert!(!s.trading_enabled() && !s.cancel_all_enabled());
        let bracket = rithmic_host::backend::PlaceBracketOrder {
            symbol: "NQU6".into(),
            exchange: "CME".into(),
            side: "BUY".into(),
            price_type: "LIMIT".into(),
            quantity: 1,
            localid: "b".into(),
            price: Some(1.0),
            trigger_price: None,
            duration: "DAY".into(),
            stop_ticks: Some(10),
            target_ticks: None,
        };
        let modify = rithmic_host::backend::ModifyOrder {
            basket_id: "B1".into(),
            symbol: "NQU6".into(),
            exchange: "CME".into(),
            quantity: 1,
            price_type: "LIMIT".into(),
            price: Some(1.0),
            trigger_price: None,
            trail_by_ticks: None,
        };
        let denials = [
            s.place_order(&place()).await.unwrap_err(),
            s.place_bracket_order(&bracket).await.unwrap_err(),
            s.adjust_bracket_stop("B1", 1, None).await.unwrap_err(),
            s.adjust_bracket_target("B1", 1, None).await.unwrap_err(),
            s.cancel_order("B1").await.unwrap_err(),
            s.modify_order(&modify).await.unwrap_err(),
            s.ensure_order_plant().await.unwrap_err(),
            s.subscribe_order_updates().await.unwrap_err(),
            s.subscribe_bracket_updates().await.unwrap_err(),
            s.load_orders(0, 0).await.unwrap_err(),
            s.load_product_rms_info().await.unwrap_err(),
            s.load_account_rms_info().await.unwrap_err(),
            s.load_fill_history(0, 0).await.unwrap_err(),
            s.load_order_history("20260101").await.unwrap_err(),
            s.load_order_history_dates().await.unwrap_err(),
        ];
        for err in &denials {
            assert_eq!(code(err), "trading_disabled", "{err}");
        }
        assert_eq!(denials.len(), 15);
    }

    #[tokio::test]
    async fn cancel_all_has_its_own_gate_that_trading_does_not_open() {
        let s = offline().with_gates(true, false);
        assert!(s.trading_enabled() && !s.cancel_all_enabled());
        let err = s.cancel_all_orders().await.unwrap_err();
        assert_eq!(code(&err), "cancel_all_denied", "{err}");
    }

    #[tokio::test]
    async fn gates_only_ever_open_and_do_not_depend_on_each_other() {
        let s = offline();
        s.allow(false, true);
        assert!(!s.trading_enabled() && s.cancel_all_enabled());
        s.allow(true, false);
        assert!(s.trading_enabled() && s.cancel_all_enabled());
        // A later holder that did not ask for trading cannot close it.
        s.allow(false, false);
        assert!(s.trading_enabled() && s.cancel_all_enabled());
    }

    #[tokio::test]
    async fn an_order_op_during_a_disconnect_is_not_sent() {
        let s = Arc::new(offline().with_gates(true, true));
        s.disconnecting.store(1, Ordering::SeqCst);
        let err = s.place_order(&place()).await.unwrap_err();
        assert!(matches!(err, Error::NotSent(_)), "{err}");
    }

    #[tokio::test]
    async fn order_ops_run_concurrently_and_do_not_queue_behind_each_other() {
        let s = offline();
        let first = s.begin_order_op().await.expect("first op");
        // A second op while the first is still in flight must not wait for it.
        let second = tokio::time::timeout(Duration::from_millis(100), s.begin_order_op())
            .await
            .expect("second op started without waiting")
            .expect("second op");
        drop((first, second));
    }

    #[tokio::test]
    async fn a_waiting_disconnect_keeps_new_order_ops_out() {
        let s = Arc::new(offline());
        let op = s.begin_order_op().await.expect("order op");
        let disconnecting = Arc::clone(&s);
        let task = tokio::spawn(async move { disconnecting.disconnect().await });
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(
            s.begin_order_op().await.is_err(),
            "refused during disconnect"
        );
        drop(op);
        task.await.expect("join").expect("disconnect ok");
    }

    #[tokio::test]
    async fn disconnect_waits_for_an_inflight_order_op_then_clears_the_gate() {
        let s = Arc::new(offline());
        let op = s.begin_order_op().await.expect("order op");

        let disconnecting = Arc::clone(&s);
        let task = tokio::spawn(async move { disconnecting.disconnect().await });

        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(!task.is_finished(), "disconnect must wait for the order op");
        assert_eq!(s.disconnecting.load(Ordering::SeqCst), 1, "gate is raised");
        // A new order op started during the disconnect is refused outright.
        assert!(s.begin_order_op().await.is_err());

        drop(op);
        task.await.expect("join").expect("disconnect ok");
        assert_eq!(s.disconnecting.load(Ordering::SeqCst), 0, "gate is cleared");
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
        assert_eq!(s.disconnecting.load(Ordering::SeqCst), 0);
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
