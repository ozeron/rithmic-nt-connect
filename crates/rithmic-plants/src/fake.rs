//! `FakeSession`: a scripted in-memory [`Session`] for testing the gateway layer
//! without Rithmic (feature `testing`; never part of the shipped wheel).
//!
//! It models only what the gateway layer depends on: events delivered per
//! stream, injected failures, a plant drop that clears on reconnect, and a
//! record of every call. Live smokes remain the authority on real plant
//! behavior.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::backend::{AccountTriple, ModifyOrder, PlaceBracketOrder, PlaceOrder, Session};
use crate::dto::{
    AccountRmsInfoDto, FrontMonthDto, HistoryBarDto, HistoryTickDto, OrderNotificationDto,
    PlantEvent, ProductRmsInfoDto, ReferenceDataDto, TimeBarProbeRow,
};
use crate::error::{Error, Result};
use crate::plants::PlantSet;

const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// One of the four plant event streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stream {
    Ticker,
    History,
    Pnl,
    Order,
}

impl Stream {
    fn plant(self) -> &'static str {
        match self {
            Self::Ticker => "ticker",
            Self::History => "history",
            Self::Pnl => "pnl",
            Self::Order => "order",
        }
    }
}

/// A failure to inject into the next call of a method.
#[derive(Debug, Clone)]
pub enum FakeFailure {
    /// A venue/plant error (`Error::Rithmic`): a connection issue to the pump.
    Rithmic(String),
    /// The subscription channel closed (`Error::ChannelClosed`).
    ChannelClosed(&'static str),
    /// A config / validation style error that is not a connection issue.
    Config(String),
}

impl FakeFailure {
    fn to_error(&self) -> Error {
        match self {
            Self::Rithmic(msg) => Error::Rithmic(msg.clone()),
            Self::ChannelClosed(plant) => Error::ChannelClosed { plant },
            Self::Config(msg) => Error::Config(msg.clone()),
        }
    }
}

#[derive(Default)]
struct State {
    connected: bool,
    pnl_plant: bool,
    order_plant: bool,
    /// Plants dropped by `drop_plants()`; polls fail until the next `connect`.
    dropped: bool,
    events: HashMap<Stream, VecDeque<PlantEvent>>,
    calls: Vec<String>,
    failures: HashMap<String, VecDeque<FakeFailure>>,
    account: Option<AccountTriple>,
    front_months: HashMap<(String, String), FrontMonthDto>,
    ticks: Vec<HistoryTickDto>,
    bars: Vec<HistoryBarDto>,
    probe_rows: Vec<TimeBarProbeRow>,
    orders: Vec<OrderNotificationDto>,
    product_rms: Vec<ProductRmsInfoDto>,
    account_rms: Vec<AccountRmsInfoDto>,
}

/// Scripted in-memory plant session.
#[derive(Default)]
pub struct FakeSession {
    state: Mutex<State>,
}

impl FakeSession {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("fake session state")
    }

    /// Record `method detail` and fail if a failure was queued for `method`.
    fn enter(&self, method: &str, detail: &str) -> Result<MutexGuard<'_, State>> {
        let mut state = self.lock();
        let line = if detail.is_empty() {
            method.to_string()
        } else {
            format!("{method} {detail}")
        };
        state.calls.push(line);
        if let Some(failure) = state.failures.get_mut(method).and_then(VecDeque::pop_front) {
            return Err(failure.to_error());
        }
        Ok(state)
    }

    /// Record a call that needs no state change.
    fn note(&self, method: &str, detail: &str) -> Result<()> {
        self.enter(method, detail).map(drop)
    }

    // --- scripting -------------------------------------------------------

    pub fn push_event(&self, stream: Stream, event: PlantEvent) {
        self.lock()
            .events
            .entry(stream)
            .or_default()
            .push_back(event);
    }

    /// Fail the next call to `method` (its name as recorded in [`Self::calls`]).
    pub fn fail_next(&self, method: &str, failure: FakeFailure) {
        self.lock()
            .failures
            .entry(method.to_string())
            .or_default()
            .push_back(failure);
    }

    /// Simulate the plants dropping: polls fail with `ChannelClosed` until the
    /// next `connect`, exactly what the parent's event pump reacts to.
    pub fn drop_plants(&self) {
        self.lock().dropped = true;
    }

    pub fn set_account(&self, account: AccountTriple) {
        self.lock().account = Some(account);
    }

    pub fn set_front_month(&self, symbol: &str, exchange: &str, dto: FrontMonthDto) {
        self.lock()
            .front_months
            .insert((symbol.to_string(), exchange.to_string()), dto);
    }

    pub fn set_ticks(&self, ticks: Vec<HistoryTickDto>) {
        self.lock().ticks = ticks;
    }

    pub fn set_bars(&self, bars: Vec<HistoryBarDto>) {
        self.lock().bars = bars;
    }

    pub fn set_orders(&self, orders: Vec<OrderNotificationDto>) {
        self.lock().orders = orders;
    }

    pub fn set_product_rms(&self, rows: Vec<ProductRmsInfoDto>) {
        self.lock().product_rms = rows;
    }

    pub fn set_account_rms(&self, rows: Vec<AccountRmsInfoDto>) {
        self.lock().account_rms = rows;
    }

    // --- observation -----------------------------------------------------

    /// Every call so far, in order (`"subscribe NQ CME"`, `"place_order tag-1"`, ...).
    pub fn calls(&self) -> Vec<String> {
        self.lock().calls.clone()
    }

    pub fn clear_calls(&self) {
        self.lock().calls.clear();
    }

    /// How many recorded calls start with `prefix`.
    pub fn count(&self, prefix: &str) -> usize {
        self.lock()
            .calls
            .iter()
            .filter(|c| c.starts_with(prefix))
            .count()
    }

    pub fn is_connected(&self) -> bool {
        self.lock().connected
    }

    async fn poll(&self, stream: Stream, timeout: Duration) -> Result<Option<PlantEvent>> {
        let deadline = Instant::now() + timeout;
        loop {
            {
                let mut state = self.lock();
                match stream {
                    Stream::Ticker | Stream::History if !state.connected => {
                        return Err(Error::NotConnected {
                            plant: stream.plant(),
                        });
                    }
                    Stream::Pnl if !state.pnl_plant => return Ok(None),
                    Stream::Order if !state.order_plant => {
                        return Err(Error::NotConnected { plant: "order" });
                    }
                    _ => {}
                }
                if state.dropped && stream != Stream::Pnl {
                    return Err(Error::ChannelClosed {
                        plant: stream.plant(),
                    });
                }
                if let Some(event) = state.events.entry(stream).or_default().pop_front() {
                    return Ok(Some(event));
                }
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
impl Session for FakeSession {
    async fn connect(&self) -> Result<()> {
        let mut state = self.enter("connect", "")?;
        state.connected = true;
        state.dropped = false;
        Ok(())
    }

    async fn disconnect(&self) -> Result<()> {
        let mut state = self.enter("disconnect", "")?;
        state.connected = false;
        state.pnl_plant = false;
        state.order_plant = false;
        Ok(())
    }

    async fn request_plants(&self, extra: PlantSet) -> Result<()> {
        self.note("request_plants", &format!("{extra:?}"))?;
        Ok(())
    }

    async fn ensure_order_plant(&self) -> Result<()> {
        self.enter("ensure_order_plant", "")?.order_plant = true;
        Ok(())
    }

    async fn ensure_pnl_plant(&self) -> Result<()> {
        self.enter("ensure_pnl_plant", "")?.pnl_plant = true;
        Ok(())
    }

    async fn disconnect_order_plant(&self) -> Result<()> {
        self.enter("disconnect_order_plant", "")?.order_plant = false;
        Ok(())
    }

    async fn disconnect_pnl_plant(&self) -> Result<()> {
        self.enter("disconnect_pnl_plant", "")?.pnl_plant = false;
        Ok(())
    }

    async fn reset_ticker(&self) -> Result<()> {
        self.note("reset_ticker", "")?;
        Ok(())
    }

    async fn resolved_account(&self) -> Option<AccountTriple> {
        self.lock().account.clone()
    }

    async fn subscribe(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.note("subscribe", &format!("{symbol} {exchange}"))?;
        Ok(())
    }

    async fn unsubscribe(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.note("unsubscribe", &format!("{symbol} {exchange}"))?;
        Ok(())
    }

    async fn subscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.note("subscribe_book", &format!("{symbol} {exchange}"))?;
        Ok(())
    }

    async fn unsubscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.note("unsubscribe_book", &format!("{symbol} {exchange}"))?;
        Ok(())
    }

    async fn subscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> Result<()> {
        self.note(
            "subscribe_time_bars",
            &format!("{symbol} {exchange} {bar_type} {period}"),
        )?;
        Ok(())
    }

    async fn unsubscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> Result<()> {
        self.note(
            "unsubscribe_time_bars",
            &format!("{symbol} {exchange} {bar_type} {period}"),
        )?;
        Ok(())
    }

    async fn subscribe_pnl(&self) -> Result<()> {
        self.enter("subscribe_pnl", "")?.pnl_plant = true;
        Ok(())
    }

    async fn subscribe_order_updates(&self) -> Result<()> {
        self.enter("subscribe_order_updates", "")?.order_plant = true;
        Ok(())
    }

    async fn subscribe_bracket_updates(&self) -> Result<()> {
        self.enter("subscribe_bracket_updates", "")?.order_plant = true;
        Ok(())
    }

    async fn get_front_month(&self, symbol: &str, exchange: &str) -> Result<FrontMonthDto> {
        let state = self.enter("get_front_month", &format!("{symbol} {exchange}"))?;
        Ok(state
            .front_months
            .get(&(symbol.to_string(), exchange.to_string()))
            .cloned()
            .unwrap_or_else(|| FrontMonthDto {
                symbol: Some(symbol.to_string()),
                exchange: Some(exchange.to_string()),
                trading_symbol: Some(format!("{symbol}H6")),
                trading_exchange: Some(exchange.to_string()),
                symbol_name: None,
                is_front_month_symbol: Some(true),
            }))
    }

    async fn get_reference_data(&self, symbol: &str, exchange: &str) -> Result<ReferenceDataDto> {
        self.note("get_reference_data", &format!("{symbol} {exchange}"))?;
        Ok(ReferenceDataDto {
            symbol: Some(symbol.to_string()),
            exchange: Some(exchange.to_string()),
            trading_symbol: Some(symbol.to_string()),
            trading_exchange: Some(exchange.to_string()),
            symbol_name: None,
            product_code: Some(symbol.chars().take(2).collect()),
            instrument_type: Some("Future".into()),
            underlying: None,
            currency: Some("USD".into()),
            expiration_date: None,
            tick_size: Some(0.25),
            point_value: Some(20.0),
            price_precision: 2,
            is_tradable: true,
        })
    }

    async fn load_ticks(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<HistoryTickDto>> {
        let state = self.enter(
            "load_ticks",
            &format!("{symbol} {exchange} {start_time_sec} {end_time_sec}"),
        )?;
        Ok(state.ticks.clone())
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
        let state = self.enter(
            "load_time_bars",
            &format!("{symbol} {exchange} {start_time_sec} {end_time_sec} {bar_type} {period}"),
        )?;
        Ok(state.bars.clone())
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
        let state = self.enter(
            "probe_time_bars",
            &format!("{symbol} {exchange} {start_time_sec} {end_time_sec} {bar_type} {period}"),
        )?;
        Ok(state.probe_rows.clone())
    }

    async fn load_orders(
        &self,
        _start_sec: i32,
        _end_sec: i32,
    ) -> Result<Vec<OrderNotificationDto>> {
        let mut state = self.enter("load_orders", "")?;
        state.order_plant = true;
        Ok(state.orders.clone())
    }

    async fn load_product_rms_info(&self) -> Result<Vec<ProductRmsInfoDto>> {
        let mut state = self.enter("load_product_rms_info", "")?;
        state.order_plant = true;
        Ok(state.product_rms.clone())
    }

    async fn load_account_rms_info(&self) -> Result<Vec<AccountRmsInfoDto>> {
        let mut state = self.enter("load_account_rms_info", "")?;
        state.order_plant = true;
        Ok(state.account_rms.clone())
    }

    async fn place_order(&self, o: &PlaceOrder) -> Result<()> {
        let mut state = self.enter("place_order", &o.user_tag)?;
        state.order_plant = true;
        Ok(())
    }

    async fn place_bracket_order(&self, o: &PlaceBracketOrder) -> Result<()> {
        let mut state = self.enter("place_bracket_order", &o.localid)?;
        state.order_plant = true;
        Ok(())
    }

    async fn adjust_bracket_stop(
        &self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()> {
        self.note(
            "adjust_bracket_stop",
            &format!("{basket_id} {ticks} {level:?}"),
        )?;
        Ok(())
    }

    async fn adjust_bracket_target(
        &self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()> {
        self.note(
            "adjust_bracket_target",
            &format!("{basket_id} {ticks} {level:?}"),
        )?;
        Ok(())
    }

    async fn cancel_order(&self, basket_id: &str) -> Result<()> {
        self.note("cancel_order", basket_id)?;
        Ok(())
    }

    async fn modify_order(&self, o: &ModifyOrder) -> Result<()> {
        self.note("modify_order", &o.basket_id)?;
        Ok(())
    }

    async fn cancel_all_orders(&self) -> Result<()> {
        self.note("cancel_all_orders", "")?;
        Ok(())
    }

    async fn poll_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.poll(Stream::Ticker, timeout).await
    }

    async fn poll_history_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.poll(Stream::History, timeout).await
    }

    async fn poll_pnl_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.poll(Stream::Pnl, timeout).await
    }

    async fn poll_order_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.poll(Stream::Order, timeout).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::LastTradeDto;

    fn last_trade(symbol: &str, price: f64) -> PlantEvent {
        PlantEvent::LastTrade(LastTradeDto {
            symbol: Some(symbol.into()),
            exchange: Some("CME".into()),
            trade_price: Some(price),
            trade_size: Some(1),
            aggressor: None,
            ssboe: None,
            usecs: None,
            ts_event_ns: None,
            is_snapshot: None,
        })
    }

    #[tokio::test]
    async fn records_calls_in_order() {
        let fake = FakeSession::new();
        fake.connect().await.unwrap();
        fake.subscribe("NQH6", "CME").await.unwrap();
        fake.unsubscribe("NQH6", "CME").await.unwrap();
        assert_eq!(
            fake.calls(),
            ["connect", "subscribe NQH6 CME", "unsubscribe NQH6 CME"]
        );
        assert_eq!(fake.count("subscribe"), 1);
    }

    #[tokio::test]
    async fn scripted_events_are_delivered_once_in_order() {
        let fake = FakeSession::new();
        fake.connect().await.unwrap();
        fake.push_event(Stream::Ticker, last_trade("NQH6", 1.0));
        fake.push_event(Stream::Ticker, last_trade("NQH6", 2.0));
        let first = fake.poll_event(Duration::ZERO).await.unwrap();
        let second = fake.poll_event(Duration::ZERO).await.unwrap();
        assert_eq!(first, Some(last_trade("NQH6", 1.0)));
        assert_eq!(second, Some(last_trade("NQH6", 2.0)));
        assert_eq!(fake.poll_event(Duration::ZERO).await.unwrap(), None);
    }

    #[tokio::test]
    async fn injected_failure_hits_exactly_the_next_call() {
        let fake = FakeSession::new();
        fake.fail_next("subscribe", FakeFailure::Rithmic("boom".into()));
        assert!(fake.subscribe("NQH6", "CME").await.is_err());
        assert!(fake.subscribe("NQH6", "CME").await.is_ok());
        assert_eq!(
            fake.count("subscribe"),
            2,
            "failed calls are still recorded"
        );
    }

    #[tokio::test]
    async fn dropped_plants_fail_polls_until_reconnect() {
        let fake = FakeSession::new();
        fake.connect().await.unwrap();
        fake.drop_plants();
        let err = fake.poll_event(Duration::ZERO).await.unwrap_err();
        assert!(matches!(err, Error::ChannelClosed { plant: "ticker" }));
        fake.disconnect().await.unwrap();
        fake.connect().await.unwrap();
        assert!(fake.poll_event(Duration::ZERO).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn order_poll_is_not_connected_until_the_order_plant_exists() {
        let fake = FakeSession::new();
        fake.connect().await.unwrap();
        let err = fake.poll_order_event(Duration::ZERO).await.unwrap_err();
        assert!(matches!(err, Error::NotConnected { plant: "order" }));
        fake.subscribe_order_updates().await.unwrap();
        assert!(fake
            .poll_order_event(Duration::ZERO)
            .await
            .unwrap()
            .is_none());
    }
}
