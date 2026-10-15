//! `FakeVenue`: a scripted in-memory [`Venue`] for testing the gateway layer
//! without Rithmic (feature `testing`; never part of the shipped wheel).
//!
//! It models only what the gateway layer depends on: events delivered per
//! stream, injected failures, a plant drop that clears on reconnect, and a
//! record of every call. Live smokes remain the authority on real plant
//! behavior.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;

use crate::backend::{AccountTriple, ModifyOrder, PlaceBracketOrder, PlaceOrder, Venue};
use crate::dto::{
    AccountRmsInfoDto, FillHistoryDto, FrontMonthDto, HistoryBarDto, HistoryTickDto,
    OrderNotificationDto, PlantEvent, ProductRmsInfoDto, ReferenceDataDto, TimeBarProbeRow,
};
use crate::error::{Error, Result};
use crate::plants::PlantSet;
use crate::streams::{StreamHub, StreamTx};

pub use crate::streams::Stream;

/// A failure to inject into the next call of a method.
#[derive(Debug, Clone)]
pub enum FakeFailure {
    /// A venue/plant error (`Error::Rithmic`): a connection issue to the pump.
    Rithmic(String),
    /// The subscription channel closed (`Error::ChannelClosed`).
    ChannelClosed(&'static str),
    /// A config / validation style error that is not a connection issue.
    Config(String),
    /// The venue definitively refused the request (`Error::Rejected`).
    Rejected(String),
    /// The request failed before anything was written (`Error::NotSent`).
    NotSent(String),
    /// The request may have reached the venue (`Error::OutcomeUnknown`).
    Unknown(String),
    /// The call panics (a bug in a handler), for testing that a caller survives it.
    Panic(String),
}

impl FakeFailure {
    fn to_error(&self) -> Error {
        match self {
            Self::Rithmic(msg) => Error::Rithmic(msg.clone()),
            Self::ChannelClosed(plant) => Error::ChannelClosed { plant },
            Self::Config(msg) => Error::Config(msg.clone()),
            Self::Rejected(msg) => Error::Rejected(msg.clone()),
            Self::NotSent(msg) => Error::NotSent(msg.clone()),
            Self::Unknown(msg) => Error::OutcomeUnknown(msg.clone()),
            Self::Panic(msg) => panic!("injected panic: {msg}"),
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
    /// Feeding side of each attached lane.
    txs: HashMap<Stream, StreamTx>,
    /// Events pushed while their plant was not attached; delivered on attach.
    pending: HashMap<Stream, VecDeque<PlantEvent>>,
    calls: Vec<String>,
    failures: HashMap<String, VecDeque<FakeFailure>>,
    /// Per-method artificial latency applied by [`FakeVenue::delay`].
    delays: HashMap<String, Duration>,
    account: Option<AccountTriple>,
    /// Gates, as on `RithmicVenue`; closed gates deny before anything else.
    trading_closed: bool,
    cancel_all_closed: bool,
    /// A `disconnect` is in progress (held for its scripted delay): order
    /// operations started now fail as not sent, as on `RithmicVenue`.
    disconnecting: bool,
    /// The order plant was disconnected on request; commands are not sent until
    /// it is ensured or subscribed again (as on `RithmicVenue`).
    order_closed: bool,
}

/// Scripted in-memory plant session.
pub struct FakeVenue {
    state: Mutex<State>,
    hub: Arc<StreamHub>,
}

impl Default for FakeVenue {
    fn default() -> Self {
        Self {
            state: Mutex::new(State::default()),
            hub: StreamHub::new(),
        }
    }
}

impl FakeVenue {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the trading and cancel-all gates, with the same denials as
    /// `RithmicVenue::with_gates`. A new fake has both open: gateway tests
    /// exercise the parent's own gate in front of it.
    pub fn with_gates(self, trading_enabled: bool, cancel_all_enabled: bool) -> Self {
        {
            let mut state = self.lock();
            state.trading_closed = !trading_enabled;
            state.cancel_all_closed = !cancel_all_enabled;
        }
        self
    }

    /// The trading gate check `RithmicVenue` makes before `what`.
    fn require_trading(&self, what: &str) -> Result<()> {
        if self.lock().trading_closed {
            return Err(Error::Gateway {
                code: "trading_disabled".into(),
                message: format!("{what} denied: trading disabled"),
            });
        }
        Ok(())
    }

    /// A state-changing order call: trading gate, then the in-progress
    /// disconnect check, then the order plant comes up (as `command_handle` does).
    fn order_op(&self, method: &str, detail: &str) -> Result<()> {
        self.require_trading(method)?;
        {
            let state = self.lock();
            if state.disconnecting {
                return Err(Error::NotSent(
                    crate::backend::DISCONNECT_IN_PROGRESS.into(),
                ));
            }
            if state.order_closed {
                return Err(Error::NotSent(crate::backend::ORDER_PLANT_CLOSED.into()));
            }
        }
        self.enter_plant(method, detail, Stream::Order)
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
            drop(state);
            return Err(failure.to_error());
        }
        Ok(state)
    }

    /// Record a call that implicitly brings up `stream`'s plant (any order or
    /// PnL call does in the real session) and attach its lane.
    fn enter_plant(&self, method: &str, detail: &str, stream: Stream) -> Result<()> {
        let mut state = self.enter(method, detail)?;
        match stream {
            Stream::Pnl => state.pnl_plant = true,
            _ => state.order_plant = true,
        }
        self.attach(&mut state, stream);
        Ok(())
    }

    /// Record a call that needs no state change.
    fn note(&self, method: &str, detail: &str) -> Result<()> {
        self.enter(method, detail).map(drop)
    }

    // --- scripting -------------------------------------------------------

    pub fn push_event(&self, stream: Stream, event: PlantEvent) {
        let mut state = self.lock();
        match state.txs.get(&stream) {
            Some(tx) => {
                tx.try_event(event);
            }
            None => state.pending.entry(stream).or_default().push_back(event),
        }
    }

    /// Attach a stream lane and deliver anything pushed before the plant existed.
    fn attach(&self, state: &mut State, stream: Stream) {
        if state.txs.contains_key(&stream) {
            return;
        }
        let tx = self.hub.attach(stream);
        for event in state.pending.remove(&stream).unwrap_or_default() {
            tx.try_event(event);
        }
        state.txs.insert(stream, tx);
    }

    fn detach(&self, state: &mut State, stream: Stream) {
        if let Some(tx) = state.txs.remove(&stream) {
            self.hub.detach(stream, tx.generation());
        }
    }

    /// Fail the next call to `method` (its name as recorded in [`Self::calls`]).
    pub fn fail_next(&self, method: &str, failure: FakeFailure) {
        self.lock()
            .failures
            .entry(method.to_string())
            .or_default()
            .push_back(failure);
    }

    /// Inject a connection issue into only this attached stream. Other plants
    /// stay up; the stream consumer sees `Error::Rithmic` after queued events.
    pub fn fail_stream(&self, stream: Stream, message: &str) {
        self.lock()
            .txs
            .get(&stream)
            .expect("fake stream must be attached before failing it")
            .fail(message.into());
    }

    /// Simulate the plants dropping: polls fail with `ChannelClosed` until the
    /// next `connect`, exactly what the parent's event pump reacts to.
    pub fn drop_plants(&self) {
        self.lock().dropped = true;
        for stream in [Stream::Ticker, Stream::History, Stream::Order] {
            self.hub.close(stream);
        }
    }

    /// Make every call to `method` take at least `delay` (a slow venue).
    pub fn set_delay(&self, method: &str, delay: Duration) {
        self.lock().delays.insert(method.to_string(), delay);
    }

    async fn delay(&self, method: &str) {
        let delay = self.lock().delays.get(method).copied();
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
    }

    pub fn set_account(&self, account: AccountTriple) {
        self.lock().account = Some(account);
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

    async fn poll(&self, stream: Stream, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.hub.poll(stream, timeout).await
    }
}

#[async_trait]
impl Venue for FakeVenue {
    async fn connect(&self) -> Result<()> {
        let mut state = self.enter("connect", "")?;
        state.connected = true;
        state.order_closed = false;
        state.dropped = false;
        self.attach(&mut state, Stream::Ticker);
        self.attach(&mut state, Stream::History);
        Ok(())
    }

    async fn disconnect(&self) -> Result<()> {
        self.lock().disconnecting = true;
        self.delay("disconnect").await;
        let entered = self.enter("disconnect", "");
        let mut state = match entered {
            Ok(state) => state,
            Err(err) => {
                self.lock().disconnecting = false;
                return Err(err);
            }
        };
        state.disconnecting = false;
        state.connected = false;
        state.pnl_plant = false;
        state.order_plant = false;
        for stream in Stream::ALL {
            self.detach(&mut state, stream);
        }
        Ok(())
    }

    async fn request_plants(&self, extra: PlantSet) -> Result<()> {
        self.note("request_plants", &format!("{extra:?}"))?;
        Ok(())
    }

    async fn ensure_order_plant(&self) -> Result<()> {
        self.require_trading("ensure_order_plant")?;
        let mut state = self.enter("ensure_order_plant", "")?;
        state.order_plant = true;
        state.order_closed = false;
        self.attach(&mut state, Stream::Order);
        Ok(())
    }

    async fn ensure_pnl_plant(&self) -> Result<()> {
        let mut state = self.enter("ensure_pnl_plant", "")?;
        state.pnl_plant = true;
        self.attach(&mut state, Stream::Pnl);
        Ok(())
    }

    async fn disconnect_order_plant(&self) -> Result<()> {
        self.delay("disconnect_order_plant").await;
        let mut state = self.enter("disconnect_order_plant", "")?;
        state.order_plant = false;
        state.order_closed = true;
        self.detach(&mut state, Stream::Order);
        Ok(())
    }

    async fn disconnect_pnl_plant(&self) -> Result<()> {
        let mut state = self.enter("disconnect_pnl_plant", "")?;
        state.pnl_plant = false;
        self.detach(&mut state, Stream::Pnl);
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
        self.enter_plant("subscribe_pnl", "", Stream::Pnl)?;
        Ok(())
    }

    async fn subscribe_order_updates(&self) -> Result<()> {
        self.require_trading("subscribe_order_updates")?;
        self.enter_plant("subscribe_order_updates", "", Stream::Order)?;
        self.lock().order_closed = false;
        Ok(())
    }

    async fn subscribe_bracket_updates(&self) -> Result<()> {
        self.require_trading("subscribe_bracket_updates")?;
        self.enter_plant("subscribe_bracket_updates", "", Stream::Order)?;
        self.lock().order_closed = false;
        Ok(())
    }

    async fn get_front_month(&self, symbol: &str, exchange: &str) -> Result<FrontMonthDto> {
        self.note("get_front_month", &format!("{symbol} {exchange}"))?;
        self.delay("get_front_month").await;
        Ok(FrontMonthDto {
            symbol: Some(symbol.to_string()),
            exchange: Some(exchange.to_string()),
            trading_symbol: Some(format!("{symbol}H6")),
            trading_exchange: Some(exchange.to_string()),
            symbol_name: None,
            is_front_month_symbol: Some(true),
        })
    }

    async fn get_reference_data(&self, symbol: &str, exchange: &str) -> Result<ReferenceDataDto> {
        self.note("get_reference_data", &format!("{symbol} {exchange}"))?;
        self.delay("get_reference_data").await;
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
        self.note(
            "load_ticks",
            &format!("{symbol} {exchange} {start_time_sec} {end_time_sec}"),
        )?;
        self.delay("load_ticks").await;
        Ok(Vec::new())
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
        self.note(
            "load_time_bars",
            &format!("{symbol} {exchange} {start_time_sec} {end_time_sec} {bar_type} {period}"),
        )?;
        self.delay("load_time_bars").await;
        Ok(Vec::new())
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
        self.note(
            "probe_time_bars",
            &format!("{symbol} {exchange} {start_time_sec} {end_time_sec} {bar_type} {period}"),
        )?;
        Ok(Vec::new())
    }

    async fn load_orders(
        &self,
        _start_sec: i32,
        _end_sec: i32,
    ) -> Result<Vec<OrderNotificationDto>> {
        self.require_trading("load_orders")?;
        self.enter_plant("load_orders", "", Stream::Order)?;
        Ok(Vec::new())
    }

    async fn load_product_rms_info(&self) -> Result<Vec<ProductRmsInfoDto>> {
        self.require_trading("load_product_rms_info")?;
        self.enter_plant("load_product_rms_info", "", Stream::Order)?;
        Ok(Vec::new())
    }

    async fn load_account_rms_info(&self) -> Result<Vec<AccountRmsInfoDto>> {
        self.require_trading("load_account_rms_info")?;
        self.enter_plant("load_account_rms_info", "", Stream::Order)?;
        Ok(Vec::new())
    }

    async fn load_fill_history(&self, start: i32, finish: i32) -> Result<Vec<FillHistoryDto>> {
        self.require_trading("load_fill_history")?;
        self.enter_plant(
            "load_fill_history",
            &format!("{start} {finish}"),
            Stream::Order,
        )?;
        self.delay("load_fill_history").await;
        Ok(Vec::new())
    }

    async fn load_order_history(&self, date: &str) -> Result<Vec<OrderNotificationDto>> {
        self.require_trading("load_order_history")?;
        self.enter_plant("load_order_history", date, Stream::Order)?;
        Ok(Vec::new())
    }

    async fn load_order_history_dates(&self) -> Result<Vec<String>> {
        self.require_trading("load_order_history_dates")?;
        self.enter_plant("load_order_history_dates", "", Stream::Order)?;
        Ok(Vec::new())
    }

    async fn place_order(&self, o: &PlaceOrder) -> Result<()> {
        self.order_op("place_order", &o.user_tag)
    }

    async fn place_bracket_order(&self, o: &PlaceBracketOrder) -> Result<()> {
        self.order_op("place_bracket_order", &o.localid)
    }

    async fn adjust_bracket_stop(
        &self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()> {
        self.order_op(
            "adjust_bracket_stop",
            &format!("{basket_id} {ticks} {level:?}"),
        )
    }

    async fn adjust_bracket_target(
        &self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()> {
        self.order_op(
            "adjust_bracket_target",
            &format!("{basket_id} {ticks} {level:?}"),
        )
    }

    async fn cancel_order(&self, basket_id: &str) -> Result<()> {
        self.order_op("cancel_order", basket_id)
    }

    async fn modify_order(&self, o: &ModifyOrder) -> Result<()> {
        self.order_op("modify_order", &o.basket_id)
    }

    async fn cancel_all_orders(&self) -> Result<()> {
        if self.lock().cancel_all_closed {
            return Err(Error::Gateway {
                code: "cancel_all_denied".into(),
                message: "cancel_all_orders denied: cancel_all disabled".into(),
            });
        }
        if self.lock().disconnecting {
            return Err(Error::NotSent(
                crate::backend::DISCONNECT_IN_PROGRESS.into(),
            ));
        }
        self.enter_plant("cancel_all_orders", "", Stream::Order)
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

    async fn next_events(
        &self,
        stream: Stream,
        max: usize,
        timeout: Duration,
    ) -> Result<Vec<PlantEvent>> {
        self.hub.next_events(stream, max, timeout).await
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
        let fake = FakeVenue::new();
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
        let fake = FakeVenue::new();
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
        let fake = FakeVenue::new();
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
        let fake = FakeVenue::new();
        fake.connect().await.unwrap();
        fake.drop_plants();
        let err = fake.poll_event(Duration::ZERO).await.unwrap_err();
        assert!(matches!(err, Error::ChannelClosed { plant: "ticker" }));
        fake.disconnect().await.unwrap();
        fake.connect().await.unwrap();
        assert!(fake.poll_event(Duration::ZERO).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn failing_one_stream_leaves_the_other_plants_up() {
        let fake = FakeVenue::new();
        fake.connect().await.unwrap();
        fake.ensure_pnl_plant().await.unwrap();
        fake.ensure_order_plant().await.unwrap();
        fake.clear_calls();

        fake.fail_stream(Stream::Ticker, "ticker connection lost");
        assert!(matches!(
            fake.poll_event(Duration::ZERO).await,
            Err(Error::Rithmic(message)) if message == "ticker connection lost"
        ));
        for stream in [Stream::History, Stream::Pnl, Stream::Order] {
            assert!(fake.poll(stream, Duration::ZERO).await.unwrap().is_none());
        }
        assert!(fake.calls().is_empty(), "injection does not rebuild plants");
    }

    #[tokio::test]
    async fn order_poll_is_not_connected_until_the_order_plant_exists() {
        let fake = FakeVenue::new();
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
