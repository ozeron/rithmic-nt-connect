//! Multi-plant Rithmic session facade (Phase 2: MD + history + PnL + orders).
//!
//! [`PlantSet`] chooses ticker/history/PnL on [`RithmicSession::connect`].
//! The order plant stays lazy via [`RithmicSession::ensure_order_plant`].

use std::future::Future;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use rithmic_rs::{
    ConnectStrategy, ManualOrAutoEntry, OrderSide, OrderType, RithmicAccount,
    RithmicCancelAllOrders, RithmicCancelOrder, RithmicConfig, RithmicHistoryPlant,
    RithmicHistoryPlantHandle, RithmicModifyOrder, RithmicOrder, RithmicOrderPlant,
    RithmicOrderPlantHandle, RithmicPnlPlant, RithmicPnlPlantHandle, RithmicTickerPlant,
    RithmicTickerPlantHandle, TimeBarType, TimeInForce,
};
use rithmic_rs::rti::request_time_bar_update;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::error::TryRecvError;

use crate::config::SessionConfig;
use crate::dto::{
    FrontMonthDto, HistoryBarDto, HistoryTickDto, PlantEvent, ReferenceDataDto, TimeBarProbeRow,
};
use crate::error::{Error, Result};
use crate::history::{
    DEFAULT_TICK_SLICE_SECS, bar_replay_index, bar_slice_secs, dedup_bars, dedup_ticks,
    load_sliced, parse_time_bar_type,
};
use crate::plants::PlantSet;

fn noop_waker() -> Waker {
    fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    fn wake(_: *const ()) {}
    fn wake_by_ref(_: *const ()) {}
    fn drop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
    // SAFETY: noop waker; never used to wake a task.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

/// Poll a future once without waiting (for non-blocking subscription drains).
fn now_or_never<F: Future>(fut: F) -> Option<F::Output> {
    let mut fut = std::pin::pin!(fut);
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

struct TickerPlant {
    _plant: RithmicTickerPlant,
    handle: RithmicTickerPlantHandle,
}

struct HistoryPlant {
    _plant: RithmicHistoryPlant,
    handle: RithmicHistoryPlantHandle,
}

struct PnlPlant {
    _plant: RithmicPnlPlant,
    handle: RithmicPnlPlantHandle,
}

struct OrderPlant {
    _plant: RithmicOrderPlant,
    handle: RithmicOrderPlantHandle,
}

/// Connected multi-plant session used by the Python adapter.
pub struct RithmicSession {
    config: SessionConfig,
    plants: PlantSet,
    ticker: Option<TickerPlant>,
    history: Option<HistoryPlant>,
    pnl: Option<PnlPlant>,
    order: Option<OrderPlant>,
}

impl std::fmt::Debug for RithmicSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RithmicSession")
            .field("config", &self.config)
            .field("plants", &self.plants)
            .field("ticker_connected", &self.ticker.is_some())
            .field("history_connected", &self.history.is_some())
            .field("pnl_connected", &self.pnl.is_some())
            .field("order_connected", &self.order.is_some())
            .finish()
    }
}

impl RithmicSession {
    /// Create a disconnected **market-data** session (ticker + history).
    pub fn new(config: SessionConfig) -> Self {
        Self::with_plants(config, PlantSet::MARKET_DATA)
    }

    /// Create a disconnected session that will attach `plants` on connect.
    pub fn with_plants(config: SessionConfig, plants: PlantSet) -> Self {
        Self {
            config,
            plants,
            ticker: None,
            history: None,
            pnl: None,
            order: None,
        }
    }

    /// Plants requested at construction.
    pub fn plants(&self) -> PlantSet {
        self.plants
    }

    /// Union extra plants into the set. If already connected, attach any
    /// newly requested plants that are not online yet.
    pub async fn request_plants(&mut self, extra: PlantSet) -> Result<()> {
        self.plants.ticker |= extra.ticker;
        self.plants.history |= extra.history;
        self.plants.pnl |= extra.pnl;

        let connected =
            self.ticker.is_some() || self.history.is_some() || self.pnl.is_some();
        if !connected {
            return Ok(());
        }

        let rc = self.config.to_rithmic_config()?;
        // Attach without connect_or_rollback: a failed add must not tear down
        // plants that were already live (and their subscription intent).
        if self.plants.ticker && self.ticker.is_none() {
            match connect_ticker(&rc).await {
                Ok(plant) => self.ticker = Some(plant),
                Err(err) => return Err(err),
            }
        }
        if self.plants.history && self.history.is_none() {
            match connect_history(&rc).await {
                Ok(plant) => self.history = Some(plant),
                Err(err) => return Err(err),
            }
        }
        if self.plants.pnl && self.pnl.is_none() && self.config.account().is_some() {
            self.ensure_pnl_plant().await?;
        }
        Ok(())
    }

    /// Session configuration.
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    /// Connect plants selected by [`PlantSet`].
    ///
    /// Uses [`ConnectStrategy::Retry`]. The order plant is **not** connected here —
    /// call [`Self::ensure_order_plant`] (or any place/cancel/subscribe order API).
    /// Partial failure disconnects whatever already logged in.
    pub async fn connect(&mut self) -> Result<()> {
        if self.ticker.is_some() || self.history.is_some() || self.pnl.is_some() {
            return Err(Error::Session("already connected".into()));
        }

        let rc = self.config.to_rithmic_config()?;

        if self.plants.ticker {
            self.connect_or_rollback(connect_ticker(&rc), |s, p| s.ticker = Some(p))
                .await?;
        }

        if self.plants.history {
            self.connect_or_rollback(connect_history(&rc), |s, p| s.history = Some(p))
                .await?;
        }

        if self.plants.pnl {
            if let Some(account) = self.config.account() {
                self.connect_or_rollback(connect_pnl(&rc, &account), |s, p| s.pnl = Some(p))
                    .await?;
            }
        }

        self.order = None;
        Ok(())
    }

    async fn connect_or_rollback<T, F>(
        &mut self,
        fut: F,
        assign: impl FnOnce(&mut Self, T),
    ) -> Result<()>
    where
        F: Future<Output = Result<T>>,
    {
        match fut.await {
            Ok(plant) => {
                assign(self, plant);
                Ok(())
            }
            Err(err) => {
                let _ = self.disconnect().await;
                Err(err)
            }
        }
    }

    /// Connect + login the order plant (idempotent). Requires account triple.
    pub async fn ensure_order_plant(&mut self) -> Result<()> {
        if self.order.is_some() {
            return Ok(());
        }
        if self.ticker.is_none() {
            return Err(Error::NotConnected { plant: "ticker" });
        }
        let account = self.config.account().ok_or_else(|| {
            Error::Config("order plant requires account_id/fcm_id/ib_id".into())
        })?;
        let rc = self.config.to_rithmic_config()?;
        let order_plant = RithmicOrderPlant::connect(&rc, ConnectStrategy::Retry).await?;
        let order_handle = order_plant.get_handle(&account);
        check_response(order_handle.login().await?, "order login")?;
        self.order = Some(OrderPlant {
            _plant: order_plant,
            handle: order_handle,
        });
        Ok(())
    }

    /// Gracefully disconnect connected plants (order first, then PnL, history, ticker).
    pub async fn disconnect(&mut self) -> Result<()> {
        if let Some(order) = self.order.take() {
            let _ = order.handle.disconnect().await;
        }
        if let Some(pnl) = self.pnl.take() {
            let _ = pnl.handle.disconnect().await;
        }
        if let Some(history) = self.history.take() {
            let _ = history.handle.disconnect().await;
        }
        if let Some(ticker) = self.ticker.take() {
            let _ = ticker.handle.disconnect().await;
        }
        Ok(())
    }

    /// Subscribe to LastTrade + BBO for `symbol` / `exchange`.
    pub async fn subscribe(&self, symbol: &str, exchange: &str) -> Result<()> {
        let handle = self.ticker_handle()?;
        check_response(handle.subscribe(symbol, exchange).await?, "subscribe")?;
        Ok(())
    }

    /// Unsubscribe LastTrade + BBO.
    pub async fn unsubscribe(&self, symbol: &str, exchange: &str) -> Result<()> {
        let handle = self.ticker_handle()?;
        check_response(handle.unsubscribe(symbol, exchange).await?, "unsubscribe")?;
        Ok(())
    }

    /// Resolve front-month contract for a root symbol.
    pub async fn get_front_month(&self, symbol: &str, exchange: &str) -> Result<FrontMonthDto> {
        let handle = self.ticker_handle()?;
        let resp = handle
            .get_front_month_contract(symbol, exchange, false)
            .await?;
        check_response_ref(&resp, "get_front_month")?;
        FrontMonthDto::from_response(&resp).ok_or_else(|| {
            Error::Protocol("unexpected front-month response variant".into())
        })
    }

    /// Subscribe to order-book summary (level aggregated).
    pub async fn subscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()> {
        let handle = self.ticker_handle()?;
        check_response(
            handle
                .subscribe_order_book_summary(symbol, exchange)
                .await?,
            "subscribe_order_book_summary",
        )?;
        Ok(())
    }

    /// Unsubscribe order-book summary (does not drop LastTrade/BBO).
    pub async fn unsubscribe_order_book_summary(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<()> {
        let handle = self.ticker_handle()?;
        check_response(
            handle
                .unsubscribe_order_book_summary(symbol, exchange)
                .await?,
            "unsubscribe_order_book_summary",
        )?;
        Ok(())
    }

    /// Fetch instrument reference data for a trading symbol/exchange.
    pub async fn get_reference_data(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<ReferenceDataDto> {
        let handle = self.ticker_handle()?;
        let resp = handle.get_reference_data(symbol, exchange).await?;
        check_response_ref(&resp, "get_reference_data")?;
        ReferenceDataDto::from_response(&resp).ok_or_else(|| {
            Error::Session(format!(
                "get_reference_data: unexpected response {:?}",
                resp.message
            ))
        })
    }

    /// Load ticks in `[start_time_sec, end_time_sec]` (sliced, retried, deduped).
    pub async fn load_ticks_all(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<HistoryTickDto>> {
        let handle = self.history_handle()?;
        load_sliced(
            start_time_sec,
            end_time_sec,
            DEFAULT_TICK_SLICE_SECS,
            |slice_start, slice_end| {
                let handle = handle;
                let symbol = symbol;
                let exchange = exchange;
                async move {
                    load_tick_slice(handle, symbol, exchange, slice_start, slice_end).await
                }
            },
            |t| t.ts_event_ns.unwrap_or(0),
            dedup_ticks,
        )
        .await
    }

    /// Load time bars in the window (sliced, retried, deduped).
    pub async fn load_time_bars_all(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: TimeBarType,
        bar_type_period: i32,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<HistoryBarDto>> {
        let handle = self.history_handle()?;
        load_sliced(
            start_time_sec,
            end_time_sec,
            bar_slice_secs(bar_type, bar_type_period),
            |slice_start, slice_end| {
                let handle = handle;
                let symbol = symbol;
                let exchange = exchange;
                async move {
                    load_bar_slice(
                        handle,
                        symbol,
                        exchange,
                        bar_type,
                        bar_type_period,
                        slice_start,
                        slice_end,
                    )
                    .await
                }
            },
            |b| b.ts_event_ns.unwrap_or(0),
            dedup_bars,
        )
        .await
    }

    /// One history-plant time-bar replay with every raw row (including dropped).
    pub async fn probe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: TimeBarType,
        bar_type_period: i32,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<TimeBarProbeRow>> {
        let handle = self.history_handle()?;
        let responses = handle
            .load_time_bars_all(
                symbol.to_string(),
                exchange.to_string(),
                bar_type,
                bar_type_period,
                start_time_sec,
                end_time_sec,
            )
            .await?;
        Ok(responses.iter().map(TimeBarProbeRow::from_response).collect())
    }

    /// Live time-bar feed on the history plant (ack only; bars via `poll_history_event`).
    pub async fn subscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> Result<()> {
        self.time_bar_updates(
            symbol,
            exchange,
            bar_type,
            period,
            request_time_bar_update::Request::Subscribe,
            "subscribe_time_bars",
        )
        .await
    }

    /// Stop a live time-bar feed.
    pub async fn unsubscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> Result<()> {
        self.time_bar_updates(
            symbol,
            exchange,
            bar_type,
            period,
            request_time_bar_update::Request::Unsubscribe,
            "unsubscribe_time_bars",
        )
        .await
    }

    async fn time_bar_updates(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
        request: request_time_bar_update::Request,
        ctx: &str,
    ) -> Result<()> {
        let handle = self.history_handle()?;
        let bt = update_bar_type(bar_type)?;
        check_response(
            handle
                .subscribe_time_bar_updates(symbol, exchange, bt, period, request)
                .await?,
            ctx,
        )
    }

    /// Non-blocking poll of the history-plant subscription channel (live TimeBar).
    pub fn poll_history_event(&mut self) -> Result<Option<PlantEvent>> {
        let handle = self.history_handle_mut()?;
        map_broadcast_poll(
            "history",
            now_or_never(handle.subscription_receiver.recv()),
        )
    }

    /// Subscribe to PnL updates and request a snapshot when account is configured.
    pub async fn subscribe_pnl(&self) -> Result<()> {
        let handle = self.pnl_handle()?;
        check_response(handle.subscribe_pnl_updates().await?, "subscribe_pnl")?;
        check_response(
            handle.get_pnl_position_snapshot().await?,
            "pnl_snapshot",
        )?;
        Ok(())
    }

    /// Subscribe to order plant notifications (Rithmic + exchange).
    pub async fn subscribe_order_updates(&mut self) -> Result<()> {
        self.ensure_order_plant().await?;
        let handle = self.order_handle()?;
        if let Err(e) = check_response(
            handle.subscribe_order_updates().await?,
            "subscribe_order_updates",
        ) {
            let _ = self.disconnect_order_plant().await;
            return Err(e);
        }
        Ok(())
    }

    /// Connect + login the PnL plant (idempotent). Requires account triple.
    pub async fn ensure_pnl_plant(&mut self) -> Result<()> {
        if self.pnl.is_some() {
            return Ok(());
        }
        if self.ticker.is_none() {
            return Err(Error::NotConnected { plant: "ticker" });
        }
        let account = self.config.account().ok_or_else(|| {
            Error::Config("pnl plant requires account_id/fcm_id/ib_id".into())
        })?;
        let rc = self.config.to_rithmic_config()?;
        let pnl_plant = RithmicPnlPlant::connect(&rc, ConnectStrategy::Retry).await?;
        let pnl_handle = pnl_plant.get_handle(&account);
        check_response(pnl_handle.login().await?, "pnl login")?;
        self.pnl = Some(PnlPlant {
            _plant: pnl_plant,
            handle: pnl_handle,
        });
        Ok(())
    }

    /// Disconnect only the order plant (leaves ticker/history/PnL connected).
    pub async fn disconnect_order_plant(&mut self) -> Result<()> {
        if let Some(order) = self.order.take() {
            let _ = order.handle.disconnect().await;
        }
        Ok(())
    }

    /// Disconnect only the PnL plant (leaves ticker/history/order connected).
    pub async fn disconnect_pnl_plant(&mut self) -> Result<()> {
        if let Some(pnl) = self.pnl.take() {
            let _ = pnl.handle.disconnect().await;
        }
        Ok(())
    }

    /// Place a new order on the order plant.
    #[allow(clippy::too_many_arguments)]
    pub async fn place_order(
        &mut self,
        symbol: &str,
        exchange: &str,
        side: &str,
        price_type: &str,
        quantity: i32,
        user_tag: &str,
        price: Option<f64>,
        trigger_price: Option<f64>,
        duration: &str,
        trail_by_ticks: Option<i32>,
        trail_by_price_id: Option<i32>,
    ) -> Result<()> {
        self.ensure_order_plant().await?;
        place_order_on(
            self.order_handle()?,
            symbol,
            exchange,
            side,
            price_type,
            quantity,
            user_tag,
            price,
            trigger_price,
            duration,
            trail_by_ticks,
            trail_by_price_id,
        )
        .await
    }

    /// Cancel an order by basket id.
    pub async fn cancel_order(&mut self, basket_id: &str) -> Result<()> {
        self.ensure_order_plant().await?;
        cancel_order_on(self.order_handle()?, basket_id).await
    }

    /// Modify an existing order.
    #[allow(clippy::too_many_arguments)]
    pub async fn modify_order(
        &mut self,
        basket_id: &str,
        symbol: &str,
        exchange: &str,
        quantity: i32,
        price_type: &str,
        price: Option<f64>,
        trigger_price: Option<f64>,
        trail_by_ticks: Option<i32>,
    ) -> Result<()> {
        self.ensure_order_plant().await?;
        modify_order_on(
            self.order_handle()?,
            basket_id,
            symbol,
            exchange,
            quantity,
            price_type,
            price,
            trigger_price,
            trail_by_ticks,
        )
        .await
    }

    /// Cancel all working orders on the account.
    pub async fn cancel_all_orders(&mut self) -> Result<()> {
        self.ensure_order_plant().await?;
        cancel_all_orders_on(self.order_handle()?).await
    }

    /// Non-blocking poll of the next ticker-plant subscription message.
    pub fn poll_event(&mut self) -> Result<Option<PlantEvent>> {
        let handle = self.ticker_handle_mut()?;
        match handle.subscription_receiver.try_recv() {
            Ok(resp) => {
                if let Some(err) = &resp.error {
                    if err.is_connection_issue() {
                        return Err(Error::Rithmic(err.to_string()));
                    }
                }
                Ok(Some(PlantEvent::from(&resp)))
            }
            Err(TryRecvError::Empty) | Err(TryRecvError::Lagged(_)) => Ok(None),
            Err(TryRecvError::Closed) => Err(Error::ChannelClosed { plant: "ticker" }),
        }
    }

    /// Non-blocking poll of the next PnL-plant subscription message.
    pub fn poll_pnl_event(&mut self) -> Result<Option<PlantEvent>> {
        let Some(pnl) = self.pnl.as_mut() else {
            return Ok(None);
        };
        map_broadcast_poll(
            "pnl",
            now_or_never(pnl.handle.subscription_receiver.recv()),
        )
    }

    /// Non-blocking poll of the next order-plant subscription message.
    pub fn poll_order_event(&mut self) -> Result<Option<PlantEvent>> {
        let handle = self.order_handle_mut()?;
        map_broadcast_poll(
            "order",
            now_or_never(handle.subscription_receiver.recv()),
        )
    }

    /// Blocking receive of the next ticker message (async).
    pub async fn recv_event(&mut self) -> Result<PlantEvent> {
        let handle = self.ticker_handle_mut()?;
        let resp = handle
            .subscription_receiver
            .recv()
            .await
            .map_err(|_| Error::ChannelClosed { plant: "ticker" })?;
        if let Some(err) = &resp.error {
            if err.is_connection_issue() {
                return Err(Error::Rithmic(err.to_string()));
            }
        }
        Ok(PlantEvent::from(&resp))
    }

    fn ticker_handle(&self) -> Result<&RithmicTickerPlantHandle> {
        self.ticker
            .as_ref()
            .map(|t| &t.handle)
            .ok_or(Error::NotConnected { plant: "ticker" })
    }

    fn ticker_handle_mut(&mut self) -> Result<&mut RithmicTickerPlantHandle> {
        self.ticker
            .as_mut()
            .map(|t| &mut t.handle)
            .ok_or(Error::NotConnected { plant: "ticker" })
    }

    fn history_handle(&self) -> Result<&RithmicHistoryPlantHandle> {
        self.history
            .as_ref()
            .map(|h| &h.handle)
            .ok_or(Error::NotConnected { plant: "history" })
    }

    fn history_handle_mut(&mut self) -> Result<&mut RithmicHistoryPlantHandle> {
        self.history
            .as_mut()
            .map(|h| &mut h.handle)
            .ok_or(Error::NotConnected { plant: "history" })
    }

    fn pnl_handle(&self) -> Result<&RithmicPnlPlantHandle> {
        self.pnl
            .as_ref()
            .map(|p| &p.handle)
            .ok_or(Error::NotConnected { plant: "pnl" })
    }

    fn order_handle(&self) -> Result<&RithmicOrderPlantHandle> {
        self.order
            .as_ref()
            .map(|o| &o.handle)
            .ok_or(Error::NotConnected { plant: "order" })
    }

    fn order_handle_mut(&mut self) -> Result<&mut RithmicOrderPlantHandle> {
        self.order
            .as_mut()
            .map(|o| &mut o.handle)
            .ok_or(Error::NotConnected { plant: "order" })
    }

    /// Clone a command handle for order I/O outside the session lock.
    ///
    /// The clone shares the plant command channel; its subscription receiver is a
    /// separate resubscribe and is unused for place/cancel/modify.
    pub fn clone_order_handle(&self) -> Result<RithmicOrderPlantHandle> {
        Ok(self.order_handle()?.clone())
    }
}

/// Place an order on an already-connected order-plant handle (no session lock).
#[allow(clippy::too_many_arguments)]
pub async fn place_order_on(
    handle: &RithmicOrderPlantHandle,
    symbol: &str,
    exchange: &str,
    side: &str,
    price_type: &str,
    quantity: i32,
    user_tag: &str,
    price: Option<f64>,
    trigger_price: Option<f64>,
    duration: &str,
    trail_by_ticks: Option<i32>,
    trail_by_price_id: Option<i32>,
) -> Result<()> {
    let side: OrderSide = side
        .parse()
        .map_err(|e| Error::Config(format!("invalid order side: {e}")))?;
    let price_type: OrderType = price_type
        .parse()
        .map_err(|e| Error::Config(format!("invalid price type: {e}")))?;
    let duration: TimeInForce = if duration.is_empty() {
        TimeInForce::Day
    } else {
        duration
            .parse()
            .map_err(|e| Error::Config(format!("invalid duration: {e}")))?
    };

    let mut builder = RithmicOrder::new()
        .symbol(symbol)
        .exchange(exchange)
        .quantity(quantity)
        .transaction_type(side)
        .price_type(price_type)
        .user_tag(user_tag)
        .duration(duration)
        .manual_or_auto(ManualOrAutoEntry::Auto);
    if let Some(p) = price {
        builder = builder.price(p);
    }
    if let Some(t) = trigger_price {
        builder = builder.trigger_price(t);
    }
    if let Some(ticks) = trail_by_ticks {
        let price_id = trail_by_price_id.unwrap_or(1);
        builder = builder.trailing_stop_by(ticks, price_id);
    }
    let order = builder.build()?;
    check_responses(&handle.place_order(order).await?, "place_order")
}

/// Cancel an order on an already-connected handle (no session lock).
pub async fn cancel_order_on(handle: &RithmicOrderPlantHandle, basket_id: &str) -> Result<()> {
    let cancel = RithmicCancelOrder::new()
        .id(basket_id)
        .manual_or_auto(ManualOrAutoEntry::Auto)
        .build()?;
    check_responses(&handle.cancel_order(cancel).await?, "cancel_order")
}

/// Modify an order on an already-connected handle (no session lock).
#[allow(clippy::too_many_arguments)]
pub async fn modify_order_on(
    handle: &RithmicOrderPlantHandle,
    basket_id: &str,
    symbol: &str,
    exchange: &str,
    quantity: i32,
    price_type: &str,
    price: Option<f64>,
    trigger_price: Option<f64>,
    trail_by_ticks: Option<i32>,
) -> Result<()> {
    let price_type: OrderType = price_type
        .parse()
        .map_err(|e| Error::Config(format!("invalid price type: {e}")))?;

    let mut builder = RithmicModifyOrder::new()
        .id(basket_id)
        .symbol(symbol)
        .exchange(exchange)
        .quantity(quantity)
        .price_type(price_type)
        .manual_or_auto(ManualOrAutoEntry::Auto);
    if let Some(p) = price {
        builder = builder.price(p);
    }
    if let Some(t) = trigger_price {
        builder = builder.trigger_price(t);
    }
    if let Some(ticks) = trail_by_ticks {
        builder = builder.trail_by_ticks(ticks);
    }
    let order = builder.build()?;
    check_responses(&handle.modify_order(order).await?, "modify_order")
}

/// Cancel all orders on an already-connected handle (no session lock).
pub async fn cancel_all_orders_on(handle: &RithmicOrderPlantHandle) -> Result<()> {
    let cmd = RithmicCancelAllOrders::new()
        .manual_or_auto(ManualOrAutoEntry::Auto)
        .build()?;
    check_response(handle.cancel_all_orders(cmd).await?, "cancel_all_orders")
}

async fn connect_ticker(rc: &RithmicConfig) -> Result<TickerPlant> {
    let plant = RithmicTickerPlant::connect(rc, ConnectStrategy::Retry).await?;
    let handle = plant.get_handle();
    check_response(handle.login().await?, "ticker login")?;
    Ok(TickerPlant {
        _plant: plant,
        handle,
    })
}

async fn connect_history(rc: &RithmicConfig) -> Result<HistoryPlant> {
    let plant = RithmicHistoryPlant::connect(rc, ConnectStrategy::Retry).await?;
    let handle = plant.get_handle();
    check_response(handle.login().await?, "history login")?;
    Ok(HistoryPlant {
        _plant: plant,
        handle,
    })
}

async fn connect_pnl(rc: &RithmicConfig, account: &RithmicAccount) -> Result<PnlPlant> {
    let plant = RithmicPnlPlant::connect(rc, ConnectStrategy::Retry).await?;
    let handle = plant.get_handle(account);
    check_response(handle.login().await?, "pnl login")?;
    Ok(PnlPlant {
        _plant: plant,
        handle,
    })
}

async fn load_tick_slice(
    handle: &RithmicHistoryPlantHandle,
    symbol: &str,
    exchange: &str,
    start_time_sec: i32,
    end_time_sec: i32,
) -> Result<Vec<HistoryTickDto>> {
    let responses = handle
        .load_ticks_all(
            symbol.to_string(),
            exchange.to_string(),
            start_time_sec,
            end_time_sec,
        )
        .await?;
    // Multi-response replay: keep valid rows; if nothing parsed and the plant
    // reported an error on any frame, surface that instead of Ok([]).
    collect_history_rows(responses, "load_ticks", HistoryTickDto::from_response)
}

async fn load_bar_slice(
    handle: &RithmicHistoryPlantHandle,
    symbol: &str,
    exchange: &str,
    bar_type: TimeBarType,
    bar_type_period: i32,
    start_time_sec: i32,
    end_time_sec: i32,
) -> Result<Vec<HistoryBarDto>> {
    let start_index = bar_replay_index(bar_type, start_time_sec);
    let finish_index = bar_replay_index(bar_type, end_time_sec);
    let responses = handle
        .load_time_bars_all(
            symbol.to_string(),
            exchange.to_string(),
            bar_type,
            bar_type_period,
            start_index,
            finish_index,
        )
        .await?;
    collect_history_rows(responses, "load_time_bars", HistoryBarDto::from_response)
}

fn collect_history_rows<T>(
    responses: Vec<rithmic_rs::RithmicResponse>,
    ctx: &str,
    parse: impl Fn(&rithmic_rs::RithmicResponse) -> Option<T>,
) -> Result<Vec<T>> {
    let mut rows = Vec::new();
    let mut first_err: Option<String> = None;
    for resp in &responses {
        if let Some(err) = &resp.error {
            if first_err.is_none() {
                first_err = Some(err.to_string());
            }
            continue;
        }
        if let Some(row) = parse(resp) {
            rows.push(row);
        }
    }
    if rows.is_empty() {
        if let Some(err) = first_err {
            return Err(Error::Rithmic(format!("{ctx}: {err}")));
        }
    }
    Ok(rows)
}

fn update_bar_type(bar_type: i32) -> Result<request_time_bar_update::BarType> {
    Ok(match parse_time_bar_type(bar_type)? {
        TimeBarType::SecondBar => request_time_bar_update::BarType::SecondBar,
        TimeBarType::MinuteBar => request_time_bar_update::BarType::MinuteBar,
        TimeBarType::DailyBar => request_time_bar_update::BarType::DailyBar,
        TimeBarType::WeeklyBar => request_time_bar_update::BarType::WeeklyBar,
        other => {
            return Err(Error::Protocol(format!(
                "unsupported time bar type {other:?}"
            )));
        }
    })
}

fn map_broadcast_poll(
    plant: &'static str,
    polled: Option<std::result::Result<rithmic_rs::RithmicResponse, RecvError>>,
) -> Result<Option<PlantEvent>> {
    match polled {
        Some(Ok(resp)) => {
            if let Some(err) = &resp.error {
                if err.is_connection_issue() {
                    return Err(Error::Rithmic(err.to_string()));
                }
            }
            Ok(Some(PlantEvent::from(&resp)))
        }
        Some(Err(RecvError::Lagged(skipped))) => Err(Error::ChannelLagged { plant, skipped }),
        Some(Err(RecvError::Closed)) => Err(Error::ChannelClosed { plant }),
        None => Ok(None),
    }
}

fn check_response(resp: rithmic_rs::RithmicResponse, ctx: &str) -> Result<()> {
    check_response_ref(&resp, ctx)
}

fn check_response_ref(resp: &rithmic_rs::RithmicResponse, ctx: &str) -> Result<()> {
    if let Some(err) = &resp.error {
        return Err(Error::Rithmic(format!("{ctx}: {err}")));
    }
    Ok(())
}

fn check_responses<'a, I>(resps: I, ctx: &str) -> Result<()>
where
    I: IntoIterator<Item = &'a rithmic_rs::RithmicResponse>,
{
    for resp in resps {
        check_response_ref(resp, ctx)?;
    }
    Ok(())
}
