//! Multi-plant Rithmic session facade (Phase 2: MD + history + PnL + orders).
//!
//! [`PlantSet`] chooses ticker/history/PnL on [`RithmicSession::connect`].
//! The order plant stays lazy via [`RithmicSession::ensure_order_plant`].

use std::future::Future;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::{Duration, Instant};

use rithmic_rs::rti::request_time_bar_update;
use rithmic_rs::SubscriptionFilter;
use rithmic_rs::{
    ConnectStrategy, ManualOrAutoEntry, OrderSide, OrderType, RithmicAccount,
    RithmicBracketLevelAdjustment, RithmicBracketOrder, RithmicCancelAllOrders, RithmicCancelOrder,
    RithmicConfig, RithmicHistoryPlant, RithmicHistoryPlantHandle, RithmicModifyOrder,
    RithmicOrder, RithmicOrderPlant, RithmicOrderPlantHandle, RithmicPnlPlant,
    RithmicPnlPlantHandle, RithmicTickerPlant, RithmicTickerPlantHandle, TimeBarType, TimeInForce,
};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::error::TryRecvError;

use crate::account::{pick_account, rows_from_account_list};
use crate::config::SessionConfig;
use crate::dto::{
    AccountRmsInfoDto, FrontMonthDto, HistoryBarDto, HistoryTickDto, OrderNotificationDto,
    PlantEvent, ProductRmsInfoDto, ReferenceDataDto, TimeBarProbeRow,
};
use crate::error::{Error, Result};
use crate::history::{
    bar_replay_index, bar_slice_secs, dedup_bars, dedup_ticks, history_ready_probe_root,
    load_sliced, looks_like_listed_contract, parse_time_bar_type, DEFAULT_TICK_SLICE_SECS,
};
use crate::plants::PlantSet;

/// Silence window (ms) that ends the best-effort `load_orders` drain.
const ORDER_DRAIN_SETTLE_MS: u64 = 1_000;
/// Hard cap (ms) on the `load_orders` drain against continuous live traffic.
const ORDER_DRAIN_MAX_MS: u64 = 10_000;

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
    /// Cached account after explicit override or wire discovery.
    resolved_account: Option<RithmicAccount>,
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
        let resolved_account = config.account();
        Self {
            config,
            plants,
            ticker: None,
            history: None,
            pnl: None,
            order: None,
            resolved_account,
        }
    }

    /// Resolved FCM/IB/account after override or discovery (if any).
    pub fn resolved_account(&self) -> Option<&RithmicAccount> {
        self.resolved_account.as_ref()
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

        let connected = self.ticker.is_some() || self.history.is_some() || self.pnl.is_some();
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
    ///
    /// When the history plant is selected, [`Self::prove_history_ready`] runs
    /// before this returns (RC4): login alone is not enough to advertise Ready.
    pub async fn connect(&mut self) -> Result<()> {
        if self.ticker.is_some() || self.history.is_some() || self.pnl.is_some() {
            return Err(Error::AlreadyConnected);
        }

        let rc = self.config.to_rithmic_config()?;

        if self.plants.ticker {
            self.connect_or_rollback(connect_ticker(&rc), |s, p| s.ticker = Some(p))
                .await?;
        }

        if self.plants.history {
            self.connect_or_rollback(connect_history(&rc), |s, p| s.history = Some(p))
                .await?;
            if let Err(err) = self.prove_history_ready().await {
                let _ = self.disconnect().await;
                return Err(err);
            }
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

    /// RC4: prove the history plant can serve a tiny Load* before advertising
    /// connected / accepting production hydrate RPCs.
    ///
    /// Empty rows are success (plant answered). `RequestTimeout` retries via
    /// [`crate::history::is_transient`]. Probe root defaults to ES/CME (env
    /// overrides — see [`history_ready_probe_root`]).
    pub async fn prove_history_ready(&self) -> Result<()> {
        if self.history.is_none() {
            return Ok(());
        }
        let (root, exchange) = history_ready_probe_root();
        let (symbol, exchange) = if looks_like_listed_contract(&root) {
            (root, exchange)
        } else if self.ticker.is_some() {
            let fm = self.get_front_month(&root, &exchange).await?;
            let symbol = fm.trading_symbol.or(fm.symbol).ok_or_else(|| {
                Error::Protocol(format!(
                    "history ready probe: front-month missing trading_symbol for {root}/{exchange}"
                ))
            })?;
            let exchange = fm.trading_exchange.unwrap_or(exchange);
            (symbol, exchange)
        } else {
            (root, exchange)
        };

        let end = i32::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| Error::Protocol(format!("history ready probe clock: {e}")))?
                .as_secs(),
        )
        .map_err(|_| Error::Protocol("history ready probe timestamp exceeds i32 range".into()))?;
        let start = end.saturating_sub(120);
        // Tiny 1m window — Ok([]) means the plant answered (not silent).
        let _ = self
            .load_time_bars_all(&symbol, &exchange, TimeBarType::MinuteBar, 1, start, end)
            .await?;
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

    /// Connect + login the order plant (idempotent).
    ///
    /// Uses config triple when set; otherwise [`Self::resolve_account`].
    pub async fn ensure_order_plant(&mut self) -> Result<()> {
        if self.order.is_some() {
            return Ok(());
        }
        if self.ticker.is_none() {
            return Err(Error::NotConnected { plant: "ticker" });
        }
        let account = self.resolve_account().await?;
        let rc = self.config.to_rithmic_config()?;
        let order_plant = RithmicOrderPlant::connect(&rc, ConnectStrategy::Retry).await?;
        let order_handle = order_plant.get_handle(&account);
        let result = login_result(order_handle.login().await, "order login");
        after_failed_login(
            async {
                let _ = order_handle.disconnect().await;
            },
            result,
        )
        .await?;
        self.order = Some(OrderPlant {
            _plant: order_plant,
            handle: order_handle,
        });
        Ok(())
    }

    /// Resolve and cache trading account (config override or wire account list).
    pub async fn resolve_account(&mut self) -> Result<RithmicAccount> {
        if let Some(acct) = self.config.account() {
            self.resolved_account = Some(acct.clone());
            return Ok(acct);
        }
        if let Some(cached) = self.resolved_account.clone() {
            return Ok(cached);
        }

        let rc = self.config.to_rithmic_config()?;
        let order_plant = RithmicOrderPlant::connect(&rc, ConnectStrategy::Retry).await?;
        let bootstrap_handle = order_plant.get_handle(&RithmicAccount::new("", "", ""));

        let outcome = async {
            check_response(bootstrap_handle.login().await?, "order login (discover)")?;
            let list = bootstrap_handle.get_account_list().await?;
            pick_account(
                &rows_from_account_list(&list),
                self.config.account_id_selector(),
            )
        }
        .await;

        let _ = bootstrap_handle.disconnect().await;
        drop(order_plant);

        let account = outcome?;
        self.resolved_account = Some(account.clone());
        Ok(account)
    }

    /// Connect + login the PnL plant (idempotent). Discovers account when needed.
    pub async fn ensure_pnl_plant(&mut self) -> Result<()> {
        if self.pnl.is_some() {
            return Ok(());
        }
        if self.ticker.is_none() {
            return Err(Error::NotConnected { plant: "ticker" });
        }
        let account = self.resolve_account().await?;
        let rc = self.config.to_rithmic_config()?;
        let pnl_plant = RithmicPnlPlant::connect(&rc, ConnectStrategy::Retry).await?;
        let pnl_handle = pnl_plant.get_handle(&account);
        let result = login_result(pnl_handle.login().await, "pnl login");
        after_failed_login(
            async {
                let _ = pnl_handle.disconnect().await;
            },
            result,
        )
        .await?;
        self.pnl = Some(PnlPlant {
            _plant: pnl_plant,
            handle: pnl_handle,
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
        FrontMonthDto::from_response(&resp)
            .ok_or_else(|| Error::Protocol("unexpected front-month response variant".into()))
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
    pub async fn unsubscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()> {
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
            |slice_start, slice_end| async move {
                load_tick_slice(handle, symbol, exchange, slice_start, slice_end).await
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
            |slice_start, slice_end| async move {
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
        Ok(responses
            .iter()
            .map(TimeBarProbeRow::from_response)
            .collect())
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
        map_broadcast_poll("history", now_or_never(handle.subscription_receiver.recv()))
    }

    /// Subscribe to PnL updates and request a snapshot when account is configured.
    pub async fn subscribe_pnl(&self) -> Result<()> {
        let handle = self.pnl_handle()?;
        check_response(handle.subscribe_pnl_updates().await?, "subscribe_pnl")?;
        check_response(handle.get_pnl_position_snapshot().await?, "pnl_snapshot")?;
        Ok(())
    }

    /// Subscribe to order plant notifications (Rithmic + exchange).
    pub async fn subscribe_order_updates(&mut self) -> Result<()> {
        self.ensure_order_plant().await?;
        let result = async {
            let handle = self.order_handle()?;
            check_response(
                handle.subscribe_order_updates().await?,
                "subscribe_order_updates",
            )
        }
        .await;
        rollback_order_plant_subscribe(self, result, true).await
    }

    /// Load current working orders via a bounded silence-window drain.
    ///
    /// Rithmic has no end-of-list signal for order notifications: `show_orders`
    /// triggers a replay of the current working orders, which arrive on the
    /// order-updates subscription channel with no completion marker. This drains
    /// that channel until `ORDER_DRAIN_SETTLE_MS` of silence (or the hard cap)
    /// and returns every order notification seen in arrival order.
    ///
    /// **Best-effort, not provably complete.** Replays silently cap at 10,000
    /// records and a quiet channel cannot distinguish "venue has no orders" from
    /// "rows were lost". Callers that need a hard guarantee must treat the
    /// result as advisory (see `death_policy=trust_stop`) rather than as an
    /// authoritative venue snapshot. The `start`/`end` window is intentionally
    /// ignored: only the *current* working set is requested.
    pub async fn load_orders(&mut self, start: i32, end: i32) -> Result<Vec<OrderNotificationDto>> {
        let _ = (start, end);
        self.ensure_order_plant().await?;
        load_orders_on(self.order_handle()?).await
    }

    /// Load product-level RMS info (per-product commission fill rates).
    ///
    /// One row per product the account can trade; rows without a published
    /// ``commission_fill_rate`` keep the field `None`. Requires the order plant
    /// (read-only query — never places or cancels).
    pub async fn load_product_rms_info(&mut self) -> Result<Vec<ProductRmsInfoDto>> {
        self.ensure_order_plant().await?;
        load_product_rms_info_on(self.order_handle()?).await
    }

    /// Load account-level RMS info (default commission rate).
    ///
    /// Read-only query on the order plant; rows without a published
    /// ``default_commission`` keep the field `None`.
    pub async fn load_account_rms_info(&mut self) -> Result<Vec<AccountRmsInfoDto>> {
        self.ensure_order_plant().await?;
        load_account_rms_info_on(self.order_handle()?).await
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

    /// Disconnect + reconnect only the ticker plant (leaves history/PnL/order
    /// connected).
    ///
    /// The data client's channel-error resync needs a fresh ticker receiver,
    /// but the exec client shares this session, so the PnL/order plants must
    /// not be disturbed. A reconnect failure leaves the ticker disconnected
    /// (the poll loop keeps retrying the resync) and never tears down the
    /// sibling plants.
    pub async fn reset_ticker_plant(&mut self) -> Result<()> {
        if let Some(ticker) = self.ticker.take() {
            let _ = ticker.handle.disconnect().await;
        }
        if !self.plants.ticker {
            return Ok(());
        }
        let rc = self.config.to_rithmic_config()?;
        self.ticker = Some(connect_ticker(&rc).await?);
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

    /// Subscribe to bracket update notifications (required for plant brackets).
    pub async fn subscribe_bracket_updates(&mut self) -> Result<()> {
        let opened_here = self.order.is_none();
        self.ensure_order_plant().await?;
        let result = async {
            let handle = self.order_handle()?;
            check_response(
                handle.subscribe_bracket_updates().await?,
                "subscribe_bracket_updates",
            )
        }
        .await;
        rollback_order_plant_subscribe(self, result, opened_here).await
    }

    /// Place a server-side bracket (entry + stop_ticks / target_ticks from fill).
    #[allow(clippy::too_many_arguments)]
    pub async fn place_bracket_order(
        &mut self,
        symbol: &str,
        exchange: &str,
        side: &str,
        price_type: &str,
        quantity: i32,
        localid: &str,
        price: Option<f64>,
        trigger_price: Option<f64>,
        duration: &str,
        stop_ticks: Option<i32>,
        target_ticks: Option<i32>,
    ) -> Result<()> {
        self.ensure_order_plant().await?;
        place_bracket_order_on(
            self.order_handle()?,
            symbol,
            exchange,
            side,
            price_type,
            quantity,
            localid,
            price,
            trigger_price,
            duration,
            stop_ticks,
            target_ticks,
        )
        .await
    }

    /// Adjust a bracket stop leg by basket id (ticks from fill).
    pub async fn adjust_bracket_stop(
        &mut self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()> {
        self.ensure_order_plant().await?;
        adjust_bracket_stop_on(self.order_handle()?, basket_id, ticks, level).await
    }

    /// Adjust a bracket target leg by basket id (ticks from fill).
    pub async fn adjust_bracket_target(
        &mut self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()> {
        self.ensure_order_plant().await?;
        adjust_bracket_target_on(self.order_handle()?, basket_id, ticks, level).await
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
        map_try_recv_poll("ticker", handle.subscription_receiver.try_recv())
    }

    /// Non-blocking poll of the next PnL-plant subscription message.
    pub fn poll_pnl_event(&mut self) -> Result<Option<PlantEvent>> {
        let Some(pnl) = self.pnl.as_mut() else {
            return Ok(None);
        };
        map_broadcast_poll("pnl", now_or_never(pnl.handle.subscription_receiver.recv()))
    }

    /// Non-blocking poll of the next order-plant subscription message.
    pub fn poll_order_event(&mut self) -> Result<Option<PlantEvent>> {
        let handle = self.order_handle_mut()?;
        map_broadcast_poll("order", now_or_never(handle.subscription_receiver.recv()))
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

/// Place a plant bracket on an already-connected handle (no session lock).
///
/// Exit geometry is ticks from fill (`stop_ticks` and/or `target_ticks`).
#[allow(clippy::too_many_arguments)]
pub async fn place_bracket_order_on(
    handle: &RithmicOrderPlantHandle,
    symbol: &str,
    exchange: &str,
    side: &str,
    price_type: &str,
    quantity: i32,
    localid: &str,
    price: Option<f64>,
    trigger_price: Option<f64>,
    duration: &str,
    stop_ticks: Option<i32>,
    target_ticks: Option<i32>,
) -> Result<()> {
    if stop_ticks.is_none() && target_ticks.is_none() {
        return Err(Error::Config(
            "place_bracket_order requires stop_ticks and/or target_ticks".into(),
        ));
    }
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

    let mut builder = RithmicBracketOrder::new()
        .symbol(symbol)
        .exchange(exchange)
        .quantity(quantity)
        .action(side)
        .price_type(price_type)
        .duration(duration)
        .localid(localid)
        .manual_or_auto(ManualOrAutoEntry::Auto);
    if let Some(p) = price {
        builder = builder.price(p);
    }
    if let Some(t) = trigger_price {
        builder = builder.trigger_price(t);
    }
    if let Some(ticks) = target_ticks {
        builder = builder.target(ticks);
    }
    if let Some(ticks) = stop_ticks {
        builder = builder.stop(ticks);
    }
    let order = builder.build()?;
    check_responses(
        &handle.place_bracket_order(order).await?,
        "place_bracket_order",
    )
}

/// Adjust bracket stop ticks on an already-connected handle.
pub async fn adjust_bracket_stop_on(
    handle: &RithmicOrderPlantHandle,
    basket_id: &str,
    ticks: i32,
    level: Option<i32>,
) -> Result<()> {
    let mut builder = RithmicBracketLevelAdjustment::new()
        .id(basket_id)
        .ticks(ticks);
    if let Some(lvl) = level {
        builder = builder.level(lvl);
    }
    let adj = builder.build()?;
    check_response(handle.adjust_stop(adj).await?, "adjust_bracket_stop")
}

/// Adjust bracket target ticks on an already-connected handle.
pub async fn adjust_bracket_target_on(
    handle: &RithmicOrderPlantHandle,
    basket_id: &str,
    ticks: i32,
    level: Option<i32>,
) -> Result<()> {
    let mut builder = RithmicBracketLevelAdjustment::new()
        .id(basket_id)
        .ticks(ticks);
    if let Some(lvl) = level {
        builder = builder.level(lvl);
    }
    let adj = builder.build()?;
    check_response(handle.adjust_target(adj).await?, "adjust_bracket_target")
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

/// Drain current working orders on an already-connected handle (no session lock).
///
/// `show_orders` triggers a replay of the current working orders, which arrive
/// on the order-updates subscription channel with no completion marker. This
/// drains that channel until `ORDER_DRAIN_SETTLE_MS` of silence (or the hard
/// cap) and returns every order notification seen in arrival order.
///
/// **Best-effort, not provably complete.** Replays silently cap at 10,000
/// records and a quiet channel cannot distinguish "venue has no orders" from
/// "rows were lost". Callers that need a hard guarantee must treat the result
/// as advisory (see `death_policy=trust_stop`) rather than as an authoritative
/// venue snapshot.
/// A pull source the order-drain reads `PlantEvent`s from.
///
/// Implemented for `SubscriptionFilter` (production, over the order-plant
/// subscription) and for `broadcast::Receiver<PlantEvent>` (tests). Keeping the
/// drain generic over this trait lets the timing/collection logic be unit-tested
/// without constructing rithmic-rs's non-exhaustive `RithmicResponse`.
pub(crate) trait OrderDrainSource {
    async fn recv_order(&mut self) -> std::result::Result<PlantEvent, Error>;
}

impl OrderDrainSource for SubscriptionFilter {
    async fn recv_order(&mut self) -> std::result::Result<PlantEvent, Error> {
        let resp = match self.recv().await {
            Ok(resp) => resp,
            Err(RecvError::Lagged(skipped)) => {
                return Err(Error::ChannelLagged {
                    plant: "order",
                    skipped,
                });
            }
            Err(RecvError::Closed) => return Err(Error::ChannelClosed { plant: "order" }),
        };
        if let Some(err) = &resp.error {
            if err.is_connection_issue() {
                return Err(Error::Rithmic(err.to_string()));
            }
        }
        Ok(PlantEvent::from(&resp))
    }
}

/// Drain order notifications off an order-plant subscription source.
///
/// The drain collects `OrderNotification` events until
/// `ORDER_DRAIN_SETTLE_MS` of silence (or the `ORDER_DRAIN_MAX_MS` hard cap)
/// and returns them in arrival order. A `Closed` / `Lagged` source aborts the
/// drain with the corresponding error (so a dropped order-plant connection
/// surfaces as `ChannelClosed`).
///
/// **Best-effort, not provably complete.** Replays silently cap at 10,000
/// records and a quiet channel cannot distinguish "venue has no orders" from
/// "rows were lost". Callers that need a hard guarantee must treat the result
/// as advisory (see `death_policy=trust_stop`) rather than as an authoritative
/// venue snapshot.
pub(crate) async fn drain_order_notifications<S>(mut src: S) -> Result<Vec<OrderNotificationDto>>
where
    S: OrderDrainSource,
{
    let settle = Duration::from_millis(ORDER_DRAIN_SETTLE_MS);
    let deadline = Instant::now() + Duration::from_millis(ORDER_DRAIN_MAX_MS);
    let mut out: Vec<OrderNotificationDto> = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(settle.min(remaining), src.recv_order()).await {
            Ok(Ok(PlantEvent::OrderNotification(dto))) => out.push(dto),
            Ok(Ok(_)) => {} // non-order events (PnL, brackets, …) are ignored
            Ok(Err(Error::ChannelLagged { skipped, .. })) => {
                return Err(Error::ChannelLagged {
                    plant: "order",
                    skipped,
                });
            }
            Ok(Err(Error::ChannelClosed { .. })) => {
                return Err(Error::ChannelClosed { plant: "order" });
            }
            Ok(Err(err)) => return Err(err),
            Err(_elapsed) => break, // silence window elapsed -> drain complete
        }
    }
    Ok(out)
}

/// On subscribe failure, disconnect the order plant when `disconnect_on_err`.
/// Order-updates always pass `true`. Brackets pass `opened_here` so a failure
/// on an already-live plant does not tear down a shared order stream.
async fn rollback_order_plant_subscribe(
    session: &mut RithmicSession,
    result: Result<()>,
    disconnect_on_err: bool,
) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(err) => {
            if disconnect_on_err {
                let _ = session.disconnect_order_plant().await;
            }
            Err(err)
        }
    }
}

/// Drain current working orders on an already-connected handle (no session lock).
///
/// `show_orders` triggers a replay of the current working orders, which arrive
/// on the order-updates subscription channel with no completion marker. This
/// drains that channel until `ORDER_DRAIN_SETTLE_MS` of silence (or the hard
/// cap) and returns every order notification seen in arrival order.
///
/// **Best-effort, not provably complete.** Replays silently cap at 10,000
/// records and a quiet channel cannot distinguish "venue has no orders" from
/// "rows were lost". Callers that need a hard guarantee must treat the result
/// as advisory (see `death_policy=trust_stop`) rather than as an authoritative
/// venue snapshot.
pub async fn load_orders_on(handle: &RithmicOrderPlantHandle) -> Result<Vec<OrderNotificationDto>> {
    // A fresh receiver so the gateway event pump's own receiver is not consumed.
    let rx = handle.subscription_receiver.resubscribe();
    check_response(handle.show_orders().await?, "show_orders")?;
    drain_order_notifications(rx).await
}

/// Fetch product-level RMS info from an already-connected order-plant handle
/// (no session lock). Read-only venue config — never places or cancels.
pub async fn load_product_rms_info_on(
    handle: &RithmicOrderPlantHandle,
) -> Result<Vec<ProductRmsInfoDto>> {
    let responses = handle.get_product_rms_info().await?;
    collect_history_rows(
        responses,
        "load_product_rms_info",
        ProductRmsInfoDto::from_response,
    )
}

/// Fetch account-level RMS info from an already-connected order-plant handle
/// (no session lock). Read-only venue config — never places or cancels.
pub async fn load_account_rms_info_on(
    handle: &RithmicOrderPlantHandle,
) -> Result<Vec<AccountRmsInfoDto>> {
    let responses = handle.get_account_rms_info().await?;
    collect_history_rows(
        responses,
        "load_account_rms_info",
        AccountRmsInfoDto::from_response,
    )
}

/// Best-effort logout after a failed plant login so Drop does not leave a
/// hung Rithmic session (one-login-session rule).
async fn after_failed_login(
    disconnect: impl Future<Output = ()>,
    result: Result<()>,
) -> Result<()> {
    if result.is_err() {
        disconnect.await;
    }
    result
}

fn login_result(
    login: std::result::Result<rithmic_rs::RithmicResponse, rithmic_rs::RithmicError>,
    ctx: &str,
) -> Result<()> {
    match login {
        Ok(resp) => check_response(resp, ctx),
        Err(e) => Err(Error::from(e)),
    }
}

async fn connect_ticker(rc: &RithmicConfig) -> Result<TickerPlant> {
    let plant = RithmicTickerPlant::connect(rc, ConnectStrategy::Retry).await?;
    let handle = plant.get_handle();
    let result = login_result(handle.login().await, "ticker login");
    after_failed_login(
        async {
            let _ = handle.disconnect().await;
        },
        result,
    )
    .await?;
    Ok(TickerPlant {
        _plant: plant,
        handle,
    })
}

async fn connect_history(rc: &RithmicConfig) -> Result<HistoryPlant> {
    let plant = RithmicHistoryPlant::connect(rc, ConnectStrategy::Retry).await?;
    let handle = plant.get_handle();
    let result = login_result(handle.login().await, "history login");
    after_failed_login(
        async {
            let _ = handle.disconnect().await;
        },
        result,
    )
    .await?;
    Ok(HistoryPlant {
        _plant: plant,
        handle,
    })
}

async fn connect_pnl(rc: &RithmicConfig, account: &RithmicAccount) -> Result<PnlPlant> {
    let plant = RithmicPnlPlant::connect(rc, ConnectStrategy::Retry).await?;
    let handle = plant.get_handle(account);
    let result = login_result(handle.login().await, "pnl login");
    after_failed_login(
        async {
            let _ = handle.disconnect().await;
        },
        result,
    )
    .await?;
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

fn map_connection_response(resp: rithmic_rs::RithmicResponse) -> Result<Option<PlantEvent>> {
    if let Some(err) = &resp.error {
        if err.is_connection_issue() {
            return Err(Error::Rithmic(err.to_string()));
        }
    }
    Ok(Some(PlantEvent::from(&resp)))
}

/// Map a ticker `try_recv` result. Lag surfaces as [`Error::ChannelLagged`].
fn map_try_recv_poll(
    plant: &'static str,
    polled: std::result::Result<rithmic_rs::RithmicResponse, TryRecvError>,
) -> Result<Option<PlantEvent>> {
    match polled {
        Ok(resp) => map_connection_response(resp),
        Err(TryRecvError::Empty) => Ok(None),
        Err(TryRecvError::Lagged(skipped)) => Err(Error::ChannelLagged { plant, skipped }),
        Err(TryRecvError::Closed) => Err(Error::ChannelClosed { plant }),
    }
}

fn map_broadcast_poll(
    plant: &'static str,
    polled: Option<std::result::Result<rithmic_rs::RithmicResponse, RecvError>>,
) -> Result<Option<PlantEvent>> {
    match polled {
        Some(Ok(resp)) => map_connection_response(resp),
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

#[cfg(test)]
mod login_disconnect_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn failed_login_runs_disconnect() {
        let called = AtomicBool::new(false);
        let err = after_failed_login(
            async {
                called.store(true, Ordering::SeqCst);
            },
            Err(Error::Rithmic("order login: boom".into())),
        )
        .await
        .unwrap_err();
        assert!(
            called.load(Ordering::SeqCst),
            "failed login must disconnect before drop"
        );
        assert!(matches!(
            err,
            Error::Rithmic(ref msg) if msg.contains("order login")
        ));
    }

    #[tokio::test]
    async fn successful_login_skips_disconnect() {
        let called = AtomicBool::new(false);
        after_failed_login(
            async {
                called.store(true, Ordering::SeqCst);
            },
            Ok(()),
        )
        .await
        .expect("ok");
        assert!(
            !called.load(Ordering::SeqCst),
            "successful login must not disconnect"
        );
    }
}

#[cfg(test)]
mod order_subscribe_rollback_tests {
    use super::*;

    fn disconnected_session() -> RithmicSession {
        let cfg = SessionConfig::builder()
            .user("alice")
            .password("pw")
            .build()
            .unwrap();
        RithmicSession::new(cfg)
    }

    #[tokio::test]
    async fn err_with_disconnect_propagates() {
        let mut session = disconnected_session();
        let err = rollback_order_plant_subscribe(
            &mut session,
            Err(Error::Rithmic("subscribe_order_updates: boom".into())),
            true,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            Error::Rithmic(ref msg) if msg.contains("subscribe_order_updates")
        ));
        assert!(format!("{session:?}").contains("order_connected: false"));
    }

    #[tokio::test]
    async fn err_without_disconnect_still_propagates() {
        let mut session = disconnected_session();
        let err = rollback_order_plant_subscribe(
            &mut session,
            Err(Error::Rithmic("subscribe_bracket_updates: boom".into())),
            false,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            Error::Rithmic(ref msg) if msg.contains("subscribe_bracket_updates")
        ));
    }

    #[tokio::test]
    async fn ok_path() {
        let mut session = disconnected_session();
        rollback_order_plant_subscribe(&mut session, Ok(()), true)
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod try_recv_poll_tests {
    use super::*;

    #[test]
    fn lagged_surfaces_channel_lagged() {
        let err = map_try_recv_poll("ticker", Err(TryRecvError::Lagged(42))).unwrap_err();
        assert!(
            matches!(
                err,
                Error::ChannelLagged {
                    plant: "ticker",
                    skipped: 42
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn empty_is_idle() {
        assert!(matches!(
            map_try_recv_poll("ticker", Err(TryRecvError::Empty)),
            Ok(None)
        ));
    }

    #[test]
    fn closed_surfaces_channel_closed() {
        let err = map_try_recv_poll("ticker", Err(TryRecvError::Closed)).unwrap_err();
        assert!(matches!(err, Error::ChannelClosed { plant: "ticker" }));
    }
}

#[cfg(test)]
mod drain_tests {
    use super::*;
    use tokio::sync::broadcast;
    use tokio::time::{timeout, Duration};

    // Make the production drain loop testable with a plain `PlantEvent` channel
    // instead of rithmic-rs's non-exhaustive `RithmicResponse`.
    impl OrderDrainSource for broadcast::Receiver<PlantEvent> {
        async fn recv_order(&mut self) -> std::result::Result<PlantEvent, Error> {
            match self.recv().await {
                Ok(event) => Ok(event),
                Err(RecvError::Lagged(skipped)) => Err(Error::ChannelLagged {
                    plant: "order",
                    skipped,
                }),
                Err(RecvError::Closed) => Err(Error::ChannelClosed { plant: "order" }),
            }
        }
    }

    fn order_event() -> PlantEvent {
        PlantEvent::OrderNotification(OrderNotificationDto {
            source: "rithmic".into(),
            kind: None,
            notify_type: None,
            notify_type_name: None,
            status: None,
            basket_id: Some("B1".into()),
            exchange_order_id: None,
            user_tag: None,
            account_id: None,
            symbol: None,
            exchange: None,
            quantity: None,
            total_fill_size: None,
            total_unfilled_size: None,
            fill_size: None,
            price: None,
            trigger_price: None,
            avg_fill_price: None,
            fill_price: None,
            transaction_type: None,
            price_type: None,
            duration: None,
            fill_id: None,
            text: None,
            report_text: None,
            completion_reason: None,
            ssboe: None,
            usecs: None,
            ts_event_ns: None,
            is_snapshot: None,
        })
    }

    #[tokio::test]
    async fn drain_collects_order_notifications() {
        let (tx, rx) = broadcast::channel(16);
        tx.send(order_event()).unwrap();
        tx.send(order_event()).unwrap();
        let got = timeout(Duration::from_secs(5), drain_order_notifications(rx))
            .await
            .expect("drain should finish within the silence window")
            .expect("drain should succeed");
        assert_eq!(got.len(), 2, "both replayed order notifications collected");
    }

    #[tokio::test]
    async fn drain_errors_on_closed_channel() {
        let (tx, rx) = broadcast::channel(16);
        drop(tx);
        let res = drain_order_notifications(rx).await;
        assert!(
            matches!(res, Err(Error::ChannelClosed { plant: "order" })),
            "closed channel must surface as ChannelClosed, got {res:?}"
        );
    }

    #[tokio::test]
    async fn drain_errors_on_lagged_channel() {
        let (tx, rx) = broadcast::channel(8);
        // Overflow the buffer so the never-drained receiver falls behind.
        for _ in 0..32 {
            let _ = tx.send(order_event());
        }
        let res = drain_order_notifications(rx).await;
        assert!(
            matches!(res, Err(Error::ChannelLagged { plant: "order", .. })),
            "overflowed channel must surface as ChannelLagged, got {res:?}"
        );
    }

    #[tokio::test]
    async fn drain_propagates_response_level_connection_error() {
        // A source carrying a response-level connection issue (the venue echoed
        // an error inside a received message, not a channel drop) must abort
        // the drain instead of being ignored as a non-order event.
        struct ConnectionIssueSource;

        impl OrderDrainSource for ConnectionIssueSource {
            async fn recv_order(&mut self) -> std::result::Result<PlantEvent, Error> {
                Err(Error::Rithmic("connection closed by venue".into()))
            }
        }

        let res = drain_order_notifications(ConnectionIssueSource).await;
        assert!(
            matches!(res, Err(Error::Rithmic(ref msg)) if msg.contains("connection closed")),
            "response-level connection error must abort the drain, got {res:?}"
        );
    }
}
