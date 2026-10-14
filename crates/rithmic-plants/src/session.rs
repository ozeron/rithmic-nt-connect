//! Multi-plant Rithmic session facade (Phase 2: MD + history + PnL + orders).
//!
//! [`PlantSet`] chooses ticker/history/PnL on [`RithmicSession::connect`].
//! The order plant stays lazy via [`RithmicSession::ensure_order_plant`].

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rithmic_rs::rti::request_time_bar_update;
use rithmic_rs::SubscriptionFilter;
use rithmic_rs::{
    ConnectStrategy, FillHistoryRange, ManualOrAutoEntry, OrderSide, OrderType, RithmicAccount,
    RithmicBracketLevelAdjustment, RithmicBracketOrder, RithmicCancelAllOrders, RithmicCancelOrder,
    RithmicConfig, RithmicHistoryPlant, RithmicHistoryPlantHandle, RithmicModifyOrder,
    RithmicOrder, RithmicOrderPlant, RithmicOrderPlantHandle, RithmicPnlPlant,
    RithmicPnlPlantHandle, RithmicTickerPlant, RithmicTickerPlantHandle, TimeBarType, TimeInForce,
};
use tokio::sync::broadcast::error::RecvError;

use crate::account::{pick_account, rows_from_account_list};
use crate::config::SessionConfig;
use crate::dto::{
    AccountRmsInfoDto, FillHistoryDto, FrontMonthDto, HistoryBarDto, HistoryTickDto,
    OrderNotificationDto, PlantEvent, ProductRmsInfoDto, ReferenceDataDto, TimeBarProbeRow,
};
use crate::error::{Error, Result};
use crate::history::{
    bar_replay_index, bar_slice_secs, dedup_bars, dedup_ticks, history_ready_probe_enabled,
    history_ready_probe_root, load_sliced, looks_like_listed_contract, parse_time_bar_type,
    DEFAULT_TICK_SLICE_SECS,
};
use crate::plants::PlantSet;
use crate::streams::{Stream, StreamGuard, StreamHub};

/// Silence window (ms) that ends the best-effort `load_orders` drain.
const ORDER_DRAIN_SETTLE_MS: u64 = 1_000;
/// Hard cap (ms) on the `load_orders` drain against continuous live traffic.
const ORDER_DRAIN_MAX_MS: u64 = 10_000;
/// Venue cap on one fill-history reply. A reply that reaches it may be
/// truncated and carries no truncation marker, so it fails closed.
const FILL_HISTORY_MAX_RECORDS: usize = 10_000;

struct TickerPlant {
    _plant: RithmicTickerPlant,
    handle: RithmicTickerPlantHandle,
    _stream: StreamGuard,
}

struct HistoryPlant {
    _plant: RithmicHistoryPlant,
    handle: RithmicHistoryPlantHandle,
    _stream: StreamGuard,
}

struct PnlPlant {
    _plant: RithmicPnlPlant,
    handle: RithmicPnlPlantHandle,
    _stream: StreamGuard,
}

struct OrderPlant {
    _plant: RithmicOrderPlant,
    handle: RithmicOrderPlantHandle,
    _stream: StreamGuard,
}

/// Anything that yields plant messages one at a time (the broadcast receiver
/// of the ticker / history plants and the account filter of the PnL / order plants).
#[async_trait::async_trait]
trait Feed: Send {
    type Item: Send + 'static;

    async fn next_item(&mut self) -> std::result::Result<Self::Item, RecvError>;
}

#[async_trait::async_trait]
impl Feed for tokio::sync::broadcast::Receiver<rithmic_rs::RithmicResponse> {
    type Item = rithmic_rs::RithmicResponse;

    async fn next_item(&mut self) -> std::result::Result<Self::Item, RecvError> {
        self.recv().await
    }
}

#[async_trait::async_trait]
impl Feed for SubscriptionFilter {
    type Item = rithmic_rs::RithmicResponse;

    async fn next_item(&mut self) -> std::result::Result<Self::Item, RecvError> {
        self.recv().await
    }
}

/// How long one plant connection may take before it fails with a typed error
/// instead of retrying forever (rithmic-rs' `Retry` strategy has no deadline).
pub const PLANT_CONNECT_DEADLINE: Duration = Duration::from_secs(60);

/// Bound a plant connect. Nothing is assigned to the session until the connect
/// returns, so abandoning it at the deadline leaves no half-built state.
async fn connect_within_deadline<T>(
    plant: &'static str,
    connect: impl Future<Output = std::result::Result<T, rithmic_rs::RithmicError>>,
) -> Result<T> {
    match tokio::time::timeout(PLANT_CONNECT_DEADLINE, connect).await {
        Ok(connected) => Ok(connected?),
        Err(_) => Err(Error::Session(format!(
            "{plant} plant connect timed out after {}s",
            PLANT_CONNECT_DEADLINE.as_secs()
        ))),
    }
}

/// Attach a stream lane and start the single forwarder that moves a plant's
/// broadcast messages into it. Lag becomes an in-band gap, a venue connection
/// error and a closed channel are recorded for the consumer to see in order.
fn forward<R: Feed + 'static>(
    hub: &Arc<StreamHub>,
    stream: Stream,
    mut rx: R,
    to_event: fn(R::Item) -> Result<Option<PlantEvent>>,
) -> StreamGuard {
    let tx = hub.attach(stream);
    let task_tx = tx.clone();
    let task = tokio::spawn(async move {
        loop {
            match rx.next_item().await {
                Ok(item) => match to_event(item) {
                    Ok(Some(event)) => task_tx.event(event).await,
                    Ok(None) => {}
                    Err(Error::Rithmic(text)) => task_tx.fail(text),
                    Err(other) => task_tx.fail(other.to_string()),
                },
                Err(RecvError::Lagged(skipped)) => task_tx.gap(skipped).await,
                Err(RecvError::Closed) => {
                    task_tx.close();
                    break;
                }
            }
        }
    });
    StreamGuard::new(&tx, task)
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
    /// Event lanes shared with consumers; they outlive plant reconnects.
    streams: Arc<StreamHub>,
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

/// Owned clones of the connected plant handles.
///
/// Taken under the session lock for an instant and used after it is released, so
/// a long read (a history load, a slow subscribe) never holds the lock that every
/// other call needs. Every handle clone sends requests on the plant's own
/// channel; the plant serializes them.
#[derive(Clone, Default)]
pub struct Handles {
    ticker: Option<RithmicTickerPlantHandle>,
    history: Option<RithmicHistoryPlantHandle>,
    pnl: Option<RithmicPnlPlantHandle>,
}

impl Handles {
    fn ticker_handle(&self) -> Result<&RithmicTickerPlantHandle> {
        self.ticker
            .as_ref()
            .ok_or(Error::NotConnected { plant: "ticker" })
    }

    fn history_handle(&self) -> Result<&RithmicHistoryPlantHandle> {
        self.history
            .as_ref()
            .ok_or(Error::NotConnected { plant: "history" })
    }

    fn pnl_handle(&self) -> Result<&RithmicPnlPlantHandle> {
        self.pnl
            .as_ref()
            .ok_or(Error::NotConnected { plant: "pnl" })
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

    /// Subscribe to PnL updates and request a snapshot when account is configured.
    pub async fn subscribe_pnl(&self) -> Result<()> {
        let handle = self.pnl_handle()?;
        check_response(handle.subscribe_pnl_updates().await?, "subscribe_pnl")?;
        check_response(handle.get_pnl_position_snapshot().await?, "pnl_snapshot")?;
        Ok(())
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
            streams: StreamHub::new(),
        }
    }

    /// Owned clones of the ticker / history / PnL handles for lock-free I/O.
    pub fn handles(&self) -> Handles {
        Handles {
            ticker: self.ticker.as_ref().map(|t| t.handle.clone()),
            history: self.history.as_ref().map(|h| h.handle.clone()),
            pnl: self.pnl.as_ref().map(|p| p.handle.clone()),
        }
    }

    /// The event lanes. Consumers wait on these without touching the session.
    pub fn streams(&self) -> Arc<StreamHub> {
        Arc::clone(&self.streams)
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
            match connect_ticker(&rc, &self.streams).await {
                Ok(plant) => self.ticker = Some(plant),
                Err(err) => return Err(err),
            }
        }
        if self.plants.history && self.history.is_none() {
            match connect_history(&rc, &self.streams).await {
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
        let hub = Arc::clone(&self.streams);

        if self.plants.ticker {
            self.connect_or_rollback(connect_ticker(&rc, &hub), |s, p| s.ticker = Some(p))
                .await?;
        }

        if self.plants.history {
            self.connect_or_rollback(connect_history(&rc, &hub), |s, p| s.history = Some(p))
                .await?;
            if let Err(err) = self.prove_history_ready().await {
                let _ = self.disconnect().await;
                return Err(err);
            }
        }

        if self.plants.pnl {
            if let Some(account) = self.config.account() {
                self.connect_or_rollback(connect_pnl(&rc, &account, &hub), |s, p| s.pnl = Some(p))
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
    /// overrides — see [`history_ready_probe_root`]); opt out with
    /// `RITHMIC_HISTORY_READY_PROBE=0` ([`history_ready_probe_enabled`]).
    pub async fn prove_history_ready(&self) -> Result<()> {
        if self.history.is_none() || !history_ready_probe_enabled() {
            return Ok(());
        }
        let (root, exchange) = history_ready_probe_root();
        let (symbol, exchange) = if looks_like_listed_contract(&root) {
            (root, exchange)
        } else if self.ticker.is_some() {
            let fm = self.handles().get_front_month(&root, &exchange).await?;
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
            .handles()
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
        let order_plant = connect_within_deadline(
            "order",
            RithmicOrderPlant::connect(&rc, ConnectStrategy::Retry),
        )
        .await?;
        let order_handle = order_plant.get_handle(&account);
        let result = login_result(order_handle.login().await, "order login");
        after_failed_login(
            async {
                let _ = order_handle.disconnect().await;
            },
            result,
        )
        .await?;
        let stream = forward(
            &self.streams,
            Stream::Order,
            order_handle.subscription_receiver.resubscribe(),
            map_connection_response,
        );
        self.order = Some(OrderPlant {
            _plant: order_plant,
            handle: order_handle,
            _stream: stream,
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
        let order_plant = connect_within_deadline(
            "order",
            RithmicOrderPlant::connect(&rc, ConnectStrategy::Retry),
        )
        .await?;
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
        let pnl_plant =
            connect_within_deadline("pnl", RithmicPnlPlant::connect(&rc, ConnectStrategy::Retry))
                .await?;
        let pnl_handle = pnl_plant.get_handle(&account);
        let result = login_result(pnl_handle.login().await, "pnl login");
        after_failed_login(
            async {
                let _ = pnl_handle.disconnect().await;
            },
            result,
        )
        .await?;
        let stream = forward(
            &self.streams,
            Stream::Pnl,
            pnl_handle.subscription_receiver.resubscribe(),
            map_connection_response,
        );
        self.pnl = Some(PnlPlant {
            _plant: pnl_plant,
            handle: pnl_handle,
            _stream: stream,
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
        self.ticker = Some(connect_ticker(&rc, &self.streams.clone()).await?);
        Ok(())
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

    /// Cancel all working orders on the account.
    pub async fn cancel_all_orders(&mut self) -> Result<()> {
        self.ensure_order_plant().await?;
        cancel_all_orders_on(self.order_handle()?).await
    }

    fn order_handle(&self) -> Result<&RithmicOrderPlantHandle> {
        self.order
            .as_ref()
            .map(|o| &o.handle)
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

/// Replay one trade date's order history (`date` = CCYYMMDD). Read-only.
///
/// Like `show_orders`, the summary replays order notifications on the
/// subscription channel with no end marker, so this uses the same bounded
/// silence-window drain (best-effort; see [`load_orders_on`]). Notifications
/// carried in the request's own responses are included first.
pub async fn load_order_history_on(
    handle: &RithmicOrderPlantHandle,
    date: &str,
) -> Result<Vec<OrderNotificationDto>> {
    let rx = handle.subscription_receiver.resubscribe();
    let responses = handle.show_order_history_summary(date).await?;
    check_responses(&responses, "show_order_history_summary")?;
    let mut out: Vec<OrderNotificationDto> = responses
        .iter()
        .filter_map(|r| match PlantEvent::from(r) {
            PlantEvent::OrderNotification(dto) => Some(dto),
            _ => None,
        })
        .collect();
    out.extend(drain_order_notifications(rx).await?);
    Ok(out)
}

/// Fetch one fill-history range (`start..=finish`, CCYYMMDD) from an
/// already-connected order-plant handle (no session lock). Read-only.
///
/// Fails closed at the venue cap: a reply of [`FILL_HISTORY_MAX_RECORDS`] rows
/// may be truncated, so it becomes [`Error::ReconciliationUnavailable`].
pub async fn load_fill_history_on(
    handle: &RithmicOrderPlantHandle,
    start: i32,
    finish: i32,
) -> Result<Vec<FillHistoryDto>> {
    let responses = handle
        .show_fill_history(
            FillHistoryRange::trade_date(start, finish),
            Some(FILL_HISTORY_MAX_RECORDS as i32),
        )
        .await?;
    let rows = collect_history_rows(
        responses,
        "load_fill_history",
        FillHistoryDto::from_response,
    )?;
    check_fill_history_cap(rows.len())?;
    Ok(rows)
}

/// Pure cap check for [`load_fill_history_on`]: a count at or above the venue
/// cap means the reply may be incomplete, so it is refused.
fn check_fill_history_cap(row_count: usize) -> Result<()> {
    if row_count >= FILL_HISTORY_MAX_RECORDS {
        return Err(Error::ReconciliationUnavailable(format!(
            "fill history reached the {FILL_HISTORY_MAX_RECORDS}-row venue cap; \
             split the trade-date range and retry"
        )));
    }
    Ok(())
}

/// Trade dates (CCYYMMDD) the venue keeps order history for, from an
/// already-connected order-plant handle (no session lock). Read-only.
pub async fn load_order_history_dates_on(handle: &RithmicOrderPlantHandle) -> Result<Vec<String>> {
    let responses = handle.show_order_history_dates().await?;
    let dates = collect_history_rows(responses, "load_order_history_dates", |resp| {
        match &resp.message {
            rithmic_rs::rti::messages::RithmicMessage::ResponseShowOrderHistoryDates(m) => {
                Some(m.date.clone())
            }
            _ => None,
        }
    })?
    .concat();
    Ok(normalize_history_dates(dates))
}

/// Sort and dedup CCYYMMDD dates. CCYYMMDD strings sort chronologically, so a
/// lexical sort is the date order.
fn normalize_history_dates(mut dates: Vec<String>) -> Vec<String> {
    dates.sort();
    dates.dedup();
    dates
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

async fn connect_ticker(rc: &RithmicConfig, hub: &Arc<StreamHub>) -> Result<TickerPlant> {
    let plant = connect_within_deadline(
        "ticker",
        RithmicTickerPlant::connect(rc, ConnectStrategy::Retry),
    )
    .await?;
    let handle = plant.get_handle();
    let result = login_result(handle.login().await, "ticker login");
    after_failed_login(
        async {
            let _ = handle.disconnect().await;
        },
        result,
    )
    .await?;
    let stream = forward(
        hub,
        Stream::Ticker,
        handle.subscription_receiver.resubscribe(),
        map_connection_response,
    );
    Ok(TickerPlant {
        _plant: plant,
        handle,
        _stream: stream,
    })
}

async fn connect_history(rc: &RithmicConfig, hub: &Arc<StreamHub>) -> Result<HistoryPlant> {
    let plant = connect_within_deadline(
        "history",
        RithmicHistoryPlant::connect(rc, ConnectStrategy::Retry),
    )
    .await?;
    let handle = plant.get_handle();
    let result = login_result(handle.login().await, "history login");
    after_failed_login(
        async {
            let _ = handle.disconnect().await;
        },
        result,
    )
    .await?;
    let stream = forward(
        hub,
        Stream::History,
        handle.subscription_receiver.resubscribe(),
        map_connection_response,
    );
    Ok(HistoryPlant {
        _plant: plant,
        handle,
        _stream: stream,
    })
}

async fn connect_pnl(
    rc: &RithmicConfig,
    account: &RithmicAccount,
    hub: &Arc<StreamHub>,
) -> Result<PnlPlant> {
    let plant =
        connect_within_deadline("pnl", RithmicPnlPlant::connect(rc, ConnectStrategy::Retry))
            .await?;
    let handle = plant.get_handle(account);
    let result = login_result(handle.login().await, "pnl login");
    after_failed_login(
        async {
            let _ = handle.disconnect().await;
        },
        result,
    )
    .await?;
    let stream = forward(
        hub,
        Stream::Pnl,
        handle.subscription_receiver.resubscribe(),
        map_connection_response,
    );
    Ok(PnlPlant {
        _plant: plant,
        handle,
        _stream: stream,
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
mod history_read_tests {
    use super::*;

    #[test]
    fn fill_history_below_cap_is_returned() {
        assert!(check_fill_history_cap(0).is_ok());
        assert!(check_fill_history_cap(FILL_HISTORY_MAX_RECORDS - 1).is_ok());
    }

    #[test]
    fn fill_history_at_cap_fails_closed_as_reconciliation_unavailable() {
        let err = check_fill_history_cap(FILL_HISTORY_MAX_RECORDS).unwrap_err();
        assert!(
            matches!(err, Error::ReconciliationUnavailable(ref msg) if msg.contains("10000-row")),
            "a full page may be truncated and must not be returned as complete: {err:?}"
        );
    }

    #[test]
    fn history_dates_are_sorted_and_deduped() {
        let got = normalize_history_dates(vec![
            "20260930".into(),
            "20260928".into(),
            "20260930".into(),
            "20261001".into(),
        ]);
        assert_eq!(got, vec!["20260928", "20260930", "20261001"]);
    }

    #[test]
    fn history_dates_empty_stays_empty() {
        assert!(normalize_history_dates(Vec::new()).is_empty());
    }
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

#[cfg(test)]
mod forward_tests {
    use super::*;
    use crate::dto::LastTradeDto;
    use std::time::Duration;

    fn to_event(seq: i32) -> Result<Option<PlantEvent>> {
        Ok(Some(PlantEvent::LastTrade(LastTradeDto {
            symbol: None,
            exchange: None,
            trade_price: None,
            trade_size: Some(seq),
            aggressor: None,
            ssboe: None,
            usecs: None,
            ts_event_ns: None,
            is_snapshot: None,
        })))
    }

    #[async_trait::async_trait]
    impl Feed for tokio::sync::broadcast::Receiver<i32> {
        type Item = i32;

        async fn next_item(&mut self) -> std::result::Result<i32, RecvError> {
            self.recv().await
        }
    }

    /// The real forwarder over a real tokio broadcast channel that overflows:
    /// the lag must arrive as an in-order gap carrying the exact skipped count,
    /// not vanish into a log line.
    #[tokio::test]
    async fn a_broadcast_lag_becomes_an_exact_in_order_gap() {
        let hub = StreamHub::new();
        let (tx, rx) = tokio::sync::broadcast::channel(4);
        // Overflow before the forwarder runs: 10 sent into a 4-slot ring.
        for i in 0..10 {
            tx.send(i).unwrap();
        }
        let _guard = forward(&hub, Stream::Ticker, rx, to_event);
        let items = hub
            .next_events(Stream::Ticker, 64, Duration::from_secs(2))
            .await
            .unwrap();
        // 6 were overwritten, then the 4 retained events (6..=9) follow.
        assert!(
            matches!(
                items[0],
                PlantEvent::Gap(crate::dto::GapDto { skipped: 6, .. })
            ),
            "{items:?}"
        );
        let sizes: Vec<i32> = items[1..]
            .iter()
            .map(|e| match e {
                PlantEvent::LastTrade(t) => t.trade_size.unwrap(),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(sizes, [6, 7, 8, 9]);
    }

    #[tokio::test]
    async fn a_closed_broadcast_reports_closed_after_the_queued_events() {
        let hub = StreamHub::new();
        let (tx, rx) = tokio::sync::broadcast::channel(8);
        let _guard = forward(&hub, Stream::History, rx, to_event);
        tx.send(1).unwrap();
        drop(tx);
        let first = hub
            .next_events(Stream::History, 8, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        let err = hub
            .next_events(Stream::History, 8, Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::ChannelClosed { plant: "history" }),
            "{err}"
        );
    }

    #[tokio::test]
    async fn a_venue_connection_error_surfaces_after_the_queued_events() {
        fn failing(seq: i32) -> Result<Option<PlantEvent>> {
            if seq < 0 {
                Err(Error::Rithmic("connection reset".into()))
            } else {
                to_event(seq)
            }
        }
        let hub = StreamHub::new();
        let (tx, rx) = tokio::sync::broadcast::channel(8);
        let _guard = forward(&hub, Stream::Order, rx, failing);
        tx.send(1).unwrap();
        tx.send(-1).unwrap();
        let first = hub
            .next_events(Stream::Order, 8, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        let err = hub
            .next_events(Stream::Order, 8, Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::Rithmic(m) if m == "connection reset"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn dropping_the_guard_detaches_the_lane() {
        let hub = StreamHub::new();
        let (_tx, rx) = tokio::sync::broadcast::channel::<i32>(8);
        let guard = forward(&hub, Stream::Ticker, rx, to_event);
        assert!(hub.is_up(Stream::Ticker));
        drop(guard);
        assert!(!hub.is_up(Stream::Ticker));
    }
}

#[cfg(test)]
mod connect_deadline_tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_connect_that_never_completes_fails_with_a_typed_error() {
        let never = std::future::pending::<std::result::Result<(), rithmic_rs::RithmicError>>();
        let err = connect_within_deadline("order", never).await.unwrap_err();
        assert!(
            matches!(&err, Error::Session(m) if m.contains("order plant connect timed out after 60s")),
            "{err}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_connect_that_finishes_in_time_is_returned() {
        let quick = async { Ok::<_, rithmic_rs::RithmicError>(7) };
        assert_eq!(connect_within_deadline("ticker", quick).await.unwrap(), 7);
    }
}
