//! Multi-plant Rithmic session facade (Phase 2: MD + history + PnL + orders).
//!
//! [`PlantSet`] chooses ticker/history/PnL on [`Plants::connect`].
//! The order plant stays lazy via [`Plants::ensure_order_plant`].

use std::future::Future;
use std::panic::AssertUnwindSafe;
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
use crate::history::{
    bar_dedup_key, bar_replay_index, bar_slice_secs, history_ready_probe_enabled,
    history_ready_probe_root, load_sliced, looks_like_listed_contract, parse_time_bar_type,
    tick_dedup_key, DEFAULT_TICK_SLICE_SECS,
};
use rithmic_host::dto::{
    AccountRmsInfoDto, FillHistoryDto, FrontMonthDto, HistoryBarDto, HistoryTickDto,
    OrderNotificationDto, PlantEvent, ProductRmsInfoDto, ReferenceDataDto, TimeBarProbeRow,
};
use rithmic_host::plants::PlantSet;
use rithmic_host::streams::{Stream, StreamGuard, StreamHub};
use rithmic_host::{Error, Result};

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

macro_rules! plant_close {
    ($($plant:ident),*) => {$(
        impl $plant {
            /// Detach the event lane, then log the plant out. A deliberate close
            /// must never read as a lost connection: once the lane is detached
            /// nothing the closing plant emits reaches a consumer.
            async fn close(self) {
                let Self { _plant, handle, _stream } = self;
                drop(_stream);
                let _ = handle.disconnect().await;
            }
        }
    )*};
}

plant_close!(TickerPlant, HistoryPlant, PnlPlant, OrderPlant);

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
        Ok(connected) => Ok(connected.map_err(crate::error::rithmic_error)?),
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
                // A panic in the conversion (a venue value it cannot handle)
                // must not kill the task silently: the lane would stay up with
                // no producer and its consumers would wait forever.
                Ok(item) => match std::panic::catch_unwind(AssertUnwindSafe(|| to_event(item))) {
                    Ok(Ok(Some(event))) => task_tx.event(event).await,
                    Ok(Ok(None)) => {}
                    Ok(Err(Error::Rithmic(text))) => task_tx.fail(text),
                    Ok(Err(other)) => task_tx.fail(other.to_string()),
                    Err(_) => {
                        // Fail, then close: the failure is reported once and
                        // every later poll sees a closed lane, never a silent one.
                        task_tx.fail(format!(
                            "{} event conversion panicked; plant stream stopped",
                            stream.plant()
                        ));
                        task_tx.close();
                        break;
                    }
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
pub(crate) struct Plants {
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
    /// Bumped by every `connect` / `disconnect`, so a ticker reconnect run
    /// without the session lock can tell the session moved on underneath it.
    epoch: u64,
}

/// A ticker reconnect prepared under the session lock ([`Plants::begin_ticker_reset`])
/// and run without it ([`TickerReset::reconnect`]), so order calls and account
/// lookups never wait behind the venue round trips.
pub struct TickerReset {
    old: Option<TickerPlant>,
    epoch: u64,
    /// `None` when the session does not want a ticker (nothing to reconnect).
    connect: Option<(RithmicConfig, Arc<StreamHub>)>,
}

/// The outcome of [`TickerReset::reconnect`], installed by
/// [`Plants::finish_ticker_reset`].
pub struct ReconnectedTicker {
    plant: Option<TickerPlant>,
    epoch: u64,
}

impl TickerReset {
    /// Log the old ticker out and connect a new one. A failed reconnect leaves
    /// the ticker down (the data client's resync retries); the sibling plants
    /// are never touched.
    pub async fn reconnect(self) -> Result<ReconnectedTicker> {
        if let Some(old) = self.old {
            old.close().await;
        }
        let plant = match self.connect {
            Some((rc, hub)) => Some(connect_ticker(&rc, &hub).await?),
            None => None,
        };
        Ok(ReconnectedTicker {
            plant,
            epoch: self.epoch,
        })
    }
}

impl std::fmt::Debug for Plants {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Plants")
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
        check_response(
            handle
                .subscribe(symbol, exchange)
                .await
                .map_err(crate::error::rithmic_error)?,
            "subscribe",
        )?;
        Ok(())
    }

    /// Unsubscribe LastTrade + BBO.
    pub async fn unsubscribe(&self, symbol: &str, exchange: &str) -> Result<()> {
        let handle = self.ticker_handle()?;
        check_response(
            handle
                .unsubscribe(symbol, exchange)
                .await
                .map_err(crate::error::rithmic_error)?,
            "unsubscribe",
        )?;
        Ok(())
    }

    /// Resolve front-month contract for a root symbol.
    pub async fn get_front_month(&self, symbol: &str, exchange: &str) -> Result<FrontMonthDto> {
        let handle = self.ticker_handle()?;
        let resp = handle
            .get_front_month_contract(symbol, exchange, false)
            .await
            .map_err(crate::error::rithmic_error)?;
        check_response_ref(&resp, "get_front_month")?;
        crate::dto::front_month_from_response(&resp)
            .ok_or_else(|| Error::Protocol("unexpected front-month response variant".into()))
    }

    /// Subscribe to order-book summary (level aggregated).
    pub async fn subscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()> {
        let handle = self.ticker_handle()?;
        check_response(
            handle
                .subscribe_order_book_summary(symbol, exchange)
                .await
                .map_err(crate::error::rithmic_error)?,
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
                .await
                .map_err(crate::error::rithmic_error)?,
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
        let resp = handle
            .get_reference_data(symbol, exchange)
            .await
            .map_err(crate::error::rithmic_error)?;
        check_response_ref(&resp, "get_reference_data")?;
        crate::dto::reference_data_from_response(&resp).ok_or_else(|| {
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
            tick_dedup_key,
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
            bar_dedup_key,
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
            .await
            .map_err(crate::error::rithmic_error)?;
        Ok(responses
            .iter()
            .map(crate::dto::time_bar_probe_from_response)
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
                .await
                .map_err(crate::error::rithmic_error)?,
            ctx,
        )
    }

    /// Subscribe to PnL updates and request a snapshot when account is configured.
    pub async fn subscribe_pnl(&self) -> Result<()> {
        let handle = self.pnl_handle()?;
        check_response(
            handle
                .subscribe_pnl_updates()
                .await
                .map_err(crate::error::rithmic_error)?,
            "subscribe_pnl",
        )?;
        check_response(
            handle
                .get_pnl_position_snapshot()
                .await
                .map_err(crate::error::rithmic_error)?,
            "pnl_snapshot",
        )?;
        Ok(())
    }
}

// Retain the former public facade helpers unchanged during the contract move.
#[allow(dead_code)]
impl Plants {
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
            epoch: 0,
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
            self.history = Some(connect_history(&rc, &self.streams).await?);
            // Same bar as `connect`: logged in is not ready to serve loads (RC4).
            if let Err(err) = self.prove_history_ready().await {
                if let Some(history) = self.history.take() {
                    history.close().await;
                }
                return Err(err);
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
        self.epoch += 1;

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
        self.order = Some(connect_order(&rc, &account, &self.streams).await?);
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
            check_response(
                bootstrap_handle
                    .login()
                    .await
                    .map_err(crate::error::rithmic_error)?,
                "order login (discover)",
            )?;
            let list = bootstrap_handle
                .get_account_list()
                .await
                .map_err(crate::error::rithmic_error)?;
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
        self.pnl = Some(connect_pnl(&rc, &account, &self.streams).await?);
        Ok(())
    }

    /// Gracefully disconnect connected plants (order first, then PnL, history, ticker).
    pub async fn disconnect(&mut self) -> Result<()> {
        self.epoch += 1;
        if let Some(order) = self.order.take() {
            order.close().await;
        }
        if let Some(pnl) = self.pnl.take() {
            pnl.close().await;
        }
        if let Some(history) = self.history.take() {
            history.close().await;
        }
        if let Some(ticker) = self.ticker.take() {
            ticker.close().await;
        }
        Ok(())
    }

    /// Subscribe to order plant notifications (Rithmic + exchange).
    pub async fn subscribe_order_updates(&mut self) -> Result<()> {
        let opened_here = self.order.is_none();
        self.ensure_order_plant().await?;
        let result = async {
            let handle = self.order_handle()?;
            check_response(
                handle
                    .subscribe_order_updates()
                    .await
                    .map_err(crate::error::rithmic_error)?,
                "subscribe_order_updates",
            )
        }
        .await;
        rollback_order_plant_subscribe(self, result, opened_here).await
    }

    /// Disconnect only the order plant (leaves ticker/history/PnL connected).
    pub async fn disconnect_order_plant(&mut self) -> Result<()> {
        if let Some(order) = self.order.take() {
            order.close().await;
        }
        Ok(())
    }

    /// Disconnect only the PnL plant (leaves ticker/history/order connected).
    pub async fn disconnect_pnl_plant(&mut self) -> Result<()> {
        if let Some(pnl) = self.pnl.take() {
            pnl.close().await;
        }
        Ok(())
    }

    /// Start a ticker-only reconnect (leaves history/PnL/order connected).
    ///
    /// The data client's channel-error resync needs a fresh ticker receiver,
    /// but the exec client shares this session, so the PnL/order plants must
    /// not be disturbed. The old ticker is taken out here; the venue round
    /// trips run in [`TickerReset::reconnect`] without the session lock.
    pub fn begin_ticker_reset(&mut self) -> Result<TickerReset> {
        let connect = if self.plants.ticker {
            Some((self.config.to_rithmic_config()?, Arc::clone(&self.streams)))
        } else {
            None
        };
        Ok(TickerReset {
            old: self.ticker.take(),
            epoch: self.epoch,
            connect,
        })
    }

    /// Install a reconnected ticker. If the session connected or disconnected
    /// while the reset ran, the new plant belongs to nobody: it is logged out.
    pub async fn finish_ticker_reset(&mut self, done: ReconnectedTicker) {
        let Some(plant) = done.plant else {
            return;
        };
        if done.epoch == self.epoch && self.ticker.is_none() {
            self.ticker = Some(plant);
        } else {
            plant.close().await;
        }
    }

    /// Subscribe to bracket update notifications (required for plant brackets).
    pub async fn subscribe_bracket_updates(&mut self) -> Result<()> {
        let opened_here = self.order.is_none();
        self.ensure_order_plant().await?;
        let result = async {
            let handle = self.order_handle()?;
            check_response(
                handle
                    .subscribe_bracket_updates()
                    .await
                    .map_err(crate::error::rithmic_error)?,
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

    /// The order handle if the order plant is up (no connect attempt).
    pub fn current_order_handle(&self) -> Option<RithmicOrderPlantHandle> {
        self.order.as_ref().map(|o| o.handle.clone())
    }

    /// Clone a command handle for order I/O outside the session lock.
    ///
    /// The clone shares the plant command channel; its subscription receiver is a
    /// separate resubscribe and is unused for place/cancel/modify.
    pub fn clone_order_handle(&self) -> Result<RithmicOrderPlantHandle> {
        Ok(self.order_handle()?.clone())
    }
}

/// Side, price type and duration from their venue names. A bad value is caught
/// before anything is sent (`NotSent`); an empty duration is a day order.
fn parse_order_terms(
    side: &str,
    price_type: &str,
    duration: &str,
) -> Result<(OrderSide, OrderType, TimeInForce)> {
    let side: OrderSide = side
        .parse()
        .map_err(|e| Error::NotSent(format!("invalid order side: {e}")))?;
    let price_type: OrderType = price_type
        .parse()
        .map_err(|e| Error::NotSent(format!("invalid price type: {e}")))?;
    let duration: TimeInForce = if duration.is_empty() {
        TimeInForce::Day
    } else {
        duration
            .parse()
            .map_err(|e| Error::NotSent(format!("invalid duration: {e}")))?
    };
    Ok((side, price_type, duration))
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
    let (side, price_type, duration) = parse_order_terms(side, price_type, duration)?;

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
    let order = builder.build().map_err(|e| Error::NotSent(e.to_string()))?;
    order_result_many(handle.place_order(order).await, "place_order")
}

/// Cancel an order on an already-connected handle (no session lock).
pub async fn cancel_order_on(handle: &RithmicOrderPlantHandle, basket_id: &str) -> Result<()> {
    let cancel = RithmicCancelOrder::new()
        .id(basket_id)
        .manual_or_auto(ManualOrAutoEntry::Auto)
        .build()
        .map_err(|e| Error::NotSent(e.to_string()))?;
    order_result_many(handle.cancel_order(cancel).await, "cancel_order")
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
        return Err(Error::NotSent(
            "place_bracket_order requires stop_ticks and/or target_ticks".into(),
        ));
    }
    let (side, price_type, duration) = parse_order_terms(side, price_type, duration)?;

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
    let order = builder.build().map_err(|e| Error::NotSent(e.to_string()))?;
    order_result_many(
        handle.place_bracket_order(order).await,
        "place_bracket_order",
    )
}

/// Which leg of a plant bracket an adjustment moves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BracketLeg {
    Stop,
    Target,
}

/// Move a bracket leg to `ticks` from the fill, on an already-connected handle.
pub async fn adjust_bracket_on(
    handle: &RithmicOrderPlantHandle,
    leg: BracketLeg,
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
    let adj = builder.build().map_err(|e| Error::NotSent(e.to_string()))?;
    match leg {
        BracketLeg::Stop => order_result_one(handle.adjust_stop(adj).await, "adjust_bracket_stop"),
        BracketLeg::Target => {
            order_result_one(handle.adjust_target(adj).await, "adjust_bracket_target")
        }
    }
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
        .map_err(|e| Error::NotSent(format!("invalid price type: {e}")))?;

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
    let order = builder.build().map_err(|e| Error::NotSent(e.to_string()))?;
    order_result_many(handle.modify_order(order).await, "modify_order")
}

/// Cancel all orders on an already-connected handle (no session lock).
pub async fn cancel_all_orders_on(handle: &RithmicOrderPlantHandle) -> Result<()> {
    let cmd = RithmicCancelAllOrders::new()
        .manual_or_auto(ManualOrAutoEntry::Auto)
        .build()
        .map_err(|e| Error::NotSent(e.to_string()))?;
    order_result_one(handle.cancel_all_orders(cmd).await, "cancel_all_orders")
}

/// Classify a failed state-changing order call at its source, so the class
/// survives every layer above (trait, gateway wire, Python).
///
/// * a reject the venue sent back: [`Error::Rejected`] (definitively not live);
/// * a call that never left (invalid argument, no trade route): [`Error::NotSent`];
/// * anything else (connection closed or write failed mid-call, an undecodable
///   answer): [`Error::OutcomeUnknown`], the order may or may not be live.
///
/// The reject boundary rests on rithmic-rs 3.0.0, which reports every non-zero
/// `rp_code` (except `["7", "no data"]`) as `RequestRejected`. That type cannot
/// be built outside the crate, so no unit test pins it; replay a captured venue
/// reject through the client when a fixture exists, and revisit this arm on a
/// rithmic-rs upgrade.
fn order_failure(e: rithmic_rs::RithmicError) -> Error {
    use rithmic_rs::RithmicError as E;
    match &e {
        E::RequestRejected(_) => Error::Rejected(e.to_string()),
        E::NoTradeRoute { .. } | E::InvalidArgument(_) => Error::NotSent(e.to_string()),
        _ => Error::OutcomeUnknown(e.to_string()),
    }
}

fn order_response_failure(resp: &rithmic_rs::RithmicResponse, ctx: &str) -> Option<Error> {
    resp.error.as_ref().map(|e| {
        let class = order_failure(e.clone());
        let message = format!("{ctx}: {e}");
        match class {
            Error::Rejected(_) => Error::Rejected(message),
            Error::NotSent(_) => Error::NotSent(message),
            _ => Error::OutcomeUnknown(message),
        }
    })
}

fn order_result_many(
    result: std::result::Result<Vec<rithmic_rs::RithmicResponse>, rithmic_rs::RithmicError>,
    ctx: &str,
) -> Result<()> {
    let responses = result.map_err(order_failure)?;
    match responses
        .iter()
        .find_map(|r| order_response_failure(r, ctx))
    {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

fn order_result_one(
    result: std::result::Result<rithmic_rs::RithmicResponse, rithmic_rs::RithmicError>,
    ctx: &str,
) -> Result<()> {
    let response = result.map_err(order_failure)?;
    match order_response_failure(&response, ctx) {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

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
        Ok(crate::dto::plant_event(&resp))
    }
}

/// Drain order notifications off an order-plant subscription source.
///
/// The drain collects `OrderNotification` events until
/// `ORDER_DRAIN_SETTLE_MS` of silence (or the `ORDER_DRAIN_MAX_MS` hard cap)
/// and returns them in arrival order. A `Closed` / `Lagged` source aborts the
/// drain with the corresponding error (so a dropped order-plant connection
/// surfaces as `ChannelClosed`). Best-effort: see [`load_orders_on`].
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
            Ok(Err(err)) => return Err(err),
            Err(_elapsed) => break, // silence window elapsed -> drain complete
        }
    }
    Ok(out)
}

/// On subscribe failure, disconnect the order plant when `disconnect_on_err`.
/// Both subscribes pass `opened_here`: a plant this call opened is closed again,
/// while a failure on an already-live plant leaves the shared order stream, its
/// other subscription and in-flight orders alone. No order operation can be in
/// flight on a plant opened by this call: its handle is published only after
/// the call returns.
async fn rollback_order_plant_subscribe(
    session: &mut Plants,
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
    check_response(
        handle
            .show_orders()
            .await
            .map_err(crate::error::rithmic_error)?,
        "show_orders",
    )?;
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
    let responses = handle
        .show_order_history_summary(date)
        .await
        .map_err(crate::error::rithmic_error)?;
    check_responses(&responses, "show_order_history_summary")?;
    let mut out: Vec<OrderNotificationDto> = responses
        .iter()
        .filter_map(|r| match crate::dto::plant_event(r) {
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
        .await
        .map_err(crate::error::rithmic_error)?;
    // Recon must not act on a reply the venue abandoned part-way.
    let rows = collect_history_rows(
        responses,
        "load_fill_history",
        crate::dto::fill_history_from_response,
    )
    .map_err(|e| {
        if is_partial_reply(&e) {
            Error::ReconciliationUnavailable(e.to_string())
        } else {
            e
        }
    })?;
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
    let responses = handle
        .show_order_history_dates()
        .await
        .map_err(crate::error::rithmic_error)?;
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
    let responses = handle
        .get_product_rms_info()
        .await
        .map_err(crate::error::rithmic_error)?;
    collect_history_rows(
        responses,
        "load_product_rms_info",
        crate::dto::product_rms_info_from_response,
    )
}

/// Fetch account-level RMS info from an already-connected order-plant handle
/// (no session lock). Read-only venue config — never places or cancels.
pub async fn load_account_rms_info_on(
    handle: &RithmicOrderPlantHandle,
) -> Result<Vec<AccountRmsInfoDto>> {
    let responses = handle
        .get_account_rms_info()
        .await
        .map_err(crate::error::rithmic_error)?;
    collect_history_rows(
        responses,
        "load_account_rms_info",
        crate::dto::account_rms_info_from_response,
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
        Err(e) => Err(crate::error::rithmic_error(e)),
    }
}

/// Bring one plant up: connect within the deadline, log in (logging out again
/// on a failed login, so no half session lingers), then attach its event lane.
macro_rules! connect_plant {
    ($label:literal, $plant:ident, $stream:expr, $rc:expr, $hub:expr, $out:ident, |$p:ident| $handle:expr) => {{
        let $p =
            connect_within_deadline($label, $plant::connect($rc, ConnectStrategy::Retry)).await?;
        let handle = $handle;
        let result = login_result(handle.login().await, concat!($label, " login"));
        after_failed_login(
            async {
                let _ = handle.disconnect().await;
            },
            result,
        )
        .await?;
        let stream = forward(
            $hub,
            $stream,
            handle.subscription_receiver.resubscribe(),
            map_connection_response,
        );
        $out {
            _plant: $p,
            handle,
            _stream: stream,
        }
    }};
}

async fn connect_ticker(rc: &RithmicConfig, hub: &Arc<StreamHub>) -> Result<TickerPlant> {
    Ok(connect_plant!(
        "ticker",
        RithmicTickerPlant,
        Stream::Ticker,
        rc,
        hub,
        TickerPlant,
        |plant| { plant.get_handle() }
    ))
}

async fn connect_history(rc: &RithmicConfig, hub: &Arc<StreamHub>) -> Result<HistoryPlant> {
    Ok(connect_plant!(
        "history",
        RithmicHistoryPlant,
        Stream::History,
        rc,
        hub,
        HistoryPlant,
        |plant| { plant.get_handle() }
    ))
}

async fn connect_pnl(
    rc: &RithmicConfig,
    account: &RithmicAccount,
    hub: &Arc<StreamHub>,
) -> Result<PnlPlant> {
    Ok(connect_plant!(
        "pnl",
        RithmicPnlPlant,
        Stream::Pnl,
        rc,
        hub,
        PnlPlant,
        |plant| { plant.get_handle(account) }
    ))
}

async fn connect_order(
    rc: &RithmicConfig,
    account: &RithmicAccount,
    hub: &Arc<StreamHub>,
) -> Result<OrderPlant> {
    Ok(connect_plant!(
        "order",
        RithmicOrderPlant,
        Stream::Order,
        rc,
        hub,
        OrderPlant,
        |plant| { plant.get_handle(account) }
    ))
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
        .await
        .map_err(crate::error::rithmic_error)?;
    // Multi-response replay: an error frame anywhere fails the slice, so the
    // sliced loader retries or fails it instead of leaving a silent gap.
    collect_history_rows(
        responses,
        "load_ticks",
        crate::dto::history_tick_from_response,
    )
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
        .await
        .map_err(crate::error::rithmic_error)?;
    collect_history_rows(
        responses,
        "load_time_bars",
        crate::dto::history_bar_from_response,
    )
}

/// Rows of a multi-frame reply. Any error frame fails the whole reply: an error
/// after some rows means the venue stopped part-way, and returning the rows that
/// did arrive would pass a partial answer off as complete. The partial case says
/// so in its text (`partial reply`) so callers can tell it apart.
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
    // "Partial" means rows arrived before the error. rithmic-rs 3 puts `rp_code`
    // only on the terminal frame, so in practice the error frame ends the reply;
    // the row count, not frame order, is what is classified here.
    match first_err {
        None => Ok(rows),
        Some(err) if rows.is_empty() => Err(Error::Rithmic(format!("{ctx}: {err}"))),
        Some(err) => Err(Error::Rithmic(format!(
            "{ctx}: {PARTIAL_REPLY} after {} rows: {err}",
            rows.len()
        ))),
    }
}

/// Marks a reply that failed after some rows (see [`collect_history_rows`]).
const PARTIAL_REPLY: &str = "partial reply";

fn is_partial_reply(err: &Error) -> bool {
    matches!(err, Error::Rithmic(text) if text.contains(PARTIAL_REPLY))
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
    Ok(Some(crate::dto::plant_event(&resp)))
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

    fn disconnected_session() -> Plants {
        let cfg = SessionConfig::builder()
            .user("alice")
            .password("pw")
            .build()
            .unwrap();
        Plants::new(cfg)
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
    use rithmic_host::dto::LastTradeDto;
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
                PlantEvent::Gap(rithmic_host::dto::GapDto { skipped: 6, .. })
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
    async fn a_panicking_conversion_fails_the_lane_instead_of_stalling_it() {
        fn panicking(seq: i32) -> Result<Option<PlantEvent>> {
            if seq < 0 {
                panic!("bad venue timestamp");
            }
            to_event(seq)
        }
        let hub = StreamHub::new();
        let (tx, rx) = tokio::sync::broadcast::channel(8);
        let _guard = forward(&hub, Stream::Order, rx, panicking);
        tx.send(1).unwrap();
        tx.send(-1).unwrap();
        tx.send(2).unwrap();
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
            matches!(&err, Error::Rithmic(m) if m.contains("panicked")),
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

#[cfg(test)]
mod order_class_tests {
    use super::*;
    use rithmic_rs::RithmicError as E;

    // `RequestRejected` has no public constructor (the type is non-exhaustive), so
    // the reject arm is exercised end to end through `FakeFailure::Rejected` in the
    // gateway scenarios; the arms below are the ones a test can build.

    #[test]
    fn a_call_that_never_left_is_not_sent() {
        assert!(matches!(
            order_failure(E::InvalidArgument("quantity".into())),
            Error::NotSent(_)
        ));
    }

    #[test]
    fn anything_that_may_have_reached_the_venue_is_unknown() {
        for e in [
            E::ConnectionClosed,
            E::SendFailed,
            E::EmptyResponse,
            E::ProtocolError("bad frame".into()),
            E::ConnectionFailed("reset".into()),
        ] {
            assert!(
                matches!(order_failure(e.clone()), Error::OutcomeUnknown(_)),
                "{e:?}"
            );
        }
    }

    #[test]
    fn a_failed_call_keeps_its_class_through_the_result_helpers() {
        let unknown = order_result_many(Err(E::ConnectionClosed), "place_order");
        assert!(matches!(unknown, Err(Error::OutcomeUnknown(_))));
        let not_sent = order_result_one(Err(E::InvalidArgument("x".into())), "cancel_all_orders");
        assert!(matches!(not_sent, Err(Error::NotSent(_))));
        assert!(order_result_many(Ok(Vec::new()), "place_order").is_ok());
    }
}

#[cfg(test)]
mod forwarder_tests {
    use super::*;

    struct Items(tokio::sync::mpsc::Receiver<u32>);

    #[async_trait::async_trait]
    impl Feed for Items {
        type Item = u32;

        async fn next_item(&mut self) -> std::result::Result<u32, RecvError> {
            self.0.recv().await.ok_or(RecvError::Closed)
        }
    }

    fn panics(_: u32) -> Result<Option<PlantEvent>> {
        panic!("unconvertible venue value")
    }

    #[tokio::test]
    async fn a_conversion_panic_fails_the_lane_once_then_reads_closed_forever() {
        let hub = StreamHub::new();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let _guard = forward(&hub, Stream::Ticker, Items(rx), panics);
        tx.send(1).await.unwrap();

        let wait = Duration::from_secs(2);
        let first = hub.poll(Stream::Ticker, wait).await.unwrap_err();
        assert!(first.to_string().contains("panicked"), "{first}");
        for _ in 0..3 {
            let later = hub.poll(Stream::Ticker, wait).await.unwrap_err();
            assert!(matches!(later, Error::ChannelClosed { .. }), "{later}");
        }
    }
}
