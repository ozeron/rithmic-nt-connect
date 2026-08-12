//! Multi-plant Rithmic session facade (Phase 1: MD + history + PnL; no orders).
//!
//! Intentionally omits any public place / cancel / modify order APIs.

use std::future::Future;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use rithmic_rs::rti::messages::RithmicMessage;
use rithmic_rs::{
    ConnectStrategy, RithmicHistoryPlant, RithmicHistoryPlantHandle, RithmicPnlPlant,
    RithmicPnlPlantHandle, RithmicTickerPlant, RithmicTickerPlantHandle, TimeBarType,
};
use tokio::sync::broadcast::error::TryRecvError;

use crate::config::SessionConfig;
use crate::dto::{FrontMonthDto, HistoryTickDto, ReferenceDataDto, TickerEvent};
use crate::error::{Error, Result};

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

/// Connected multi-plant session used by the Python adapter.
pub struct RithmicSession {
    config: SessionConfig,
    ticker: Option<TickerPlant>,
    history: Option<HistoryPlant>,
    pnl: Option<PnlPlant>,
}

impl std::fmt::Debug for RithmicSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RithmicSession")
            .field("config", &self.config)
            .field("ticker_connected", &self.ticker.is_some())
            .field("history_connected", &self.history.is_some())
            .field("pnl_connected", &self.pnl.is_some())
            .finish()
    }
}

impl RithmicSession {
    /// Create a disconnected session from config.
    pub fn new(config: SessionConfig) -> Self {
        Self {
            config,
            ticker: None,
            history: None,
            pnl: None,
        }
    }

    /// Session configuration.
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    /// Connect ticker + history plants (and PnL when account is configured).
    ///
    /// Uses [`ConnectStrategy::Retry`]. Does **not** expose order-plant trading.
    pub async fn connect(&mut self) -> Result<()> {
        if self.ticker.is_some() {
            return Err(Error::Session("already connected".into()));
        }

        let rc = self.config.to_rithmic_config()?;

        let ticker_plant =
            RithmicTickerPlant::connect(&rc, ConnectStrategy::Retry).await?;
        let ticker_handle = ticker_plant.get_handle();
        check_response(ticker_handle.login().await?, "ticker login")?;

        let history_plant =
            RithmicHistoryPlant::connect(&rc, ConnectStrategy::Retry).await?;
        let history_handle = history_plant.get_handle();
        check_response(history_handle.login().await?, "history login")?;

        let mut pnl = None;
        if let Some(account) = self.config.account() {
            let pnl_plant = RithmicPnlPlant::connect(&rc, ConnectStrategy::Retry).await?;
            let pnl_handle = pnl_plant.get_handle(&account);
            check_response(pnl_handle.login().await?, "pnl login")?;
            pnl = Some(PnlPlant {
                _plant: pnl_plant,
                handle: pnl_handle,
            });
        }

        self.ticker = Some(TickerPlant {
            _plant: ticker_plant,
            handle: ticker_handle,
        });
        self.history = Some(HistoryPlant {
            _plant: history_plant,
            handle: history_handle,
        });
        self.pnl = pnl;
        Ok(())
    }

    /// Gracefully disconnect connected plants.
    pub async fn disconnect(&mut self) -> Result<()> {
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

    /// Load all ticks in `[start_time_sec, end_time_sec]` via history `*_all`.
    pub async fn load_ticks_all(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<HistoryTickDto>> {
        let handle = self.history_handle()?;
        let responses = handle
            .load_ticks_all(
                symbol.to_string(),
                exchange.to_string(),
                start_time_sec,
                end_time_sec,
            )
            .await?;
        Ok(responses
            .iter()
            .filter_map(HistoryTickDto::from_response)
            .filter(|t| t.close_price.is_some() || t.open_price.is_some())
            .collect())
    }

    /// Load all time bars in the window via history `*_all`.
    pub async fn load_time_bars_all(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: TimeBarType,
        bar_type_period: i32,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<RithmicMessage>> {
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
        Ok(responses.into_iter().map(|r| r.message).collect())
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

    /// Non-blocking poll of the next ticker-plant subscription message.
    pub fn poll_event(&mut self) -> Result<Option<TickerEvent>> {
        let handle = self.ticker_handle_mut()?;
        match handle.subscription_receiver.try_recv() {
            Ok(resp) => {
                if let Some(err) = &resp.error {
                    if err.is_connection_issue() {
                        return Err(Error::Rithmic(err.to_string()));
                    }
                }
                Ok(Some(TickerEvent::from(&resp)))
            }
            Err(TryRecvError::Empty) | Err(TryRecvError::Lagged(_)) => Ok(None),
            Err(TryRecvError::Closed) => Err(Error::Session(
                "ticker subscription channel closed".into(),
            )),
        }
    }

    /// Non-blocking poll of the next PnL-plant subscription message.
    pub fn poll_pnl_event(&mut self) -> Result<Option<TickerEvent>> {
        let Some(pnl) = self.pnl.as_mut() else {
            return Ok(None);
        };
        match now_or_never(pnl.handle.subscription_receiver.recv()) {
            Some(Ok(resp)) => {
                if let Some(err) = &resp.error {
                    if err.is_connection_issue() {
                        return Err(Error::Rithmic(err.to_string()));
                    }
                }
                Ok(Some(TickerEvent::from(&resp)))
            }
            Some(Err(_)) => Err(Error::Session("pnl subscription channel closed".into())),
            None => Ok(None),
        }
    }

    /// Blocking receive of the next ticker message (async).
    pub async fn recv_event(&mut self) -> Result<TickerEvent> {
        let handle = self.ticker_handle_mut()?;
        let resp = handle
            .subscription_receiver
            .recv()
            .await
            .map_err(|_| Error::Session("ticker subscription channel closed".into()))?;
        if let Some(err) = &resp.error {
            if err.is_connection_issue() {
                return Err(Error::Rithmic(err.to_string()));
            }
        }
        Ok(TickerEvent::from(&resp))
    }

    fn ticker_handle(&self) -> Result<&RithmicTickerPlantHandle> {
        self.ticker
            .as_ref()
            .map(|t| &t.handle)
            .ok_or_else(|| Error::Session("ticker plant not connected".into()))
    }

    fn ticker_handle_mut(&mut self) -> Result<&mut RithmicTickerPlantHandle> {
        self.ticker
            .as_mut()
            .map(|t| &mut t.handle)
            .ok_or_else(|| Error::Session("ticker plant not connected".into()))
    }

    fn history_handle(&self) -> Result<&RithmicHistoryPlantHandle> {
        self.history
            .as_ref()
            .map(|h| &h.handle)
            .ok_or_else(|| Error::Session("history plant not connected".into()))
    }

    fn pnl_handle(&self) -> Result<&RithmicPnlPlantHandle> {
        self.pnl
            .as_ref()
            .map(|p| &p.handle)
            .ok_or_else(|| Error::Session("pnl plant not connected (account required)".into()))
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
