//! Python bindings for the Phase 2 Rithmic session facade.

use std::sync::{Mutex, OnceLock};

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use tokio::runtime::Runtime;

use crate::config::SessionConfig;
use crate::dto::{
    AccountPnlDto, BboDto, FrontMonthDto, HistoryBarDto, HistoryTickDto, InstrumentPnlDto,
    LastTradeDto, OrderBookDto, OrderNotificationDto, ReferenceDataDto, TickerEvent,
};
use crate::error::Error;
use crate::session::{
    RithmicSession, cancel_all_orders_on, cancel_order_on, modify_order_on, place_order_on,
};

fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().expect("failed to create tokio runtime"))
}

fn to_py_err(err: Error) -> PyErr {
    match err {
        Error::Config(msg) => PyValueError::new_err(msg),
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

fn set_opt_str(dict: &Bound<'_, PyDict>, key: &str, value: Option<String>) -> PyResult<()> {
    if let Some(v) = value {
        dict.set_item(key, v)?;
    }
    Ok(())
}

fn set_opt_f64(dict: &Bound<'_, PyDict>, key: &str, value: Option<f64>) -> PyResult<()> {
    if let Some(v) = value {
        dict.set_item(key, v)?;
    }
    Ok(())
}

fn set_opt_i32(dict: &Bound<'_, PyDict>, key: &str, value: Option<i32>) -> PyResult<()> {
    if let Some(v) = value {
        dict.set_item(key, v)?;
    }
    Ok(())
}

fn set_opt_u64(dict: &Bound<'_, PyDict>, key: &str, value: Option<u64>) -> PyResult<()> {
    if let Some(v) = value {
        dict.set_item(key, v)?;
    }
    Ok(())
}

fn set_opt_bool(dict: &Bound<'_, PyDict>, key: &str, value: Option<bool>) -> PyResult<()> {
    if let Some(v) = value {
        dict.set_item(key, v)?;
    }
    Ok(())
}

fn last_trade_dict(py: Python<'_>, t: LastTradeDto) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("type", "last_trade")?;
    set_opt_str(&d, "symbol", t.symbol)?;
    set_opt_str(&d, "exchange", t.exchange)?;
    set_opt_f64(&d, "trade_price", t.trade_price)?;
    set_opt_i32(&d, "trade_size", t.trade_size)?;
    set_opt_i32(&d, "aggressor", t.aggressor)?;
    set_opt_i32(&d, "ssboe", t.ssboe)?;
    set_opt_i32(&d, "usecs", t.usecs)?;
    set_opt_u64(&d, "ts_event_ns", t.ts_event_ns)?;
    set_opt_bool(&d, "is_snapshot", t.is_snapshot)?;
    Ok(d.unbind())
}

fn bbo_dict(py: Python<'_>, b: BboDto) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("type", "bbo")?;
    set_opt_str(&d, "symbol", b.symbol)?;
    set_opt_str(&d, "exchange", b.exchange)?;
    set_opt_f64(&d, "bid_price", b.bid_price)?;
    set_opt_i32(&d, "bid_size", b.bid_size)?;
    set_opt_f64(&d, "ask_price", b.ask_price)?;
    set_opt_i32(&d, "ask_size", b.ask_size)?;
    set_opt_i32(&d, "ssboe", b.ssboe)?;
    set_opt_i32(&d, "usecs", b.usecs)?;
    set_opt_u64(&d, "ts_event_ns", b.ts_event_ns)?;
    set_opt_bool(&d, "is_snapshot", b.is_snapshot)?;
    Ok(d.unbind())
}

fn account_pnl_dict(py: Python<'_>, a: AccountPnlDto) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("type", "account_pnl")?;
    set_opt_str(&d, "account_id", a.account_id)?;
    set_opt_str(&d, "fcm_id", a.fcm_id)?;
    set_opt_str(&d, "ib_id", a.ib_id)?;
    set_opt_str(&d, "account_balance", a.account_balance)?;
    set_opt_str(&d, "cash_on_hand", a.cash_on_hand)?;
    set_opt_str(&d, "margin_balance", a.margin_balance)?;
    set_opt_str(&d, "day_pnl", a.day_pnl)?;
    set_opt_str(&d, "open_position_pnl", a.open_position_pnl)?;
    set_opt_str(&d, "closed_position_pnl", a.closed_position_pnl)?;
    set_opt_str(&d, "available_buying_power", a.available_buying_power)?;
    set_opt_str(&d, "used_buying_power", a.used_buying_power)?;
    set_opt_bool(&d, "is_snapshot", a.is_snapshot)?;
    set_opt_i32(&d, "ssboe", a.ssboe)?;
    set_opt_i32(&d, "usecs", a.usecs)?;
    Ok(d.unbind())
}

fn instrument_pnl_dict(py: Python<'_>, i: InstrumentPnlDto) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("type", "instrument_pnl")?;
    set_opt_str(&d, "account_id", i.account_id)?;
    set_opt_str(&d, "symbol", i.symbol)?;
    set_opt_str(&d, "exchange", i.exchange)?;
    set_opt_str(&d, "product_code", i.product_code)?;
    set_opt_str(&d, "instrument_type", i.instrument_type)?;
    set_opt_str(&d, "open_position_pnl", i.open_position_pnl)?;
    set_opt_str(&d, "closed_position_pnl", i.closed_position_pnl)?;
    set_opt_str(&d, "mtm_security", i.mtm_security)?;
    set_opt_i32(&d, "open_position_quantity", i.open_position_quantity)?;
    set_opt_i32(&d, "closed_position_quantity", i.closed_position_quantity)?;
    set_opt_i32(&d, "net_quantity", i.net_quantity)?;
    set_opt_f64(&d, "avg_open_fill_price", i.avg_open_fill_price)?;
    set_opt_bool(&d, "is_snapshot", i.is_snapshot)?;
    set_opt_i32(&d, "ssboe", i.ssboe)?;
    set_opt_i32(&d, "usecs", i.usecs)?;
    Ok(d.unbind())
}

fn order_book_dict(py: Python<'_>, o: OrderBookDto) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("type", "order_book")?;
    set_opt_str(&d, "symbol", o.symbol)?;
    set_opt_str(&d, "exchange", o.exchange)?;
    set_opt_i32(&d, "update_type", o.update_type)?;
    d.set_item("bid_price", o.bid_price)?;
    d.set_item("bid_size", o.bid_size)?;
    d.set_item("ask_price", o.ask_price)?;
    d.set_item("ask_size", o.ask_size)?;
    set_opt_i32(&d, "ssboe", o.ssboe)?;
    set_opt_i32(&d, "usecs", o.usecs)?;
    set_opt_u64(&d, "ts_event_ns", o.ts_event_ns)?;
    Ok(d.unbind())
}

fn order_notification_dict(py: Python<'_>, n: OrderNotificationDto) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("type", "order_notification")?;
    d.set_item("source", n.source)?;
    set_opt_i32(&d, "notify_type", n.notify_type)?;
    set_opt_str(&d, "notify_type_name", n.notify_type_name)?;
    set_opt_str(&d, "status", n.status)?;
    set_opt_str(&d, "basket_id", n.basket_id)?;
    set_opt_str(&d, "exchange_order_id", n.exchange_order_id)?;
    set_opt_str(&d, "user_tag", n.user_tag)?;
    set_opt_str(&d, "account_id", n.account_id)?;
    set_opt_str(&d, "symbol", n.symbol)?;
    set_opt_str(&d, "exchange", n.exchange)?;
    set_opt_i32(&d, "quantity", n.quantity)?;
    set_opt_i32(&d, "total_fill_size", n.total_fill_size)?;
    set_opt_i32(&d, "total_unfilled_size", n.total_unfilled_size)?;
    set_opt_i32(&d, "fill_size", n.fill_size)?;
    set_opt_f64(&d, "price", n.price)?;
    set_opt_f64(&d, "trigger_price", n.trigger_price)?;
    set_opt_f64(&d, "avg_fill_price", n.avg_fill_price)?;
    set_opt_f64(&d, "fill_price", n.fill_price)?;
    set_opt_i32(&d, "transaction_type", n.transaction_type)?;
    set_opt_i32(&d, "price_type", n.price_type)?;
    set_opt_str(&d, "fill_id", n.fill_id)?;
    set_opt_str(&d, "text", n.text)?;
    set_opt_str(&d, "report_text", n.report_text)?;
    set_opt_str(&d, "completion_reason", n.completion_reason)?;
    set_opt_i32(&d, "ssboe", n.ssboe)?;
    set_opt_i32(&d, "usecs", n.usecs)?;
    set_opt_u64(&d, "ts_event_ns", n.ts_event_ns)?;
    set_opt_bool(&d, "is_snapshot", n.is_snapshot)?;
    Ok(d.unbind())
}

fn reference_data_dict(py: Python<'_>, r: ReferenceDataDto) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("type", "reference_data")?;
    set_opt_str(&d, "symbol", r.symbol)?;
    set_opt_str(&d, "exchange", r.exchange)?;
    set_opt_str(&d, "trading_symbol", r.trading_symbol)?;
    set_opt_str(&d, "trading_exchange", r.trading_exchange)?;
    set_opt_str(&d, "symbol_name", r.symbol_name)?;
    set_opt_str(&d, "product_code", r.product_code)?;
    set_opt_str(&d, "instrument_type", r.instrument_type)?;
    set_opt_str(&d, "underlying", r.underlying)?;
    set_opt_str(&d, "currency", r.currency)?;
    set_opt_str(&d, "expiration_date", r.expiration_date)?;
    set_opt_f64(&d, "tick_size", r.tick_size)?;
    set_opt_f64(&d, "point_value", r.point_value)?;
    d.set_item("price_precision", r.price_precision)?;
    d.set_item("is_tradable", r.is_tradable)?;
    Ok(d.unbind())
}

fn event_to_dict(py: Python<'_>, event: TickerEvent) -> PyResult<Py<PyDict>> {
    match event {
        TickerEvent::LastTrade(t) => last_trade_dict(py, t),
        TickerEvent::Bbo(b) => bbo_dict(py, b),
        TickerEvent::OrderBook(o) => order_book_dict(py, o),
        TickerEvent::AccountPnl(a) => account_pnl_dict(py, a),
        TickerEvent::InstrumentPnl(i) => instrument_pnl_dict(py, i),
        TickerEvent::OrderNotification(n) => order_notification_dict(py, n),
        TickerEvent::Other { type_name, source } => {
            let d = PyDict::new(py);
            d.set_item("type", "other")?;
            d.set_item("type_name", type_name)?;
            d.set_item("source", source)?;
            Ok(d.unbind())
        }
    }
}

fn front_month_dict(py: Python<'_>, f: FrontMonthDto) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("type", "front_month")?;
    set_opt_str(&d, "symbol", f.symbol)?;
    set_opt_str(&d, "exchange", f.exchange)?;
    set_opt_str(&d, "trading_symbol", f.trading_symbol)?;
    set_opt_str(&d, "trading_exchange", f.trading_exchange)?;
    set_opt_str(&d, "symbol_name", f.symbol_name)?;
    set_opt_bool(&d, "is_front_month_symbol", f.is_front_month_symbol)?;
    Ok(d.unbind())
}

fn history_tick_dict(py: Python<'_>, t: HistoryTickDto) -> PyResult<Py<PyDict>> {
    // Tick-bar replay schema: last price = close_price, size = num_trades.
    // Both must be present; no open_price / invented-size substitutes.
    let Some(trade_price) = t.close_price else {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "history_tick missing close_price",
        ));
    };
    let Some(num_trades) = t.num_trades else {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "history_tick missing num_trades",
        ));
    };
    if num_trades == 0 || num_trades > i32::MAX as u64 {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "history_tick num_trades out of range: {num_trades}"
        )));
    }
    let d = PyDict::new(py);
    d.set_item("type", "history_tick")?;
    set_opt_str(&d, "symbol", t.symbol)?;
    set_opt_str(&d, "exchange", t.exchange)?;
    set_opt_f64(&d, "open_price", t.open_price)?;
    set_opt_f64(&d, "high_price", t.high_price)?;
    set_opt_f64(&d, "low_price", t.low_price)?;
    set_opt_f64(&d, "close_price", t.close_price)?;
    d.set_item("trade_price", trade_price)?;
    d.set_item("trade_size", num_trades as i32)?;
    set_opt_u64(&d, "volume", t.volume)?;
    d.set_item("num_trades", num_trades)?;
    set_opt_i32(&d, "ssboe", t.ssboe)?;
    set_opt_i32(&d, "usecs", t.usecs)?;
    set_opt_u64(&d, "ts_event_ns", t.ts_event_ns)?;
    Ok(d.unbind())
}

fn history_bar_dict(py: Python<'_>, b: HistoryBarDto) -> PyResult<Py<PyDict>> {
    let Some(symbol) = b.symbol.clone() else {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "history_bar missing symbol",
        ));
    };
    let Some(open_price) = b.open_price else {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "history_bar missing open_price",
        ));
    };
    let Some(high_price) = b.high_price else {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "history_bar missing high_price",
        ));
    };
    let Some(low_price) = b.low_price else {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "history_bar missing low_price",
        ));
    };
    let Some(close_price) = b.close_price else {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "history_bar missing close_price",
        ));
    };
    let Some(volume) = b.volume else {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "history_bar missing volume",
        ));
    };
    let d = PyDict::new(py);
    d.set_item("type", "history_bar")?;
    d.set_item("symbol", symbol)?;
    set_opt_str(&d, "exchange", b.exchange)?;
    set_opt_i32(&d, "bar_type", b.bar_type)?;
    set_opt_str(&d, "period", b.period)?;
    set_opt_i32(&d, "marker", b.marker)?;
    d.set_item("open_price", open_price)?;
    d.set_item("high_price", high_price)?;
    d.set_item("low_price", low_price)?;
    d.set_item("close_price", close_price)?;
    d.set_item("volume", volume)?;
    set_opt_u64(&d, "num_trades", b.num_trades)?;
    set_opt_u64(&d, "bid_volume", b.bid_volume)?;
    set_opt_u64(&d, "ask_volume", b.ask_volume)?;
    set_opt_u64(&d, "ts_event_ns", b.ts_event_ns)?;
    Ok(d.unbind())
}

/// Python-facing Rithmic multi-plant session.
#[pyclass(name = "Session")]
pub struct PySession {
    inner: Mutex<RithmicSession>,
}

#[pymethods]
impl PySession {
    #[new]
    #[pyo3(signature = (
        user,
        password,
        system_name="LucidTrading",
        url="wss://rprotocol.rithmic.com:443",
        app_name="rithmic-connect",
        app_version="0.1.0",
        env="Live",
        account_id=None,
        fcm_id=None,
        ib_id=None,
        beta_url=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        user: String,
        password: String,
        system_name: &str,
        url: &str,
        app_name: &str,
        app_version: &str,
        env: &str,
        account_id: Option<String>,
        fcm_id: Option<String>,
        ib_id: Option<String>,
        beta_url: Option<String>,
    ) -> PyResult<Self> {
        let renv = match env {
            "Live" | "live" => rithmic_rs::RithmicEnv::Live,
            "Demo" | "demo" => rithmic_rs::RithmicEnv::Demo,
            "Test" | "test" => rithmic_rs::RithmicEnv::Test,
            other => {
                return Err(PyValueError::new_err(format!(
                    "invalid env {other:?}; expected Live, Demo, or Test"
                )));
            }
        };
        let mut builder = SessionConfig::builder()
            .user(user)
            .password(password)
            .system_name(system_name)
            .url(url)
            .app_name(app_name)
            .app_version(app_version)
            .env(renv);
        if let Some(beta) = beta_url {
            builder = builder.beta_url(beta);
        }
        if let Some(v) = account_id {
            builder = builder.account_id(v);
        }
        if let Some(v) = fcm_id {
            builder = builder.fcm_id(v);
        }
        if let Some(v) = ib_id {
            builder = builder.ib_id(v);
        }
        let config = builder.build().map_err(to_py_err)?;
        Ok(Self {
            inner: Mutex::new(RithmicSession::new(config)),
        })
    }

    fn connect(&self) -> PyResult<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        runtime().block_on(inner.connect()).map_err(to_py_err)
    }

    fn disconnect(&self) -> PyResult<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        runtime().block_on(inner.disconnect()).map_err(to_py_err)
    }

    fn subscribe(&self, symbol: &str, exchange: &str) -> PyResult<()> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        runtime()
            .block_on(inner.subscribe(symbol, exchange))
            .map_err(to_py_err)
    }

    fn unsubscribe(&self, symbol: &str, exchange: &str) -> PyResult<()> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        runtime()
            .block_on(inner.unsubscribe(symbol, exchange))
            .map_err(to_py_err)
    }

    fn get_front_month(&self, symbol: &str, exchange: &str) -> PyResult<Py<PyDict>> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let dto = runtime()
            .block_on(inner.get_front_month(symbol, exchange))
            .map_err(to_py_err)?;
        Python::with_gil(|py| front_month_dict(py, dto))
    }

    fn get_reference_data(&self, symbol: &str, exchange: &str) -> PyResult<Py<PyDict>> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let dto = runtime()
            .block_on(inner.get_reference_data(symbol, exchange))
            .map_err(to_py_err)?;
        Python::with_gil(|py| reference_data_dict(py, dto))
    }

    /// Non-blocking poll; returns a dict or None.
    fn poll_event(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        match inner.poll_event().map_err(to_py_err)? {
            Some(event) => Ok(Some(event_to_dict(py, event)?)),
            None => Ok(None),
        }
    }

    /// Non-blocking poll of the PnL plant; returns a dict or None.
    fn poll_pnl_event(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        match inner.poll_pnl_event().map_err(to_py_err)? {
            Some(event) => Ok(Some(event_to_dict(py, event)?)),
            None => Ok(None),
        }
    }

    /// Load historical ticks for a window; returns a list of dicts.
    fn load_ticks(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> PyResult<Py<PyList>> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let ticks = runtime()
            .block_on(inner.load_ticks_all(symbol, exchange, start_time_sec, end_time_sec))
            .map_err(to_py_err)?;
        Python::with_gil(|py| {
            let list = PyList::empty(py);
            for tick in ticks {
                list.append(history_tick_dict(py, tick)?)?;
            }
            Ok(list.unbind())
        })
    }

    fn subscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> PyResult<()> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        runtime()
            .block_on(inner.subscribe_order_book_summary(symbol, exchange))
            .map_err(to_py_err)
    }

    /// Load minute time bars for a window; returns structured OHLCV dicts.
    #[pyo3(signature = (symbol, exchange, start_time_sec, end_time_sec, bar_type=2, period=1))]
    fn load_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
        bar_type: i32,
        period: i32,
    ) -> PyResult<Py<PyList>> {
        let rithmic_bar_type = match bar_type {
            1 => rithmic_rs::TimeBarType::SecondBar,
            2 => rithmic_rs::TimeBarType::MinuteBar,
            3 => rithmic_rs::TimeBarType::DailyBar,
            4 => rithmic_rs::TimeBarType::WeeklyBar,
            other => {
                return Err(PyValueError::new_err(format!(
                    "unsupported bar_type {other}; expected 1=second, 2=minute, 3=daily, 4=weekly"
                )));
            }
        };
        let period = period.max(1);
        let inner = self
            .inner
            .lock()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let bars = runtime()
            .block_on(inner.load_time_bars_all(
                symbol,
                exchange,
                rithmic_bar_type,
                period,
                start_time_sec,
                end_time_sec,
            ))
            .map_err(to_py_err)?;
        Python::with_gil(|py| {
            let list = PyList::empty(py);
            for bar in bars {
                list.append(history_bar_dict(py, bar)?)?;
            }
            Ok(list.unbind())
        })
    }

    fn subscribe_pnl(&self) -> PyResult<()> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        runtime().block_on(inner.subscribe_pnl()).map_err(to_py_err)
    }

    fn subscribe_order_updates(&self) -> PyResult<()> {
        // Subscribe must use the session's own handle (the one poll_order_event reads).
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        runtime()
            .block_on(inner.subscribe_order_updates())
            .map_err(to_py_err)
    }

    #[pyo3(signature = (
        symbol,
        exchange,
        side,
        price_type,
        quantity,
        user_tag,
        price=None,
        trigger_price=None,
        duration="DAY",
    ))]
    #[allow(clippy::too_many_arguments)]
    fn place_order(
        &self,
        symbol: &str,
        exchange: &str,
        side: &str,
        price_type: &str,
        quantity: i32,
        user_tag: &str,
        price: Option<f64>,
        trigger_price: Option<f64>,
        duration: &str,
    ) -> PyResult<()> {
        // Ensure + clone under a short lock, then release before network I/O so
        // poll_order_event can run concurrently.
        let handle = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            runtime()
                .block_on(inner.ensure_order_plant())
                .map_err(to_py_err)?;
            inner.clone_order_handle().map_err(to_py_err)?
        };
        runtime()
            .block_on(place_order_on(
                &handle,
                symbol,
                exchange,
                side,
                price_type,
                quantity,
                user_tag,
                price,
                trigger_price,
                duration,
            ))
            .map_err(to_py_err)
    }

    fn cancel_order(&self, basket_id: &str) -> PyResult<()> {
        let handle = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            runtime()
                .block_on(inner.ensure_order_plant())
                .map_err(to_py_err)?;
            inner.clone_order_handle().map_err(to_py_err)?
        };
        runtime()
            .block_on(cancel_order_on(&handle, basket_id))
            .map_err(to_py_err)
    }

    #[pyo3(signature = (
        basket_id,
        symbol,
        exchange,
        quantity,
        price_type,
        price=None,
        trigger_price=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn modify_order(
        &self,
        basket_id: &str,
        symbol: &str,
        exchange: &str,
        quantity: i32,
        price_type: &str,
        price: Option<f64>,
        trigger_price: Option<f64>,
    ) -> PyResult<()> {
        let handle = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            runtime()
                .block_on(inner.ensure_order_plant())
                .map_err(to_py_err)?;
            inner.clone_order_handle().map_err(to_py_err)?
        };
        runtime()
            .block_on(modify_order_on(
                &handle,
                basket_id,
                symbol,
                exchange,
                quantity,
                price_type,
                price,
                trigger_price,
            ))
            .map_err(to_py_err)
    }

    fn cancel_all_orders(&self) -> PyResult<()> {
        let handle = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            runtime()
                .block_on(inner.ensure_order_plant())
                .map_err(to_py_err)?;
            inner.clone_order_handle().map_err(to_py_err)?
        };
        runtime()
            .block_on(cancel_all_orders_on(&handle))
            .map_err(to_py_err)
    }

    /// Non-blocking poll of the order plant; returns a dict or None.
    fn poll_order_event(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        match inner.poll_order_event().map_err(to_py_err)? {
            Some(event) => Ok(Some(event_to_dict(py, event)?)),
            None => Ok(None),
        }
    }
}

/// Register the PyO3 module `rithmic_connect._lib`.
#[pymodule]
fn _lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySession>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
