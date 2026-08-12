//! Python bindings for the Phase 1 Rithmic session facade.

use std::sync::{Mutex, OnceLock};

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use tokio::runtime::Runtime;

use crate::config::SessionConfig;
use crate::dto::{
    AccountPnlDto, BboDto, FrontMonthDto, HistoryTickDto, InstrumentPnlDto, LastTradeDto,
    OrderBookDto, ReferenceDataDto, TickerEvent,
};
use crate::error::Error;
use crate::session::RithmicSession;

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
    let d = PyDict::new(py);
    d.set_item("type", "history_tick")?;
    set_opt_str(&d, "symbol", t.symbol)?;
    set_opt_str(&d, "exchange", t.exchange)?;
    set_opt_f64(&d, "open_price", t.open_price)?;
    set_opt_f64(&d, "high_price", t.high_price)?;
    set_opt_f64(&d, "low_price", t.low_price)?;
    set_opt_f64(&d, "close_price", t.close_price)?;
    set_opt_f64(&d, "trade_price", t.close_price.or(t.open_price))?;
    set_opt_u64(&d, "volume", t.volume)?;
    set_opt_u64(&d, "num_trades", t.num_trades)?;
    if let Some(n) = t.num_trades {
        set_opt_i32(&d, "trade_size", Some(n as i32))?;
    }
    set_opt_i32(&d, "ssboe", t.ssboe)?;
    set_opt_i32(&d, "usecs", t.usecs)?;
    set_opt_u64(&d, "ts_event_ns", t.ts_event_ns)?;
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

    /// Load minute time bars for a window; returns opaque message dicts (Phase 1).
    #[pyo3(signature = (symbol, exchange, start_time_sec, end_time_sec, bar_type=1, period=1))]
    fn load_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
        bar_type: i32,
        period: i32,
    ) -> PyResult<Py<PyList>> {
        let _ = bar_type; // Phase 1: always MinuteBar
        let inner = self
            .inner
            .lock()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let messages = runtime()
            .block_on(inner.load_time_bars_all(
                symbol,
                exchange,
                rithmic_rs::TimeBarType::MinuteBar,
                period,
                start_time_sec,
                end_time_sec,
            ))
            .map_err(to_py_err)?;
        Python::with_gil(|py| {
            let list = PyList::empty(py);
            for message in messages {
                let d = PyDict::new(py);
                d.set_item("type", "time_bar")?;
                d.set_item("type_name", format!("{message:?}"))?;
                list.append(d)?;
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
}

/// Register the PyO3 module `rithmic_connect._lib`.
#[pymodule]
fn _lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySession>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
