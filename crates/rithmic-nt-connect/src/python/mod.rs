//! Python bindings for the Phase 2 Rithmic session facade.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use tokio::runtime::Runtime;

mod exec;
mod session_events;
#[cfg(feature = "testing")]
mod testing;
use session_events::*;

use rithmic_gateway::client::{GatewayConfig, RemoteSession};
use rithmic_host::dto::{
    AccountPnlDto, AccountRmsInfoDto, BboDto, BracketUpdateDto, FillHistoryDto, FrontMonthDto,
    HistoryBarDto, HistoryTickDto, InstrumentPnlDto, LastTradeDto, OrderBookDto,
    OrderNotificationDto, PlantEvent, ProductRmsInfoDto, ReferenceDataDto, TimeBarProbeRow,
};
use rithmic_host::error::Error;
use rithmic_host::plants::PlantSet;
use rithmic_host::{
    HostFingerprint, HostOptions, HostRegistry, ModifyOrder, PlaceBracketOrder, PlaceOrder,
    Session, SessionEvent,
};
use rithmic_plants::config::{SessionConfig, DEFAULT_LUCID_URL};
use rithmic_plants::RithmicVenue;

pyo3::create_exception!(rithmic_nt_connect, ChannelLaggedError, PyRuntimeError);
pyo3::create_exception!(rithmic_nt_connect, ChannelClosedError, PyRuntimeError);
pyo3::create_exception!(rithmic_nt_connect, NotConnectedError, PyRuntimeError);
// Raised by gateway-mode sessions; carries `.code` / `.message` like the
// pure-Python gateway client's error, so the adapter classifies it the same way.
pyo3::create_exception!(rithmic_nt_connect, GatewayError, PyRuntimeError);
pyo3::create_exception!(rithmic_nt_connect, AlreadyConnectedError, PyRuntimeError);
pyo3::create_exception!(
    rithmic_nt_connect,
    ReconciliationUnavailableError,
    PyRuntimeError
);

// Another live process holds the credential flock (direct-mode singleton).
pyo3::create_exception!(rithmic_nt_connect, SessionLockError, PyRuntimeError);

/// The credential flock shared with the gateway parent (same Rust implementation,
/// so the lock file can never drift from the parent's). Released on `close()` or drop.
#[pyclass(name = "SessionLock")]
struct PySessionLock {
    lock: std::sync::Mutex<Option<rithmic_gateway::singleton::SessionLock>>,
    path: String,
}

#[pymethods]
impl PySessionLock {
    #[staticmethod]
    #[pyo3(signature = (user, system_name, url, env = "Live"))]
    fn try_acquire(user: &str, system_name: &str, url: &str, env: &str) -> PyResult<Self> {
        let lock =
            rithmic_gateway::singleton::SessionLock::try_acquire(user, system_name, url, env)
                .map_err(|e| SessionLockError::new_err(e.to_string()))?;
        let path = lock.path().display().to_string();
        Ok(Self {
            lock: std::sync::Mutex::new(Some(lock)),
            path,
        })
    }

    #[getter]
    fn path(&self) -> &str {
        &self.path
    }

    fn close(&self) {
        self.lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}

fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().expect("failed to create tokio runtime"))
}

/// Wire code the adapter matches on; history refused under live MD is a capability denial.
fn normalize_gateway_code(code: &str) -> &str {
    if code == "history_denied_live_md" {
        "capability_denied"
    } else {
        code
    }
}

fn gateway_error(code: &str, message: &str) -> PyErr {
    let code = normalize_gateway_code(code).to_string();
    let message = message.to_string();
    let err = GatewayError::new_err(format!("{code}: {message}"));
    Python::attach(|py| {
        let value = err.value(py);
        let _ = value.setattr("code", &code);
        let _ = value.setattr("message", &message);
    });
    err
}

fn to_py_err(err: Error) -> PyErr {
    match err {
        Error::Gateway { code, message } => gateway_error(&code, &message),
        // Written but unanswered: the order may or may not be live. The adapter
        // latches the order plant on any failure after send.
        Error::OutcomeUnknown(msg) => gateway_error("outcome_unknown", &msg),
        // The venue refused it: definitively not live.
        Error::Rejected(msg) => gateway_error("venue_rejected", &msg),
        // Nothing was written: never at risk, not a venue reject.
        Error::NotSent(msg) => gateway_error("not_sent", &msg),
        // A transport failure is a reconnectable gateway code.
        Error::Transport(msg) => gateway_error("transport_reset", &msg),
        Error::Config(msg) => PyValueError::new_err(msg),
        Error::AlreadyConnected => AlreadyConnectedError::new_err("already connected"),
        Error::ChannelLagged { plant, skipped } => {
            ChannelLaggedError::new_err(format!("{plant}:{skipped}"))
        }
        Error::ChannelClosed { plant } => ChannelClosedError::new_err(plant),
        Error::NotConnected { plant } => NotConnectedError::new_err(plant),
        Error::ReconciliationUnavailable(msg) => ReconciliationUnavailableError::new_err(msg),
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

/// Set `ts_event_ns` to the event's effective time: the plant's own when present,
/// else derived from `ssboe` + `usecs` (the `rithmic-exec` rule). Computed here,
/// once, so Python reads one field instead of crossing back into Rust per tick.
fn set_event_ts(
    dict: &Bound<'_, PyDict>,
    ts_event_ns: Option<u64>,
    ssboe: Option<i32>,
    usecs: Option<i32>,
) -> PyResult<()> {
    let ts = rithmic_exec::event_ts_ns(
        ts_event_ns.map(i128::from),
        ssboe.map(i128::from),
        usecs.map(i128::from),
    );
    if let Some(ts) = ts {
        dict.set_item("ts_event_ns", ts)?;
    }
    Ok(())
}

fn set_opt_u32(dict: &Bound<'_, PyDict>, key: &str, value: Option<u32>) -> PyResult<()> {
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
    set_event_ts(&d, t.ts_event_ns, t.ssboe, t.usecs)?;
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
    set_event_ts(&d, b.ts_event_ns, b.ssboe, b.usecs)?;
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
    set_event_ts(&d, o.ts_event_ns, o.ssboe, o.usecs)?;
    Ok(d.unbind())
}

fn order_notification_dict(py: Python<'_>, n: OrderNotificationDto) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("type", "order_notification")?;
    d.set_item("source", n.source)?;
    set_opt_str(&d, "kind", n.kind)?;
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
    set_opt_i32(&d, "duration", n.duration)?;
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

fn product_rms_info_dict(py: Python<'_>, r: ProductRmsInfoDto) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("type", "product_rms_info")?;
    set_opt_str(&d, "product_code", r.product_code)?;
    set_opt_f64(&d, "commission_fill_rate", r.commission_fill_rate)?;
    set_opt_u32(&d, "presence_bits", r.presence_bits)?;
    Ok(d.unbind())
}

fn account_rms_info_dict(py: Python<'_>, r: AccountRmsInfoDto) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("type", "account_rms_info")?;
    set_opt_str(&d, "account_id", r.account_id)?;
    set_opt_f64(&d, "default_commission", r.default_commission)?;
    set_opt_u32(&d, "presence_bits", r.presence_bits)?;
    Ok(d.unbind())
}

fn fill_history_dict(py: Python<'_>, r: FillHistoryDto) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("type", "fill_history")?;
    set_opt_str(&d, "account_id", r.account_id)?;
    set_opt_str(&d, "basket_id", r.basket_id)?;
    set_opt_str(&d, "symbol", r.symbol)?;
    set_opt_str(&d, "exchange", r.exchange)?;
    set_opt_str(&d, "transaction_type", r.transaction_type)?;
    set_opt_str(&d, "price_type", r.price_type)?;
    set_opt_str(&d, "duration", r.duration)?;
    set_opt_str(&d, "manual_or_auto", r.manual_or_auto)?;
    set_opt_str(&d, "fill_id", r.fill_id)?;
    set_opt_str(&d, "fill_date", r.fill_date)?;
    set_opt_str(&d, "fill_time", r.fill_time)?;
    set_opt_f64(&d, "fill_price", r.fill_price)?;
    set_opt_u64(&d, "fill_size", r.fill_size)?;
    set_opt_f64(&d, "price", r.price)?;
    set_opt_f64(&d, "trigger_price", r.trigger_price)?;
    set_opt_i32(&d, "ssboe", r.ssboe)?;
    set_opt_i32(&d, "usecs", r.usecs)?;
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

fn bracket_update_dict(py: Python<'_>, b: BracketUpdateDto) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("type", "bracket_update")?;
    set_opt_str(&d, "account_id", b.account_id)?;
    set_opt_str(&d, "fcm_id", b.fcm_id)?;
    set_opt_str(&d, "ib_id", b.ib_id)?;
    set_opt_str(&d, "basket_id", b.basket_id)?;
    set_opt_i32(&d, "stop_ticks", b.stop_ticks)?;
    set_opt_i32(&d, "stop_quantity", b.stop_quantity)?;
    set_opt_i32(&d, "stop_quantity_released", b.stop_quantity_released)?;
    set_opt_i32(&d, "target_ticks", b.target_ticks)?;
    set_opt_i32(&d, "target_quantity", b.target_quantity)?;
    set_opt_i32(&d, "target_quantity_released", b.target_quantity_released)?;
    Ok(d.unbind())
}

fn event_to_dict(py: Python<'_>, event: PlantEvent) -> PyResult<Py<PyDict>> {
    match event {
        PlantEvent::LastTrade(t) => last_trade_dict(py, t),
        PlantEvent::Bbo(b) => bbo_dict(py, b),
        PlantEvent::OrderBook(o) => order_book_dict(py, o),
        PlantEvent::AccountPnl(a) => account_pnl_dict(py, a),
        PlantEvent::InstrumentPnl(i) => instrument_pnl_dict(py, i),
        PlantEvent::OrderNotification(n) => order_notification_dict(py, n),
        PlantEvent::BracketUpdate(b) => bracket_update_dict(py, b),
        PlantEvent::TimeBar(b) => time_bar_event_dict(py, b),
        PlantEvent::Gap(g) => {
            // Polls report a gap as `ChannelLaggedError`; this form is for
            // batch consumers that receive it in order with the events.
            let d = PyDict::new(py);
            d.set_item("type", "gap")?;
            d.set_item("stream", g.stream.plant())?;
            d.set_item("skipped", g.skipped)?;
            Ok(d.unbind())
        }
        PlantEvent::Other { type_name, source } => {
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

fn time_bar_probe_row_dict(py: Python<'_>, r: TimeBarProbeRow) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("variant", r.variant)?;
    d.set_item("source", r.source)?;
    d.set_item("parsed", r.parsed)?;
    d.set_item("rp_code", r.rp_code)?;
    set_opt_str(&d, "error", r.error)?;
    set_opt_str(&d, "skip_reason", r.skip_reason)?;
    set_opt_str(&d, "symbol", r.symbol)?;
    set_opt_str(&d, "exchange", r.exchange)?;
    set_opt_i32(&d, "bar_type", r.bar_type)?;
    set_opt_str(&d, "period", r.period)?;
    set_opt_i32(&d, "marker", r.marker)?;
    set_opt_f64(&d, "open_price", r.open_price)?;
    set_opt_f64(&d, "high_price", r.high_price)?;
    set_opt_f64(&d, "low_price", r.low_price)?;
    set_opt_f64(&d, "close_price", r.close_price)?;
    set_opt_f64(&d, "settlement_price", r.settlement_price)?;
    set_opt_bool(&d, "has_settlement_price", r.has_settlement_price)?;
    set_opt_u64(&d, "volume", r.volume)?;
    set_opt_u64(&d, "num_trades", r.num_trades)?;
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

fn time_bar_event_dict(py: Python<'_>, b: HistoryBarDto) -> PyResult<Py<PyDict>> {
    let d = history_bar_dict(py, b)?;
    d.bind(py).set_item("type", "time_bar")?;
    Ok(d)
}

/// Python-facing Rithmic multi-plant session.
///
/// A thin shell over the [`Session`] trait: every blocking call runs on the
/// extension runtime with the GIL released, and records are converted to plain
/// dicts in this module only. Order-op safety (disconnect gate, detached order
/// handle) lives in `RithmicVenue`, not here.
#[pyclass(name = "Session")]
pub struct PySession {
    session: Arc<dyn Session>,
    /// The gateway client, when `connect_mode` is `gateway` (transport hooks).
    gateway: Option<Arc<RemoteSession>>,
    #[cfg(feature = "testing")]
    script: Option<Arc<rithmic_host::fake::FakeVenue>>,
    #[cfg(feature = "testing")]
    fault_venue: Option<Arc<RithmicVenue>>,
}

impl PySession {
    /// Run `fut` to completion on the extension runtime with the GIL released.
    fn run<T, F>(&self, py: Python<'_>, fut: F) -> PyResult<T>
    where
        T: Send,
        F: std::future::Future<Output = Result<T, Error>> + Send,
    {
        py.detach(|| runtime().block_on(fut)).map_err(to_py_err)
    }
}

fn event_or_none(py: Python<'_>, event: Option<PlantEvent>) -> PyResult<Option<Py<PyDict>>> {
    event.map(|e| event_to_dict(py, e)).transpose()
}

#[pymethods]
impl PySession {
    #[new]
    #[pyo3(signature = (
        user,
        password=None,
        system_name="LucidTrading",
        url="wss://rprotocol.rithmic.com:443",
        app_name="rithmic-nt-connect",
        app_version="0.1.0",
        env="Live",
        account_id=None,
        fcm_id=None,
        ib_id=None,
        beta_url=None,
        plants="market_data",
        connect_mode="direct",
        gateway_listen=None,
        gateway_auth_token=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        user: String,
        password: Option<String>,
        system_name: &str,
        url: &str,
        app_name: &str,
        app_version: &str,
        env: &str,
        account_id: Option<String>,
        fcm_id: Option<String>,
        ib_id: Option<String>,
        beta_url: Option<String>,
        plants: &str,
        connect_mode: &str,
        gateway_listen: Option<String>,
        gateway_auth_token: Option<String>,
    ) -> PyResult<Self> {
        let (renv, env_name) = match env {
            "Live" | "live" => (rithmic_host::Env::Live, "Live"),
            "Demo" | "demo" => (rithmic_host::Env::Demo, "Demo"),
            "Test" | "test" => (rithmic_host::Env::Test, "Test"),
            other => {
                return Err(PyValueError::new_err(format!(
                    "invalid env {other:?}; expected Live, Demo, or Test"
                )));
            }
        };
        match connect_mode {
            "gateway" => {
                // The gateway parent owns the Rithmic login: a gateway client has
                // no password and never reads or sends one.
                let mut cfg = GatewayConfig::new(&user, system_name, url, env_name);
                cfg.account_id = account_id.unwrap_or_default();
                cfg.fcm_id = fcm_id.unwrap_or_default();
                cfg.ib_id = ib_id.unwrap_or_default();
                if let Some(listen) = gateway_listen {
                    cfg = cfg.with_listen(&listen).map_err(to_py_err)?;
                }
                if let Some(token) = gateway_auth_token {
                    cfg = cfg.with_auth_token(&token);
                }
                let gateway = Arc::new(RemoteSession::new(cfg));
                Ok(Self {
                    session: gateway.clone(),
                    gateway: Some(gateway),
                    #[cfg(feature = "testing")]
                    script: None,
                    #[cfg(feature = "testing")]
                    fault_venue: None,
                })
            }
            "direct" => {
                let password = password
                    .ok_or_else(|| PyValueError::new_err("direct mode requires a password"))?;
                let mut key = HostFingerprint::new(&user, system_name, url, env_name);
                key.account_id = account_id.clone();
                key.fcm_id = fcm_id.clone();
                key.ib_id = ib_id.clone();
                key.gateway_listen = gateway_listen;
                key.beta_url = beta_url.clone();
                key.app_name = app_name.into();
                key.app_version = app_version.into();
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
                // Construction creates local paths only. Extra plants are requested
                // separately, before or after the first idempotent connect.
                PlantSet::parse(plants).map_err(to_py_err)?;
                static REGISTRY: OnceLock<HostRegistry<RithmicVenue>> = OnceLock::new();
                let lock_path = rithmic_gateway::singleton::SessionLock::lock_path(
                    &key.user,
                    &key.system_name,
                    &key.url,
                    &key.env,
                );
                let lock_dir = lock_path.parent().expect("lock directory").to_path_buf();
                let options = HostOptions {
                    lock_filename: lock_path.file_name().map(ToOwned::to_owned),
                    ..HostOptions::default()
                };
                py.detach(|| {
                    runtime().block_on(async move {
                        let registry = REGISTRY.get_or_init(HostRegistry::default);
                        loop {
                            let host = registry
                                .get_or_start(
                                    key.clone(),
                                    || {
                                        Arc::new(
                                            RithmicVenue::new(
                                                config.clone(),
                                                PlantSet::MARKET_DATA,
                                            )
                                            .with_gates(true, true),
                                        )
                                    },
                                    &lock_dir,
                                    options.clone(),
                                )
                                .await
                                .map_err(|e| SessionLockError::new_err(e.to_string()))?;
                            if let Ok(session) = host.attach_owned() {
                                break Ok(Self {
                                    session: Arc::new(session),
                                    gateway: None,
                                    #[cfg(feature = "testing")]
                                    script: None,
                                    #[cfg(feature = "testing")]
                                    fault_venue: Some(
                                        host.state
                                            .session
                                            .as_ref()
                                            .expect("direct venue")
                                            .checkout()
                                            .await,
                                    ),
                                });
                            }
                        }
                    })
                })
            }
            other => Err(PyValueError::new_err(format!(
                "invalid connect_mode {other:?}; expected 'direct' or 'gateway'"
            ))),
        }
    }

    /// Credential-free attachment to the real host lifecycle, for dev/CI only.
    #[cfg(feature = "testing")]
    #[staticmethod]
    #[pyo3(signature = (user="fake", *, lock_dir=None, system_name="Fake", url="wss://fake", env="Test", account_id=None, fcm_id=None, ib_id=None, beta_url=None, app_name="rithmic-nt-connect", app_version="0.1.0"))]
    #[allow(clippy::too_many_arguments)]
    fn fake(
        py: Python<'_>,
        user: &str,
        lock_dir: Option<String>,
        system_name: &str,
        url: &str,
        env: &str,
        account_id: Option<String>,
        fcm_id: Option<String>,
        ib_id: Option<String>,
        beta_url: Option<String>,
        app_name: &str,
        app_version: &str,
    ) -> PyResult<Self> {
        let mut key = HostFingerprint::new(user, system_name, url, env);
        key.account_id = account_id;
        key.fcm_id = fcm_id;
        key.ib_id = ib_id;
        key.beta_url = beta_url;
        key.app_name = app_name.into();
        key.app_version = app_version.into();
        testing::new(py, key, lock_dir)
    }
    #[cfg(feature = "testing")]
    #[getter]
    fn script(&self) -> Option<testing::PyFakeScript> {
        self.script.as_ref().map(|venue| testing::PyFakeScript {
            venue: venue.clone(),
        })
    }

    /// Dev-only transport fault; shared direct holders observe the same outage.
    #[cfg(feature = "testing")]
    fn _fault_drop_plant(&self, py: Python<'_>, plant: &str) -> PyResult<()> {
        let venue = self.fault_venue.as_ref().ok_or_else(|| {
            PyValueError::new_err("fault injection requires a real direct session; gateway and fake sessions are unsupported")
        })?;
        if plant != "ticker" {
            return Err(PyValueError::new_err(
                "fault injection supports only ticker",
            ));
        }
        self.run(
            py,
            venue.fault_inject_drop_plant(rithmic_host::Stream::Ticker),
        )
    }

    #[getter]
    fn trading_enabled(&self, py: Python<'_>) -> bool {
        self.open_gates(py, false, false).trading
    }
    #[getter]
    fn cancel_all_enabled(&self, py: Python<'_>) -> bool {
        self.open_gates(py, false, false).cancel_all
    }
    fn open_gates(&self, py: Python<'_>, trading: bool, cancel_all: bool) -> PyGates {
        let gates = py.detach(|| runtime().block_on(self.session.open_gates(trading, cancel_all)));
        PyGates {
            trading: gates.trading,
            cancel_all: gates.cancel_all,
        }
    }

    /// Incarnation of the gateway transport (bumps on every re-dial); 0 in direct mode.
    fn transport_generation(&self) -> u64 {
        self.gateway
            .as_ref()
            .map_or(0, |g| g.transport_generation())
    }

    /// Whether the gateway socket is up; always true in direct mode.
    fn transport_connected(&self) -> bool {
        self.gateway
            .as_ref()
            .is_none_or(|g| g.transport_connected())
    }

    /// Recover the gateway transport if it is down (single-flight) and return
    /// its generation. A no-op returning 0 in direct mode.
    fn ensure_transport_live(&self, py: Python<'_>) -> PyResult<u64> {
        match &self.gateway {
            Some(gateway) => py
                .detach(|| runtime().block_on(gateway.ensure_transport_live()))
                .map_err(to_py_err),
            None => Ok(0),
        }
    }

    /// Deliver host and socket recovery events on a dedicated Python delivery
    /// thread. Each registration has an independent receiver and cancellation.
    fn subscribe_session_events(
        &self,
        listener: Py<PyAny>,
    ) -> PyResult<PySessionEventSubscription> {
        session_events::subscribe(self.session.session_events(), listener, runtime())
    }

    fn request_plants(&self, py: Python<'_>, plants: &str) -> PyResult<()> {
        let extra = PlantSet::parse(plants).map_err(to_py_err)?;
        self.run(py, self.session.request_plants(extra))
    }

    fn connect(&self, py: Python<'_>) -> PyResult<()> {
        self.run(py, self.session.connect())
    }

    fn disconnect(&self, py: Python<'_>) -> PyResult<()> {
        self.run(py, self.session.disconnect())
    }

    fn disconnect_order_plant(&self, py: Python<'_>) -> PyResult<()> {
        self.run(py, self.session.disconnect_order_plant())
    }

    fn disconnect_pnl_plant(&self, py: Python<'_>) -> PyResult<()> {
        self.run(py, self.session.disconnect_pnl_plant())
    }

    fn ensure_pnl_plant(&self, py: Python<'_>) -> PyResult<()> {
        self.run(py, self.session.ensure_pnl_plant())
    }

    fn ensure_order_plant(&self, py: Python<'_>) -> PyResult<()> {
        self.run(py, self.session.ensure_order_plant())
    }

    /// Resolved account triple after override or discovery (`None` until ensure).
    fn resolved_account(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let account = py.detach(|| runtime().block_on(self.session.resolved_account()));
        let Some(acct) = account else {
            return Ok(None);
        };
        let d = PyDict::new(py);
        d.set_item("account_id", &acct.account_id)?;
        d.set_item("fcm_id", &acct.fcm_id)?;
        d.set_item("ib_id", &acct.ib_id)?;
        Ok(Some(d.into()))
    }

    fn subscribe(&self, py: Python<'_>, symbol: &str, exchange: &str) -> PyResult<()> {
        self.run(py, self.session.subscribe(symbol, exchange))
    }

    fn unsubscribe(&self, py: Python<'_>, symbol: &str, exchange: &str) -> PyResult<()> {
        self.run(py, self.session.unsubscribe(symbol, exchange))
    }

    fn get_front_month(
        &self,
        py: Python<'_>,
        symbol: &str,
        exchange: &str,
    ) -> PyResult<Py<PyDict>> {
        let dto = self.run(py, self.session.get_front_month(symbol, exchange))?;
        front_month_dict(py, dto)
    }

    fn get_reference_data(
        &self,
        py: Python<'_>,
        symbol: &str,
        exchange: &str,
    ) -> PyResult<Py<PyDict>> {
        let dto = self.run(py, self.session.get_reference_data(symbol, exchange))?;
        reference_data_dict(py, dto)
    }

    /// Next ticker-plant event as a dict, or `None` after `timeout_ms` (default: non-blocking).
    #[pyo3(signature = (timeout_ms=0))]
    fn poll_event(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<Option<Py<PyDict>>> {
        let event = self.run(
            py,
            self.session.poll_event(Duration::from_millis(timeout_ms)),
        )?;
        event_or_none(py, event)
    }

    /// Event-driven batch: wait up to `timeout_ms` for the stream's first event
    /// (GIL released, no session lock held), then return up to `max_events`
    /// already-queued events at once. `stream` is `ticker`, `history`, `pnl` or
    /// `order`. A lag is `{"type": "gap", "stream": ..., "skipped": N}` in order
    /// with the events. Empty on timeout and while the plant is not attached.
    #[pyo3(signature = (stream, max_events=256, timeout_ms=100))]
    fn next_events(
        &self,
        py: Python<'_>,
        stream: &str,
        max_events: usize,
        timeout_ms: u64,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let parsed = rithmic_host::Stream::from_plant(stream).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "unknown stream {stream:?}; expected ticker, history, pnl or order"
            ))
        })?;
        let events = self.run(
            py,
            self.session
                .next_events(parsed, max_events.max(1), Duration::from_millis(timeout_ms)),
        )?;
        // The batch is already dequeued, so one event that fails to convert must not
        // discard its neighbours. It becomes a gap of one, which the consumer
        // resynchronizes on exactly like a lag: nothing is dropped silently.
        events
            .into_iter()
            .map(|e| match event_to_dict(py, e) {
                Ok(dict) => Ok(dict),
                Err(_) => {
                    let gap = PyDict::new(py);
                    gap.set_item("type", "gap")?;
                    gap.set_item("stream", stream)?;
                    gap.set_item("skipped", 1u64)?;
                    Ok(gap.unbind())
                }
            })
            .collect()
    }

    /// Next PnL-plant event as a dict, or `None` after `timeout_ms`.
    #[pyo3(signature = (timeout_ms=0))]
    fn poll_pnl_event(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<Option<Py<PyDict>>> {
        let event = self.run(
            py,
            self.session
                .poll_pnl_event(Duration::from_millis(timeout_ms)),
        )?;
        event_or_none(py, event)
    }

    /// Next history-plant (time bar) event as a dict, or `None` after `timeout_ms`.
    #[pyo3(signature = (timeout_ms=0))]
    fn poll_history_event(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<Option<Py<PyDict>>> {
        let event = self.run(
            py,
            self.session
                .poll_history_event(Duration::from_millis(timeout_ms)),
        )?;
        event_or_none(py, event)
    }

    /// Next order-plant event as a dict, or `None` after `timeout_ms`.
    #[pyo3(signature = (timeout_ms=0))]
    fn poll_order_event(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<Option<Py<PyDict>>> {
        let event = self.run(
            py,
            self.session
                .poll_order_event(Duration::from_millis(timeout_ms)),
        )?;
        event_or_none(py, event)
    }

    /// Load historical ticks for a window; returns a list of dicts.
    fn load_ticks(
        &self,
        py: Python<'_>,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> PyResult<Py<PyList>> {
        let ticks = self.run(
            py,
            self.session
                .load_ticks(symbol, exchange, start_time_sec, end_time_sec),
        )?;
        let list = PyList::empty(py);
        for tick in ticks {
            list.append(history_tick_dict(py, tick)?)?;
        }
        Ok(list.unbind())
    }

    fn subscribe_time_bars(
        &self,
        py: Python<'_>,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> PyResult<()> {
        self.run(
            py,
            self.session
                .subscribe_time_bars(symbol, exchange, bar_type, period),
        )
    }

    fn unsubscribe_time_bars(
        &self,
        py: Python<'_>,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> PyResult<()> {
        self.run(
            py,
            self.session
                .unsubscribe_time_bars(symbol, exchange, bar_type, period),
        )
    }

    fn subscribe_order_book_summary(
        &self,
        py: Python<'_>,
        symbol: &str,
        exchange: &str,
    ) -> PyResult<()> {
        self.run(
            py,
            self.session.subscribe_order_book_summary(symbol, exchange),
        )
    }

    fn unsubscribe_order_book_summary(
        &self,
        py: Python<'_>,
        symbol: &str,
        exchange: &str,
    ) -> PyResult<()> {
        self.run(
            py,
            self.session
                .unsubscribe_order_book_summary(symbol, exchange),
        )
    }

    /// Load minute time bars for a window; returns structured OHLCV dicts.
    #[pyo3(signature = (symbol, exchange, start_time_sec, end_time_sec, bar_type=2, period=1))]
    #[allow(clippy::too_many_arguments)]
    fn load_time_bars(
        &self,
        py: Python<'_>,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
        bar_type: i32,
        period: i32,
    ) -> PyResult<Py<PyList>> {
        let bars = self.run(
            py,
            self.session.load_time_bars(
                symbol,
                exchange,
                start_time_sec,
                end_time_sec,
                bar_type,
                period,
            ),
        )?;
        let list = PyList::empty(py);
        for bar in bars {
            list.append(history_bar_dict(py, bar)?)?;
        }
        Ok(list.unbind())
    }

    /// Raw history-plant time-bar replay (including dropped rows) for diagnostics.
    #[pyo3(signature = (symbol, exchange, start_time_sec, end_time_sec, bar_type=2, period=1))]
    #[allow(clippy::too_many_arguments)]
    fn probe_time_bars(
        &self,
        py: Python<'_>,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
        bar_type: i32,
        period: i32,
    ) -> PyResult<Py<PyList>> {
        let rows = self.run(
            py,
            self.session.probe_time_bars(
                symbol,
                exchange,
                start_time_sec,
                end_time_sec,
                bar_type,
                period,
            ),
        )?;
        let list = PyList::empty(py);
        for row in rows {
            list.append(time_bar_probe_row_dict(py, row)?)?;
        }
        Ok(list.unbind())
    }

    fn subscribe_pnl(&self, py: Python<'_>) -> PyResult<()> {
        self.run(py, self.session.subscribe_pnl())
    }

    fn subscribe_order_updates(&self, py: Python<'_>) -> PyResult<()> {
        self.run(py, self.session.subscribe_order_updates())
    }

    fn subscribe_bracket_updates(&self, py: Python<'_>) -> PyResult<()> {
        self.run(py, self.session.subscribe_bracket_updates())
    }

    /// Load order events (fills + cancels + rejects + working) over a window.
    fn load_orders(
        &self,
        py: Python<'_>,
        start_sec: i32,
        end_sec: i32,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let events = self.run(py, self.session.load_orders(start_sec, end_sec))?;
        events
            .into_iter()
            .map(|n| order_notification_dict(py, n))
            .collect()
    }

    /// Fetch product-level RMS info (per-product commission fill rates).
    fn load_product_rms_info(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        let rows = self.run(py, self.session.load_product_rms_info())?;
        rows.into_iter()
            .map(|r| product_rms_info_dict(py, r))
            .collect()
    }

    /// Fetch account-level RMS info (default commission rate).
    fn load_account_rms_info(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        let rows = self.run(py, self.session.load_account_rms_info())?;
        rows.into_iter()
            .map(|r| account_rms_info_dict(py, r))
            .collect()
    }

    /// Replay one trade date's order history (CCYYMMDD) as order notifications. Read-only.
    fn load_order_history(&self, py: Python<'_>, date: &str) -> PyResult<Vec<Py<PyDict>>> {
        let events = self.run(py, self.session.load_order_history(date))?;
        events
            .into_iter()
            .map(|n| order_notification_dict(py, n))
            .collect()
    }

    /// Fetch the account's fills for trade dates `start..=finish` (CCYYMMDD). Read-only.
    fn load_fill_history(
        &self,
        py: Python<'_>,
        start_date: i32,
        finish_date: i32,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let rows = self.run(py, self.session.load_fill_history(start_date, finish_date))?;
        rows.into_iter().map(|r| fill_history_dict(py, r)).collect()
    }

    /// Trade dates (CCYYMMDD) for which the venue keeps order history. Read-only.
    fn load_order_history_dates(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        self.run(py, self.session.load_order_history_dates())
    }

    #[pyo3(signature = (
        symbol,
        exchange,
        side,
        price_type,
        quantity,
        localid,
        price=None,
        trigger_price=None,
        duration="DAY",
        stop_ticks=None,
        target_ticks=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn place_bracket_order(
        &self,
        py: Python<'_>,
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
    ) -> PyResult<()> {
        let order = PlaceBracketOrder {
            symbol: symbol.into(),
            exchange: exchange.into(),
            side: side.into(),
            price_type: price_type.into(),
            quantity,
            localid: localid.into(),
            price,
            trigger_price,
            duration: duration.into(),
            stop_ticks,
            target_ticks,
        };
        self.run(py, self.session.place_bracket_order(&order))
    }

    #[pyo3(signature = (basket_id, ticks, level=None))]
    fn adjust_bracket_stop(
        &self,
        py: Python<'_>,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> PyResult<()> {
        self.run(
            py,
            self.session.adjust_bracket_stop(basket_id, ticks, level),
        )
    }

    #[pyo3(signature = (basket_id, ticks, level=None))]
    fn adjust_bracket_target(
        &self,
        py: Python<'_>,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> PyResult<()> {
        self.run(
            py,
            self.session.adjust_bracket_target(basket_id, ticks, level),
        )
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
        trail_by_ticks=None,
        trail_by_price_id=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn place_order(
        &self,
        py: Python<'_>,
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
    ) -> PyResult<()> {
        let order = PlaceOrder {
            symbol: symbol.into(),
            exchange: exchange.into(),
            side: side.into(),
            price_type: price_type.into(),
            quantity,
            user_tag: user_tag.into(),
            price,
            trigger_price,
            duration: duration.into(),
            trail_by_ticks,
            trail_by_price_id,
        };
        self.run(py, self.session.place_order(&order))
    }

    fn cancel_order(&self, py: Python<'_>, basket_id: &str) -> PyResult<()> {
        self.run(py, self.session.cancel_order(basket_id))
    }

    #[pyo3(signature = (
        basket_id,
        symbol,
        exchange,
        quantity,
        price_type,
        price=None,
        trigger_price=None,
        trail_by_ticks=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn modify_order(
        &self,
        py: Python<'_>,
        basket_id: &str,
        symbol: &str,
        exchange: &str,
        quantity: i32,
        price_type: &str,
        price: Option<f64>,
        trigger_price: Option<f64>,
        trail_by_ticks: Option<i32>,
    ) -> PyResult<()> {
        let order = ModifyOrder {
            basket_id: basket_id.into(),
            symbol: symbol.into(),
            exchange: exchange.into(),
            quantity,
            price_type: price_type.into(),
            price,
            trigger_price,
            trail_by_ticks,
        };
        self.run(py, self.session.modify_order(&order))
    }

    fn cancel_all_orders(&self, py: Python<'_>) -> PyResult<()> {
        self.run(py, self.session.cancel_all_orders())
    }
}

/// List system names advertised by a gateway (no login).
#[pyfunction]
#[pyo3(signature = (url=None))]
fn list_systems(py: Python<'_>, url: Option<&str>) -> PyResult<Vec<String>> {
    let url = url.unwrap_or(DEFAULT_LUCID_URL).to_string();
    // The probe can wait on the network for up to its deadline; release the GIL
    // so the rest of the Python process keeps running meanwhile.
    py.detach(|| runtime().block_on(rithmic_plants::systems::list_systems(&url)))
        .map_err(to_py_err)
}

/// Register the PyO3 module `rithmic_nt_connect._lib`.
#[pymodule]
fn _lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySession>()?;
    m.add_class::<PySessionLock>()?;
    session_events::register(m)?;
    #[cfg(feature = "testing")]
    m.add_class::<testing::PyFakeScript>()?;
    exec::register(m)?;
    m.add("SessionLockError", m.py().get_type::<SessionLockError>())?;
    m.add_function(wrap_pyfunction!(list_systems, m)?)?;
    m.add(
        "ChannelLaggedError",
        m.py().get_type::<ChannelLaggedError>(),
    )?;
    m.add(
        "ChannelClosedError",
        m.py().get_type::<ChannelClosedError>(),
    )?;
    m.add("NotConnectedError", m.py().get_type::<NotConnectedError>())?;
    m.add("GatewayError", m.py().get_type::<GatewayError>())?;
    m.add(
        "AlreadyConnectedError",
        m.py().get_type::<AlreadyConnectedError>(),
    )?;
    m.add(
        "ReconciliationUnavailableError",
        m.py().get_type::<ReconciliationUnavailableError>(),
    )?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
