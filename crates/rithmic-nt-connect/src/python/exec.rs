//! Bindings for the pure execution rules in `rithmic-exec`.
//!
//! Thin by design: every rule lives in the Rust crate; this file only converts
//! Python values to and from it.

use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;
use std::collections::BTreeMap;

use rithmic_exec::{CommissionTable, FillParts, Lru, PlantPolicy, PlantState, PositionRow};

fn parse_state(name: &str) -> PyResult<PlantState> {
    PlantState::parse(name)
        .ok_or_else(|| PyValueError::new_err(format!("unknown order plant state {name:?}")))
}

/// Order-plant lifecycle policy. States cross the boundary as their wire names.
#[pyclass(name = "OrderPlantPolicy")]
pub(super) struct PyOrderPlantPolicy {
    inner: PlantPolicy,
}

#[pymethods]
impl PyOrderPlantPolicy {
    #[new]
    #[pyo3(signature = (state = "disconnected"))]
    fn new(state: &str) -> PyResult<Self> {
        Ok(Self {
            inner: PlantPolicy::new(parse_state(state)?),
        })
    }

    #[getter]
    fn state(&self) -> &'static str {
        self.inner.state().as_str()
    }

    #[getter]
    fn latched(&self) -> bool {
        self.inner.latched()
    }

    fn begin_connect(&mut self) {
        self.inner.begin_connect();
    }

    fn disconnect(&mut self) {
        self.inner.disconnect();
    }

    fn latch(&mut self) {
        self.inner.latch();
    }

    fn resync_start(&mut self) {
        self.inner.resync_start();
    }

    fn resync_complete(&mut self) {
        self.inner.resync_complete();
    }

    fn resync_failed(&mut self) {
        self.inner.resync_failed();
    }

    fn rearm(&mut self, poll_alive: bool) -> bool {
        self.inner.rearm(poll_alive)
    }

    fn allow_submit(&self) -> bool {
        self.inner.allow_submit()
    }

    fn allow_modify(&self) -> bool {
        self.inner.allow_modify()
    }

    fn allow_cancel(&self) -> bool {
        self.inner.allow_cancel()
    }

    fn reject_reason(&self, action: &str) -> String {
        self.inner.reject_reason(action)
    }
}

#[pyfunction]
fn product_root(symbol: &str) -> String {
    rithmic_exec::product_root(symbol)
}

#[pyfunction]
fn matches_root(symbol: &str, root: &str) -> bool {
    rithmic_exec::matches_root(symbol, root)
}

/// `rows` are `(symbol, quantity, position_side)` triples.
#[pyfunction]
fn signed_net(rows: Vec<(String, i64, String)>, root: &str) -> i64 {
    let rows: Vec<PositionRow<'_>> = rows
        .iter()
        .map(|(symbol, quantity, side)| PositionRow {
            symbol,
            quantity: *quantity,
            side,
        })
        .collect();
    rithmic_exec::signed_net(&rows, root)
}

#[pyfunction]
fn exposure_complete(
    net: Option<i64>,
    open_orders: Option<i64>,
    pnl_ts_ns: Option<i64>,
    drain_ts_ns: Option<i64>,
) -> bool {
    rithmic_exec::exposure_complete(net, open_orders, pnl_ts_ns, drain_ts_ns)
}

/// Commission rates as decimal text; the caller owns the decimal arithmetic.
#[pyclass(name = "CommissionTable")]
pub(super) struct PyCommissionTable {
    inner: CommissionTable,
}

#[pymethods]
impl PyCommissionTable {
    #[new]
    fn new() -> Self {
        Self {
            inner: CommissionTable::new(),
        }
    }

    /// `rows` are `(product_code, commission_fill_rate)` as text.
    fn load_products(&mut self, rows: Vec<(Option<String>, Option<String>)>) {
        self.inner.load_products(rows);
    }

    /// `rows` are `(account_id, default_commission)` as text.
    #[pyo3(signature = (rows, active_account = None))]
    fn load_account_default(
        &mut self,
        rows: Vec<(String, Option<String>)>,
        active_account: Option<&str>,
    ) {
        self.inner.load_account_default(rows, active_account);
    }

    #[getter]
    fn rates(&self) -> BTreeMap<String, String> {
        self.inner.rates().clone()
    }

    #[setter]
    fn set_rates(&mut self, rates: BTreeMap<String, String>) {
        self.inner.set_rates(rates);
    }

    #[getter]
    fn default_rate(&self) -> Option<String> {
        self.inner.default_rate().map(str::to_string)
    }

    #[setter]
    fn set_default_rate(&mut self, default: Option<String>) {
        self.inner.set_default(default);
    }

    #[pyo3(signature = (product_code = None))]
    fn rate_for(&self, product_code: Option<&str>) -> Option<String> {
        self.inner.rate_for(product_code).map(str::to_string)
    }
}

#[pyfunction]
#[pyo3(signature = (source, notify_type_name, status = None, text = None))]
fn kind_from_notify(
    source: &str,
    notify_type_name: &str,
    status: Option<&str>,
    text: Option<&str>,
) -> Option<&'static str> {
    rithmic_exec::kind_from_notify(source, notify_type_name, status, text)
}

/// Fill identity parts arrive as text: the printed form of a number is part of
/// the identity, so the caller decides it.
#[pyfunction]
#[pyo3(signature = (fill_id, basket_id, exchange_order_id, fill_size, fill_price, ts_event))]
fn fill_trade_id(
    fill_id: Option<&str>,
    basket_id: &str,
    exchange_order_id: &str,
    fill_size: &str,
    fill_price: &str,
    ts_event: i64,
) -> String {
    rithmic_exec::trade_id(
        &FillParts {
            fill_id,
            basket_id,
            exchange_order_id,
            fill_size,
            fill_price,
        },
        ts_event,
    )
}

#[pyfunction]
#[pyo3(signature = (account_id, instrument_id, fill_id, basket_id, exchange_order_id, fill_size, fill_price, ts_event))]
#[allow(clippy::too_many_arguments)]
fn fill_dedup_key(
    account_id: &str,
    instrument_id: &str,
    fill_id: Option<&str>,
    basket_id: &str,
    exchange_order_id: &str,
    fill_size: &str,
    fill_price: &str,
    ts_event: i64,
) -> String {
    rithmic_exec::dedup_key(
        account_id,
        instrument_id,
        &FillParts {
            fill_id,
            basket_id,
            exchange_order_id,
            fill_size,
            fill_price,
        },
        ts_event,
    )
}

/// `ValueError` carrying the adapter's historical message; the Python wrapper
/// re-raises it as `ConvertError`.
#[pyfunction]
#[pyo3(signature = (symbol, exchange, venue))]
fn instrument_id_from_symbol(
    symbol: &str,
    exchange: Option<&str>,
    venue: &str,
) -> PyResult<String> {
    rithmic_exec::instrument_id(symbol, exchange, venue)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
fn symbol_and_exchange_from_instrument_id(
    instrument_id: &str,
    venue: &str,
) -> PyResult<(String, Option<String>)> {
    rithmic_exec::split_instrument_id(instrument_id, venue)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
fn event_ts_ns(
    ts_event_ns: Option<i128>,
    ssboe: Option<i128>,
    usecs: Option<i128>,
) -> Option<i128> {
    rithmic_exec::event_ts_ns(ts_event_ns, ssboe, usecs)
}

/// Bounded recency map; hits refresh recency, the oldest entry is evicted first.
#[pyclass(name = "SeenKeyCache")]
pub(super) struct PySeenKeyCache {
    inner: Lru<Py<PyAny>>,
}

#[pymethods]
impl PySeenKeyCache {
    #[new]
    #[pyo3(signature = (max_size = 10_000))]
    fn new(max_size: usize) -> Self {
        Self {
            inner: Lru::new(max_size),
        }
    }

    fn has_seen(&mut self, key: &str) -> bool {
        self.inner.has(key)
    }

    #[pyo3(signature = (key, value = None))]
    fn mark(&mut self, py: Python<'_>, key: &str, value: Option<Py<PyAny>>) {
        self.inner.insert(key, value.unwrap_or_else(|| py.None()));
    }

    #[pyo3(signature = (key, default = None))]
    fn get(&mut self, py: Python<'_>, key: &str, default: Option<Py<PyAny>>) -> Py<PyAny> {
        match self.inner.get(key) {
            Some(value) => value.clone_ref(py),
            None => default.unwrap_or_else(|| py.None()),
        }
    }

    fn clear(&mut self) {
        self.inner.clear();
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __contains__(&mut self, key: &str) -> bool {
        self.inner.has(key)
    }

    fn __getitem__(&mut self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        match self.inner.get(key) {
            Some(value) => Ok(value.clone_ref(py)),
            None => Err(PyKeyError::new_err(key.to_string())),
        }
    }

    fn __setitem__(&mut self, key: &str, value: Py<PyAny>) {
        self.inner.insert(key, value);
    }
}

pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyOrderPlantPolicy>()?;
    m.add_class::<PyCommissionTable>()?;
    m.add_class::<PySeenKeyCache>()?;
    m.add_function(wrap_pyfunction!(kind_from_notify, m)?)?;
    m.add_function(wrap_pyfunction!(fill_trade_id, m)?)?;
    m.add_function(wrap_pyfunction!(fill_dedup_key, m)?)?;
    m.add_function(wrap_pyfunction!(instrument_id_from_symbol, m)?)?;
    m.add_function(wrap_pyfunction!(symbol_and_exchange_from_instrument_id, m)?)?;
    m.add_function(wrap_pyfunction!(event_ts_ns, m)?)?;
    m.add_function(wrap_pyfunction!(product_root, m)?)?;
    m.add_function(wrap_pyfunction!(matches_root, m)?)?;
    m.add_function(wrap_pyfunction!(signed_net, m)?)?;
    m.add_function(wrap_pyfunction!(exposure_complete, m)?)?;
    m.add("PNL_SETTLE_NS", rithmic_exec::exposure::PNL_SETTLE_NS)?;
    Ok(())
}
