//! Bindings for the pure execution rules in `rithmic-exec`.
//!
//! Thin by design: every rule lives in the Rust crate; this file only converts
//! Python values to and from it.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::BTreeMap;

use rithmic_exec::{CommissionTable, PlantPolicy, PlantState, PositionRow};

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

pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyOrderPlantPolicy>()?;
    m.add_class::<PyCommissionTable>()?;
    m.add_function(wrap_pyfunction!(product_root, m)?)?;
    m.add_function(wrap_pyfunction!(matches_root, m)?)?;
    m.add_function(wrap_pyfunction!(signed_net, m)?)?;
    m.add_function(wrap_pyfunction!(exposure_complete, m)?)?;
    m.add("PNL_SETTLE_NS", rithmic_exec::exposure::PNL_SETTLE_NS)?;
    Ok(())
}
