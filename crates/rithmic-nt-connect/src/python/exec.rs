//! Bindings for the pure execution rules in `rithmic-exec`.
//!
//! Thin by design: every rule lives in the Rust crate; this file only converts
//! Python values to and from it.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use rithmic_exec::{PlantPolicy, PlantState};

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

pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyOrderPlantPolicy>()?;
    Ok(())
}
