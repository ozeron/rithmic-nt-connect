//! Frozen D9 values and off-runtime Python listener delivery.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[pyclass(name = "Gates", frozen, get_all, eq, skip_from_py_object)]
#[derive(Clone, PartialEq)]
pub(super) struct PyGates {
    pub trading: bool,
    pub cancel_all: bool,
}
#[pymethods]
impl PyGates {
    #[new]
    fn new(trading: bool, cancel_all: bool) -> Self {
        Self {
            trading,
            cancel_all,
        }
    }
}
#[pyclass(name = "Interrupted", frozen, get_all, eq, skip_from_py_object)]
#[derive(Clone, PartialEq)]
struct PyInterrupted {
    generation: u64,
    cause: Option<String>,
}
#[pymethods]
impl PyInterrupted {
    #[new]
    #[pyo3(signature = (generation, cause=None))]
    fn new(generation: u64, cause: Option<String>) -> Self {
        Self { generation, cause }
    }
}
#[pyclass(name = "Resumed", frozen, get_all, eq, skip_from_py_object)]
#[derive(Clone, PartialEq)]
struct PyResumed {
    generation: u64,
}
#[pymethods]
impl PyResumed {
    #[new]
    fn new(generation: u64) -> Self {
        Self { generation }
    }
}
#[pyclass(name = "RecoveryFailed", frozen, get_all, eq, skip_from_py_object)]
#[derive(Clone, PartialEq)]
struct PyRecoveryFailed {
    generation: u64,
    attempts: u32,
    cause: Option<String>,
}
#[pymethods]
impl PyRecoveryFailed {
    #[new]
    #[pyo3(signature = (generation, attempts, cause=None))]
    fn new(generation: u64, attempts: u32, cause: Option<String>) -> Self {
        Self {
            generation,
            attempts,
            cause,
        }
    }
}
#[pyclass(name = "SessionEventSubscription")]
pub(super) struct PySessionEventSubscription {
    task: tokio::task::AbortHandle,
    cancelled: Arc<AtomicBool>,
}
#[pymethods]
impl PySessionEventSubscription {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.task.abort();
    }
}
impl Drop for PySessionEventSubscription {
    fn drop(&mut self) {
        self.cancel();
    }
}
pub(super) fn subscribe(
    mut events: tokio::sync::broadcast::Receiver<SessionEvent>,
    listener: Py<PyAny>,
    rt: &Runtime,
) -> PyResult<PySessionEventSubscription> {
    let (tx, rx) = std::sync::mpsc::channel();
    let cancelled = Arc::new(AtomicBool::new(false));
    let stopped = cancelled.clone();
    std::thread::Builder::new()
        .name("rithmic-session-events".into())
        .spawn(move || {
            while let Ok(event) = rx.recv() {
                Python::attach(|py| {
                    if stopped.load(Ordering::SeqCst) {
                        return;
                    }
                    let result = match event {
                        SessionEvent::Interrupted { generation, cause } => {
                            listener.call1(py, (PyInterrupted { generation, cause },))
                        }
                        SessionEvent::Resumed { generation } => {
                            listener.call1(py, (PyResumed { generation },))
                        }
                        SessionEvent::RecoveryFailed {
                            generation,
                            attempts,
                            cause,
                        } => listener.call1(
                            py,
                            (PyRecoveryFailed {
                                generation,
                                attempts,
                                cause,
                            },),
                        ),
                    };
                    if let Err(err) = result {
                        err.print(py);
                    }
                });
            }
        })
        .map_err(|e| PyRuntimeError::new_err(format!("session event thread did not start: {e}")))?;
    let task = rt.spawn(async move {
        loop {
            match events.recv().await {
                Ok(event) => {
                    if tx.send(event).is_err() {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    });
    Ok(PySessionEventSubscription {
        task: task.abort_handle(),
        cancelled,
    })
}
pub(super) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyGates>()?;
    m.add_class::<PyInterrupted>()?;
    m.add_class::<PyResumed>()?;
    m.add_class::<PyRecoveryFailed>()?;
    m.add_class::<PySessionEventSubscription>()?;
    Ok(())
}
