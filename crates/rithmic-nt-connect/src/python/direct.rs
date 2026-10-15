//! A direct facade renews its registry attachment only after explicit disconnect.
use super::*;
use std::sync::{Mutex, RwLock};

pub(super) struct Attachment {
    pub session: Arc<dyn Session>,
    pub failed: Box<dyn Fn() -> bool + Send + Sync>,
    #[cfg(feature = "testing")]
    pub script: Option<Arc<rithmic_host::fake::FakeVenue>>,
    #[cfg(feature = "testing")]
    pub fault_venue: Option<Arc<RithmicVenue>>,
}

pub(super) struct DirectState {
    factory: Box<dyn Fn() -> PyResult<Attachment> + Send + Sync>,
    disconnected: bool,
    pub plants: PlantSet,
    pub trading: bool,
    pub cancel_all: bool,
}

pub(super) fn new(
    py: Python<'_>,
    plants: PlantSet,
    factory: impl Fn() -> PyResult<Attachment> + Send + Sync + 'static,
) -> PyResult<PySession> {
    let attachment = py.detach(&factory)?;
    Ok(PySession {
        attachment: RwLock::new(attachment),
        direct: Some(Mutex::new(DirectState {
            factory: Box::new(factory),
            disconnected: false,
            plants,
            trading: false,
            cancel_all: false,
        })),
        gateway: None,
    })
}

impl PySession {
    pub(super) fn session(&self) -> Arc<dyn Session> {
        self.attachment
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .session
            .clone()
    }

    pub(super) fn connect_attachment(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            let mut direct = self
                .direct
                .as_ref()
                .map(|state| state.lock().unwrap_or_else(|e| e.into_inner()));
            if let Some(state) = &mut direct {
                if state.disconnected {
                    let attachment = (state.factory)()?;
                    runtime()
                        .block_on(async {
                            attachment.session.request_plants(state.plants).await?;
                            attachment
                                .session
                                .open_gates(state.trading, state.cancel_all)
                                .await;
                            Ok::<_, Error>(())
                        })
                        .map_err(to_py_err)?;
                    *self.attachment.write().unwrap_or_else(|e| e.into_inner()) = attachment;
                    state.disconnected = false;
                }
            }
            runtime()
                .block_on(self.session().connect())
                .map_err(to_py_err)
        })
    }

    pub(super) fn disconnect_attachment(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            let mut direct = self
                .direct
                .as_ref()
                .map(|state| state.lock().unwrap_or_else(|e| e.into_inner()));
            let result = runtime()
                .block_on(self.session().disconnect())
                .map_err(to_py_err);
            if let Some(state) = &mut direct {
                // A recovery failure is permanent for this facade, even after disconnect.
                let failed = (self
                    .attachment
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .failed)();
                state.disconnected = !failed;
            }
            result
        })
    }
}
