//! One weakly-held host per credential fingerprint; the flock precedes venue construction.
use crate::flock::SingletonError;
use crate::{HistoryPolicy, ParentGates, SessionHost, SessionLock, Timing, Venue};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;

/// Same identity fields as the Python session factory, excluding password and plants.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct HostFingerprint {
    pub connect_mode: String,
    pub user: String,
    pub system_name: String,
    pub url: String,
    pub env: String,
    pub account_id: Option<String>,
    pub fcm_id: Option<String>,
    pub ib_id: Option<String>,
    pub gateway_listen: Option<String>,
    pub beta_url: Option<String>,
    pub app_name: String,
    pub app_version: String,
}
impl HostFingerprint {
    pub fn new(user: &str, system_name: &str, url: &str, env: &str) -> Self {
        Self {
            connect_mode: "direct".into(),
            user: user.into(),
            system_name: system_name.into(),
            url: url.into(),
            env: env.to_ascii_lowercase(),
            account_id: None,
            fcm_id: None,
            ib_id: None,
            gateway_listen: None,
            beta_url: None,
            app_name: "rithmic-nt-connect".into(),
            app_version: "0.1.0".into(),
        }
    }
}
#[derive(Clone)]
pub struct HostOptions {
    pub gates: ParentGates,
    pub timing: Timing,
    pub history_policy: HistoryPolicy,
    /// Roots preserve the historical socket-sibling filename after path clamping.
    pub lock_filename: Option<std::ffi::OsString>,
}
impl Default for HostOptions {
    fn default() -> Self {
        Self {
            gates: ParentGates::default(),
            timing: Timing::default(),
            history_policy: HistoryPolicy::Allow,
            lock_filename: None,
        }
    }
}
pub struct HostRegistry<V: Venue + ?Sized + 'static = dyn Venue> {
    hosts: Arc<Mutex<HashMap<HostFingerprint, Weak<SessionHost<V>>>>>,
}
impl<V: Venue + ?Sized + 'static> Default for HostRegistry<V> {
    fn default() -> Self {
        Self {
            hosts: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}
impl<V: Venue + ?Sized + 'static> HostRegistry<V> {
    pub async fn get_or_start(
        &self,
        key: HostFingerprint,
        make_venue: impl FnOnce() -> Arc<V>,
        lock_dir: &Path,
        options: HostOptions,
    ) -> Result<Arc<SessionHost<V>>, SingletonError> {
        let mut hosts = self.hosts.lock().await;
        hosts.retain(|_, host| host.strong_count() > 0);
        if let Some(host) = hosts.get(&key).and_then(Weak::upgrade) {
            if !host.is_terminal() {
                return Ok(host);
            }
            // A failed host is never reusable, even while old facades still hold it.
            host.shutdown().await;
        }
        let lock = match &options.lock_filename {
            Some(name) => SessionLock::try_acquire_path(lock_dir.join(name))?,
            None => SessionLock::try_acquire_in(lock_dir, &key)?,
        };
        let host = SessionHost::with_handle(
            Some(crate::VenueHandle::new(make_venue())),
            options.gates,
            options.timing,
            options.history_policy,
            false,
            crate::fanout::DEFAULT_QUEUE_CAP,
        );
        host.hold_lock(lock);
        let weak_host = Arc::downgrade(&host);
        let weak_registry = Arc::downgrade(&self.hosts);
        let evict_key = key.clone();
        host.on_shutdown(move || {
            if let Some(hosts) = weak_registry.upgrade() {
                tokio::spawn(async move {
                    let mut hosts = hosts.lock().await;
                    // A replacement may already own this key when cleanup runs.
                    if hosts
                        .get(&evict_key)
                        .is_some_and(|entry| entry.ptr_eq(&weak_host))
                    {
                        hosts.remove(&evict_key);
                    }
                });
            }
        });
        hosts.insert(key, Arc::downgrade(&host));
        Ok(host)
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests;
