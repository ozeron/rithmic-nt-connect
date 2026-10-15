//! Gateway system-name discovery (no login).
//!
//! Same wire as parbhatc/Rithmic `npm run systems`:
//! open a WebSocket, send template 16 (`RequestRithmicSystemInfo`),
//! read template 17 (`ResponseRithmicSystemInfo.system_name`).

use std::time::Duration;

use rithmic_rs::rti::messages::RithmicMessage;
use rithmic_rs::{ConnectStrategy, RithmicConfig, RithmicEnv, RithmicTickerPlant};

use crate::config::{DEFAULT_APP_NAME, DEFAULT_APP_VERSION, DEFAULT_LUCID_URL};
use rithmic_host::{Error, Result};

/// Normalize a gateway host or URL into `wss://host:port`.
pub fn normalize_gateway_url(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return DEFAULT_LUCID_URL.to_string();
    }
    if trimmed.contains("://") {
        return trimmed.to_string();
    }
    format!("wss://{trimmed}")
}

/// How long [`list_systems`] may take: the probe retries its connection, so an
/// unreachable gateway must fail with a typed error instead of waiting forever.
pub const LIST_SYSTEMS_DEADLINE: Duration = Duration::from_secs(30);

/// List system names advertised by a Rithmic gateway. Does not log in. Fails
/// with a timeout error after [`LIST_SYSTEMS_DEADLINE`].
pub async fn list_systems(url: &str) -> Result<Vec<String>> {
    tokio::time::timeout(LIST_SYSTEMS_DEADLINE, probe_systems(url))
        .await
        .map_err(|_| {
            Error::Session(format!(
                "listing systems timed out after {}s",
                LIST_SYSTEMS_DEADLINE.as_secs()
            ))
        })?
}

async fn probe_systems(url: &str) -> Result<Vec<String>> {
    let url = normalize_gateway_url(url);
    // rithmic-rs requires login fields on config even though this probe never
    // sends RequestLogin. Dummy values stay off the wire.
    let rc = RithmicConfig::builder(RithmicEnv::Live)
        .user("_")
        .password("_")
        .system_name("unused")
        .url(url.clone())
        .beta_url(url)
        .app_name(DEFAULT_APP_NAME)
        .app_version(DEFAULT_APP_VERSION)
        .build()
        .map_err(|e| Error::Config(e.to_string()))?;

    let plant = RithmicTickerPlant::connect(&rc, ConnectStrategy::Retry)
        .await
        .map_err(crate::error::rithmic_error)?;
    let handle = plant.get_handle();
    let resp = handle
        .get_system_info()
        .await
        .map_err(crate::error::rithmic_error)?;
    if let Some(err) = resp.error {
        let _ = handle.disconnect().await;
        return Err(Error::Rithmic(format!("system info: {err}")));
    }
    let names = match resp.message {
        RithmicMessage::ResponseRithmicSystemInfo(info) => info.system_name,
        other => {
            let _ = handle.disconnect().await;
            return Err(Error::Protocol(format!(
                "expected ResponseRithmicSystemInfo, got {other:?}"
            )));
        }
    };
    let _ = handle.disconnect().await;
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_empty_uses_lucid_default() {
        assert_eq!(normalize_gateway_url(""), DEFAULT_LUCID_URL);
        assert_eq!(normalize_gateway_url("  "), DEFAULT_LUCID_URL);
    }

    #[test]
    fn normalize_prepending_wss() {
        assert_eq!(
            normalize_gateway_url("rituz00100.rithmic.com:443"),
            "wss://rituz00100.rithmic.com:443"
        );
        assert_eq!(
            normalize_gateway_url("wss://rprotocol.rithmic.com:443"),
            "wss://rprotocol.rithmic.com:443"
        );
    }
}
