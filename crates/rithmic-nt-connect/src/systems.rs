//! Gateway system-name discovery (no login).
//!
//! Same wire as parbhatc/Rithmic `npm run systems`:
//! open a WebSocket, send template 16 (`RequestRithmicSystemInfo`),
//! read template 17 (`ResponseRithmicSystemInfo.system_name`).

use rithmic_rs::rti::messages::RithmicMessage;
use rithmic_rs::{ConnectStrategy, RithmicConfig, RithmicEnv, RithmicTickerPlant};

use crate::config::{DEFAULT_APP_NAME, DEFAULT_APP_VERSION, DEFAULT_LUCID_URL};
use crate::error::{Error, Result};

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

/// List system names advertised by a Rithmic gateway. Does not log in.
pub async fn list_systems(url: &str) -> Result<Vec<String>> {
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

    let plant = RithmicTickerPlant::connect(&rc, ConnectStrategy::Retry).await?;
    let handle = plant.get_handle();
    let resp = handle.get_system_info().await?;
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
