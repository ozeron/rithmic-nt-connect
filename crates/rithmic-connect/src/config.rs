//! Session configuration for the Phase 1 Rithmic facade.

use rithmic_rs::{RithmicAccount, RithmicConfig, RithmicEnv};

use crate::error::{Error, Result};

/// LucidTrading production R|Protocol WebSocket URL.
pub const DEFAULT_LUCID_URL: &str = "wss://rprotocol.rithmic.com:443";
/// Default system name for LucidTrading prop access.
pub const DEFAULT_SYSTEM_NAME: &str = "LucidTrading";
/// Default application name used for smoke / Phase 1 MD.
pub const DEFAULT_APP_NAME: &str = "rithmic-connect";
/// Default application version.
pub const DEFAULT_APP_VERSION: &str = "0.1.0";

/// Wire-level session settings mapped onto [`RithmicConfig`].
#[derive(Clone)]
pub struct SessionConfig {
    user: String,
    password: String,
    system_name: String,
    url: String,
    beta_url: String,
    app_name: String,
    app_version: String,
    env: RithmicEnv,
    account_id: Option<String>,
    fcm_id: Option<String>,
    ib_id: Option<String>,
}

impl std::fmt::Debug for SessionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionConfig")
            .field("user", &self.user)
            .field("password", &"***")
            .field("system_name", &self.system_name)
            .field("url", &self.url)
            .field("beta_url", &self.beta_url)
            .field("app_name", &self.app_name)
            .field("app_version", &self.app_version)
            .field("env", &self.env)
            .field("account_id", &self.account_id)
            .field("fcm_id", &self.fcm_id)
            .field("ib_id", &self.ib_id)
            .finish()
    }
}

impl SessionConfig {
    /// Start a builder (defaults to Live + LucidTrading URL/system).
    pub fn builder() -> SessionConfigBuilder {
        SessionConfigBuilder::default()
    }

    /// Username.
    pub fn user(&self) -> &str {
        &self.user
    }

    /// Password (avoid logging; [`Debug`] redacts it).
    pub fn password(&self) -> &str {
        &self.password
    }

    /// Rithmic system name (e.g. `LucidTrading`).
    pub fn system_name(&self) -> &str {
        &self.system_name
    }

    /// Primary WebSocket URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Application name.
    pub fn app_name(&self) -> &str {
        &self.app_name
    }

    /// Application version.
    pub fn app_version(&self) -> &str {
        &self.app_version
    }

    /// Environment (Live / Demo / Test).
    pub fn env(&self) -> RithmicEnv {
        self.env
    }

    /// Optional account triple for PnL plant.
    pub fn account(&self) -> Option<RithmicAccount> {
        match (&self.account_id, &self.fcm_id, &self.ib_id) {
            (Some(account_id), Some(fcm_id), Some(ib_id))
                if !account_id.is_empty() && !fcm_id.is_empty() && !ib_id.is_empty() =>
            {
                Some(RithmicAccount::new(fcm_id, ib_id, account_id))
            }
            _ => None,
        }
    }

    /// Convert into a [`RithmicConfig`] for plant connect.
    pub fn to_rithmic_config(&self) -> Result<RithmicConfig> {
        RithmicConfig::builder(self.env)
            .user(self.user.clone())
            .password(self.password.clone())
            .system_name(self.system_name.clone())
            .url(self.url.clone())
            .beta_url(self.beta_url.clone())
            .app_name(self.app_name.clone())
            .app_version(self.app_version.clone())
            .build()
            .map_err(|e| Error::Config(e.to_string()))
    }
}

/// Builder for [`SessionConfig`].
#[derive(Debug, Clone)]
pub struct SessionConfigBuilder {
    user: Option<String>,
    password: Option<String>,
    system_name: Option<String>,
    url: Option<String>,
    beta_url: Option<String>,
    app_name: Option<String>,
    app_version: Option<String>,
    env: RithmicEnv,
    account_id: Option<String>,
    fcm_id: Option<String>,
    ib_id: Option<String>,
}

impl Default for SessionConfigBuilder {
    fn default() -> Self {
        Self {
            user: None,
            password: None,
            system_name: Some(DEFAULT_SYSTEM_NAME.to_string()),
            url: Some(DEFAULT_LUCID_URL.to_string()),
            beta_url: None,
            app_name: Some(DEFAULT_APP_NAME.to_string()),
            app_version: Some(DEFAULT_APP_VERSION.to_string()),
            env: RithmicEnv::Live,
            account_id: None,
            fcm_id: None,
            ib_id: None,
        }
    }
}

impl SessionConfigBuilder {
    /// Set username.
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Set password.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Set system name.
    pub fn system_name(mut self, system_name: impl Into<String>) -> Self {
        self.system_name = Some(system_name.into());
        self
    }

    /// Set primary WebSocket URL.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Set alternate / beta WebSocket URL.
    pub fn beta_url(mut self, beta_url: impl Into<String>) -> Self {
        self.beta_url = Some(beta_url.into());
        self
    }

    /// Set application name.
    pub fn app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app_name = Some(app_name.into());
        self
    }

    /// Set application version.
    pub fn app_version(mut self, app_version: impl Into<String>) -> Self {
        self.app_version = Some(app_version.into());
        self
    }

    /// Set environment.
    pub fn env(mut self, env: RithmicEnv) -> Self {
        self.env = env;
        self
    }

    /// Set optional account id.
    pub fn account_id(mut self, account_id: impl Into<String>) -> Self {
        self.account_id = Some(account_id.into());
        self
    }

    /// Set optional FCM id.
    pub fn fcm_id(mut self, fcm_id: impl Into<String>) -> Self {
        self.fcm_id = Some(fcm_id.into());
        self
    }

    /// Set optional IB id.
    pub fn ib_id(mut self, ib_id: impl Into<String>) -> Self {
        self.ib_id = Some(ib_id.into());
        self
    }

    /// Validate and build.
    pub fn build(self) -> Result<SessionConfig> {
        let user = require_field("user", self.user)?;
        let password = require_field("password", self.password)?;
        let system_name = require_field("system_name", self.system_name)?;
        let url = require_field("url", self.url)?;
        let app_name = require_field("app_name", self.app_name)?;
        let app_version = require_field("app_version", self.app_version)?;
        let beta_url = self
            .beta_url
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| url.clone());

        Ok(SessionConfig {
            user,
            password,
            system_name,
            url,
            beta_url,
            app_name,
            app_version,
            env: self.env,
            account_id: self.account_id,
            fcm_id: self.fcm_id,
            ib_id: self.ib_id,
        })
    }
}

fn require_field(name: &str, value: Option<String>) -> Result<String> {
    match value {
        Some(v) if !v.trim().is_empty() => Ok(v),
        _ => Err(Error::Config(format!(
            "missing or empty required field: {name}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_user() {
        let err = SessionConfig::builder()
            .user("")
            .password("secret")
            .build()
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("user"));
        assert!(!msg.contains("secret"));
    }

    #[test]
    fn rejects_empty_password() {
        let err = SessionConfig::builder()
            .user("alice")
            .password("")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("password"));
    }

    #[test]
    fn builds_lucid_defaults() {
        let cfg = SessionConfig::builder()
            .user("alice")
            .password("secret")
            .build()
            .unwrap();
        assert_eq!(cfg.system_name(), DEFAULT_SYSTEM_NAME);
        assert_eq!(cfg.url(), DEFAULT_LUCID_URL);
        assert_eq!(cfg.env(), RithmicEnv::Live);
        assert!(format!("{cfg:?}").contains("***"));
        assert!(!format!("{cfg:?}").contains("secret"));
    }

    #[test]
    fn to_rithmic_config_ok() {
        let cfg = SessionConfig::builder()
            .user("alice")
            .password("secret")
            .build()
            .unwrap();
        let rc = cfg.to_rithmic_config().unwrap();
        assert_eq!(rc.user, "alice");
        assert_eq!(rc.system_name, "LucidTrading");
    }
}
