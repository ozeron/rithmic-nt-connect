//! Credential/endpoint env keys for the parent binary, in the same precedence as
//! the Python `GatewayConfig.from_env` / `SessionConfig` (`GATEWAY` before `URL`,
//! `SYSTEM` before `SYSTEM_NAME`, `USER` before `USERNAME`).
//!
//! One env file must mean the same login to the parent and to its Python clients:
//! a binary that read only `RITHMIC_URL` sent a test-system login (set via
//! `RITHMIC_GATEWAY`) to the production server under the default system name.

pub const DEFAULT_URL: &str = "wss://rprotocol.rithmic.com:443";
pub const DEFAULT_SYSTEM_NAME: &str = "LucidTrading";
pub const DEFAULT_ENV: &str = "Live";

pub const USER_KEYS: &[&str] = &["RITHMIC_USER", "RITHMIC_USERNAME"];
pub const URL_KEYS: &[&str] = &["RITHMIC_GATEWAY", "RITHMIC_URL"];
pub const SYSTEM_KEYS: &[&str] = &["RITHMIC_SYSTEM", "RITHMIC_SYSTEM_NAME"];
pub const ENV_KEYS: &[&str] = &["RITHMIC_ENV"];

/// First non-empty (trimmed) value among `keys`, like Python `_env_first`.
pub fn env_first(lookup: impl Fn(&str) -> Option<String>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        lookup(key)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    })
}

/// [`env_first`] over the process environment.
pub fn process_env_first(keys: &[&str]) -> Option<String> {
    env_first(|key| std::env::var(key).ok(), keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |key| map.get(key).cloned()
    }

    #[test]
    fn gateway_key_wins_over_url() {
        let env = lookup(&[
            ("RITHMIC_GATEWAY", "wss://rituz00100.rithmic.com:443"),
            ("RITHMIC_URL", "wss://rprotocol.rithmic.com:443"),
        ]);
        assert_eq!(
            env_first(&env, URL_KEYS).as_deref(),
            Some("wss://rituz00100.rithmic.com:443")
        );
    }

    #[test]
    fn url_alone_still_works() {
        let env = lookup(&[("RITHMIC_URL", "wss://x:443")]);
        assert_eq!(env_first(&env, URL_KEYS).as_deref(), Some("wss://x:443"));
    }

    #[test]
    fn system_key_wins_and_blank_is_unset() {
        let env = lookup(&[
            ("RITHMIC_SYSTEM", "  "),
            ("RITHMIC_SYSTEM_NAME", "Rithmic Test"),
        ]);
        assert_eq!(
            env_first(&env, SYSTEM_KEYS).as_deref(),
            Some("Rithmic Test")
        );
        let env = lookup(&[
            ("RITHMIC_SYSTEM", "Rithmic Test"),
            ("RITHMIC_SYSTEM_NAME", "LucidTrading"),
        ]);
        assert_eq!(
            env_first(&env, SYSTEM_KEYS).as_deref(),
            Some("Rithmic Test")
        );
    }

    #[test]
    fn nothing_set_is_none() {
        assert_eq!(env_first(lookup(&[]), USER_KEYS), None);
    }
}
