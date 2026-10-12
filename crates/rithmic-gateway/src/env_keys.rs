//! Credential/endpoint env keys for the parent binary, in the same precedence as
//! the Python `GatewayConfig.from_env` / `SessionConfig` (`GATEWAY` before `URL`,
//! `SYSTEM` before `SYSTEM_NAME`, `USER` before `USERNAME`).
//!
//! [`resolve_endpoint`] also mirrors the Python Test/Demo guard: a test system with
//! no explicit URL is refused, and its env follows the system name.
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

/// Same markers as Python `config.system_kind` (production checked first).
const TEST_SYSTEM_MARKERS: &[&str] = &["TEST", "DEMO", "SANDBOX", "SIM", "PAPER"];
const PRODUCTION_SYSTEM_MARKERS: &[&str] = &["LUCID", "PRODUCTION", "RITHMIC 01", "RITHMIC 02"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemKind {
    Production,
    Test,
    Unknown,
}

pub fn system_kind(system_name: &str) -> SystemKind {
    let upper = system_name.to_uppercase();
    if PRODUCTION_SYSTEM_MARKERS.iter().any(|m| upper.contains(m)) {
        SystemKind::Production
    } else if TEST_SYSTEM_MARKERS.iter().any(|m| upper.contains(m)) {
        SystemKind::Test
    } else {
        SystemKind::Unknown
    }
}

/// The login the parent will make: system, URL and env, resolved like the Python
/// `SessionConfig` and refused where Python refuses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub system_name: String,
    pub url: String,
    pub env_name: String,
}

/// Resolve the endpoint from env. A Test/Demo system without an explicit URL is
/// refused (never dialled at the production default); its env follows the system
/// name (`Demo` for DEMO/PAPER, else `Test`) unless `RITHMIC_ENV` says otherwise.
pub fn resolve_endpoint(lookup: impl Fn(&str) -> Option<String>) -> Result<Endpoint, String> {
    let system_name =
        env_first(&lookup, SYSTEM_KEYS).unwrap_or_else(|| DEFAULT_SYSTEM_NAME.to_string());
    let explicit_url = env_first(&lookup, URL_KEYS);
    let explicit_env = env_first(&lookup, ENV_KEYS);
    let kind = system_kind(&system_name);
    if kind == SystemKind::Test && explicit_url.is_none() {
        return Err(format!(
            "system {system_name:?} is a Test/Demo system but no RITHMIC_GATEWAY / RITHMIC_URL \
             is set; refusing to dial the production default {DEFAULT_URL}"
        ));
    }
    let env_name = explicit_env.unwrap_or_else(|| match kind {
        SystemKind::Test => {
            let upper = system_name.to_uppercase();
            if upper.contains("DEMO") || upper.contains("PAPER") {
                "Demo".to_string()
            } else {
                "Test".to_string()
            }
        }
        SystemKind::Production | SystemKind::Unknown => DEFAULT_ENV.to_string(),
    });
    Ok(Endpoint {
        system_name,
        url: explicit_url.unwrap_or_else(|| DEFAULT_URL.to_string()),
        env_name,
    })
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
    fn test_system_without_url_is_refused() {
        let err = resolve_endpoint(lookup(&[("RITHMIC_SYSTEM_NAME", "Rithmic Test")])).unwrap_err();
        assert!(err.contains("refusing"), "{err}");
    }

    #[test]
    fn test_system_with_url_derives_its_env() {
        let ep = resolve_endpoint(lookup(&[
            ("RITHMIC_SYSTEM_NAME", "Rithmic Test"),
            ("RITHMIC_GATEWAY", "wss://rituz00100.rithmic.com:443"),
        ]))
        .unwrap();
        assert_eq!(ep.env_name, "Test");
        assert_eq!(ep.url, "wss://rituz00100.rithmic.com:443");
        let ep = resolve_endpoint(lookup(&[
            ("RITHMIC_SYSTEM", "Rithmic Paper Trading"),
            ("RITHMIC_URL", "wss://x:443"),
        ]))
        .unwrap();
        assert_eq!(ep.env_name, "Demo");
        let ep = resolve_endpoint(lookup(&[
            ("RITHMIC_SYSTEM", "Rithmic Test"),
            ("RITHMIC_GATEWAY", "wss://x:443"),
            ("RITHMIC_ENV", "Demo"),
        ]))
        .unwrap();
        assert_eq!(ep.env_name, "Demo"); // explicit env wins
    }

    #[test]
    fn production_defaults_unchanged() {
        let ep = resolve_endpoint(lookup(&[])).unwrap();
        assert_eq!(
            ep,
            Endpoint {
                system_name: DEFAULT_SYSTEM_NAME.into(),
                url: DEFAULT_URL.into(),
                env_name: DEFAULT_ENV.into(),
            }
        );
        assert_eq!(system_kind("LucidTrading"), SystemKind::Production);
        assert_eq!(system_kind("Rithmic Test"), SystemKind::Test);
        assert_eq!(system_kind("Apex"), SystemKind::Unknown);
    }

    #[test]
    fn nothing_set_is_none() {
        assert_eq!(env_first(lookup(&[]), USER_KEYS), None);
    }
}
