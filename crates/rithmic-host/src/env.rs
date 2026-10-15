//! Adapter-owned environment identity; venue mapping stays in rithmic-plants.
use std::{fmt, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Env {
    Live,
    Demo,
    Test,
}

impl FromStr for Env {
    type Err = ParseEnvError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "demo" | "development" => Ok(Self::Demo),
            "live" | "production" => Ok(Self::Live),
            "test" => Ok(Self::Test),
            _ => Err(ParseEnvError(s.to_string())),
        }
    }
}
impl fmt::Display for Env {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Live => "live",
            Self::Demo => "demo",
            Self::Test => "test",
        })
    }
}

/// Same parse failure text as the former venue enum.
#[derive(Debug, thiserror::Error)]
#[error("Invalid environment: {0}")]
pub struct ParseEnvError(String);
