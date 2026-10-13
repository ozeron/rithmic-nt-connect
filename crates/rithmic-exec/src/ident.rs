//! Instrument-id encoding and event timestamps.
//!
//! The exchange rides inside the symbol component (`NQU6-CME.RITHMIC`) so
//! distinct venue instruments (`NQU6.CME` vs `NQU6.CBOT`) never share an id.
//! `-` is the separator and `.` the venue delimiter, so neither may appear in
//! a symbol or exchange.

use std::fmt;

/// Why an id could not be built or parsed. `Display` matches the messages the
/// Python adapter has always raised.
#[derive(Debug, PartialEq, Eq)]
pub enum IdentError {
    ForbiddenChar { label: &'static str, value: String },
    EmptySymbol,
    MissingVenue { venue_suffix: String, raw: String },
    EmptyComponent { raw: String },
    BadHyphenEncoding { raw: String },
}

impl fmt::Display for IdentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IdentError::ForbiddenChar { label, value } => {
                write!(f, "{label} must not contain '-' or '.': {}", py_repr(value))
            }
            IdentError::EmptySymbol => write!(f, "symbol must be non-empty"),
            IdentError::MissingVenue { venue_suffix, raw } => write!(
                f,
                "instrument_id must end with {}: {}",
                py_repr(venue_suffix),
                py_repr(raw)
            ),
            IdentError::EmptyComponent { raw } => {
                write!(
                    f,
                    "instrument_id symbol component is empty: {}",
                    py_repr(raw)
                )
            }
            IdentError::BadHyphenEncoding { raw } => {
                write!(f, "invalid instrument_id hyphen encoding: {}", py_repr(raw))
            }
        }
    }
}

impl std::error::Error for IdentError {}

/// Python's `repr()` of a `str`, so error text is identical to the old adapter's.
fn py_repr(s: &str) -> String {
    let quote = if s.contains('\'') && !s.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(s.len() + 2);
    out.push(quote);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                out.push_str(&format!("\\x{:02x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

fn require_safe(value: &str, label: &'static str) -> Result<String, IdentError> {
    if value.contains('-') || value.contains('.') {
        return Err(IdentError::ForbiddenChar {
            label,
            value: value.to_string(),
        });
    }
    Ok(value.to_string())
}

/// `NQU6-CME.RITHMIC` when an exchange is present, else `NQU6.RITHMIC`.
pub fn instrument_id(
    symbol: &str,
    exchange: Option<&str>,
    venue: &str,
) -> Result<String, IdentError> {
    let sym = require_safe(symbol.trim(), "symbol")?;
    if sym.is_empty() {
        return Err(IdentError::EmptySymbol);
    }
    match exchange.map(str::trim).filter(|e| !e.is_empty()) {
        Some(exchange) => {
            let exch = require_safe(&exchange.to_uppercase(), "exchange")?;
            Ok(format!("{sym}-{exch}.{venue}"))
        }
        None => Ok(format!("{sym}.{venue}")),
    }
}

/// Reverse of [`instrument_id`]: `(symbol, exchange)`.
pub fn split_instrument_id(
    instrument_id: &str,
    venue: &str,
) -> Result<(String, Option<String>), IdentError> {
    let raw = instrument_id.trim();
    let venue_suffix = format!(".{venue}");
    let Some(sym_exch) = raw.strip_suffix(venue_suffix.as_str()) else {
        return Err(IdentError::MissingVenue {
            venue_suffix,
            raw: raw.to_string(),
        });
    };
    if sym_exch.is_empty() {
        return Err(IdentError::EmptyComponent {
            raw: raw.to_string(),
        });
    }
    match sym_exch.split_once('-') {
        Some((sym, exch)) => {
            if sym.is_empty() || exch.is_empty() {
                return Err(IdentError::BadHyphenEncoding {
                    raw: raw.to_string(),
                });
            }
            if sym.contains('.') {
                return Err(IdentError::ForbiddenChar {
                    label: "symbol",
                    value: sym.to_string(),
                });
            }
            Ok((
                sym.to_string(),
                Some(require_safe(&exch.to_uppercase(), "exchange")?),
            ))
        }
        None => Ok((require_safe(sym_exch, "symbol")?, None)),
    }
}

/// Event time in unix nanoseconds: the plant's `ts_event_ns` when present, else
/// derived from `ssboe` (+ `usecs`); `None` when the event carries neither.
pub fn event_ts_ns(
    ts_event_ns: Option<i128>,
    ssboe: Option<i128>,
    usecs: Option<i128>,
) -> Option<i128> {
    if let Some(ts) = ts_event_ns {
        return Some(ts);
    }
    ssboe.map(|s| s * 1_000_000_000 + usecs.unwrap_or(0) * 1_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    const V: &str = "RITHMIC";

    #[test]
    fn ids_round_trip_with_and_without_an_exchange() {
        let with = instrument_id("NQU6", Some("cme"), V).unwrap();
        assert_eq!(with, "NQU6-CME.RITHMIC");
        assert_eq!(
            split_instrument_id(&with, V).unwrap(),
            ("NQU6".into(), Some("CME".into()))
        );
        let without = instrument_id("NQU6", None, V).unwrap();
        assert_eq!(without, "NQU6.RITHMIC");
        assert_eq!(
            split_instrument_id(&without, V).unwrap(),
            ("NQU6".into(), None)
        );
        // A blank exchange is no exchange.
        assert_eq!(
            instrument_id("NQU6", Some("  "), V).unwrap(),
            "NQU6.RITHMIC"
        );
    }

    #[test]
    fn symbols_are_trimmed_and_validated_before_the_empty_check() {
        assert_eq!(instrument_id("  NQU6 ", None, V).unwrap(), "NQU6.RITHMIC");
        assert_eq!(instrument_id("  ", None, V), Err(IdentError::EmptySymbol));
        let err = instrument_id("NQ-U6", Some("CME"), V).unwrap_err();
        assert_eq!(
            err.to_string(),
            "symbol must not contain '-' or '.': 'NQ-U6'"
        );
        let err = instrument_id("NQU6", Some("CM.E"), V).unwrap_err();
        assert_eq!(
            err.to_string(),
            "exchange must not contain '-' or '.': 'CM.E'"
        );
    }

    #[test]
    fn parsing_rejects_malformed_ids() {
        for raw in ["NQU6-.RITHMIC", "-CME.RITHMIC"] {
            assert!(
                matches!(
                    split_instrument_id(raw, V),
                    Err(IdentError::BadHyphenEncoding { .. })
                ),
                "{raw}"
            );
        }
        assert!(matches!(
            split_instrument_id("NQU6.CME", V),
            Err(IdentError::MissingVenue { .. })
        ));
        assert!(matches!(
            split_instrument_id(".RITHMIC", V),
            Err(IdentError::EmptyComponent { .. })
        ));
        let err = split_instrument_id("NQ.U6.RITHMIC", V).unwrap_err();
        assert_eq!(
            err.to_string(),
            "symbol must not contain '-' or '.': 'NQ.U6'"
        );
        let err = split_instrument_id("NQU6-C.ME.RITHMIC", V).unwrap_err();
        assert!(err.to_string().starts_with("exchange must not contain"));
    }

    #[test]
    fn error_text_quotes_values_like_python_repr() {
        assert_eq!(py_repr("a"), "'a'");
        assert_eq!(py_repr("it's"), "\"it's\"");
        assert_eq!(py_repr("both'\""), "'both\\'\"'");
        assert_eq!(py_repr("tab\t\\"), "'tab\\t\\\\'");
        assert_eq!(py_repr("\u{1}"), "'\\x01'");
    }

    #[test]
    fn event_time_prefers_the_plant_timestamp() {
        assert_eq!(event_ts_ns(Some(5), Some(9), Some(9)), Some(5));
        assert_eq!(event_ts_ns(None, Some(2), Some(3)), Some(2_000_003_000));
        assert_eq!(event_ts_ns(None, Some(2), None), Some(2_000_000_000));
        assert_eq!(event_ts_ns(None, None, Some(3)), None);
    }
}
