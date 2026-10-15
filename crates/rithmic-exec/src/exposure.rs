//! Venue exposure rules for a product root.
//!
//! `complete` is the only proof field. A net of `None` means the PnL stream has
//! not been observed since the last (re)connect, and `open_orders` of `None`
//! means the drain failed: unknown is never read as zero.

/// The PnL plant answers a (re)subscribe with a burst: an account row may land
/// before the instrument rows. A drain is proof only once the stream has been
/// live this long.
pub const PNL_SETTLE_NS: i64 = 3_000_000_000;

const MONTH_CODES: &str = "FGHJKMNQUVXZ";

/// `MNQZ6` -> `MNQ`; a bare root (`MNQ`) is returned unchanged.
///
/// A contract symbol is `<root><month letter><1-2 digit year>`; anything else
/// (no month suffix, empty root, non-alphanumeric characters) is returned as the
/// trimmed, uppercased input.
pub fn product_root(symbol: &str) -> String {
    let sym = symbol.trim().to_uppercase();
    let bytes = sym.as_bytes();
    let digits = bytes
        .iter()
        .rev()
        .take_while(|b| b.is_ascii_digit())
        .count();
    // <root: at least one char><month letter><1-2 digits>
    if (1..=2).contains(&digits) && bytes.len() >= digits + 2 {
        let letter_at = bytes.len() - digits - 1;
        let root = &bytes[..letter_at];
        if MONTH_CODES.as_bytes().contains(&bytes[letter_at])
            && root
                .iter()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        {
            return sym[..letter_at].to_string();
        }
    }
    sym
}

/// Whether `symbol` names one listed contract (`ESZ6`, `ESZ26`, `esu6`) rather
/// than a product root (`ES`, `6E`).
pub fn is_listed_contract(symbol: &str) -> bool {
    product_root(symbol) != symbol.trim().to_uppercase()
}

/// `MNQZ6` belongs to root `MNQ` (and exact `MNQ` matches itself). Never a
/// prefix match: `ES` must not claim `ESTX6`.
pub fn matches_root(symbol: &str, root: &str) -> bool {
    let want = root.trim().to_uppercase();
    !want.is_empty() && product_root(symbol) == want
}

/// One `instrument_pnl` row reduced to what the net needs.
pub struct PositionRow<'a> {
    pub symbol: &'a str,
    pub quantity: i64,
    /// `LONG`, `SHORT`, or anything else (flat).
    pub side: &'a str,
}

/// Sum of signed quantities over every contract of `root`.
pub fn signed_net(rows: &[PositionRow<'_>], root: &str) -> i64 {
    rows.iter()
        .filter(|r| matches_root(r.symbol, root))
        .map(|r| match r.side {
            "LONG" => r.quantity,
            "SHORT" => -r.quantity,
            _ => 0,
        })
        .sum()
}

/// Both legs read from the venue, the drain after the PnL burst settled.
pub fn exposure_complete(
    net: Option<i64>,
    open_orders: Option<i64>,
    pnl_ts_ns: Option<i64>,
    drain_ts_ns: Option<i64>,
) -> bool {
    match (net, open_orders, pnl_ts_ns, drain_ts_ns) {
        (Some(_), Some(_), Some(pnl), Some(drain)) => drain - pnl >= PNL_SETTLE_NS,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roots_come_from_contract_symbols() {
        for (symbol, root) in [
            ("MNQZ6", "MNQ"),
            ("MNQ", "MNQ"),
            ("ESH27", "ES"),
            (" mnqz6 ", "MNQ"),
            ("NQU6", "NQ"),
            ("6EH7", "6E"),
        ] {
            assert_eq!(product_root(symbol), root, "{symbol}");
        }
    }

    #[test]
    fn non_contract_symbols_are_returned_uppercased() {
        // No month suffix, three-digit year, empty root, or foreign characters.
        for symbol in ["ES", "ESZ123", "Z6", "6", "", "ES-Z6", "ESA6"] {
            assert_eq!(
                product_root(symbol),
                symbol.trim().to_uppercase(),
                "{symbol}"
            );
        }
    }

    #[test]
    fn matching_is_exact_never_a_prefix() {
        assert!(matches_root("MNQZ6", "MNQ"));
        assert!(matches_root("MNQ", "mnq"));
        assert!(!matches_root("MNQZ6", "MN"));
        assert!(!matches_root("ESTX6", "ES"));
        assert!(!matches_root("MNQZ6", "  "));
    }

    #[test]
    fn net_sums_signed_quantities_over_the_root_only() {
        let rows = [
            PositionRow {
                symbol: "MNQZ6",
                quantity: 2,
                side: "LONG",
            },
            PositionRow {
                symbol: "MNQU6",
                quantity: 1,
                side: "SHORT",
            },
            PositionRow {
                symbol: "NQZ6",
                quantity: 5,
                side: "LONG",
            },
            PositionRow {
                symbol: "MNQH7",
                quantity: 9,
                side: "FLAT",
            },
        ];
        assert_eq!(signed_net(&rows, "MNQ"), 1);
        assert_eq!(signed_net(&rows, "NQ"), 5);
        assert_eq!(signed_net(&rows, "ES"), 0);
    }

    #[test]
    fn completeness_needs_both_legs_and_a_settled_drain() {
        let ok = |n, o, p, d| exposure_complete(n, o, p, d);
        assert!(ok(Some(-1), Some(1), Some(10), Some(10 + PNL_SETTLE_NS)));
        assert!(!ok(Some(-1), Some(1), Some(10), Some(9 + PNL_SETTLE_NS)));
        assert!(!ok(None, Some(1), Some(10), Some(10 + PNL_SETTLE_NS)));
        assert!(!ok(Some(0), None, Some(10), Some(10 + PNL_SETTLE_NS)));
        assert!(!ok(Some(0), Some(0), None, Some(10 + PNL_SETTLE_NS)));
        assert!(!ok(Some(0), Some(0), Some(10), None));
    }
}
