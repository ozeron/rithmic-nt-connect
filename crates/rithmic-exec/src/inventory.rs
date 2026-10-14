//! Rules for reducing venue events into an inventory of working orders and
//! net positions, without a node.
//!
//! Unknown is never absent: an order whose unfilled size is unknown stays
//! working, and a position row that cannot be read is not flat.

/// How the adapter's status mapping classifies an order row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusClass {
    /// Not terminal: still working.
    Working,
    /// Mapped to FILLED (every `filled` row, partial or not).
    Filled,
    /// Canceled, rejected or expired.
    OtherTerminal,
}

/// Whether an order is finished and may leave the working set.
///
/// A fill counts only when provably complete: the unfilled size is zero, or (when
/// the unfilled size is unknown) the total filled covers a positive quantity.
pub fn order_is_gone(
    class: StatusClass,
    unfilled: Option<i64>,
    quantity: Option<i64>,
    total_fill: Option<i64>,
) -> bool {
    match class {
        StatusClass::Working => false,
        StatusClass::OtherTerminal => true,
        StatusClass::Filled => match unfilled {
            Some(unfilled) => unfilled <= 0,
            None => matches!((quantity, total_fill), (Some(q), Some(f)) if q > 0 && f >= q),
        },
    }
}

/// What a position row does to the book.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PositionChange {
    /// The net could not be read: exposure unknown (and the contract is not flat).
    Unknown,
    /// Flat: the contract leaves the book.
    Flat,
    /// A non-zero net.
    Net(i64),
}

pub fn position_change(net: Option<i64>) -> PositionChange {
    match net {
        None => PositionChange::Unknown,
        Some(0) => PositionChange::Flat,
        Some(n) => PositionChange::Net(n),
    }
}

/// Positions are trustworthy only once the PnL stream was observed for the settle
/// window (account rows land before the instrument burst) and every position row
/// read cleanly. Times are monotonic seconds.
pub fn pnl_live(
    first_seen_s: Option<f64>,
    observed_until_s: Option<f64>,
    any_unreadable: bool,
    settle_s: f64,
) -> bool {
    match (first_seen_s, observed_until_s) {
        (Some(first), Some(until)) if !any_unreadable => until - first >= settle_s,
        _ => false,
    }
}

/// Buy/sell from a wire transaction type: `1`/`2`, or the words `BUY`/`B`/`SELL`/`S`.
pub fn side_from_txn(code: Option<i64>, text: &str) -> Option<&'static str> {
    match code {
        Some(1) => return Some("BUY"),
        Some(2) => return Some("SELL"),
        _ => {}
    }
    match text.trim().to_uppercase().as_str() {
        "BUY" | "B" | "1" => Some("BUY"),
        "SELL" | "S" | "2" => Some("SELL"),
        _ => None,
    }
}

/// The order that flattens a net position.
pub fn flatten_side(net: i64) -> &'static str {
    if net > 0 {
        "SELL"
    } else {
        "BUY"
    }
}

#[cfg(test)]
mod tests {
    use super::StatusClass::*;
    use super::*;

    #[test]
    fn working_orders_stay_and_other_terminals_leave() {
        assert!(!order_is_gone(Working, Some(0), Some(1), Some(1)));
        assert!(order_is_gone(OtherTerminal, None, None, None));
    }

    #[test]
    fn a_fill_leaves_only_when_provably_complete() {
        assert!(order_is_gone(Filled, Some(0), None, None));
        assert!(order_is_gone(Filled, Some(-1), None, None));
        assert!(
            !order_is_gone(Filled, Some(2), Some(5), Some(5)),
            "unfilled size wins"
        );
        // Unknown unfilled: fall back to quantity and total fill.
        assert!(order_is_gone(Filled, None, Some(3), Some(3)));
        assert!(order_is_gone(Filled, None, Some(3), Some(4)));
        assert!(
            !order_is_gone(Filled, None, Some(3), Some(2)),
            "partial fill stays working"
        );
        assert!(
            !order_is_gone(Filled, None, Some(0), Some(0)),
            "no quantity is not complete"
        );
        assert!(!order_is_gone(Filled, None, None, Some(3)));
        assert!(!order_is_gone(Filled, None, Some(3), None));
    }

    #[test]
    fn position_rows_are_unknown_flat_or_a_net() {
        assert_eq!(position_change(None), PositionChange::Unknown);
        assert_eq!(position_change(Some(0)), PositionChange::Flat);
        assert_eq!(position_change(Some(-2)), PositionChange::Net(-2));
    }

    #[test]
    fn pnl_is_live_only_after_the_settle_window_and_clean_rows() {
        assert!(!pnl_live(None, None, false, 3.0));
        assert!(!pnl_live(Some(1.0), None, false, 3.0));
        assert!(!pnl_live(Some(1.0), Some(3.9), false, 3.0));
        assert!(pnl_live(Some(1.0), Some(4.0), false, 3.0));
        assert!(
            !pnl_live(Some(1.0), Some(60.0), true, 3.0),
            "an unreadable row is not flat"
        );
    }

    #[test]
    fn sides_come_from_codes_or_words() {
        assert_eq!(side_from_txn(Some(1), ""), Some("BUY"));
        assert_eq!(side_from_txn(Some(2), "garbage"), Some("SELL"));
        assert_eq!(side_from_txn(None, " b "), Some("BUY"));
        assert_eq!(side_from_txn(None, "sell"), Some("SELL"));
        assert_eq!(side_from_txn(None, "2"), Some("SELL"));
        assert_eq!(side_from_txn(Some(3), "x"), None);
        assert_eq!(side_from_txn(None, ""), None);
    }

    #[test]
    fn flattening_opposes_the_net() {
        assert_eq!(flatten_side(3), "SELL");
        assert_eq!(flatten_side(-3), "BUY");
    }
}
