//! Decisions about working-order drain rows.
//!
//! The drain is a best-effort snapshot of the venue's working orders. These
//! functions decide which rows to trust, which row wins per order, and when a
//! row must not move local state; the adapter owns the effects (publishing
//! reports, writing the cache, binding venue ids).

use std::collections::HashMap;

/// Rithmic `price_type` closed set the adapter can map to an order type.
pub const TRUSTWORTHY_PRICE_TYPES: [i64; 4] = [1, 2, 3, 4];
/// Rithmic `duration` closed set the adapter can map to a time in force.
pub const TRUSTWORTHY_DURATIONS: [i64; 4] = [1, 2, 3, 4];

/// Canonical notification kinds that describe a real order state.
const RECOGNIZABLE_KINDS: [&str; 9] = [
    "accepted",
    "updated",
    "canceled",
    "filled",
    "rejected",
    "modify_rejected",
    "cancel_rejected",
    "expired",
    "triggered",
];

/// Status substrings that mark a drain row as a real venue order state.
/// `TRIGGER` covers resting stop rows (`TRIGGER_PENDING`): live-proven on
/// Rithmic Test 2026-08-21, stops never emit an OPEN notification, so this is
/// the only state their drain rows ever carry.
const STATUS_MARKERS: [&str; 6] = ["OPEN", "WORKING", "CANCEL", "REJECT", "EXPIRED", "TRIGGER"];

/// What a drain row says about its order, already coerced by the caller.
pub struct RowTerms<'a> {
    /// The row carries a buy/sell side.
    pub side_known: bool,
    /// Exact-integer `price_type`, or `None` when absent or not an exact integer.
    pub price_type: Option<i64>,
    /// Exact-integer `duration`, or `None` likewise.
    pub duration: Option<i64>,
    pub kind: Option<&'a str>,
    pub status: &'a str,
}

/// A drain row binds a venue id only when its closed-set execution terms are
/// real: side present, `price_type` and `duration` present and mappable, and a
/// recognizable order state. Terms are never fabricated: a row with missing or
/// unknown closed-set values is advisory only.
pub fn row_is_trustworthy(terms: &RowTerms<'_>) -> bool {
    if !terms.side_known {
        return false;
    }
    let (Some(price_type), Some(duration)) = (terms.price_type, terms.duration) else {
        return false;
    };
    if !TRUSTWORTHY_PRICE_TYPES.contains(&price_type) || !TRUSTWORTHY_DURATIONS.contains(&duration)
    {
        return false;
    }
    if terms.kind.is_some_and(|k| RECOGNIZABLE_KINDS.contains(&k)) {
        return true;
    }
    let status = terms.status.to_uppercase();
    STATUS_MARKERS.iter().any(|marker| status.contains(marker))
}

/// Index of the freshest row per key, in order of each key's first appearance.
///
/// The higher timestamp wins; on an equal timestamp (e.g. both 0 when the venue
/// sent none) the last-arrived row wins, so a terminal status following an
/// earlier non-terminal one is not masked.
pub fn freshest_per_key(rows: &[(&str, i128)]) -> Vec<usize> {
    let mut order: Vec<&str> = Vec::new();
    let mut winner: HashMap<&str, usize> = HashMap::new();
    for (index, (key, ts)) in rows.iter().enumerate() {
        match winner.get(key) {
            None => {
                order.push(key);
                winner.insert(key, index);
            }
            Some(&current) if *ts >= rows[current].1 => {
                winner.insert(key, index);
            }
            Some(_) => {}
        }
    }
    order.iter().map(|key| winner[key]).collect()
}

/// What the adapter knows about a tracked order when judging a drain row.
pub struct StaleFacts {
    pub order_closed: bool,
    /// Re-arm barrier (`true`): the live stream owns tracked-order state.
    /// Bulk status recon (`false`): the drain is an advisory snapshot.
    pub live_stream_authoritative: bool,
    /// The row produced a report whose status is not terminal.
    pub report_non_terminal: bool,
    /// The row's venue timestamp; 0 means the venue sent none.
    pub row_ts_ns: i128,
    /// The tracked order's last local event time.
    pub order_ts_last_ns: i128,
}

/// Why a drain row must not advance local state (`None` = forward it).
///
/// The two drain consumers run under different authority models. At the re-arm
/// barrier any row for an order the live stream already closed is stale, as is a
/// row older than the order's last event. In bulk recon only a non-terminal row
/// for a locally closed order is suppressed (venue lag would reconcile ACCEPTED
/// over CANCELED/FILLED); terminal-vs-terminal still forwards because venue
/// FILLED vs local CANCELED is a real race the engine must see.
pub fn stale_reason(facts: &StaleFacts) -> Option<&'static str> {
    if facts.order_closed {
        if facts.live_stream_authoritative {
            return Some("order closed locally");
        }
        return facts
            .report_non_terminal
            .then_some("non-terminal snapshot for locally closed order");
    }
    // A row timestamp of 0 is the synthetic "venue sent none": skipping on it
    // would drop a valid snapshot whenever the order has any live history.
    if facts.live_stream_authoritative
        && facts.row_ts_ns != 0
        && facts.order_ts_last_ns > facts.row_ts_ns
    {
        return Some("live stream advanced past the snapshot");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn good() -> RowTerms<'static> {
        RowTerms {
            side_known: true,
            price_type: Some(1),
            duration: Some(1),
            kind: Some("accepted"),
            status: "",
        }
    }

    #[test]
    fn a_row_with_real_terms_is_trusted() {
        assert!(row_is_trustworthy(&good()));
    }

    #[test]
    fn missing_or_unknown_terms_are_advisory_only() {
        let t = |f: fn(&mut RowTerms<'static>)| {
            let mut terms = good();
            f(&mut terms);
            row_is_trustworthy(&terms)
        };
        assert!(!t(|r| r.side_known = false));
        assert!(!t(|r| r.price_type = None));
        assert!(!t(|r| r.duration = None));
        assert!(!t(|r| r.price_type = Some(0)));
        assert!(!t(|r| r.price_type = Some(5)));
        assert!(!t(|r| r.duration = Some(9)));
        assert!(!t(|r| r.kind = None), "no kind and no status marker");
        assert!(!t(|r| r.kind = Some("bogus")));
    }

    #[test]
    fn a_status_marker_stands_in_for_a_kind() {
        for status in [
            "OPEN",
            "working",
            "Cancelled",
            "REJECTED",
            "expired",
            "trigger pending",
        ] {
            let mut terms = good();
            terms.kind = None;
            terms.status = status;
            assert!(row_is_trustworthy(&terms), "{status}");
        }
        let mut terms = good();
        terms.kind = None;
        terms.status = "complete";
        assert!(!row_is_trustworthy(&terms));
    }

    #[test]
    fn every_known_kind_counts() {
        for kind in RECOGNIZABLE_KINDS {
            let mut terms = good();
            terms.kind = Some(kind);
            assert!(row_is_trustworthy(&terms), "{kind}");
        }
    }

    #[test]
    fn the_freshest_row_wins_and_a_tie_goes_to_the_last_arrival() {
        let rows = [
            ("A", 5),
            ("B", 1),
            ("A", 9),
            ("B", 1),
            ("A", 3),
            ("C", 0),
            ("C", 0),
        ];
        // A: ts 9 at index 2; B: tie at 1, last arrival (3); C: tie at 0, last (6).
        assert_eq!(freshest_per_key(&rows), vec![2, 3, 6]);
        assert_eq!(freshest_per_key(&[]), Vec::<usize>::new());
    }

    #[test]
    fn keys_keep_their_first_seen_order() {
        let rows = [("B", 1), ("A", 1), ("B", 2)];
        assert_eq!(freshest_per_key(&rows), vec![2, 1], "B first, then A");
    }

    fn facts() -> StaleFacts {
        StaleFacts {
            order_closed: false,
            live_stream_authoritative: true,
            report_non_terminal: true,
            row_ts_ns: 100,
            order_ts_last_ns: 50,
        }
    }

    #[test]
    fn at_the_barrier_a_closed_order_is_always_stale() {
        let mut f = facts();
        f.order_closed = true;
        for non_terminal in [true, false] {
            f.report_non_terminal = non_terminal;
            assert_eq!(stale_reason(&f), Some("order closed locally"));
        }
    }

    #[test]
    fn in_bulk_recon_only_non_terminal_rows_for_closed_orders_are_stale() {
        let mut f = facts();
        f.order_closed = true;
        f.live_stream_authoritative = false;
        f.report_non_terminal = true;
        assert_eq!(
            stale_reason(&f),
            Some("non-terminal snapshot for locally closed order")
        );
        f.report_non_terminal = false;
        assert_eq!(
            stale_reason(&f),
            None,
            "venue FILLED vs local CANCELED must reach the engine"
        );
    }

    #[test]
    fn a_row_older_than_the_last_local_event_is_stale_only_at_the_barrier() {
        let mut f = facts();
        f.order_ts_last_ns = 200;
        assert_eq!(
            stale_reason(&f),
            Some("live stream advanced past the snapshot")
        );
        f.live_stream_authoritative = false;
        assert_eq!(stale_reason(&f), None);
    }

    #[test]
    fn a_row_without_a_venue_timestamp_is_never_stale_by_age() {
        let mut f = facts();
        f.row_ts_ns = 0;
        f.order_ts_last_ns = 10_000;
        assert_eq!(stale_reason(&f), None);
    }

    #[test]
    fn an_open_order_with_an_older_last_event_is_forwarded() {
        assert_eq!(stale_reason(&facts()), None);
    }
}
