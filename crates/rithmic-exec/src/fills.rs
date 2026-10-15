//! Fill identity: the venue trade id and the adapter's dedup key.
//!
//! Both depend only on the fill's own fields, never on the adapter clock, so a
//! replayed fill gets the same identity. Numeric parts arrive as the text the
//! caller would print for them; that text is part of the identity.

/// The fields a fill's identity is built from.
pub struct FillParts<'a> {
    pub fill_id: Option<&'a str>,
    pub basket_id: &'a str,
    pub exchange_order_id: &'a str,
    pub fill_size: &'a str,
    pub fill_price: &'a str,
}

/// Venue trade id: the venue `fill_id` when present, else a stable composite of
/// the fill's own fields (so a fill the venue never tagged still dedupes).
///
/// Accepted trade-off of the composite: two untagged partial fills of one order
/// at the same size and price in the same venue time stamp (or both without
/// one) share an identity, so the second is taken for a replay of the first.
/// Rithmic tags fills with `fill_id`, so this only matters for a fill that
/// arrives without it; keying on the adapter clock instead would make every
/// replay a new fill, which is the worse failure (double-counted position).
pub fn trade_id(parts: &FillParts<'_>, ts_event: i64) -> String {
    match parts.fill_id {
        Some(id) if !id.is_empty() => id.to_string(),
        _ => format!(
            "{}:{}:{}:{}:{}",
            parts.basket_id, parts.exchange_order_id, ts_event, parts.fill_size, parts.fill_price
        ),
    }
}

/// Stable fill identity for the dedup store, scoped by account and instrument.
pub fn dedup_key(
    account_id: &str,
    instrument_id: &str,
    parts: &FillParts<'_>,
    ts_event: i64,
) -> String {
    format!("{account_id}|{instrument_id}|{}", trade_id(parts, ts_event))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn venue_fill_ids_keep_identical_partials_apart() {
        assert_ne!(
            trade_id(&parts(Some("F1")), 7),
            trade_id(&parts(Some("F2")), 7)
        );
    }

    #[test]
    fn untagged_identical_partials_share_an_identity_by_design() {
        // See `trade_id`: without a venue fill id the fill's own fields are all
        // there is, and a replay must key the same.
        assert_eq!(trade_id(&parts(None), 7), trade_id(&parts(None), 7));
        assert_ne!(trade_id(&parts(None), 7), trade_id(&parts(None), 8));
    }

    fn parts(fill_id: Option<&'static str>) -> FillParts<'static> {
        FillParts {
            fill_id,
            basket_id: "B1",
            exchange_order_id: "X9",
            fill_size: "2",
            fill_price: "21000.25",
        }
    }

    #[test]
    fn the_venue_fill_id_wins() {
        assert_eq!(trade_id(&parts(Some("F-77")), 5), "F-77");
        assert_eq!(
            dedup_key("ACC", "MNQZ6-CME.RITHMIC", &parts(Some("F-77")), 5),
            "ACC|MNQZ6-CME.RITHMIC|F-77"
        );
    }

    #[test]
    fn an_untagged_fill_gets_a_composite_of_its_own_fields() {
        assert_eq!(trade_id(&parts(None), 1_700), "B1:X9:1700:2:21000.25");
        // An empty fill id is no id.
        assert_eq!(trade_id(&parts(Some("")), 1_700), "B1:X9:1700:2:21000.25");
    }

    #[test]
    fn identity_is_clock_free_and_repeatable() {
        let a = dedup_key("A", "I", &parts(None), 42);
        let b = dedup_key("A", "I", &parts(None), 42);
        assert_eq!(a, b);
        assert_ne!(
            a,
            dedup_key("A", "I", &parts(None), 43),
            "timestamp is part of the key"
        );
        assert_ne!(
            a,
            dedup_key("B", "I", &parts(None), 42),
            "account scopes the key"
        );
    }
}
