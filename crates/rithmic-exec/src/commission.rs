//! Venue commission rates from the order-plant RMS info.
//!
//! Rates are kept as decimal text so the exact value the venue sent survives to
//! the caller's decimal type; no float arithmetic happens here. Lookup falls
//! back product rate -> account default -> none (the caller charges zero).

use std::collections::BTreeMap;

/// Product fill rates keyed by product code (e.g. `MNQ`) plus the account default.
#[derive(Clone, Debug, Default)]
pub struct CommissionTable {
    rates: BTreeMap<String, String>,
    default: Option<String>,
}

impl CommissionTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the product table from RMS rows `(product_code, commission_fill_rate)`.
    /// A row without a (non-empty) code or without a (non-empty) rate is skipped;
    /// a rate of zero is a real rate.
    pub fn load_products<I>(&mut self, rows: I)
    where
        I: IntoIterator<Item = (Option<String>, Option<String>)>,
    {
        self.rates = rows
            .into_iter()
            .filter_map(|(code, rate)| match (code, rate) {
                (Some(code), Some(rate)) if !code.is_empty() && !rate.is_empty() => {
                    Some((code, rate))
                }
                _ => None,
            })
            .collect();
    }

    /// Replace the account default from RMS rows `(account_id, default_commission)`:
    /// the first row for the active account (any row when none is given) that
    /// carries a default.
    pub fn load_account_default<I>(&mut self, rows: I, active_account: Option<&str>)
    where
        I: IntoIterator<Item = (String, Option<String>)>,
    {
        self.default = rows
            .into_iter()
            .find(|(account, default)| {
                default.is_some() && active_account.is_none_or(|active| account == active)
            })
            .and_then(|(_, default)| default);
    }

    pub fn set_rates(&mut self, rates: BTreeMap<String, String>) {
        self.rates = rates;
    }

    pub fn rates(&self) -> &BTreeMap<String, String> {
        &self.rates
    }

    pub fn set_default(&mut self, default: Option<String>) {
        self.default = default;
    }

    pub fn default_rate(&self) -> Option<&str> {
        self.default.as_deref()
    }

    /// The per-contract rate for a fill: product rate, else the account default.
    pub fn rate_for(&self, product_code: Option<&str>) -> Option<&str> {
        product_code
            .and_then(|code| self.rates.get(code))
            .or(self.default.as_ref())
            .map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Option<String> {
        Some(v.to_string())
    }

    fn table() -> CommissionTable {
        let mut t = CommissionTable::new();
        t.load_products([(s("MNQ"), s("0.52")), (s("NQ"), s("1.4"))]);
        t.load_account_default([("ACT1".to_string(), s("2.05"))], Some("ACT1"));
        t
    }

    #[test]
    fn product_rate_wins_then_account_default_then_none() {
        let t = table();
        assert_eq!(t.rate_for(Some("MNQ")), Some("0.52"));
        assert_eq!(t.rate_for(Some("UNKNOWN")), Some("2.05"));
        assert_eq!(t.rate_for(None), Some("2.05"));
        assert_eq!(CommissionTable::new().rate_for(Some("XYZ")), None);
    }

    #[test]
    fn a_zero_rate_is_a_rate_and_unusable_rows_are_skipped() {
        let mut t = CommissionTable::new();
        t.load_products([
            (s("ZERO"), s("0")),
            (None, s("1")),
            (s(""), s("1")),
            (s("NORATE"), None),
            (s("EMPTYRATE"), s("")),
        ]);
        assert_eq!(t.rate_for(Some("ZERO")), Some("0"));
        assert_eq!(t.rates().len(), 1);
    }

    #[test]
    fn the_default_is_for_the_active_account_only() {
        let rows = || {
            [
                ("OTHER".to_string(), s("9.99")),
                ("ACT1".to_string(), None),
                ("ACT1".to_string(), s("0.25")),
            ]
        };
        let mut t = CommissionTable::new();
        t.load_account_default(rows(), Some("ACT1"));
        assert_eq!(t.default_rate(), Some("0.25"));
        t.load_account_default(rows(), Some("MISSING"));
        assert_eq!(t.default_rate(), None);
        // No active account: the first row that has a default.
        t.load_account_default(rows(), None);
        assert_eq!(t.default_rate(), Some("9.99"));
    }

    #[test]
    fn products_and_default_are_independent() {
        let mut t = table();
        t.load_account_default(Vec::<(String, Option<String>)>::new(), Some("ACT1"));
        assert_eq!(t.default_rate(), None);
        assert_eq!(t.rate_for(Some("MNQ")), Some("0.52"), "products survive");
        t.load_products(Vec::<(Option<String>, Option<String>)>::new());
        assert_eq!(t.rate_for(Some("MNQ")), None);
    }
}
