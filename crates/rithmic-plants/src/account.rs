//! Account resolution helpers (pick from account-list rows).

use rithmic_rs::RithmicAccount;
use rithmic_rs::rti::messages::RithmicMessage;
use rithmic_rs::RithmicResponse;

use crate::error::{Error, Result};

/// One row from `ResponseAccountList`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountRow {
    pub account_id: String,
    pub fcm_id: String,
    pub ib_id: String,
}

impl AccountRow {
    pub fn into_account(self) -> RithmicAccount {
        RithmicAccount::new(self.fcm_id, self.ib_id, self.account_id)
    }
}

/// Parse account-list responses into rows (skips empty / terminal-only frames).
pub fn rows_from_account_list(responses: &[RithmicResponse]) -> Vec<AccountRow> {
    let mut rows = Vec::new();
    for resp in responses {
        let RithmicMessage::ResponseAccountList(list) = &resp.message else {
            continue;
        };
        let Some(account_id) = list.account_id.as_ref().filter(|s| !s.is_empty()) else {
            continue;
        };
        let fcm = list.fcm_id.clone().unwrap_or_default();
        let ib = list.ib_id.clone().unwrap_or_default();
        rows.push(AccountRow {
            account_id: account_id.clone(),
            fcm_id: fcm,
            ib_id: ib,
        });
    }
    rows
}

/// Pick a trading account from list rows + optional `account_id` selector.
///
/// Rules:
/// - `selector` set → must match exactly one `account_id`
/// - else exactly one row → pick it
/// - else error listing account ids
///
/// Row must carry non-empty `fcm_id` and `ib_id` (no login-info fallback).
pub fn pick_account(rows: &[AccountRow], selector: Option<&str>) -> Result<RithmicAccount> {
    if rows.is_empty() {
        return Err(Error::Config(
            "account list empty; set RITHMIC_ACCOUNT_ID/FCM_ID/IB_ID overrides".into(),
        ));
    }

    let chosen = if let Some(sel) = selector.filter(|s| !s.is_empty()) {
        let matches: Vec<_> = rows.iter().filter(|r| r.account_id == sel).collect();
        match matches.as_slice() {
            [one] => (*one).clone(),
            [] => {
                let ids: Vec<_> = rows.iter().map(|r| r.account_id.as_str()).collect();
                return Err(Error::Config(format!(
                    "RITHMIC_ACCOUNT_ID={sel:?} not in account list {ids:?}"
                )));
            }
            _ => {
                return Err(Error::Config(format!(
                    "multiple account list rows for RITHMIC_ACCOUNT_ID={sel:?}"
                )));
            }
        }
    } else if rows.len() == 1 {
        rows[0].clone()
    } else {
        let ids: Vec<_> = rows.iter().map(|r| r.account_id.as_str()).collect();
        return Err(Error::Config(format!(
            "multiple accounts {ids:?}; set RITHMIC_ACCOUNT_ID as selector"
        )));
    };

    if chosen.fcm_id.is_empty() || chosen.ib_id.is_empty() || chosen.account_id.is_empty() {
        return Err(Error::Config(
            "resolved account incomplete (need fcm_id/ib_id/account_id); set RITHMIC_* overrides"
                .into(),
        ));
    }
    Ok(RithmicAccount::new(
        chosen.fcm_id,
        chosen.ib_id,
        chosen.account_id,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, fcm: &str, ib: &str) -> AccountRow {
        AccountRow {
            account_id: id.into(),
            fcm_id: fcm.into(),
            ib_id: ib.into(),
        }
    }

    #[test]
    fn picks_single_account() {
        let acct = pick_account(&[row("A1", "F1", "I1")], None).unwrap();
        assert_eq!(acct.account_id, "A1");
        assert_eq!(acct.fcm_id, "F1");
        assert_eq!(acct.ib_id, "I1");
    }

    #[test]
    fn multi_without_selector_errors() {
        let err = pick_account(&[row("A1", "F1", "I1"), row("A2", "F1", "I1")], None).unwrap_err();
        assert!(err.to_string().contains("multiple accounts"));
    }

    #[test]
    fn selector_match() {
        let acct = pick_account(
            &[row("A1", "F1", "I1"), row("A2", "F2", "I2")],
            Some("A2"),
        )
        .unwrap();
        assert_eq!(acct.account_id, "A2");
        assert_eq!(acct.fcm_id, "F2");
    }

    #[test]
    fn selector_miss_errors() {
        let err = pick_account(&[row("A1", "F1", "I1")], Some("NOPE")).unwrap_err();
        assert!(err.to_string().contains("not in account list"));
    }

    #[test]
    fn incomplete_row_errors() {
        let err = pick_account(&[row("A1", "", "I1")], None).unwrap_err();
        assert!(err.to_string().contains("incomplete"));
    }
}
