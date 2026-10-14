//! Order-notification classification.

/// Map a plant `notify_type_name` to a canonical action kind.
///
/// `source` is `"rithmic"` or `"exchange"`; anything else, and any name with no
/// action, is `None`. The plant uses this to tag notifications and the adapter
/// uses it for dicts that arrive without a kind, so there is one mapping.
pub fn kind_from_notify(
    source: &str,
    notify_type_name: &str,
    status: Option<&str>,
    text: Option<&str>,
) -> Option<&'static str> {
    let name = notify_type_name.to_uppercase();
    match (source, name.as_str()) {
        ("rithmic", "OPEN") => Some("accepted"),
        // Live-proven (Rithmic Test 2026-08-21): resting STOP_MARKET orders
        // never receive OPEN — TRIGGER_PENDING is their working state, i.e.
        // acceptance evidence (emitted guarded from SUBMITTED).
        ("rithmic", "TRIGGER_PENDING") => Some("accepted"),
        ("rithmic", "MODIFIED") => Some("updated"),
        ("rithmic", "MODIFICATION_FAILED") => Some("modify_rejected"),
        ("rithmic", "CANCELLATION_FAILED") => Some("cancel_rejected"),
        ("rithmic", "COMPLETE") => {
            let status = status.unwrap_or("").to_uppercase();
            if status == "CANCELLED" || status == "CANCELED" {
                Some("canceled")
            } else if text.is_some_and(|t| !t.trim().is_empty()) {
                Some("rejected")
            } else {
                None
            }
        }
        ("exchange", "FILL") => Some("filled"),
        ("exchange", "REJECT") => Some("rejected"),
        ("exchange", "CANCEL") => Some("canceled"),
        ("exchange", "TRIGGER") => Some("triggered"),
        ("exchange", "NOT_MODIFIED") => Some("modify_rejected"),
        ("exchange", "NOT_CANCELLED" | "NOT_CANCELED") => Some("cancel_rejected"),
        _ => None,
    }
}

/// What a tracked order's notification asks of the adapter: the action, and
/// for a refusal the reason shown (the venue's text, else its report text, else
/// its status, else a fixed default). `None` for a kind that asks for nothing.
pub fn notification_action(
    kind: &str,
    text: Option<&str>,
    report_text: Option<&str>,
    status: Option<&str>,
) -> Option<(&'static str, Option<String>)> {
    let refusal = |default: &str| {
        [text, report_text, status]
            .into_iter()
            .flatten()
            .find(|s| !s.is_empty())
            .unwrap_or(default)
            .to_string()
    };
    Some(match kind {
        "accepted" => ("accepted", None),
        "updated" => ("updated", None),
        "canceled" => ("canceled", None),
        "triggered" => ("triggered", None),
        "filled" => ("filled", None),
        "rejected" => ("rejected", Some(refusal("REJECT"))),
        "modify_rejected" => ("modify_rejected", Some(refusal("NOT_MODIFIED"))),
        "cancel_rejected" => ("cancel_rejected", Some(refusal("NOT_CANCELLED"))),
        _ => return None,
    })
}

#[cfg(test)]
mod action_tests {
    use super::notification_action;

    #[test]
    fn refusals_take_the_first_non_empty_reason_then_a_default() {
        assert_eq!(
            notification_action("rejected", Some(""), Some("margin"), Some("x")),
            Some(("rejected", Some("margin".into())))
        );
        assert_eq!(
            notification_action("modify_rejected", None, None, None),
            Some(("modify_rejected", Some("NOT_MODIFIED".into())))
        );
        assert_eq!(
            notification_action("cancel_rejected", None, None, Some("too late")),
            Some(("cancel_rejected", Some("too late".into())))
        );
    }

    #[test]
    fn other_kinds_carry_no_reason_and_unknown_kinds_ask_for_nothing() {
        assert_eq!(
            notification_action("filled", Some("t"), None, None),
            Some(("filled", None))
        );
        assert_eq!(notification_action("open", None, None, None), None);
        assert_eq!(notification_action("", None, None, None), None);
    }
}

#[cfg(test)]
mod tests {
    use super::kind_from_notify as kind;

    #[test]
    fn rithmic_names() {
        assert_eq!(kind("rithmic", "OPEN", None, None), Some("accepted"));
        assert_eq!(kind("rithmic", "open", None, None), Some("accepted"));
        assert_eq!(kind("rithmic", "MODIFIED", None, None), Some("updated"));
        assert_eq!(
            kind("rithmic", "MODIFICATION_FAILED", None, None),
            Some("modify_rejected")
        );
        assert_eq!(
            kind("rithmic", "CANCELLATION_FAILED", None, None),
            Some("cancel_rejected")
        );
        assert_eq!(kind("rithmic", "OPEN_PENDING", None, None), None);
    }

    #[test]
    fn trigger_pending_is_stop_acceptance_evidence() {
        assert_eq!(
            kind("rithmic", "TRIGGER_PENDING", None, None),
            Some("accepted")
        );
    }

    #[test]
    fn complete_depends_on_status_then_text() {
        let complete = |status, text| kind("rithmic", "COMPLETE", status, text);
        assert_eq!(complete(Some("cancelled"), None), Some("canceled"));
        assert_eq!(complete(Some("CANCELED"), None), Some("canceled"));
        assert_eq!(
            complete(Some("complete"), Some("No such route exists.")),
            Some("rejected")
        );
        assert_eq!(complete(Some("complete"), None), None);
        assert_eq!(complete(Some("complete"), Some("  ")), None);
        // A cancel status wins over text.
        assert_eq!(
            complete(Some("cancelled"), Some("because")),
            Some("canceled")
        );
    }

    #[test]
    fn exchange_names() {
        for (name, want) in [
            ("FILL", "filled"),
            ("REJECT", "rejected"),
            ("CANCEL", "canceled"),
            ("TRIGGER", "triggered"),
            ("NOT_MODIFIED", "modify_rejected"),
            ("NOT_CANCELLED", "cancel_rejected"),
            ("NOT_CANCELED", "cancel_rejected"),
        ] {
            assert_eq!(kind("exchange", name, None, None), Some(want), "{name}");
        }
    }

    #[test]
    fn a_name_is_only_meaningful_for_its_source() {
        assert_eq!(kind("exchange", "OPEN", None, None), None);
        assert_eq!(kind("rithmic", "FILL", None, None), None);
        assert_eq!(kind("other", "OPEN", None, None), None);
    }
}
