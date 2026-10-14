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
