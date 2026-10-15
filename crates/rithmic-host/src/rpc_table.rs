//! Classification of the Session contract; transport mappings live in rithmic-gateway.
/// How an RPC treats subscription intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    OneShot,
    SubscriptionIntent,
    IntentRelease,
}

/// Which parent gate guards an RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    /// No gate.
    None,
    /// `RITHMIC_ENABLE_TRADING`.
    Trading,
    /// `RITHMIC_GATEWAY_CANCEL_ALL`, independent of trading.
    CancelAll,
}

/// Which lane of a connection serves an RPC.
///
/// A connection reads requests on its own task and serves them on two lanes so a
/// slow read can never hold up a command: the **command** lane runs state-changing
/// and subscription requests one at a time, in arrival order; the **slow** lane
/// runs read RPCs that can take seconds (history and order/fill history loads,
/// and the reads that wait on the plant session: reference data, front month,
/// resolved account) concurrently, a few at a time, and replies when each
/// finishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    Command,
    Slow,
}

/// How a subscription intent is restored after a plant reconnect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Restore {
    /// Field of `reconnect::RestorePlan` that remembers this intent.
    pub plan_field: &'static str,
    /// The `Session` call `restore_intents` issues for it (as recorded by `FakeVenue`).
    pub call: &'static str,
}

#[derive(Clone, Copy)]
pub struct Rpc {
    pub method: &'static str,
    pub kind: Kind,
    pub gate: Gate,
    pub lane: Lane,
    pub restore: Option<Restore>,
}

pub const RPC_TABLE: &[Rpc] = &[
    Rpc {
        method: "subscribe",
        kind: Kind::SubscriptionIntent,
        gate: Gate::None,
        lane: Lane::Command,
        restore: Some(Restore {
            plan_field: "ticker",
            call: "subscribe",
        }),
    },
    Rpc {
        method: "subscribe_order_book_summary",
        kind: Kind::SubscriptionIntent,
        gate: Gate::None,
        lane: Lane::Command,
        restore: Some(Restore {
            plan_field: "book",
            call: "subscribe_book",
        }),
    },
    Rpc {
        method: "subscribe_time_bars",
        kind: Kind::SubscriptionIntent,
        gate: Gate::None,
        lane: Lane::Command,
        restore: Some(Restore {
            plan_field: "time_bars",
            call: "subscribe_time_bars",
        }),
    },
    Rpc {
        method: "subscribe_pnl",
        kind: Kind::SubscriptionIntent,
        gate: Gate::None,
        lane: Lane::Command,
        restore: Some(Restore {
            plan_field: "pnl",
            call: "subscribe_pnl",
        }),
    },
    Rpc {
        method: "subscribe_order_updates",
        kind: Kind::SubscriptionIntent,
        gate: Gate::Trading,
        lane: Lane::Command,
        restore: Some(Restore {
            plan_field: "order",
            call: "subscribe_order_updates",
        }),
    },
    Rpc {
        method: "subscribe_bracket_updates",
        kind: Kind::SubscriptionIntent,
        gate: Gate::Trading,
        lane: Lane::Command,
        restore: Some(Restore {
            plan_field: "brackets",
            call: "subscribe_bracket_updates",
        }),
    },
    Rpc {
        method: "unsubscribe",
        kind: Kind::IntentRelease,
        gate: Gate::None,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "unsubscribe_order_book_summary",
        kind: Kind::IntentRelease,
        gate: Gate::None,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "unsubscribe_time_bars",
        kind: Kind::IntentRelease,
        gate: Gate::None,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "disconnect_pnl_plant",
        kind: Kind::IntentRelease,
        gate: Gate::None,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "disconnect_order_plant",
        kind: Kind::IntentRelease,
        gate: Gate::None,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "request_plants",
        kind: Kind::OneShot,
        gate: Gate::None,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "ensure_pnl_plant",
        kind: Kind::OneShot,
        gate: Gate::None,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "ensure_order_plant",
        kind: Kind::OneShot,
        gate: Gate::Trading,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "open_gates",
        kind: Kind::OneShot,
        gate: Gate::None,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "get_front_month",
        kind: Kind::OneShot,
        gate: Gate::None,
        lane: Lane::Slow,
        restore: None,
    },
    Rpc {
        method: "get_reference_data",
        kind: Kind::OneShot,
        gate: Gate::None,
        lane: Lane::Slow,
        restore: None,
    },
    Rpc {
        method: "resolved_account",
        kind: Kind::OneShot,
        gate: Gate::None,
        lane: Lane::Slow,
        restore: None,
    },
    Rpc {
        method: "load_ticks",
        kind: Kind::OneShot,
        gate: Gate::None,
        lane: Lane::Slow,
        restore: None,
    },
    Rpc {
        method: "load_time_bars",
        kind: Kind::OneShot,
        gate: Gate::None,
        lane: Lane::Slow,
        restore: None,
    },
    Rpc {
        method: "probe_time_bars",
        kind: Kind::OneShot,
        gate: Gate::None,
        lane: Lane::Slow,
        restore: None,
    },
    Rpc {
        method: "load_orders",
        kind: Kind::OneShot,
        gate: Gate::Trading,
        lane: Lane::Slow,
        restore: None,
    },
    Rpc {
        method: "load_product_rms_info",
        kind: Kind::OneShot,
        gate: Gate::Trading,
        lane: Lane::Slow,
        restore: None,
    },
    Rpc {
        method: "load_account_rms_info",
        kind: Kind::OneShot,
        gate: Gate::Trading,
        lane: Lane::Slow,
        restore: None,
    },
    Rpc {
        method: "load_fill_history",
        kind: Kind::OneShot,
        gate: Gate::Trading,
        lane: Lane::Slow,
        restore: None,
    },
    Rpc {
        method: "load_order_history_dates",
        kind: Kind::OneShot,
        gate: Gate::Trading,
        lane: Lane::Slow,
        restore: None,
    },
    Rpc {
        method: "load_order_history",
        kind: Kind::OneShot,
        gate: Gate::Trading,
        lane: Lane::Slow,
        restore: None,
    },
    Rpc {
        method: "place_order",
        kind: Kind::OneShot,
        gate: Gate::Trading,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "place_bracket_order",
        kind: Kind::OneShot,
        gate: Gate::Trading,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "adjust_bracket_stop",
        kind: Kind::OneShot,
        gate: Gate::Trading,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "adjust_bracket_target",
        kind: Kind::OneShot,
        gate: Gate::Trading,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "cancel_order",
        kind: Kind::OneShot,
        gate: Gate::Trading,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "modify_order",
        kind: Kind::OneShot,
        gate: Gate::Trading,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "cancel_all_orders",
        kind: Kind::OneShot,
        gate: Gate::CancelAll,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "connect",
        kind: Kind::OneShot,
        gate: Gate::None,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "disconnect",
        kind: Kind::OneShot,
        gate: Gate::None,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "poll_event",
        kind: Kind::OneShot,
        gate: Gate::None,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "poll_history_event",
        kind: Kind::OneShot,
        gate: Gate::None,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "poll_pnl_event",
        kind: Kind::OneShot,
        gate: Gate::None,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "poll_order_event",
        kind: Kind::OneShot,
        gate: Gate::None,
        lane: Lane::Command,
        restore: None,
    },
    Rpc {
        method: "next_events",
        kind: Kind::OneShot,
        gate: Gate::None,
        lane: Lane::Command,
        restore: None,
    },
];

pub fn by_method(method: &str) -> Option<&'static Rpc> {
    RPC_TABLE.iter().find(|row| row.method == method)
}
