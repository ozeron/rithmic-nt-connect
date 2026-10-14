//! The RPC table: one row per gateway request and per `Session` trait method.
//!
//! This is the single place that classifies each RPC (kind, gate, restore) so
//! the trait, the proto, and the server dispatch cannot drift apart. The
//! integration tests in `tests/rpc_table.rs` check, against this table, that:
//!
//! - every `session.proto` request variant and every `Session` trait method has a row,
//! - every subscription intent maps to a `RestorePlan` field and is re-issued by
//!   `restore_intents`,
//! - every trading-gated row is denied with trading off, and cancel-all has its
//!   own independent gate.
//!
//! RPC kinds (AGENTS.md "gateway RPC kinds"): a [`Kind::OneShot`] applies its
//! gate, calls the plant, and maps the error — no fan-out, no intent
//! bookkeeping, no restore. A [`Kind::SubscriptionIntent`] attaches fan-out,
//! notes a refcounted intent, calls the plant only on the 0→1 transition, rolls
//! back on failure, and is re-issued after a plant reconnect. A
//! [`Kind::IntentRelease`] is the matching unsubscribe/disconnect: it releases
//! the refcount and leaves the venue only on the last peer.

use crate::pb::frame::Body;

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

/// How a subscription intent is restored after a plant reconnect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Restore {
    /// Field of `reconnect::RestorePlan` that remembers this intent.
    pub plan_field: &'static str,
    /// The `Session` call `restore_intents` issues for it (as recorded by `FakeSession`).
    pub call: &'static str,
}

/// One row of the table.
#[derive(Clone, Copy)]
pub struct Rpc {
    /// `Session` trait method this RPC drives; `None` for server-local requests.
    pub method: Option<&'static str>,
    /// `session.proto` `Frame` oneof field; `None` for trait methods that are not RPCs.
    pub body: Option<&'static str>,
    /// A default request body, for exercising the gate.
    pub make_body: Option<fn() -> Body>,
    pub kind: Kind,
    pub gate: Gate,
    /// Set for (and only for) [`Kind::SubscriptionIntent`].
    pub restore: Option<Restore>,
}

const fn rpc(
    method: Option<&'static str>,
    body: &'static str,
    make_body: fn() -> Body,
    kind: Kind,
    gate: Gate,
) -> Rpc {
    Rpc {
        method,
        body: Some(body),
        make_body: Some(make_body),
        kind,
        gate,
        restore: None,
    }
}

const fn intent(
    method: &'static str,
    body: &'static str,
    make_body: fn() -> Body,
    gate: Gate,
    plan_field: &'static str,
    call: &'static str,
) -> Rpc {
    Rpc {
        method: Some(method),
        body: Some(body),
        make_body: Some(make_body),
        kind: Kind::SubscriptionIntent,
        gate,
        restore: Some(Restore { plan_field, call }),
    }
}

/// A `Session` trait method that is not a gateway RPC (lifecycle / event polling).
const fn local(method: &'static str) -> Rpc {
    Rpc {
        method: Some(method),
        body: None,
        make_body: None,
        kind: Kind::OneShot,
        gate: Gate::None,
        restore: None,
    }
}

use Gate::{CancelAll, None as NoGate, Trading};
use Kind::{IntentRelease, OneShot};

/// Every gateway request and every `Session` trait method.
pub const RPC_TABLE: &[Rpc] = &[
    // --- server-local requests (no plant call) ---------------------------
    rpc(
        None,
        "handshake",
        || Body::Handshake(Default::default()),
        OneShot,
        NoGate,
    ),
    rpc(
        None,
        "disconnect",
        || Body::Disconnect(Default::default()),
        OneShot,
        NoGate,
    ),
    rpc(
        None,
        "get_live_md_state",
        || Body::GetLiveMdState(Default::default()),
        OneShot,
        NoGate,
    ),
    // --- subscription intents --------------------------------------------
    intent(
        "subscribe",
        "subscribe",
        || Body::Subscribe(Default::default()),
        NoGate,
        "ticker",
        "subscribe",
    ),
    intent(
        "subscribe_order_book_summary",
        "subscribe_book",
        || Body::SubscribeBook(Default::default()),
        NoGate,
        "book",
        "subscribe_book",
    ),
    intent(
        "subscribe_time_bars",
        "subscribe_time_bars",
        || Body::SubscribeTimeBars(Default::default()),
        NoGate,
        "time_bars",
        "subscribe_time_bars",
    ),
    intent(
        "subscribe_pnl",
        "subscribe_pnl",
        || Body::SubscribePnl(Default::default()),
        NoGate,
        "pnl",
        "subscribe_pnl",
    ),
    // Order updates and bracket updates ride the order-plant stream.
    intent(
        "subscribe_order_updates",
        "subscribe_order_updates",
        || Body::SubscribeOrderUpdates(Default::default()),
        Trading,
        "order",
        "subscribe_order_updates",
    ),
    intent(
        "subscribe_bracket_updates",
        "subscribe_bracket_updates",
        || Body::SubscribeBracketUpdates(Default::default()),
        Trading,
        "brackets",
        "subscribe_bracket_updates",
    ),
    // --- intent releases ---------------------------------------------------
    rpc(
        Some("unsubscribe"),
        "unsubscribe",
        || Body::Unsubscribe(Default::default()),
        IntentRelease,
        NoGate,
    ),
    rpc(
        Some("unsubscribe_order_book_summary"),
        "unsubscribe_book",
        || Body::UnsubscribeBook(Default::default()),
        IntentRelease,
        NoGate,
    ),
    rpc(
        Some("unsubscribe_time_bars"),
        "unsubscribe_time_bars",
        || Body::UnsubscribeTimeBars(Default::default()),
        IntentRelease,
        NoGate,
    ),
    rpc(
        Some("disconnect_pnl_plant"),
        "disconnect_pnl",
        || Body::DisconnectPnl(Default::default()),
        IntentRelease,
        NoGate,
    ),
    rpc(
        Some("disconnect_order_plant"),
        "disconnect_order",
        || Body::DisconnectOrder(Default::default()),
        IntentRelease,
        NoGate,
    ),
    // --- plant lifecycle one-shots ------------------------------------------
    rpc(
        Some("request_plants"),
        "request_plants",
        || Body::RequestPlants(Default::default()),
        OneShot,
        NoGate,
    ),
    rpc(
        Some("ensure_pnl_plant"),
        "ensure_pnl",
        || Body::EnsurePnl(Default::default()),
        OneShot,
        NoGate,
    ),
    rpc(
        Some("ensure_order_plant"),
        "ensure_order",
        || Body::EnsureOrder(Default::default()),
        OneShot,
        Trading,
    ),
    rpc(
        Some("reset_ticker"),
        "reset_ticker_plant",
        || Body::ResetTickerPlant(Default::default()),
        OneShot,
        NoGate,
    ),
    // --- reads -----------------------------------------------------------
    rpc(
        Some("get_front_month"),
        "get_front_month",
        || Body::GetFrontMonth(Default::default()),
        OneShot,
        NoGate,
    ),
    rpc(
        Some("get_reference_data"),
        "get_reference_data",
        || Body::GetReferenceData(Default::default()),
        OneShot,
        NoGate,
    ),
    rpc(
        Some("resolved_account"),
        "resolved_account",
        || Body::ResolvedAccount(Default::default()),
        OneShot,
        NoGate,
    ),
    rpc(
        Some("load_ticks"),
        "load_ticks",
        || Body::LoadTicks(Default::default()),
        OneShot,
        NoGate,
    ),
    rpc(
        Some("load_time_bars"),
        "load_time_bars",
        || Body::LoadTimeBars(Default::default()),
        OneShot,
        NoGate,
    ),
    rpc(
        Some("probe_time_bars"),
        "probe_time_bars",
        || Body::ProbeTimeBars(Default::default()),
        OneShot,
        NoGate,
    ),
    rpc(
        Some("load_orders"),
        "load_orders",
        || Body::LoadOrders(Default::default()),
        OneShot,
        Trading,
    ),
    rpc(
        Some("load_product_rms_info"),
        "load_product_rms_info",
        || Body::LoadProductRmsInfo(Default::default()),
        OneShot,
        Trading,
    ),
    rpc(
        Some("load_account_rms_info"),
        "load_account_rms_info",
        || Body::LoadAccountRmsInfo(Default::default()),
        OneShot,
        Trading,
    ),
    rpc(
        Some("load_fill_history"),
        "load_fill_history",
        || Body::LoadFillHistory(Default::default()),
        OneShot,
        Trading,
    ),
    rpc(
        Some("load_order_history_dates"),
        "load_order_history_dates",
        || Body::LoadOrderHistoryDates(Default::default()),
        OneShot,
        Trading,
    ),
    rpc(
        Some("load_order_history"),
        "load_order_history",
        || Body::LoadOrderHistory(Default::default()),
        OneShot,
        Trading,
    ),
    // --- one-shot trading commands (never retried; no intent, no restore) ---
    rpc(
        Some("place_order"),
        "place_order",
        || Body::PlaceOrder(Default::default()),
        OneShot,
        Trading,
    ),
    rpc(
        Some("place_bracket_order"),
        "place_bracket_order",
        || Body::PlaceBracketOrder(Default::default()),
        OneShot,
        Trading,
    ),
    rpc(
        Some("adjust_bracket_stop"),
        "adjust_bracket_stop",
        || Body::AdjustBracketStop(Default::default()),
        OneShot,
        Trading,
    ),
    rpc(
        Some("adjust_bracket_target"),
        "adjust_bracket_target",
        || Body::AdjustBracketTarget(Default::default()),
        OneShot,
        Trading,
    ),
    rpc(
        Some("cancel_order"),
        "cancel_order",
        || Body::CancelOrder(Default::default()),
        OneShot,
        Trading,
    ),
    rpc(
        Some("modify_order"),
        "modify_order",
        || Body::ModifyOrder(Default::default()),
        OneShot,
        Trading,
    ),
    rpc(
        Some("cancel_all_orders"),
        "cancel_all_orders",
        || Body::CancelAllOrders(Default::default()),
        OneShot,
        CancelAll,
    ),
    // --- trait methods that are not RPCs ---------------------------------
    local("connect"),
    local("disconnect"),
    local("poll_event"),
    local("poll_history_event"),
    local("poll_pnl_event"),
    local("poll_order_event"),
];

/// Row for a proto request field name.
pub fn by_body(body: &str) -> Option<&'static Rpc> {
    RPC_TABLE.iter().find(|r| r.body == Some(body))
}

/// Row for a `Session` trait method name.
pub fn by_method(method: &str) -> Option<&'static Rpc> {
    RPC_TABLE.iter().find(|r| r.method == Some(method))
}
