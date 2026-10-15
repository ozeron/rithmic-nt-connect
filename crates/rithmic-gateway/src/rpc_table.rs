//! Protobuf request variants mapped to the host's Session RPC classification.
use crate::pb::frame::Body;
pub use rithmic_host::rpc_table::{Gate, Kind, Lane, Restore};
use std::collections::HashMap;
use std::mem::{discriminant, Discriminant};
use std::sync::OnceLock;

/// Transport mapping only: method classification belongs to the host.
#[derive(Clone, Copy)]
pub struct Rpc {
    pub method: Option<&'static str>,
    pub body: Option<&'static str>,
    pub make_body: Option<fn() -> Body>,
}

impl std::ops::Deref for Rpc {
    type Target = rithmic_host::rpc_table::Rpc;
    fn deref(&self) -> &Self::Target {
        static LOCAL: rithmic_host::rpc_table::Rpc = rithmic_host::rpc_table::Rpc {
            method: "",
            kind: Kind::OneShot,
            gate: Gate::None,
            lane: Lane::Command,
            restore: None,
        };
        match self.method {
            Some(method) => {
                rithmic_host::rpc_table::by_method(method).expect("mapped Session method")
            }
            None => &LOCAL,
        }
    }
}

const fn rpc(method: Option<&'static str>, body: &'static str, make_body: fn() -> Body) -> Rpc {
    Rpc {
        method,
        body: Some(body),
        make_body: Some(make_body),
    }
}

pub const RPC_TABLE: &[Rpc] = &[
    rpc(None, "handshake", || Body::Handshake(Default::default())),
    rpc(None, "disconnect", || Body::Disconnect(Default::default())),
    rpc(None, "get_live_md_state", || {
        Body::GetLiveMdState(Default::default())
    }),
    rpc(Some("subscribe"), "subscribe", || {
        Body::Subscribe(Default::default())
    }),
    rpc(
        Some("subscribe_order_book_summary"),
        "subscribe_book",
        || Body::SubscribeBook(Default::default()),
    ),
    rpc(Some("subscribe_time_bars"), "subscribe_time_bars", || {
        Body::SubscribeTimeBars(Default::default())
    }),
    rpc(Some("subscribe_pnl"), "subscribe_pnl", || {
        Body::SubscribePnl(Default::default())
    }),
    rpc(
        Some("subscribe_order_updates"),
        "subscribe_order_updates",
        || Body::SubscribeOrderUpdates(Default::default()),
    ),
    rpc(
        Some("subscribe_bracket_updates"),
        "subscribe_bracket_updates",
        || Body::SubscribeBracketUpdates(Default::default()),
    ),
    rpc(Some("unsubscribe"), "unsubscribe", || {
        Body::Unsubscribe(Default::default())
    }),
    rpc(
        Some("unsubscribe_order_book_summary"),
        "unsubscribe_book",
        || Body::UnsubscribeBook(Default::default()),
    ),
    rpc(
        Some("unsubscribe_time_bars"),
        "unsubscribe_time_bars",
        || Body::UnsubscribeTimeBars(Default::default()),
    ),
    rpc(Some("disconnect_pnl_plant"), "disconnect_pnl", || {
        Body::DisconnectPnl(Default::default())
    }),
    rpc(Some("disconnect_order_plant"), "disconnect_order", || {
        Body::DisconnectOrder(Default::default())
    }),
    rpc(Some("request_plants"), "request_plants", || {
        Body::RequestPlants(Default::default())
    }),
    rpc(Some("ensure_pnl_plant"), "ensure_pnl", || {
        Body::EnsurePnl(Default::default())
    }),
    rpc(Some("ensure_order_plant"), "ensure_order", || {
        Body::EnsureOrder(Default::default())
    }),
    rpc(None, "reset_ticker_plant", || {
        Body::ResetTickerPlant(Default::default())
    }),
    rpc(Some("get_front_month"), "get_front_month", || {
        Body::GetFrontMonth(Default::default())
    }),
    rpc(Some("get_reference_data"), "get_reference_data", || {
        Body::GetReferenceData(Default::default())
    }),
    rpc(Some("resolved_account"), "resolved_account", || {
        Body::ResolvedAccount(Default::default())
    }),
    rpc(Some("load_ticks"), "load_ticks", || {
        Body::LoadTicks(Default::default())
    }),
    rpc(Some("load_time_bars"), "load_time_bars", || {
        Body::LoadTimeBars(Default::default())
    }),
    rpc(Some("probe_time_bars"), "probe_time_bars", || {
        Body::ProbeTimeBars(Default::default())
    }),
    rpc(Some("load_orders"), "load_orders", || {
        Body::LoadOrders(Default::default())
    }),
    rpc(
        Some("load_product_rms_info"),
        "load_product_rms_info",
        || Body::LoadProductRmsInfo(Default::default()),
    ),
    rpc(
        Some("load_account_rms_info"),
        "load_account_rms_info",
        || Body::LoadAccountRmsInfo(Default::default()),
    ),
    rpc(Some("load_fill_history"), "load_fill_history", || {
        Body::LoadFillHistory(Default::default())
    }),
    rpc(
        Some("load_order_history_dates"),
        "load_order_history_dates",
        || Body::LoadOrderHistoryDates(Default::default()),
    ),
    rpc(Some("load_order_history"), "load_order_history", || {
        Body::LoadOrderHistory(Default::default())
    }),
    rpc(Some("place_order"), "place_order", || {
        Body::PlaceOrder(Default::default())
    }),
    rpc(Some("place_bracket_order"), "place_bracket_order", || {
        Body::PlaceBracketOrder(Default::default())
    }),
    rpc(Some("adjust_bracket_stop"), "adjust_bracket_stop", || {
        Body::AdjustBracketStop(Default::default())
    }),
    rpc(
        Some("adjust_bracket_target"),
        "adjust_bracket_target",
        || Body::AdjustBracketTarget(Default::default()),
    ),
    rpc(Some("cancel_order"), "cancel_order", || {
        Body::CancelOrder(Default::default())
    }),
    rpc(Some("modify_order"), "modify_order", || {
        Body::ModifyOrder(Default::default())
    }),
    rpc(Some("cancel_all_orders"), "cancel_all_orders", || {
        Body::CancelAllOrders(Default::default())
    }),
];

pub fn by_body(body: &str) -> Option<&'static Rpc> {
    RPC_TABLE.iter().find(|row| row.body == Some(body))
}

/// Bodies without a request mapping are answered on the command lane.
pub fn lane_of(body: &Body) -> Lane {
    static LANES: OnceLock<HashMap<Discriminant<Body>, Lane>> = OnceLock::new();
    let lanes = LANES.get_or_init(|| {
        RPC_TABLE
            .iter()
            .filter_map(|row| row.make_body.map(|make| (discriminant(&make()), row.lane)))
            .collect()
    });
    lanes
        .get(&discriminant(body))
        .copied()
        .unwrap_or(Lane::Command)
}
