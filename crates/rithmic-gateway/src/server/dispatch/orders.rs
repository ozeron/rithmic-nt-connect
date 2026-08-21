//! Trading one-shot commands (place / cancel / modify / brackets / ensure /
//! cancel-all) and order-handle loads (recon + RMS). One-shots keep their
//! inline gate → session call → error-map shape: each has a distinct gate and
//! error code, and the post-send `plant_err_frame` unknown-vs-failed mapping
//! is the honesty boundary.

use rithmic_plants::session::{load_account_rms_info_on, load_orders_on, load_product_rms_info_on};
use rithmic_rs::plants::order_plant::RithmicOrderPlantHandle;

use crate::convert::{account_rms_info_to_pb, order_notification_to_pb, product_rms_info_to_pb};
use crate::pb::{self, frame::Body, Frame};

use super::{
    ack_frame, err_to_frame, error_frame, no_session_frame, plant_err_frame, GatewayState,
};

/// Outcome of the order-handle prelude. A dedicated enum (not
/// `Result<_, Frame>`) keeps the error path small: `pb::Frame` embeds large
/// response bodies, which clippy rightly refuses as a `Result` `Err` type.
enum OrderHandleOutcome {
    Handle(RithmicOrderPlantHandle),
    Fail(Box<Frame>),
}

/// Ensure the order plant exists and clone a detached handle for bounded
/// drains, mapping either failure to `err_code`. Callers run their load
/// without holding the session lock so the event pump keeps forwarding.
async fn order_handle_or_frame(
    state: &GatewayState,
    request_id: u64,
    err_code: &str,
) -> OrderHandleOutcome {
    let Some(session) = &state.session else {
        return OrderHandleOutcome::Fail(Box::new(no_session_frame(request_id)));
    };
    let mut guard = session.lock().await;
    match guard.ensure_order_plant().await {
        Ok(()) => {}
        Err(e) => return OrderHandleOutcome::Fail(Box::new(err_to_frame(request_id, err_code, e))),
    }
    match guard.clone_order_handle() {
        Ok(handle) => OrderHandleOutcome::Handle(handle),
        Err(e) => OrderHandleOutcome::Fail(Box::new(err_to_frame(request_id, err_code, e))),
    }
}

pub(super) async fn load_orders(state: &GatewayState, request_id: u64) -> Frame {
    // Order-plant login is a trading capability (KTD12): gate the same as
    // EnsureOrder / SubscribeOrderUpdates so `RITHMIC_ENABLE_TRADING=0`
    // cannot be bypassed via the recon RPC. The window is intentionally
    // ignored: only the current working set is requested.
    if !state.gates.trading_enabled {
        return error_frame(
            request_id,
            "trading_disabled",
            "load_orders denied: parent trading disabled",
        );
    }
    // Serialize the whole show_orders + drain: concurrent drains on the
    // shared order-updates channel would observe each other's replays
    // and return mixed/duplicated snapshots. This lock is separate from
    // the session lock so the drain never blocks the event pump.
    let _recon_guard = state.recon_lock.lock().await;
    let handle = match order_handle_or_frame(state, request_id, "load_orders_failed").await {
        OrderHandleOutcome::Handle(handle) => handle,
        OrderHandleOutcome::Fail(frame) => return *frame,
    };
    match load_orders_on(&handle).await {
        Ok(events) => Frame {
            request_id,
            body: Some(Body::LoadOrdersResponse(pb::LoadOrdersResponse {
                events: events.into_iter().map(order_notification_to_pb).collect(),
            })),
        },
        Err(rithmic_plants::Error::ReconciliationUnavailable(msg)) => {
            error_frame(request_id, "reconciliation_unavailable", &msg)
        }
        Err(e) => err_to_frame(request_id, "load_orders_failed", e),
    }
}
/// Which risk-config table to load; both variants share gate, handle prelude,
/// error code, and frame shape.
#[derive(Debug, Clone, Copy)]
enum RmsKind {
    Product,
    Account,
}

impl RmsKind {
    fn denied_msg(self) -> &'static str {
        match self {
            Self::Product => "load_product_rms_info denied: parent trading disabled",
            Self::Account => "load_account_rms_info denied: parent trading disabled",
        }
    }
}

pub(super) async fn load_product_rms_info(state: &GatewayState, request_id: u64) -> Frame {
    load_rms_info(state, request_id, RmsKind::Product).await
}

pub(super) async fn load_account_rms_info(state: &GatewayState, request_id: u64) -> Frame {
    load_rms_info(state, request_id, RmsKind::Account).await
}

// Order-plant login is a trading capability (KTD12): gate the same as
// LoadOrders / EnsureOrder so `RITHMIC_ENABLE_TRADING=0` cannot bypass it.
// The query itself is read-only (risk config).
async fn load_rms_info(state: &GatewayState, request_id: u64, kind: RmsKind) -> Frame {
    if !state.gates.trading_enabled {
        return error_frame(request_id, "trading_disabled", kind.denied_msg());
    }
    let handle = match order_handle_or_frame(state, request_id, "fetch_rms_failed").await {
        OrderHandleOutcome::Handle(handle) => handle,
        OrderHandleOutcome::Fail(frame) => return *frame,
    };
    let outcome: rithmic_plants::Result<Body> = match kind {
        RmsKind::Product => load_product_rms_info_on(&handle).await.map(|rows| {
            Body::LoadProductRmsInfoResponse(pb::LoadProductRmsInfoResponse {
                rows: rows.into_iter().map(product_rms_info_to_pb).collect(),
            })
        }),
        RmsKind::Account => load_account_rms_info_on(&handle).await.map(|rows| {
            Body::LoadAccountRmsInfoResponse(pb::LoadAccountRmsInfoResponse {
                rows: rows.into_iter().map(account_rms_info_to_pb).collect(),
            })
        }),
    };
    match outcome {
        Ok(body) => Frame {
            request_id,
            body: Some(body),
        },
        Err(e) => err_to_frame(request_id, "fetch_rms_failed", e),
    }
}
pub(super) async fn ensure_pnl(state: &GatewayState, request_id: u64) -> Frame {
    let Some(session) = &state.session else {
        return no_session_frame(request_id);
    };
    let mut guard = session.lock().await;
    if let Err(e) = guard.ensure_pnl_plant().await {
        return err_to_frame(request_id, "ensure_pnl_failed", e);
    }
    ack_frame(request_id)
}

pub(super) async fn ensure_order(state: &GatewayState, request_id: u64) -> Frame {
    if !state.gates.trading_enabled {
        return error_frame(
            request_id,
            "trading_disabled",
            "ensure_order denied: parent trading disabled",
        );
    }
    let Some(session) = &state.session else {
        return no_session_frame(request_id);
    };
    let mut guard = session.lock().await;
    if let Err(e) = guard.ensure_order_plant().await {
        return err_to_frame(request_id, "ensure_order_failed", e);
    }
    ack_frame(request_id)
}

pub(super) async fn place_order(
    state: &GatewayState,
    request_id: u64,
    req: pb::PlaceOrderRequest,
) -> Frame {
    if !state.gates.allow_place() {
        return error_frame(
            request_id,
            "trading_disabled",
            "place_order denied: parent trading disabled",
        );
    }
    let Some(session) = &state.session else {
        return no_session_frame(request_id);
    };
    let mut guard = session.lock().await;
    match guard
        .place_order(
            &req.symbol,
            &req.exchange,
            &req.side,
            &req.price_type,
            req.quantity,
            &req.user_tag,
            req.price,
            req.trigger_price,
            &req.duration,
            req.trail_by_ticks,
            req.trail_by_price_id,
        )
        .await
    {
        Ok(()) => ack_frame(request_id),
        Err(e) => plant_err_frame(request_id, "place_failed", e),
    }
}

pub(super) async fn place_bracket_order(
    state: &GatewayState,
    request_id: u64,
    req: pb::PlaceBracketOrderRequest,
) -> Frame {
    if !state.gates.allow_place() {
        return error_frame(
            request_id,
            "trading_disabled",
            "place_bracket_order denied: parent trading disabled",
        );
    }
    let Some(session) = &state.session else {
        return no_session_frame(request_id);
    };
    let mut guard = session.lock().await;
    match guard
        .place_bracket_order(
            &req.symbol,
            &req.exchange,
            &req.side,
            &req.price_type,
            req.quantity,
            &req.localid,
            req.price,
            req.trigger_price,
            &req.duration,
            req.stop_ticks,
            req.target_ticks,
        )
        .await
    {
        Ok(()) => ack_frame(request_id),
        Err(e) => plant_err_frame(request_id, "place_bracket_failed", e),
    }
}

pub(super) async fn adjust_bracket_stop(
    state: &GatewayState,
    request_id: u64,
    req: pb::AdjustBracketStopRequest,
) -> Frame {
    if !state.gates.allow_place() {
        return error_frame(
            request_id,
            "trading_disabled",
            "adjust_bracket_stop denied: parent trading disabled",
        );
    }
    let Some(session) = &state.session else {
        return no_session_frame(request_id);
    };
    let mut guard = session.lock().await;
    match guard
        .adjust_bracket_stop(&req.basket_id, req.ticks, req.level)
        .await
    {
        Ok(()) => ack_frame(request_id),
        Err(e) => plant_err_frame(request_id, "adjust_bracket_stop_failed", e),
    }
}

pub(super) async fn adjust_bracket_target(
    state: &GatewayState,
    request_id: u64,
    req: pb::AdjustBracketTargetRequest,
) -> Frame {
    if !state.gates.allow_place() {
        return error_frame(
            request_id,
            "trading_disabled",
            "adjust_bracket_target denied: parent trading disabled",
        );
    }
    let Some(session) = &state.session else {
        return no_session_frame(request_id);
    };
    let mut guard = session.lock().await;
    match guard
        .adjust_bracket_target(&req.basket_id, req.ticks, req.level)
        .await
    {
        Ok(()) => ack_frame(request_id),
        Err(e) => plant_err_frame(request_id, "adjust_bracket_target_failed", e),
    }
}

pub(super) async fn cancel_order(
    state: &GatewayState,
    request_id: u64,
    req: pb::CancelOrderRequest,
) -> Frame {
    if !state.gates.allow_place() {
        return error_frame(
            request_id,
            "trading_disabled",
            "cancel_order denied: parent trading disabled",
        );
    }
    let Some(session) = &state.session else {
        return no_session_frame(request_id);
    };
    let mut guard = session.lock().await;
    match guard.cancel_order(&req.basket_id).await {
        Ok(()) => ack_frame(request_id),
        Err(e) => plant_err_frame(request_id, "cancel_failed", e),
    }
}

pub(super) async fn modify_order(
    state: &GatewayState,
    request_id: u64,
    req: pb::ModifyOrderRequest,
) -> Frame {
    if !state.gates.allow_place() {
        return error_frame(
            request_id,
            "trading_disabled",
            "modify_order denied: parent trading disabled",
        );
    }
    let Some(session) = &state.session else {
        return no_session_frame(request_id);
    };
    let mut guard = session.lock().await;
    match guard
        .modify_order(
            &req.basket_id,
            &req.symbol,
            &req.exchange,
            req.quantity,
            &req.price_type,
            req.price,
            req.trigger_price,
            req.trail_by_ticks,
        )
        .await
    {
        Ok(()) => ack_frame(request_id),
        Err(e) => plant_err_frame(request_id, "modify_failed", e),
    }
}

pub(super) async fn cancel_all_orders(state: &GatewayState, request_id: u64) -> Frame {
    if !state.gates.allow_cancel_all() {
        return error_frame(
            request_id,
            "cancel_all_denied",
            "cancel_all_orders denied: parent cancel_all disabled",
        );
    }
    let Some(session) = &state.session else {
        return no_session_frame(request_id);
    };
    let mut guard = session.lock().await;
    match guard.cancel_all_orders().await {
        Ok(()) => ack_frame(request_id),
        Err(e) => plant_err_frame(request_id, "cancel_all_failed", e),
    }
}
