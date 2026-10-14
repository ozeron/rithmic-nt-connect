//! Trading one-shot commands (place / cancel / modify / brackets / ensure /
//! cancel-all) and order-handle loads (recon + RMS). One-shots keep their
//! inline gate → session call → error-map shape: each has a distinct gate and
//! error code, and the post-send `plant_err_frame` unknown-vs-failed mapping
//! is the honesty boundary.

use std::sync::Arc;

use rithmic_plants::{ModifyOrder, PlaceBracketOrder, PlaceOrder, Session};

use crate::convert::{
    account_rms_info_to_pb, fill_history_to_pb, order_notification_to_pb, product_rms_info_to_pb,
};
use crate::pb::{self, frame::Body, Frame};

use super::{
    ack_frame, err_to_frame, error_frame, no_session_frame, plant_err_frame, GatewayState,
};

/// The shared session for a long read, taken *outside* the session gate so the
/// event pump keeps forwarding while the load runs. Waits for the gate once, so
/// a read issued mid-reconnect queues behind it.
async fn checkout_session(
    state: &GatewayState,
    request_id: u64,
) -> Result<Arc<dyn Session>, Box<Frame>> {
    match &state.session {
        Some(session) => Ok(session.checkout().await),
        None => Err(Box::new(no_session_frame(request_id))),
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
    let session = match checkout_session(state, request_id).await {
        Ok(session) => session,
        Err(frame) => return *frame,
    };
    match session.load_orders(0, 0).await {
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
    let session = match checkout_session(state, request_id).await {
        Ok(session) => session,
        Err(frame) => return *frame,
    };
    let outcome: rithmic_plants::Result<Body> = match kind {
        RmsKind::Product => session.load_product_rms_info().await.map(|rows| {
            Body::LoadProductRmsInfoResponse(pb::LoadProductRmsInfoResponse {
                rows: rows.into_iter().map(product_rms_info_to_pb).collect(),
            })
        }),
        RmsKind::Account => session.load_account_rms_info().await.map(|rows| {
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

// History reads are one-shot commands (trading gate → plant call → map error).
// No fanout, no `note_*` refcount, no restore: they are not subscriptions.
// Trading-gated like LoadOrders because they reach the order plant.

/// Map a history-read failure: an unanswerable reconciliation (fill-history
/// cap, or an order-history drain that cannot prove completeness) keeps its
/// own code so callers fail closed instead of treating it as "venue empty".
fn history_err_to_frame(request_id: u64, code: &str, e: rithmic_plants::Error) -> Frame {
    match e {
        rithmic_plants::Error::ReconciliationUnavailable(msg) => {
            error_frame(request_id, "reconciliation_unavailable", &msg)
        }
        other => err_to_frame(request_id, code, other),
    }
}

pub(super) async fn load_fill_history(
    state: &GatewayState,
    request_id: u64,
    req: pb::LoadFillHistoryRequest,
) -> Frame {
    if !state.gates.trading_enabled {
        return error_frame(
            request_id,
            "trading_disabled",
            "load_fill_history denied: parent trading disabled",
        );
    }
    let session = match checkout_session(state, request_id).await {
        Ok(session) => session,
        Err(frame) => return *frame,
    };
    match session
        .load_fill_history(req.start_date, req.finish_date)
        .await
    {
        Ok(rows) => Frame {
            request_id,
            body: Some(Body::LoadFillHistoryResponse(pb::LoadFillHistoryResponse {
                rows: rows.into_iter().map(fill_history_to_pb).collect(),
            })),
        },
        Err(e) => history_err_to_frame(request_id, "load_fill_history_failed", e),
    }
}

pub(super) async fn load_order_history_dates(state: &GatewayState, request_id: u64) -> Frame {
    if !state.gates.trading_enabled {
        return error_frame(
            request_id,
            "trading_disabled",
            "load_order_history_dates denied: parent trading disabled",
        );
    }
    let session = match checkout_session(state, request_id).await {
        Ok(session) => session,
        Err(frame) => return *frame,
    };
    match session.load_order_history_dates().await {
        Ok(dates) => Frame {
            request_id,
            body: Some(Body::LoadOrderHistoryDatesResponse(
                pb::LoadOrderHistoryDatesResponse { dates },
            )),
        },
        Err(e) => history_err_to_frame(request_id, "load_order_history_dates_failed", e),
    }
}

pub(super) async fn load_order_history(
    state: &GatewayState,
    request_id: u64,
    req: pb::LoadOrderHistoryRequest,
) -> Frame {
    if !state.gates.trading_enabled {
        return error_frame(
            request_id,
            "trading_disabled",
            "load_order_history denied: parent trading disabled",
        );
    }
    // Same serialization as load_orders: the replay drains the shared
    // order-updates channel, so concurrent drains would mix their rows.
    let _recon_guard = state.recon_lock.lock().await;
    let session = match checkout_session(state, request_id).await {
        Ok(session) => session,
        Err(frame) => return *frame,
    };
    match session.load_order_history(&req.date).await {
        Ok(events) => Frame {
            request_id,
            body: Some(Body::LoadOrderHistoryResponse(
                pb::LoadOrderHistoryResponse {
                    events: events.into_iter().map(order_notification_to_pb).collect(),
                },
            )),
        },
        Err(e) => history_err_to_frame(request_id, "load_order_history_failed", e),
    }
}

pub(super) async fn ensure_pnl(state: &GatewayState, request_id: u64) -> Frame {
    let Some(session) = &state.session else {
        return no_session_frame(request_id);
    };
    let guard = session.lock().await;
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
    let guard = session.lock().await;
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
    let order = PlaceOrder {
        symbol: req.symbol,
        exchange: req.exchange,
        side: req.side,
        price_type: req.price_type,
        quantity: req.quantity,
        user_tag: req.user_tag,
        price: req.price,
        trigger_price: req.trigger_price,
        duration: req.duration,
        trail_by_ticks: req.trail_by_ticks,
        trail_by_price_id: req.trail_by_price_id,
    };
    let guard = session.lock().await;
    match guard.place_order(&order).await {
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
    let order = PlaceBracketOrder {
        symbol: req.symbol,
        exchange: req.exchange,
        side: req.side,
        price_type: req.price_type,
        quantity: req.quantity,
        localid: req.localid,
        price: req.price,
        trigger_price: req.trigger_price,
        duration: req.duration,
        stop_ticks: req.stop_ticks,
        target_ticks: req.target_ticks,
    };
    let guard = session.lock().await;
    match guard.place_bracket_order(&order).await {
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
    let guard = session.lock().await;
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
    let guard = session.lock().await;
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
    let guard = session.lock().await;
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
    let order = ModifyOrder {
        basket_id: req.basket_id,
        symbol: req.symbol,
        exchange: req.exchange,
        quantity: req.quantity,
        price_type: req.price_type,
        price: req.price,
        trigger_price: req.trigger_price,
        trail_by_ticks: req.trail_by_ticks,
    };
    let guard = session.lock().await;
    match guard.modify_order(&order).await {
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
    let guard = session.lock().await;
    match guard.cancel_all_orders().await {
        Ok(()) => ack_frame(request_id),
        Err(e) => plant_err_frame(request_id, "cancel_all_failed", e),
    }
}
