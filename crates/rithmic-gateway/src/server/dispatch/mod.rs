//! RPC body dispatch (Subscribe / Place / …) for the gateway accept loop.
//!
//! Router only: every [`Body`](crate::pb::frame::Body) variant delegates to
//! the submodule matching its RPC kind — subscriptions (fanout + intent +
//! restore), orders (trading one-shots + order-handle loads), history (ticks /
//! time bars), info (reference data). Shared frame constructors and per-topic
//! locks live here.

mod history;
mod info;
mod orders;
mod subscriptions;

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex as TokioMutex};

use crate::pb::{frame::Body, Ack, ErrorResponse, Frame};
use crate::reconnect::{ReconnectController, TimeBarIntent};
use crate::subscriptions::{FanoutHub, ParentGates, SharedFanout, SubKey};

use super::{ClientCtx, Fingerprint, GatewayState, OutMsg};
use crate::idle_exit::{IdleExit, IdleExitPolicy};

pub(in crate::server) use subscriptions::TopicIntent;
pub(super) use subscriptions::{clear_client_brackets, reseat_order_plant_without_brackets};

pub(super) async fn topic_lock(state: &GatewayState, key: &SubKey) -> Arc<TokioMutex<()>> {
    let mut map = state.topic_locks.lock().await;
    map.entry(key.clone())
        .or_insert_with(|| Arc::new(TokioMutex::new(())))
        .clone()
}

pub(super) fn ack_frame(request_id: u64) -> Frame {
    Frame {
        request_id,
        body: Some(Body::Ack(Ack {})),
    }
}

pub(super) fn error_frame(request_id: u64, code: &str, message: &str) -> Frame {
    Frame {
        request_id,
        body: Some(Body::Error(ErrorResponse {
            code: code.into(),
            message: message.into(),
        })),
    }
}

fn no_session_frame(request_id: u64) -> Frame {
    error_frame(request_id, "no_session", "gateway has no plant session")
}

fn err_to_frame(request_id: u64, code: &str, err: impl std::fmt::Display) -> Frame {
    error_frame(request_id, code, &err.to_string())
}

/// Post-send plant transport / connection failures must stay "unknown"
/// (not definitive `*_failed` rejects) so the adapter does not OrderReject.
pub(super) fn plant_err_frame(
    request_id: u64,
    failed_code: &str,
    e: rithmic_plants::Error,
) -> Frame {
    let code = if matches!(
        &e,
        rithmic_plants::Error::Rithmic(_)
            | rithmic_plants::Error::ChannelClosed { .. }
            | rithmic_plants::Error::NotConnected { .. }
    ) {
        "venue_unknown"
    } else {
        failed_code
    };
    err_to_frame(request_id, code, e)
}

/// Dispatch one request frame body to a response frame, applying parent
/// gates and (when a session is attached) calling through to
/// `rithmic_plants::RithmicSession`. Subscribe/unsubscribe bodies also
/// update this client's fan-out registration in `client`.
pub(super) async fn dispatch(
    state: &GatewayState,
    client: &mut ClientCtx,
    request_id: u64,
    body: Body,
) -> Frame {
    match body {
        Body::Subscribe(req) => {
            subscriptions::subscribe_topic(
                state,
                client,
                request_id,
                TopicIntent::Ticker(SubKey {
                    symbol: req.symbol,
                    exchange: req.exchange,
                }),
            )
            .await
        }
        Body::Unsubscribe(req) => {
            subscriptions::unsubscribe_topic(
                state,
                client,
                request_id,
                TopicIntent::Ticker(SubKey {
                    symbol: req.symbol,
                    exchange: req.exchange,
                }),
            )
            .await
        }
        Body::SubscribeBook(req) => {
            subscriptions::subscribe_topic(
                state,
                client,
                request_id,
                TopicIntent::Book(SubKey {
                    symbol: req.symbol,
                    exchange: req.exchange,
                }),
            )
            .await
        }
        Body::UnsubscribeBook(req) => {
            subscriptions::unsubscribe_topic(
                state,
                client,
                request_id,
                TopicIntent::Book(SubKey {
                    symbol: req.symbol,
                    exchange: req.exchange,
                }),
            )
            .await
        }
        Body::SubscribeTimeBars(req) => {
            subscriptions::subscribe_topic(
                state,
                client,
                request_id,
                TopicIntent::TimeBars(TimeBarIntent {
                    symbol: req.symbol,
                    exchange: req.exchange,
                    bar_type: req.bar_type,
                    period: req.period,
                }),
            )
            .await
        }
        Body::UnsubscribeTimeBars(req) => {
            subscriptions::unsubscribe_topic(
                state,
                client,
                request_id,
                TopicIntent::TimeBars(TimeBarIntent {
                    symbol: req.symbol,
                    exchange: req.exchange,
                    bar_type: req.bar_type,
                    period: req.period,
                }),
            )
            .await
        }
        Body::SubscribePnl(_) => subscriptions::subscribe_pnl(state, client, request_id).await,
        Body::DisconnectPnl(_) => subscriptions::disconnect_pnl(state, client, request_id).await,
        Body::SubscribeOrderUpdates(_) => {
            // Order-plant login is a trading capability (KTD12): gate it the
            // same as EnsureOrder even though this RPC only asks for
            // notifications, so `RITHMIC_ENABLE_TRADING=0` cannot be
            // bypassed by subscribing instead of calling EnsureOrder.
            subscriptions::subscribe_order_plant_stream(
                state,
                client,
                request_id,
                "subscribe_order_updates denied: parent trading disabled",
                false,
            )
            .await
        }
        Body::SubscribeBracketUpdates(_) => {
            subscriptions::subscribe_order_plant_stream(
                state,
                client,
                request_id,
                "subscribe_bracket_updates denied: parent trading disabled",
                true,
            )
            .await
        }
        Body::DisconnectOrder(_) => {
            subscriptions::disconnect_order(state, client, request_id).await
        }
        Body::ResetTickerPlant(_) => subscriptions::reset_ticker_plant(state, request_id).await,
        Body::RequestPlants(req) => info::request_plants(state, request_id, req).await,
        Body::GetFrontMonth(req) => info::get_front_month(state, request_id, req).await,
        Body::GetReferenceData(req) => info::get_reference_data(state, request_id, req).await,
        Body::ResolvedAccount(_) => info::resolved_account(state, request_id).await,
        Body::LoadOrders(_req) => orders::load_orders(state, request_id).await,
        Body::LoadProductRmsInfo(_) => orders::load_product_rms_info(state, request_id).await,
        Body::LoadAccountRmsInfo(_) => orders::load_account_rms_info(state, request_id).await,
        Body::EnsurePnl(_) => orders::ensure_pnl(state, request_id).await,
        Body::EnsureOrder(_) => orders::ensure_order(state, request_id).await,
        Body::PlaceOrder(req) => orders::place_order(state, request_id, req).await,
        Body::PlaceBracketOrder(req) => orders::place_bracket_order(state, request_id, req).await,
        Body::AdjustBracketStop(req) => orders::adjust_bracket_stop(state, request_id, req).await,
        Body::AdjustBracketTarget(req) => {
            orders::adjust_bracket_target(state, request_id, req).await
        }
        Body::CancelOrder(req) => orders::cancel_order(state, request_id, req).await,
        Body::ModifyOrder(req) => orders::modify_order(state, request_id, req).await,
        Body::CancelAllOrders(_) => orders::cancel_all_orders(state, request_id).await,
        Body::LoadTicks(req) => history::load_ticks(state, request_id, req).await,
        Body::LoadTimeBars(req) => history::load_time_bars(state, request_id, req).await,
        Body::ProbeTimeBars(req) => history::probe_time_bars(state, request_id, req).await,
        Body::Disconnect(_) => ack_frame(request_id),
        // Responses / events are gateway→client only; a client sending one
        // back is a protocol misuse, not a crash.
        _ => error_frame(request_id, "unsupported", "unsupported request body"),
    }
}

/// Test helper: process one RPC against `gates` (no session, no socket) and
/// return the response body. Used by `tests/gates.rs`.
pub fn gate_rpc_for_test(gates: &ParentGates, body: Body) -> Body {
    rpc_sequence_with_gates(*gates, vec![body])
        .0
        .into_iter()
        .next()
        .expect("body")
}

pub(super) fn test_state(gates: ParentGates, hub: SharedFanout) -> GatewayState {
    let (gateway_instance_id, transport_generation, transport_epoch) =
        super::boot_gateway_metadata();
    GatewayState {
        gates,
        hub: hub.clone(),
        fingerprint: Fingerprint {
            user: "alice".into(),
            system_name: "LucidTrading".into(),
            url: "wss://example".into(),
            env: "Live".into(),
            account_id: "A1".into(),
            fcm_id: "F1".into(),
            ib_id: "I1".into(),
        },
        ready: AtomicBool::new(true),
        force_reconnect: AtomicBool::new(false),
        expected_auth_token: "secret".into(),
        session: None,
        reconnect: Arc::new(ReconnectController::new(hub)),
        topic_locks: TokioMutex::new(HashMap::new()),
        recon_lock: Arc::new(TokioMutex::new(())),
        md_history_gate: Arc::new(TokioMutex::new(())),
        idle: IdleExit::new(IdleExitPolicy::Never),
        gateway_instance_id,
        transport_generation,
        transport_epoch,
    }
}

/// Run several RPCs on one client and return response bodies + restore plan.
pub fn rpc_sequence_with_gates(
    gates: ParentGates,
    bodies: Vec<Body>,
) -> (Vec<Body>, crate::reconnect::RestorePlan) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    rt.block_on(async {
        let hub: SharedFanout = Arc::new(FanoutHub::new(8));
        let state = test_state(gates, hub);
        let (out_tx, _out_rx) = mpsc::channel::<OutMsg>(8);
        let mut client = ClientCtx::new(out_tx);
        let mut out = Vec::with_capacity(bodies.len());
        for (i, body) in bodies.into_iter().enumerate() {
            let resp = dispatch(&state, &mut client, (i as u64) + 1, body).await;
            out.push(resp.body.expect("body"));
        }
        let plan = state.reconnect.restore_plan().await;
        (out, plan)
    })
}

/// Note a ticker intent then dispatch a history body (no plant session).
/// Used to prove Load* is refused while live MD intents are active (RC2.3).
pub fn history_rpc_with_live_ticker_intent_for_test(body: Body) -> Body {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    rt.block_on(async {
        let hub: SharedFanout = Arc::new(FanoutHub::new(8));
        let state = test_state(ParentGates::default(), hub);
        state
            .reconnect
            .note_ticker(SubKey {
                symbol: "MNQU6".into(),
                exchange: "CME".into(),
            })
            .await;
        let (out_tx, _out_rx) = mpsc::channel::<OutMsg>(8);
        let mut client = ClientCtx::new(out_tx);
        dispatch(&state, &mut client, 1, body)
            .await
            .body
            .expect("body")
    })
}
