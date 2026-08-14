//! RPC body dispatch (Subscribe / Place / …) for the gateway accept loop.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex as TokioMutex};

use rithmic_plants::history::parse_time_bar_type;
use rithmic_plants::PlantSet;

use crate::convert::{
    order_key, pnl_key, front_month_to_pb, history_bar_to_pb, history_tick_to_pb,
    reference_data_to_pb, time_bar_probe_row_to_pb,
};
use crate::pb::{self, frame::Body, Ack, ErrorResponse, Frame};
use crate::reconnect::{ReconnectController, TimeBarIntent};
use crate::subscriptions::{FanoutHub, ParentGates, SharedFanout, SubKey};

use super::{ClientCtx, Fingerprint, GatewayState, OutMsg};
use crate::idle_exit::{IdleExit, IdleExitPolicy};

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

fn err_to_frame(request_id: u64, code: &str, err: impl std::fmt::Display) -> Frame {
    error_frame(request_id, code, &err.to_string())
}

/// Post-send plant transport / connection failures must stay "unknown"
/// (not definitive `*_failed` rejects) so the adapter does not OrderReject.
fn plant_err_frame(request_id: u64, failed_code: &str, e: rithmic_plants::Error) -> Frame {
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

/// Attach this client to a shared hub topic (refcount once per client per key).
/// Returns whether this client already had a forwarder (no new hub bump).
async fn attach_shared_topic(
    state: &GatewayState,
    client: &mut ClientCtx,
    key: &SubKey,
) -> bool {
    let had_forwarder = client.subscribed.contains(key);
    if !had_forwarder {
        state.reconnect.add_hub_interest(key.clone()).await;
    }
    client.ensure_forwarder(&state.hub, key).await;
    had_forwarder
}

async fn rollback_shared_topic(
    state: &GatewayState,
    client: &mut ClientCtx,
    key: &SubKey,
    had_forwarder: bool,
) {
    if !had_forwarder {
        client.drop_forwarder(key);
        state.reconnect.remove_hub_interest(key).await;
    }
}

async fn release_md_if_unused(state: &GatewayState, client: &mut ClientCtx, key: &SubKey) {
    if !client.wants_md(key) {
        client.drop_forwarder(key);
        if state.reconnect.remove_hub_interest(key).await {
            prune_topic_lock(state, key).await;
        }
    }
}

async fn prune_topic_lock(state: &GatewayState, key: &SubKey) {
    let mut map = state.topic_locks.lock().await;
    if let Some(lock) = map.get(key) {
        // Only the map holds the Arc → safe to drop.
        if Arc::strong_count(lock) == 1 {
            map.remove(key);
        }
    }
}

/// Dispatch one request frame body to a response frame, applying parent
/// gates and (when a session is attached) calling through to
/// `rithmic_plants::RithmicSession`. Subscribe/unsubscribe bodies also
/// update this client's fan-out registration in `client`.
pub(super) async fn dispatch(state: &GatewayState, client: &mut ClientCtx, request_id: u64, body: Body) -> Frame {
    match body {
        Body::Subscribe(req) => {
            let key = SubKey {
                symbol: req.symbol.clone(),
                exchange: req.exchange.clone(),
            };
            if client.ticker.contains(&key) {
                return ack_frame(request_id);
            }
            let lock = topic_lock(state, &key).await;
            let _guard = lock.lock().await;
            let had_forwarder = attach_shared_topic(state, client, &key).await;
            let is_first = state.reconnect.note_ticker(key.clone()).await;
            client.ticker.insert(key.clone());
            if is_first {
                if let Some(session) = &state.session {
                    let session_guard = session.lock().await;
                    if let Err(e) = session_guard.subscribe(&req.symbol, &req.exchange).await {
                        drop(session_guard);
                        client.ticker.remove(&key);
                        state.reconnect.forget_ticker(&key).await;
                        rollback_shared_topic(state, client, &key, had_forwarder).await;
                        return err_to_frame(request_id, "subscribe_failed", e);
                    }
                }
            }
            ack_frame(request_id)
        }
        Body::Unsubscribe(req) => {
            let key = SubKey {
                symbol: req.symbol.clone(),
                exchange: req.exchange.clone(),
            };
            if !client.ticker.contains(&key) {
                return ack_frame(request_id);
            }
            // Mirror Subscribe: hold topic_lock across forget + venue teardown so a
            // concurrent first-peer Subscribe cannot have its venue join undone.
            let lock = topic_lock(state, &key).await;
            let _guard = lock.lock().await;
            if !client.ticker.contains(&key) {
                return ack_frame(request_id);
            }
            client.ticker.remove(&key);
            let last = state.reconnect.forget_ticker(&key).await;
            if last {
                if let Some(session) = &state.session {
                    let guard = session.lock().await;
                    if let Err(e) = guard.unsubscribe(&req.symbol, &req.exchange).await {
                        drop(guard);
                        // Restore intent so reconnect still re-joins venue MD.
                        state.reconnect.note_ticker(key.clone()).await;
                        client.ticker.insert(key.clone());
                        return err_to_frame(request_id, "unsubscribe_failed", e);
                    }
                }
            }
            release_md_if_unused(state, client, &key).await;
            ack_frame(request_id)
        }
        Body::SubscribeBook(req) => {
            let key = SubKey {
                symbol: req.symbol.clone(),
                exchange: req.exchange.clone(),
            };
            if client.book.contains(&key) {
                return ack_frame(request_id);
            }
            let lock = topic_lock(state, &key).await;
            let _guard = lock.lock().await;
            let had_forwarder = attach_shared_topic(state, client, &key).await;
            let is_first = state.reconnect.note_book(key.clone()).await;
            client.book.insert(key.clone());
            if is_first {
                if let Some(session) = &state.session {
                    let session_guard = session.lock().await;
                    if let Err(e) = session_guard
                        .subscribe_order_book_summary(&req.symbol, &req.exchange)
                        .await
                    {
                        drop(session_guard);
                        client.book.remove(&key);
                        state.reconnect.forget_book(&key).await;
                        rollback_shared_topic(state, client, &key, had_forwarder).await;
                        return err_to_frame(request_id, "subscribe_book_failed", e);
                    }
                }
            }
            ack_frame(request_id)
        }
        Body::UnsubscribeBook(req) => {
            let key = SubKey {
                symbol: req.symbol.clone(),
                exchange: req.exchange.clone(),
            };
            if !client.book.contains(&key) {
                return ack_frame(request_id);
            }
            let lock = topic_lock(state, &key).await;
            let _guard = lock.lock().await;
            if !client.book.contains(&key) {
                return ack_frame(request_id);
            }
            client.book.remove(&key);
            let last = state.reconnect.forget_book(&key).await;
            if last {
                if let Some(session) = &state.session {
                    let guard = session.lock().await;
                    if let Err(e) = guard
                        .unsubscribe_order_book_summary(&req.symbol, &req.exchange)
                        .await
                    {
                        drop(guard);
                        state.reconnect.note_book(key.clone()).await;
                        client.book.insert(key.clone());
                        return err_to_frame(request_id, "unsubscribe_book_failed", e);
                    }
                }
            }
            release_md_if_unused(state, client, &key).await;
            ack_frame(request_id)
        }
        Body::RequestPlants(req) => match PlantSet::parse(&req.plants) {
            Ok(extra) => {
                if let Some(session) = &state.session {
                    let mut guard = session.lock().await;
                    if let Err(e) = guard.request_plants(extra).await {
                        return err_to_frame(request_id, "request_plants_failed", e);
                    }
                }
                ack_frame(request_id)
            }
            Err(e) => err_to_frame(request_id, "invalid_plants", e),
        },
        Body::GetFrontMonth(req) => {
            let Some(session) = &state.session else {
                return ack_frame(request_id);
            };
            let guard = session.lock().await;
            match guard.get_front_month(&req.symbol, &req.exchange).await {
                Ok(dto) => Frame {
                    request_id,
                    body: Some(Body::FrontMonthResponse(front_month_to_pb(dto))),
                },
                Err(e) => err_to_frame(request_id, "get_front_month_failed", e),
            }
        }
        Body::GetReferenceData(req) => {
            let Some(session) = &state.session else {
                return ack_frame(request_id);
            };
            let guard = session.lock().await;
            match guard.get_reference_data(&req.symbol, &req.exchange).await {
                Ok(dto) => Frame {
                    request_id,
                    body: Some(Body::ReferenceDataResponse(reference_data_to_pb(dto))),
                },
                Err(e) => err_to_frame(request_id, "get_reference_data_failed", e),
            }
        }
        Body::LoadTicks(req) => {
            let Some(session) = &state.session else {
                return Frame {
                    request_id,
                    body: Some(Body::LoadTicksResponse(pb::LoadTicksResponse::default())),
                };
            };
            let guard = session.lock().await;
            match guard
                .load_ticks_all(&req.symbol, &req.exchange, req.start_time_sec, req.end_time_sec)
                .await
            {
                Ok(ticks) => Frame {
                    request_id,
                    body: Some(Body::LoadTicksResponse(pb::LoadTicksResponse {
                        ticks: ticks.into_iter().map(history_tick_to_pb).collect(),
                    })),
                },
                Err(e) => err_to_frame(request_id, "load_ticks_failed", e),
            }
        }
        Body::LoadTimeBars(req) => {
            let Some(session) = &state.session else {
                return Frame {
                    request_id,
                    body: Some(Body::LoadTimeBarsResponse(pb::LoadTimeBarsResponse::default())),
                };
            };
            let bar_type = match parse_time_bar_type(req.bar_type) {
                Ok(bt) => bt,
                Err(e) => return err_to_frame(request_id, "invalid_bar_type", e),
            };
            let guard = session.lock().await;
            match guard
                .load_time_bars_all(
                    &req.symbol,
                    &req.exchange,
                    bar_type,
                    req.period,
                    req.start_time_sec,
                    req.end_time_sec,
                )
                .await
            {
                Ok(bars) => Frame {
                    request_id,
                    body: Some(Body::LoadTimeBarsResponse(pb::LoadTimeBarsResponse {
                        bars: bars.into_iter().map(history_bar_to_pb).collect(),
                    })),
                },
                Err(e) => err_to_frame(request_id, "load_time_bars_failed", e),
            }
        }
        Body::ProbeTimeBars(req) => {
            let Some(session) = &state.session else {
                return Frame {
                    request_id,
                    body: Some(Body::ProbeTimeBarsResponse(pb::ProbeTimeBarsResponse::default())),
                };
            };
            let bar_type = match parse_time_bar_type(req.bar_type) {
                Ok(bt) => bt,
                Err(e) => return err_to_frame(request_id, "invalid_bar_type", e),
            };
            let guard = session.lock().await;
            match guard
                .probe_time_bars(
                    &req.symbol,
                    &req.exchange,
                    bar_type,
                    req.period,
                    req.start_time_sec,
                    req.end_time_sec,
                )
                .await
            {
                Ok(rows) => Frame {
                    request_id,
                    body: Some(Body::ProbeTimeBarsResponse(pb::ProbeTimeBarsResponse {
                        rows: rows.into_iter().map(time_bar_probe_row_to_pb).collect(),
                    })),
                },
                Err(e) => err_to_frame(request_id, "probe_time_bars_failed", e),
            }
        }
        Body::SubscribeTimeBars(req) => {
            let key = SubKey {
                symbol: req.symbol.clone(),
                exchange: req.exchange.clone(),
            };
            let intent = TimeBarIntent {
                symbol: req.symbol.clone(),
                exchange: req.exchange.clone(),
                bar_type: req.bar_type,
                period: req.period,
            };
            if client.time_bars.contains(&intent) {
                return ack_frame(request_id);
            }
            let lock = topic_lock(state, &key).await;
            let _guard = lock.lock().await;
            // Attach MD hub so TimeBar plant events (routed by symbol/exchange)
            // reach this client; venue bar joins are tracked separately.
            let had_forwarder = attach_shared_topic(state, client, &key).await;
            let is_first = state.reconnect.note_time_bar(intent.clone()).await;
            client.time_bars.insert(intent.clone());
            if is_first {
                if let Some(session) = &state.session {
                    let session_guard = session.lock().await;
                    if let Err(e) = session_guard
                        .subscribe_time_bars(&req.symbol, &req.exchange, req.bar_type, req.period)
                        .await
                    {
                        drop(session_guard);
                        client.time_bars.remove(&intent);
                        state.reconnect.forget_time_bar(&intent).await;
                        rollback_shared_topic(state, client, &key, had_forwarder).await;
                        return err_to_frame(request_id, "subscribe_time_bars_failed", e);
                    }
                }
            }
            ack_frame(request_id)
        }
        Body::UnsubscribeTimeBars(req) => {
            let key = SubKey {
                symbol: req.symbol.clone(),
                exchange: req.exchange.clone(),
            };
            let intent = TimeBarIntent {
                symbol: req.symbol.clone(),
                exchange: req.exchange.clone(),
                bar_type: req.bar_type,
                period: req.period,
            };
            if !client.time_bars.contains(&intent) {
                return ack_frame(request_id);
            }
            let lock = topic_lock(state, &key).await;
            let _guard = lock.lock().await;
            if !client.time_bars.contains(&intent) {
                return ack_frame(request_id);
            }
            client.time_bars.remove(&intent);
            let last = state.reconnect.forget_time_bar(&intent).await;
            if last {
                if let Some(session) = &state.session {
                    let guard = session.lock().await;
                    if let Err(e) = guard
                        .unsubscribe_time_bars(&req.symbol, &req.exchange, req.bar_type, req.period)
                        .await
                    {
                        drop(guard);
                        state.reconnect.note_time_bar(intent.clone()).await;
                        client.time_bars.insert(intent);
                        return err_to_frame(request_id, "unsubscribe_time_bars_failed", e);
                    }
                }
            }
            // Do not tear shared ticker/book hub interest for this symbol.
            release_md_if_unused(state, client, &key).await;
            ack_frame(request_id)
        }
        Body::SubscribePnl(_) => {
            let key = pnl_key();
            if client.pnl {
                return ack_frame(request_id);
            }
            let lock = topic_lock(state, &key).await;
            let _guard = lock.lock().await;
            let had_forwarder = attach_shared_topic(state, client, &key).await;
            let is_first = state.reconnect.note_pnl().await;
            client.pnl = true;
            if is_first {
                if let Some(session) = &state.session {
                    let mut session_guard = session.lock().await;
                    let venue_err = match session_guard.ensure_pnl_plant().await {
                        Err(e) => Some(e),
                        Ok(()) => match session_guard.subscribe_pnl().await {
                            Err(e) => Some(e),
                            Ok(()) => None,
                        },
                    };
                    if let Some(e) = venue_err {
                        drop(session_guard);
                        client.pnl = false;
                        state.reconnect.forget_pnl().await;
                        rollback_shared_topic(state, client, &key, had_forwarder).await;
                        return err_to_frame(request_id, "subscribe_pnl_failed", e);
                    }
                }
            }
            ack_frame(request_id)
        }
        Body::SubscribeOrderUpdates(_) => {
            // Order-plant login is a trading capability (KTD12): gate it the
            // same as EnsureOrder even though this RPC only asks for
            // notifications, so `RITHMIC_ENABLE_TRADING=0` cannot be
            // bypassed by subscribing instead of calling EnsureOrder.
            if !state.gates.trading_enabled {
                return error_frame(
                    request_id,
                    "trading_disabled",
                    "subscribe_order_updates denied: parent trading disabled",
                );
            }
            let key = order_key();
            if client.order {
                return ack_frame(request_id);
            }
            let lock = topic_lock(state, &key).await;
            let _guard = lock.lock().await;
            let had_forwarder = attach_shared_topic(state, client, &key).await;
            let is_first = state.reconnect.note_order().await;
            client.order = true;
            if is_first {
                if let Some(session) = &state.session {
                    let mut session_guard = session.lock().await;
                    if let Err(e) = session_guard.subscribe_order_updates().await {
                        drop(session_guard);
                        client.order = false;
                        state.reconnect.forget_order().await;
                        rollback_shared_topic(state, client, &key, had_forwarder).await;
                        return err_to_frame(request_id, "subscribe_order_updates_failed", e);
                    }
                }
            }
            ack_frame(request_id)
        }
        Body::SubscribeBracketUpdates(_) => {
            if !state.gates.trading_enabled {
                return error_frame(
                    request_id,
                    "trading_disabled",
                    "subscribe_bracket_updates denied: parent trading disabled",
                );
            }
            let Some(session) = &state.session else {
                return ack_frame(request_id);
            };
            let mut guard = session.lock().await;
            match guard.subscribe_bracket_updates().await {
                Ok(()) => ack_frame(request_id),
                Err(e) => err_to_frame(request_id, "subscribe_bracket_updates_failed", e),
            }
        }
        Body::PlaceBracketOrder(req) => {
            if !state.gates.allow_place() {
                return error_frame(
                    request_id,
                    "trading_disabled",
                    "place_bracket_order denied: parent trading disabled",
                );
            }
            let Some(session) = &state.session else {
                return ack_frame(request_id);
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
        Body::AdjustBracketStop(req) => {
            if !state.gates.allow_place() {
                return error_frame(
                    request_id,
                    "trading_disabled",
                    "adjust_bracket_stop denied: parent trading disabled",
                );
            }
            let Some(session) = &state.session else {
                return ack_frame(request_id);
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
        Body::AdjustBracketTarget(req) => {
            if !state.gates.allow_place() {
                return error_frame(
                    request_id,
                    "trading_disabled",
                    "adjust_bracket_target denied: parent trading disabled",
                );
            }
            let Some(session) = &state.session else {
                return ack_frame(request_id);
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
        Body::EnsurePnl(_) => {
            if let Some(session) = &state.session {
                let mut guard = session.lock().await;
                if let Err(e) = guard.ensure_pnl_plant().await {
                    return err_to_frame(request_id, "ensure_pnl_failed", e);
                }
            }
            ack_frame(request_id)
        }
        Body::EnsureOrder(_) => {
            if !state.gates.trading_enabled {
                return error_frame(request_id, "trading_disabled", "ensure_order denied: parent trading disabled");
            }
            if let Some(session) = &state.session {
                let mut guard = session.lock().await;
                if let Err(e) = guard.ensure_order_plant().await {
                    return err_to_frame(request_id, "ensure_order_failed", e);
                }
            }
            ack_frame(request_id)
        }
        Body::PlaceOrder(req) => {
            if !state.gates.allow_place() {
                return error_frame(request_id, "trading_disabled", "place_order denied: parent trading disabled");
            }
            let Some(session) = &state.session else { return ack_frame(request_id) };
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
        Body::CancelOrder(req) => {
            if !state.gates.allow_place() {
                return error_frame(request_id, "trading_disabled", "cancel_order denied: parent trading disabled");
            }
            let Some(session) = &state.session else { return ack_frame(request_id) };
            let mut guard = session.lock().await;
            match guard.cancel_order(&req.basket_id).await {
                Ok(()) => ack_frame(request_id),
                Err(e) => plant_err_frame(request_id, "cancel_failed", e),
            }
        }
        Body::ModifyOrder(req) => {
            if !state.gates.allow_place() {
                return error_frame(request_id, "trading_disabled", "modify_order denied: parent trading disabled");
            }
            let Some(session) = &state.session else { return ack_frame(request_id) };
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
        Body::CancelAllOrders(_) => {
            if !state.gates.allow_cancel_all() {
                return error_frame(request_id, "cancel_all_denied", "cancel_all_orders denied: parent cancel_all disabled");
            }
            let Some(session) = &state.session else { return ack_frame(request_id) };
            let mut guard = session.lock().await;
            match guard.cancel_all_orders().await {
                Ok(()) => ack_frame(request_id),
                Err(e) => plant_err_frame(request_id, "cancel_all_failed", e),
            }
        }
        Body::DisconnectPnl(_) => {
            let key = pnl_key();
            if client.pnl {
                client.pnl = false;
                let last = state.reconnect.forget_pnl().await;
                if last {
                    if let Some(session) = &state.session {
                        let mut guard = session.lock().await;
                        if let Err(e) = guard.disconnect_pnl_plant().await {
                            drop(guard);
                            state.reconnect.note_pnl().await;
                            client.pnl = true;
                            return err_to_frame(request_id, "disconnect_pnl_failed", e);
                        }
                    }
                }
            }
            if client.subscribed.contains(&key) {
                client.drop_forwarder(&key);
                state.reconnect.remove_hub_interest(&key).await;
            }
            ack_frame(request_id)
        }
        Body::DisconnectOrder(_) => {
            let key = order_key();
            if client.order {
                client.order = false;
                let last = state.reconnect.forget_order().await;
                if last {
                    if let Some(session) = &state.session {
                        let mut guard = session.lock().await;
                        if let Err(e) = guard.disconnect_order_plant().await {
                            drop(guard);
                            state.reconnect.note_order().await;
                            client.order = true;
                            return err_to_frame(request_id, "disconnect_order_failed", e);
                        }
                    }
                }
            }
            if client.subscribed.contains(&key) {
                client.drop_forwarder(&key);
                state.reconnect.remove_hub_interest(&key).await;
            }
            ack_frame(request_id)
        }
        Body::Disconnect(_) => ack_frame(request_id),
        // Responses / events are gateway→client only; a client sending one
        // back is a protocol misuse, not a crash.
        _ => error_frame(request_id, "unsupported", "unsupported request body"),
    }
}

/// Test helper: process one RPC against `gates` (no session, no socket) and
/// return the response body. Used by `tests/gates.rs`.
pub fn gate_rpc_for_test(gates: &ParentGates, body: Body) -> Body {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    rt.block_on(async {
        let hub: SharedFanout = Arc::new(FanoutHub::new(8));
        let state = test_state(*gates, hub);
        let (out_tx, _out_rx) = mpsc::channel::<OutMsg>(8);
        let mut client = ClientCtx::new(out_tx);
        dispatch(&state, &mut client, 1, body).await.body.expect("body")
    })
}

pub(super) fn test_state(gates: ParentGates, hub: SharedFanout) -> GatewayState {
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
        expected_auth_token: "secret".into(),
        session: None,
        reconnect: Arc::new(ReconnectController::new(hub)),
        topic_locks: TokioMutex::new(HashMap::new()),
        idle: IdleExit::new(IdleExitPolicy::Never),
    }
}

/// Run several RPCs on one client and return response bodies + restore plan.
/// Used by subscribe-idempotency / typed-intent tests.
pub fn rpc_sequence_for_test(bodies: Vec<Body>) -> (Vec<Body>, crate::reconnect::RestorePlan) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    rt.block_on(async {
        let hub: SharedFanout = Arc::new(FanoutHub::new(8));
        let state = test_state(ParentGates::default(), hub);
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

