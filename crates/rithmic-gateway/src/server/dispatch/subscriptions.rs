//! Subscription-intent RPCs (ticker / book / time bars / pnl / order+brackets
//! streams): hub fanout attach, reconnect-intent refcounting, venue join /
//! leave, and rollback on failure. The failure templates for join vs leave are
//! deliberately separate — subscribe rolls back on first-join failure;
//! unsubscribe re-notes the intent on teardown failure so reconnect still
//! re-joins. Do not merge them.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::convert::{order_key, pnl_key};
use crate::pb::Frame;
use crate::reconnect::{ReconnectController, TimeBarIntent};
use crate::subscriptions::SubKey;

use super::{
    ack_frame, err_to_frame, error_frame, no_session_frame, topic_lock, ClientCtx, GatewayState,
};

fn sub_key(symbol: &str, exchange: &str) -> SubKey {
    SubKey {
        symbol: symbol.to_string(),
        exchange: exchange.to_string(),
    }
}

/// A typed subscription the gateway remembers across plant reconnects.
/// Discriminates client bookkeeping, reconnect note/forget, error codes, and
/// the venue session call — everything the subscribe/unsubscribe templates
/// need except their differing rollback direction.
#[derive(Debug, Clone)]
pub(in crate::server) enum TopicIntent {
    Ticker(SubKey),
    Book(SubKey),
    TimeBars(TimeBarIntent),
}

impl TopicIntent {
    pub(in crate::server) fn key(&self) -> SubKey {
        match self {
            Self::Ticker(key) | Self::Book(key) => key.clone(),
            Self::TimeBars(intent) => sub_key(&intent.symbol, &intent.exchange),
        }
    }

    fn client_has(&self, client: &ClientCtx) -> bool {
        match self {
            Self::Ticker(key) => client.ticker.contains(key),
            Self::Book(key) => client.book.contains(key),
            Self::TimeBars(intent) => client.time_bars.contains(intent),
        }
    }

    fn client_insert(&self, client: &mut ClientCtx) {
        match self {
            Self::Ticker(key) => {
                client.ticker.insert(key.clone());
            }
            Self::Book(key) => {
                client.book.insert(key.clone());
            }
            Self::TimeBars(intent) => {
                client.time_bars.insert(intent.clone());
            }
        }
    }

    fn client_remove(&self, client: &mut ClientCtx) {
        match self {
            Self::Ticker(key) => {
                client.ticker.remove(key);
            }
            Self::Book(key) => {
                client.book.remove(key);
            }
            Self::TimeBars(intent) => {
                client.time_bars.remove(intent);
            }
        }
    }

    /// Returns whether this was the first peer for the intent (0→1).
    async fn note(&self, rc: &ReconnectController) -> bool {
        match self {
            Self::Ticker(key) => rc.note_ticker(key.clone()).await,
            Self::Book(key) => rc.note_book(key.clone()).await,
            Self::TimeBars(intent) => rc.note_time_bar(intent.clone()).await,
        }
    }

    /// Removes the reconnect intent; returns whether this was the last peer.
    pub(in crate::server) async fn forget(&self, rc: &ReconnectController) -> bool {
        match self {
            Self::Ticker(key) => rc.forget_ticker(key).await,
            Self::Book(key) => rc.forget_book(key).await,
            Self::TimeBars(intent) => rc.forget_time_bar(intent).await,
        }
    }

    pub(in crate::server) async fn venue_join(
        &self,
        session: &rithmic_plants::RithmicSession,
    ) -> rithmic_plants::Result<()> {
        match self {
            Self::Ticker(key) => session.subscribe(&key.symbol, &key.exchange).await,
            Self::Book(key) => {
                session
                    .subscribe_order_book_summary(&key.symbol, &key.exchange)
                    .await
            }
            Self::TimeBars(intent) => {
                session
                    .subscribe_time_bars(
                        &intent.symbol,
                        &intent.exchange,
                        intent.bar_type,
                        intent.period,
                    )
                    .await
            }
        }
    }

    pub(in crate::server) async fn venue_leave(
        &self,
        session: &rithmic_plants::RithmicSession,
    ) -> rithmic_plants::Result<()> {
        match self {
            Self::Ticker(key) => session.unsubscribe(&key.symbol, &key.exchange).await,
            Self::Book(key) => {
                session
                    .unsubscribe_order_book_summary(&key.symbol, &key.exchange)
                    .await
            }
            Self::TimeBars(intent) => {
                session
                    .unsubscribe_time_bars(
                        &intent.symbol,
                        &intent.exchange,
                        intent.bar_type,
                        intent.period,
                    )
                    .await
            }
        }
    }

    fn subscribe_failed_code(&self) -> &'static str {
        match self {
            Self::Ticker(_) => "subscribe_failed",
            Self::Book(_) => "subscribe_book_failed",
            Self::TimeBars(_) => "subscribe_time_bars_failed",
        }
    }

    fn unsubscribe_failed_code(&self) -> &'static str {
        match self {
            Self::Ticker(_) => "unsubscribe_failed",
            Self::Book(_) => "unsubscribe_book_failed",
            Self::TimeBars(_) => "unsubscribe_time_bars_failed",
        }
    }

    /// Log line for a failed pump-side restore; kept byte-identical to the
    /// pre-pump-relocation strings. (Pump policy is log-and-continue —
    /// dispatch's rollback policy lives in `subscribe_topic`/`fail_*`.)
    pub(in crate::server) fn resubscribe_fail_log(&self, e: rithmic_plants::Error) -> String {
        match self {
            Self::Ticker(key) => format!(
                "rithmic-gateway: resubscribe ticker {}/{} failed: {e}",
                key.symbol, key.exchange
            ),
            Self::Book(key) => format!(
                "rithmic-gateway: resubscribe book {}/{} failed: {e}",
                key.symbol, key.exchange
            ),
            Self::TimeBars(intent) => format!(
                "rithmic-gateway: resubscribe time_bars {}/{} type={} period={} failed: {e}",
                intent.symbol, intent.exchange, intent.bar_type, intent.period
            ),
        }
    }
}

/// Attach this client to a shared hub topic (refcount once per client per key).
/// Returns whether this client already had a forwarder (no new hub bump).
async fn attach_shared_topic(state: &GatewayState, client: &mut ClientCtx, key: &SubKey) -> bool {
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

pub(super) async fn subscribe_topic(
    state: &GatewayState,
    client: &mut ClientCtx,
    request_id: u64,
    intent: TopicIntent,
) -> Frame {
    let key = intent.key();
    if intent.client_has(client) {
        return ack_frame(request_id);
    }
    // Hold md_history_gate across note + venue join so history cannot pass
    // its live-MD check then acquire session while we are creating intent
    // (RC2.3 race). Lock order: md_history_gate → topic_lock → session.
    let _md_gate = state.md_history_gate.lock().await;
    let lock = topic_lock(state, &key).await;
    let _guard = lock.lock().await;
    let had_forwarder = attach_shared_topic(state, client, &key).await;
    let is_first = intent.note(&state.reconnect).await;
    intent.client_insert(client);
    if is_first {
        let Some(session) = &state.session else {
            intent.client_remove(client);
            intent.forget(&state.reconnect).await;
            rollback_shared_topic(state, client, &key, had_forwarder).await;
            return no_session_frame(request_id);
        };
        // MD joins take `&self`; holding the guard only serializes against
        // plant reset / teardown.
        let session_guard = session.lock().await;
        if let Err(e) = intent.venue_join(&session_guard).await {
            drop(session_guard);
            intent.client_remove(client);
            intent.forget(&state.reconnect).await;
            rollback_shared_topic(state, client, &key, had_forwarder).await;
            return err_to_frame(request_id, intent.subscribe_failed_code(), e);
        }
    }
    ack_frame(request_id)
}

pub(super) async fn unsubscribe_topic(
    state: &GatewayState,
    client: &mut ClientCtx,
    request_id: u64,
    intent: TopicIntent,
) -> Frame {
    let key = intent.key();
    if !intent.client_has(client) {
        return ack_frame(request_id);
    }
    // Same gate as subscribe/history so a failed leave re-note cannot race
    // a history admission check (RC2.3).
    let _md_gate = state.md_history_gate.lock().await;
    // Hold topic_lock across forget + venue teardown so a concurrent
    // first-peer Subscribe cannot have its venue join undone.
    let lock = topic_lock(state, &key).await;
    let _guard = lock.lock().await;
    if !intent.client_has(client) {
        return ack_frame(request_id);
    }
    intent.client_remove(client);
    let last = intent.forget(&state.reconnect).await;
    if last {
        if let Some(session) = &state.session {
            let session_guard = session.lock().await;
            if let Err(e) = intent.venue_leave(&session_guard).await {
                drop(session_guard);
                // Restore intent so reconnect still re-joins venue.
                intent.note(&state.reconnect).await;
                intent.client_insert(client);
                return err_to_frame(request_id, intent.unsubscribe_failed_code(), e);
            }
        }
    }
    release_md_if_unused(state, client, &key).await;
    ack_frame(request_id)
}

pub(super) async fn subscribe_pnl(
    state: &GatewayState,
    client: &mut ClientCtx,
    request_id: u64,
) -> Frame {
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
        let Some(session) = &state.session else {
            client.pnl = false;
            state.reconnect.forget_pnl().await;
            rollback_shared_topic(state, client, &key, had_forwarder).await;
            return no_session_frame(request_id);
        };
        let mut session_guard = session.lock().await;
        let venue_err = match session_guard.ensure_pnl_plant().await {
            Err(e) => Some(e),
            Ok(()) => session_guard.subscribe_pnl().await.err(),
        };
        if let Some(e) = venue_err {
            drop(session_guard);
            client.pnl = false;
            state.reconnect.forget_pnl().await;
            rollback_shared_topic(state, client, &key, had_forwarder).await;
            return err_to_frame(request_id, "subscribe_pnl_failed", e);
        }
    }
    ack_frame(request_id)
}

pub(super) async fn disconnect_pnl(
    state: &GatewayState,
    client: &mut ClientCtx,
    request_id: u64,
) -> Frame {
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

/// Order-plant notification streams (order updates / brackets): fanout + intent + restore.
/// Anything this RPC notes / attaches must roll back on failure so Error never
/// leaves a half-applied order+brackets intent.
async fn rollback_order_plant_stream_introductions(
    state: &GatewayState,
    client: &mut ClientCtx,
    key: &SubKey,
    had_forwarder: bool,
    introduced_order: bool,
    introduced_brackets: bool,
) {
    if introduced_brackets {
        client.brackets = false;
        let _ = state.reconnect.forget_brackets().await;
    }
    if !introduced_order {
        return;
    }
    client.order = false;
    let last = state.reconnect.forget_order().await;
    if last {
        if let Some(session) = &state.session {
            let mut guard = session.lock().await;
            if let Err(e) = guard.disconnect_order_plant().await {
                // Venue may still be subscribed; keep order intent for restore.
                drop(guard);
                eprintln!("rithmic-gateway: rollback disconnect_order_plant failed: {e}");
                state.reconnect.note_order().await;
                client.order = true;
                return;
            }
        }
    }
    rollback_shared_topic(state, client, key, had_forwarder).await;
}

async fn fail_order_plant_stream(
    state: &GatewayState,
    client: &mut ClientCtx,
    key: &SubKey,
    had_forwarder: bool,
    introduced_order: bool,
    introduced_brackets: bool,
    frame: Frame,
) -> Frame {
    rollback_order_plant_stream_introductions(
        state,
        client,
        key,
        had_forwarder,
        introduced_order,
        introduced_brackets,
    )
    .await;
    frame
}

/// No venue unsubscribe for brackets. When the last brackets peer leaves and
/// order peers remain, disconnect + `subscribe_order_updates` to drop brackets.
pub(in crate::server) async fn reseat_order_plant_without_brackets(
    state: &GatewayState,
) -> Result<(), rithmic_plants::Error> {
    let Some(session) = &state.session else {
        return Ok(());
    };
    let mut guard = session.lock().await;
    let _ = guard.disconnect_order_plant().await;
    guard.subscribe_order_updates().await
}

/// Drop this client's brackets flag/intent. Returns whether this was the last
/// brackets peer (caller may need [`reseat_order_plant_without_brackets`]).
pub(in crate::server) async fn clear_client_brackets(
    state: &GatewayState,
    client: &mut ClientCtx,
) -> bool {
    if !client.brackets {
        return false;
    }
    client.brackets = false;
    state.reconnect.forget_brackets().await
}

pub(super) async fn subscribe_order_plant_stream(
    state: &GatewayState,
    client: &mut ClientCtx,
    request_id: u64,
    denied_msg: &str,
    want_brackets: bool,
) -> Frame {
    if !state.gates.trading_enabled {
        return error_frame(request_id, "trading_disabled", denied_msg);
    }

    let need_order = !client.order;
    let need_brackets = want_brackets && !client.brackets;
    if !need_order && !need_brackets {
        return ack_frame(request_id);
    }

    let key = order_key();
    let lock = topic_lock(state, &key).await;
    let _guard = lock.lock().await;
    let had_forwarder = attach_shared_topic(state, client, &key).await;

    let first_order = if need_order {
        state.reconnect.note_order().await
    } else {
        false
    };
    let first_brackets = if need_brackets {
        state.reconnect.note_brackets().await
    } else {
        false
    };

    if need_order {
        client.order = true;
    }
    if need_brackets {
        client.brackets = true;
    }

    let Some(session) = &state.session else {
        return fail_order_plant_stream(
            state,
            client,
            &key,
            had_forwarder,
            need_order,
            need_brackets,
            no_session_frame(request_id),
        )
        .await;
    };
    if !first_order && !first_brackets {
        return ack_frame(request_id);
    }

    let mut session_guard = session.lock().await;
    if first_order {
        if let Err(e) = session_guard.subscribe_order_updates().await {
            drop(session_guard);
            return fail_order_plant_stream(
                state,
                client,
                &key,
                had_forwarder,
                need_order,
                need_brackets,
                err_to_frame(request_id, "subscribe_order_updates_failed", e),
            )
            .await;
        }
    }
    if first_brackets {
        if let Err(e) = session_guard.subscribe_bracket_updates().await {
            drop(session_guard);
            return fail_order_plant_stream(
                state,
                client,
                &key,
                had_forwarder,
                need_order,
                need_brackets,
                err_to_frame(request_id, "subscribe_bracket_updates_failed", e),
            )
            .await;
        }
    }

    ack_frame(request_id)
}

pub(super) async fn disconnect_order(
    state: &GatewayState,
    client: &mut ClientCtx,
    request_id: u64,
) -> Frame {
    let key = order_key();
    let lock = topic_lock(state, &key).await;
    let _guard = lock.lock().await;
    let had_brackets = client.brackets;
    let last_brackets = clear_client_brackets(state, client).await;
    let mut last_order = false;
    if client.order {
        client.order = false;
        last_order = state.reconnect.forget_order().await;
        if last_order {
            if let Some(session) = &state.session {
                let mut guard = session.lock().await;
                if let Err(e) = guard.disconnect_order_plant().await {
                    drop(guard);
                    state.reconnect.note_order().await;
                    client.order = true;
                    if had_brackets {
                        state.reconnect.note_brackets().await;
                        client.brackets = true;
                    }
                    return err_to_frame(request_id, "disconnect_order_failed", e);
                }
            }
        }
    }
    // Last brackets peer left but order peers remain: no brackets
    // unsubscribe API — re-seat order-only to drop venue brackets.
    if last_brackets && !last_order && state.reconnect.restore_plan().await.order {
        if let Err(e) = reseat_order_plant_without_brackets(state).await {
            state.ready.store(false, Ordering::SeqCst);
            state.force_reconnect.store(true, Ordering::SeqCst);
            return err_to_frame(request_id, "reseat_order_failed", e);
        }
    }
    if client.subscribed.contains(&key) {
        client.drop_forwarder(&key);
        state.reconnect.remove_hub_interest(&key).await;
    }
    ack_frame(request_id)
}

pub(super) async fn reset_ticker_plant(state: &GatewayState, request_id: u64) -> Frame {
    let Some(session) = &state.session else {
        return no_session_frame(request_id);
    };
    let mut guard = session.lock().await;
    if let Err(e) = guard.reset_ticker_plant().await {
        return err_to_frame(request_id, "reset_ticker_plant_failed", e);
    }
    // The recreated ticker plant carries no subscriptions: re-issue
    // every remembered ticker / book / time-bar intent (pnl/order ride
    // their own plants and are untouched). On any failure fall back to
    // the parent full-reconnect path — which re-issues every intent —
    // rather than silently leaving the stream dark.
    let plan = state.reconnect.restore_plan().await;
    let mut failed: Option<String> = None;
    for key in &plan.ticker {
        if let Err(e) = guard.subscribe(&key.symbol, &key.exchange).await {
            failed = Some(e.to_string());
            break;
        }
    }
    if failed.is_none() {
        for key in &plan.book {
            if let Err(e) = guard
                .subscribe_order_book_summary(&key.symbol, &key.exchange)
                .await
            {
                failed = Some(e.to_string());
                break;
            }
        }
    }
    if failed.is_none() {
        for intent in &plan.time_bars {
            if let Err(e) = guard
                .subscribe_time_bars(
                    &intent.symbol,
                    &intent.exchange,
                    intent.bar_type,
                    intent.period,
                )
                .await
            {
                failed = Some(e.to_string());
                break;
            }
        }
    }
    if let Some(msg) = failed {
        drop(guard);
        state.force_reconnect.store(true, Ordering::SeqCst);
        return err_to_frame(request_id, "reset_ticker_plant_restore_failed", msg);
    }
    ack_frame(request_id)
}
