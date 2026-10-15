//! Subscription-intent RPCs (ticker / book / time bars / pnl / order+brackets
//! streams): hub fanout attach, reconnect-intent refcounting, venue join /
//! leave, and rollback on failure. The failure templates for join vs leave are
//! deliberately separate — subscribe rolls back on first-join failure;
//! unsubscribe re-notes the intent on teardown failure so reconnect still
//! re-joins. Do not merge them.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::fanout::{order_key, pnl_key};

use crate::fanout::SubKey;
use crate::intents::{MdIntent, ReconnectController, TimeBarIntent};

use super::{coded_error, gate_error, no_venue, topic_lock, HostState, Result, SessionCtx, Venue};

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
pub(crate) enum TopicIntent {
    Ticker(SubKey),
    Book(SubKey),
    TimeBars(TimeBarIntent),
}

impl TopicIntent {
    /// This intent as the hashable key the controller tracks degradation by.
    pub(crate) fn md_intent(&self) -> MdIntent {
        match self {
            Self::Ticker(key) => MdIntent::Ticker(key.clone()),
            Self::Book(key) => MdIntent::Book(key.clone()),
            Self::TimeBars(intent) => MdIntent::TimeBars(intent.clone()),
        }
    }

    pub(crate) fn from_md(intent: &MdIntent) -> Self {
        match intent {
            MdIntent::Ticker(key) => Self::Ticker(key.clone()),
            MdIntent::Book(key) => Self::Book(key.clone()),
            MdIntent::TimeBars(intent) => Self::TimeBars(intent.clone()),
        }
    }

    pub(crate) fn key(&self) -> SubKey {
        match self {
            Self::Ticker(key) | Self::Book(key) => key.clone(),
            Self::TimeBars(intent) => sub_key(&intent.symbol, &intent.exchange),
        }
    }

    fn client_has(&self, client: &SessionCtx) -> bool {
        match self {
            Self::Ticker(key) => client.ticker.contains(key),
            Self::Book(key) => client.book.contains(key),
            Self::TimeBars(intent) => client.time_bars.contains(intent),
        }
    }

    fn client_insert(&self, client: &mut SessionCtx) {
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
        client.sync_kinds(&self.key());
    }

    fn client_remove(&self, client: &mut SessionCtx) {
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
        client.sync_kinds(&self.key());
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
    pub(crate) async fn forget(&self, rc: &ReconnectController) -> bool {
        match self {
            Self::Ticker(key) => rc.forget_ticker(key).await,
            Self::Book(key) => rc.forget_book(key).await,
            Self::TimeBars(intent) => rc.forget_time_bar(intent).await,
        }
    }

    pub(crate) async fn venue_join<V: Venue + ?Sized>(&self, session: &V) -> crate::Result<()> {
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

    pub(crate) async fn venue_leave<V: Venue + ?Sized>(&self, session: &V) -> crate::Result<()> {
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
    pub(crate) fn resubscribe_fail_log(&self, e: crate::Error) -> String {
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
/// Returns whether this client already had delivery (no new hub bump).
async fn attach_shared_topic<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
    client: &mut SessionCtx,
    key: &SubKey,
) -> bool {
    let had_delivery = client.subscribed.contains(key);
    if !had_delivery {
        state.reconnect.add_hub_interest(key.clone()).await;
    }
    client.ensure_delivery(&state.hub, key).await;
    had_delivery
}

async fn rollback_shared_topic<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
    client: &mut SessionCtx,
    key: &SubKey,
    had_delivery: bool,
) {
    if !had_delivery {
        client.drop_delivery(key);
        state.reconnect.remove_hub_interest(key).await;
    }
}

async fn release_md_if_unused<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
    client: &mut SessionCtx,
    key: &SubKey,
) {
    if !client.wants_md(key) {
        client.drop_delivery(key);
        if state.reconnect.remove_hub_interest(key).await {
            prune_topic_lock(state, key).await;
        }
    }
}

async fn prune_topic_lock<V: Venue + ?Sized + 'static>(state: &HostState<V>, key: &SubKey) {
    let mut map = state.topic_locks.lock().await;
    if let Some(lock) = map.get(key) {
        // Only the map holds the Arc → safe to drop.
        if Arc::strong_count(lock) == 1 {
            map.remove(key);
        }
    }
}

pub(super) async fn subscribe_topic<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
    client: &mut SessionCtx,
    intent: TopicIntent,
) -> Result<()> {
    let key = intent.key();
    if intent.client_has(client) {
        if state.reconnect.is_degraded(&intent.md_intent()).await {
            return rejoin_degraded(state, intent).await;
        }
        // A repeat book subscribe is the consumer's gap resync. Refcounts stay
        // unchanged, but every holder must see a fresh venue snapshot.
        if let TopicIntent::Book(key) = &intent {
            let lock = topic_lock(state, key).await;
            let _guard = lock.lock().await;
            let venue = state.session.as_ref().ok_or_else(no_venue)?;
            let guard = venue.lock().await;
            state.check_ready()?;
            if let Err(e) = guard.refresh_book(&key.symbol, &key.exchange).await {
                state.reconnect.mark_degraded(intent.md_intent()).await;
                return Err(coded_error(intent.subscribe_failed_code(), e));
            }
        }
        return Ok(());
    }
    // Hold md_history_gate across note + venue join so a history load cannot
    // pass its live-MD check while this intent is being created (RC2.3). A load
    // holds the gate only for that check, so this never waits for a load.
    // Lock order: md_history_gate → topic_lock → session.
    let _md_gate = state.md_history_gate.lock().await;
    let lock = topic_lock(state, &key).await;
    let _guard = lock.lock().await;
    let had_delivery = attach_shared_topic(state, client, &key).await;
    // Count first, then record on the connection. A failure between the two
    // leaks one count (the venue join outlives its holders until the parent's
    // next rebuild); the reverse order would let teardown release a count that
    // was never taken and end another client's subscription.
    let is_first = intent.note(&state.reconnect).await;
    intent.client_insert(client);
    // A later client joining an intent a rebuild left degraded retries the venue
    // join too: the other holders' join is dark, so this one would get nothing.
    let degraded = !is_first && state.reconnect.is_degraded(&intent.md_intent()).await;
    let refresh = !is_first && matches!(intent, TopicIntent::Book(_));
    if is_first || degraded || refresh {
        let Some(session) = &state.session else {
            intent.client_remove(client);
            intent.forget(&state.reconnect).await;
            rollback_shared_topic(state, client, &key, had_delivery).await;
            return Err(no_venue());
        };
        // MD joins take `&self`; holding the guard only serializes against
        // plant reset / teardown.
        let session_guard = session.lock().await;
        let joined = match state.check_ready() {
            Err(e) => Err(e),
            Ok(()) if refresh => session_guard.refresh_book(&key.symbol, &key.exchange).await,
            Ok(()) => intent.venue_join(&*session_guard).await,
        };
        if let Err(e) = joined {
            if refresh {
                state.reconnect.mark_degraded(intent.md_intent()).await;
            }
            drop(session_guard);
            intent.client_remove(client);
            intent.forget(&state.reconnect).await;
            rollback_shared_topic(state, client, &key, had_delivery).await;
            return Err(coded_error(intent.subscribe_failed_code(), e));
        }
        if degraded {
            state.reconnect.clear_degraded(&intent.md_intent()).await;
        }
    }
    Ok(())
}

/// Retry the venue join of an intent a rebuild left degraded, on behalf of a client
/// that already holds it. Clears the degradation on success; on failure the client
/// gets the venue's own error and the intent stays degraded for the next try.
pub(crate) async fn rejoin_degraded<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
    intent: TopicIntent,
) -> Result<()> {
    let key = intent.key();
    let _md_gate = state.md_history_gate.lock().await;
    let lock = topic_lock(state, &key).await;
    let _guard = lock.lock().await;
    // Another client's retry may have healed it while this one waited.
    if !state.reconnect.is_degraded(&intent.md_intent()).await {
        return Ok(());
    }
    let Some(session) = &state.session else {
        return Err(no_venue());
    };
    let session_guard = session.lock().await;
    state.check_ready()?;
    // A failed snapshot refresh may have left the venue join live. Rithmic
    // rejects bare repeat book subscribes, so use the snapshot-on-join path.
    let joined = match &intent {
        TopicIntent::Book(key) => session_guard.refresh_book(&key.symbol, &key.exchange).await,
        _ => intent.venue_join(&*session_guard).await,
    };
    match joined {
        Ok(()) => {
            state.reconnect.clear_degraded(&intent.md_intent()).await;
            Ok(())
        }
        Err(e) => Err(coded_error(intent.subscribe_failed_code(), e)),
    }
}

pub(super) async fn unsubscribe_topic<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
    client: &mut SessionCtx,
    intent: TopicIntent,
) -> Result<()> {
    let key = intent.key();
    if !intent.client_has(client) {
        return Ok(());
    }
    // Same gate as subscribe/history so a failed leave re-note cannot race
    // a history admission check (RC2.3).
    let _md_gate = state.md_history_gate.lock().await;
    // Hold topic_lock across forget + venue teardown so a concurrent
    // first-peer Subscribe cannot have its venue join undone.
    let lock = topic_lock(state, &key).await;
    let _guard = lock.lock().await;
    if !intent.client_has(client) {
        return Ok(());
    }
    intent.client_remove(client);
    let last = intent.forget(&state.reconnect).await;
    if last {
        if let Some(session) = &state.session {
            let session_guard = session.lock().await;
            if let Err(e) = intent.venue_leave(&*session_guard).await {
                drop(session_guard);
                // Restore intent so reconnect still re-joins venue.
                intent.note(&state.reconnect).await;
                intent.client_insert(client);
                return Err(coded_error(intent.unsubscribe_failed_code(), e));
            }
        }
    }
    release_md_if_unused(state, client, &key).await;
    Ok(())
}

pub(super) async fn subscribe_pnl<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
    client: &mut SessionCtx,
) -> Result<()> {
    let key = pnl_key();
    let lock = topic_lock(state, &key).await;
    let _guard = lock.lock().await;
    let venue = state.session.as_ref().ok_or_else(no_venue)?;
    let guard = venue.lock().await;
    state.check_ready()?;
    if client.pnl {
        return guard
            .pnl_snapshot()
            .await
            .map_err(|e| coded_error("subscribe_pnl_failed", e));
    }
    let had_delivery = attach_shared_topic(state, client, &key).await;
    let first = state.reconnect.note_pnl().await;
    client.pnl = true;
    let joined = if first {
        match guard.ensure_pnl_plant().await {
            Ok(()) => guard.subscribe_pnl().await,
            Err(e) => Err(e),
        }
    } else {
        guard.pnl_snapshot().await
    };
    if let Err(e) = joined {
        drop(guard);
        client.pnl = false;
        state.reconnect.forget_pnl().await;
        rollback_shared_topic(state, client, &key, had_delivery).await;
        return Err(coded_error("subscribe_pnl_failed", e));
    }
    Ok(())
}

/// Release `client`'s PnL intent and, for the last holder, disconnect the PnL
/// plant. With `rollback` a failed venue disconnect re-notes the intent (an
/// explicit disconnect reports the failure and keeps the intent); a dying
/// connection passes `false`. Same lock as `subscribe_pnl`: a first-peer join
/// must not have its venue subscribe undone by a leaving peer that saw the count
/// reach zero before it.
pub(super) async fn leave_pnl<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
    client: &mut SessionCtx,
    rollback: bool,
) -> std::result::Result<(), (&'static str, crate::Error)> {
    if !client.pnl {
        return Ok(());
    }
    let lock = topic_lock(state, &pnl_key()).await;
    let _guard = lock.lock().await;
    client.pnl = false;
    if !state.reconnect.forget_pnl().await {
        return Ok(());
    }
    let Some(session) = &state.session else {
        return Ok(());
    };
    let guard = session.lock().await;
    if let Err(e) = guard.disconnect_pnl_plant().await {
        drop(guard);
        if rollback {
            state.reconnect.note_pnl().await;
            client.pnl = true;
        }
        return Err(("disconnect_pnl_failed", e));
    }
    Ok(())
}

pub(super) async fn disconnect_pnl<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
    client: &mut SessionCtx,
) -> Result<()> {
    if let Err((code, e)) = leave_pnl(state, client, true).await {
        return Err(coded_error(code, e));
    }
    let key = pnl_key();
    if client.subscribed.contains(&key) {
        client.drop_delivery(&key);
        state.reconnect.remove_hub_interest(&key).await;
    }
    Ok(())
}

/// Order-plant notification streams (order updates / brackets): fanout + intent + restore.
/// Anything this RPC notes / attaches must roll back on failure so Error never
/// leaves a half-applied order+brackets intent.
async fn rollback_order_plant_stream_introductions<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
    client: &mut SessionCtx,
    key: &SubKey,
    had_delivery: bool,
    introduced_order: bool,
    introduced_brackets: bool,
) {
    if introduced_brackets {
        client.brackets = false;
        client.sync_kinds(&crate::fanout::order_key());
        let _ = state.reconnect.forget_brackets().await;
    }
    if !introduced_order {
        return;
    }
    client.order = false;
    let last = state.reconnect.forget_order().await;
    if last {
        if let Some(session) = &state.session {
            let guard = session.lock().await;
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
    rollback_shared_topic(state, client, key, had_delivery).await;
}

async fn fail_order_plant_stream<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
    client: &mut SessionCtx,
    key: &SubKey,
    had_delivery: bool,
    introduced_order: bool,
    introduced_brackets: bool,
    outcome: Result<()>,
) -> Result<()> {
    rollback_order_plant_stream_introductions(
        state,
        client,
        key,
        had_delivery,
        introduced_order,
        introduced_brackets,
    )
    .await;
    outcome
}

/// No venue unsubscribe for brackets. When the last brackets peer leaves and
/// order peers remain, disconnect + `subscribe_order_updates` to drop brackets,
/// then tell the order peers with an order gap so they reconcile the window.
pub(crate) async fn reseat_order_plant_without_brackets<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
) -> Result<()> {
    let Some(session) = &state.session else {
        return Ok(());
    };
    let reseated = {
        let guard = session.lock().await;
        let _ = guard.disconnect_order_plant().await;
        guard.subscribe_order_updates().await
    };
    // Order events in the window between the disconnect and the new subscription
    // were not delivered: every order peer reconciles on this gap (the count of
    // missed events is unknown, hence 0).
    crate::fanout::publish_plant_event(
        &state.hub,
        crate::dto::PlantEvent::Gap(crate::GapDto {
            stream: crate::Stream::Order,
            skipped: 0,
        }),
    )
    .await;
    reseated
}

/// Drop this client's brackets flag/intent. Returns whether this was the last
/// brackets peer (caller may need [`reseat_order_plant_without_brackets`]).
pub(super) async fn clear_client_brackets<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
    client: &mut SessionCtx,
) -> bool {
    if !client.brackets {
        return false;
    }
    client.brackets = false;
    client.sync_kinds(&crate::fanout::order_key());
    state.reconnect.forget_brackets().await
}

pub(super) async fn subscribe_order_plant_stream<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
    client: &mut SessionCtx,
    denied_msg: &str,
    want_brackets: bool,
) -> Result<()> {
    if !state.gates.snapshot().trading {
        return Err(gate_error("trading_disabled", denied_msg));
    }

    let need_order = !client.order;
    let need_brackets = want_brackets && !client.brackets;
    if !need_order && !need_brackets {
        return Ok(());
    }

    let key = order_key();
    let lock = topic_lock(state, &key).await;
    let _guard = lock.lock().await;
    let had_delivery = attach_shared_topic(state, client, &key).await;

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
        client.sync_kinds(&crate::fanout::order_key());
    }

    let Some(session) = &state.session else {
        return fail_order_plant_stream(
            state,
            client,
            &key,
            had_delivery,
            need_order,
            need_brackets,
            Err(no_venue()),
        )
        .await;
    };
    if !first_order && !first_brackets {
        return Ok(());
    }

    let session_guard = session.lock().await;
    if first_order {
        if let Err(e) = session_guard.subscribe_order_updates().await {
            drop(session_guard);
            return fail_order_plant_stream(
                state,
                client,
                &key,
                had_delivery,
                need_order,
                need_brackets,
                Err(coded_error("subscribe_order_updates_failed", e)),
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
                had_delivery,
                need_order,
                need_brackets,
                Err(coded_error("subscribe_bracket_updates_failed", e)),
            )
            .await;
        }
    }

    Ok(())
}

/// Release `client`'s order and bracket intents. Brackets and order share one
/// plant: the last holder of the order intent disconnects it; when only the last
/// brackets holder leaves while order holders remain, the plant is re-seated
/// order-only (the venue has no brackets unsubscribe). `rollback` as for
/// [`leave_pnl`]. A failed re-seat drops readiness and asks for a full rebuild.
pub(super) async fn leave_order<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
    client: &mut SessionCtx,
    rollback: bool,
) -> std::result::Result<(), (&'static str, crate::Error)> {
    if !client.order && !client.brackets {
        return Ok(());
    }
    let lock = topic_lock(state, &order_key()).await;
    let _guard = lock.lock().await;
    let had_brackets = client.brackets;
    let last_brackets = clear_client_brackets(state, client).await;
    let mut last_order = false;
    if client.order {
        client.order = false;
        last_order = state.reconnect.forget_order().await;
        if last_order {
            if let Some(session) = &state.session {
                let guard = session.lock().await;
                if let Err(e) = guard.disconnect_order_plant().await {
                    drop(guard);
                    if rollback {
                        state.reconnect.note_order().await;
                        client.order = true;
                        if had_brackets {
                            state.reconnect.note_brackets().await;
                            client.brackets = true;
                            client.sync_kinds(&crate::fanout::order_key());
                        }
                    }
                    return Err(("disconnect_order_failed", e));
                }
            }
        }
    }
    if last_brackets && !last_order && state.reconnect.restore_plan().await.order {
        if let Err(e) = reseat_order_plant_without_brackets(state).await {
            state.ready.store(false, Ordering::SeqCst);
            state.request_reconnect();
            return Err(("reseat_order_failed", e));
        }
    }
    Ok(())
}

pub(super) async fn disconnect_order<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
    client: &mut SessionCtx,
) -> Result<()> {
    if let Err((code, e)) = leave_order(state, client, true).await {
        return Err(coded_error(code, e));
    }
    let key = order_key();
    if client.subscribed.contains(&key) {
        client.drop_delivery(&key);
        state.reconnect.remove_hub_interest(&key).await;
    }
    Ok(())
}

pub(crate) async fn reset_ticker_plant<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
) -> Result<()> {
    let Some(session) = &state.session else {
        return Err(no_venue());
    };
    let guard = session.lock().await;
    if let Err(e) = guard.reset_ticker().await {
        return Err(coded_error("reset_ticker_plant_failed", e));
    }
    // The recreated ticker plant carries no subscriptions: re-issue every
    // remembered market-data intent through the same pass a plant rebuild uses
    // (which also clears the degraded flags of the joins it restores). PnL and
    // order ride their own plants and are untouched. On any failure fall back
    // to the full rebuild, which re-issues every intent, rather than leave the
    // stream dark.
    let pass = crate::pump::restore_pass(&*guard, &state.reconnect, None, false).await;
    if !pass.failed_md.is_empty() {
        drop(guard);
        state.request_reconnect();
        return Err(gate_error(
            "reset_ticker_plant_restore_failed",
            &format!(
                "{} of {} market-data joins failed after the ticker reset",
                pass.failed_md.len(),
                pass.attempted
            ),
        ));
    }
    Ok(())
}
