//! Consumer attachment: gates, intent ownership, rollback and read admission.
mod context;
pub(crate) mod subscriptions;

use crate::fanout::{Fan, SubKey};
use crate::host::{HistoryPolicy, HostState};
use crate::intents::TimeBarIntent;
use crate::{
    backend::*, dto::*, Error, PlantSet, Result, SessionHost, SessionId, Stream, StreamHub, Venue,
};
use async_trait::async_trait;
use context::SessionCtx;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use subscriptions::TopicIntent;
use tokio::sync::{mpsc, Mutex, RwLock};

pub(crate) fn gate_error(code: &str, message: &str) -> Error {
    Error::Gateway {
        code: code.into(),
        message: message.into(),
    }
}
pub(crate) fn no_venue() -> Error {
    gate_error("no_session", "gateway has no plant session")
}
pub(crate) fn coded_error(code: &str, error: Error) -> Error {
    match error {
        Error::Gateway { .. } => error,
        other => gate_error(code, &other.to_string()),
    }
}
async fn topic_lock<V: Venue + ?Sized + 'static>(
    state: &HostState<V>,
    key: &SubKey,
) -> Arc<Mutex<()>> {
    state.topic_lock(key).await
}

pub struct LocalSession<V: Venue + ?Sized + 'static = dyn Venue> {
    inner: Arc<LocalInner<V>>,
}
pub(crate) struct LocalInner<V: Venue + ?Sized + 'static> {
    host: Arc<SessionHost<V>>,
    can_open_gates: bool,
    id: SessionId,
    client: Mutex<SessionCtx>,
    output: std::sync::Mutex<Option<mpsc::Receiver<Fan>>>,
    streams: Arc<StreamHub>,
    delivery: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    detached: AtomicBool,
    native: AtomicBool,
    order_closed: AtomicBool,
    disconnecting_order: AtomicUsize,
    order_gate: RwLock<()>,
    events: tokio::sync::broadcast::Sender<crate::SessionEvent>,
}
impl<V: Venue + ?Sized + 'static> LocalSession<V> {
    pub(crate) fn attach(host: Arc<SessionHost<V>>, can_open_gates: bool) -> Self {
        host.attachments.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel(host.state.hub.capacity());
        let id = SessionId::new();
        host.state.hub.native_attached();
        let session = Self {
            inner: Arc::new(LocalInner {
                host,
                can_open_gates,
                id,
                client: Mutex::new(SessionCtx::with_id(tx, id)),
                output: std::sync::Mutex::new(Some(rx)),
                streams: StreamHub::new(),
                delivery: std::sync::Mutex::new(None),
                detached: AtomicBool::new(false),
                native: AtomicBool::new(true),
                order_closed: AtomicBool::new(false),
                disconnecting_order: AtomicUsize::new(0),
                order_gate: RwLock::new(()),
                events: tokio::sync::broadcast::channel(crate::events::SESSION_EVENT_CAPACITY).0,
            }),
        };
        session
            .state()
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, Arc::downgrade(&session.inner));
        session
    }
    pub(crate) fn from_inner(inner: Arc<LocalInner<V>>) -> Self {
        Self { inner }
    }
    pub(crate) async fn notify_event(&self, event: crate::SessionEvent) {
        if self.inner.detached.load(Ordering::SeqCst) {
            return;
        }
        if self.inner.native.load(Ordering::SeqCst) {
            self.ensure_native_delivery();
        }
        self.inner
            .client
            .lock()
            .await
            .flush(&self.state().hub)
            .await;
        if self.inner.detached.load(Ordering::SeqCst) {
            return;
        }
        let failed = matches!(event, crate::SessionEvent::RecoveryFailed { .. });
        let _ = self.inner.events.send(event);
        if failed {
            self.close_streams();
        }
    }
    pub(crate) fn close_streams(&self) {
        self.ensure_native_delivery();
        for stream in Stream::ALL {
            self.inner.streams.close(stream);
        }
    }
    pub fn id(&self) -> SessionId {
        self.inner.id
    }
    pub fn host(&self) -> &Arc<SessionHost<V>> {
        &self.inner.host
    }
    pub async fn resolved_account_result(&self) -> Result<Option<AccountTriple>> {
        self.active()?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        Ok(venue.lock().await.resolved_account().await)
    }
    pub async fn live_md_intent_counts(&self) -> (u32, u32, u32) {
        self.state().reconnect.live_md_intent_counts().await
    }
    fn state(&self) -> &HostState<V> {
        &self.inner.host.state
    }
    fn active(&self) -> Result<()> {
        if self.inner.detached.load(Ordering::SeqCst) {
            return Err(Error::NotSent("session detached".into()));
        }
        self.state().check_ready()
    }
    fn require_gate(&self, method: &str) -> Result<()> {
        use crate::rpc_table::Gate;
        let gate = crate::rpc_table::RPC_TABLE
            .iter()
            .find(|r| r.method == method)
            .expect("RPC table method")
            .gate;
        let what = match method {
            "ensure_order_plant" => "ensure_order",
            other => other,
        };
        match gate {
            Gate::Trading if !self.state().gates.snapshot().trading => Err(gate_error(
                "trading_disabled",
                &format!("{what} denied: parent trading disabled"),
            )),
            Gate::CancelAll if !self.state().gates.snapshot().cancel_all => Err(gate_error(
                "cancel_all_denied",
                &format!("{what} denied: parent cancel_all disabled"),
            )),
            _ => Ok(()),
        }
    }
    async fn command_guard(&self, method: &str) -> Result<tokio::sync::RwLockReadGuard<'_, ()>> {
        self.active()?;
        self.require_gate(method)?;
        if self.inner.disconnecting_order.load(Ordering::SeqCst) != 0 {
            return Err(Error::NotSent(DISCONNECT_IN_PROGRESS.into()));
        }
        let guard = self.inner.order_gate.read().await;
        self.active()?;
        if self.inner.disconnecting_order.load(Ordering::SeqCst) != 0 {
            return Err(Error::NotSent(DISCONNECT_IN_PROGRESS.into()));
        }
        if self.inner.order_closed.load(Ordering::SeqCst) {
            return Err(Error::NotSent(ORDER_PLANT_CLOSED.into()));
        }
        Ok(guard)
    }
    async fn subscribe_topic(&self, intent: TopicIntent) -> Result<()> {
        self.active()?;
        let mut client = self.inner.client.lock().await;
        self.active()?;
        subscriptions::subscribe_topic(self.state(), &mut client, intent).await
    }
    async fn unsubscribe_topic(&self, intent: TopicIntent) -> Result<()> {
        self.active()?;
        let mut client = self.inner.client.lock().await;
        self.active()?;
        subscriptions::unsubscribe_topic(self.state(), &mut client, intent).await
    }
    async fn subscribe_orders(&self, brackets: bool) -> Result<()> {
        self.active()?;
        let _gate = self.inner.order_gate.write().await;
        let msg = if brackets {
            "subscribe_bracket_updates denied: parent trading disabled"
        } else {
            "subscribe_order_updates denied: parent trading disabled"
        };
        let mut client = self.inner.client.lock().await;
        self.active()?;
        subscriptions::subscribe_order_plant_stream(self.state(), &mut client, msg, brackets)
            .await?;
        self.inner.order_closed.store(false, Ordering::SeqCst);
        Ok(())
    }
    /// Transfer the attachment's shared event records to a transport. Must be
    /// called before native polling. Encoding is a consumer concern.
    pub fn take_events(&self) -> Option<mpsc::Receiver<Fan>> {
        let output = self.take_output()?;
        if self.inner.native.swap(false, Ordering::SeqCst) {
            self.state().hub.native_detached();
        }
        Some(output)
    }
    fn take_output(&self) -> Option<mpsc::Receiver<Fan>> {
        self.inner
            .output
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }
    fn ensure_native_delivery(&self) {
        let mut delivery = self
            .inner
            .delivery
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if delivery.is_some() {
            return;
        }
        let Some(mut rx) = self.take_output() else {
            return;
        };
        let streams = self.inner.streams.clone();
        let txs: Vec<_> = Stream::ALL.into_iter().map(|s| streams.attach(s)).collect();
        *delivery = Some(tokio::spawn(async move {
            while let Some(fan) = rx.recv().await {
                if fan.acknowledge_flush() {
                    continue;
                }
                let Some(event) = fan.event() else {
                    continue;
                };
                let stream = match event {
                    PlantEvent::TimeBar(_) => Stream::History,
                    PlantEvent::AccountPnl(_) | PlantEvent::InstrumentPnl(_) => Stream::Pnl,
                    PlantEvent::OrderNotification(_) | PlantEvent::BracketUpdate(_) => {
                        Stream::Order
                    }
                    PlantEvent::Gap(g) => g.stream,
                    _ => Stream::Ticker,
                };
                txs.iter()
                    .find(|tx| tx.stream() == stream)
                    .expect("stream sender")
                    .event(event.clone())
                    .await;
            }
            for tx in txs {
                tx.close();
            }
        }));
    }
    async fn command<T>(&self, call: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        let mut interruption = self.state().interruption.subscribe();
        self.active()?;
        tokio::select! {
            biased;
            _ = interruption.changed() => Err(Error::OutcomeUnknown("venue interrupted during command; reconcile before acting".into())),
            result = call => result,
        }
    }
    async fn history_admission(&self) -> Result<()> {
        let _gate = self.state().md_history_gate.lock().await;
        if self.state().history_policy == HistoryPolicy::RefuseUnderLiveMd
            && self.state().reconnect.has_live_md_intents().await
        {
            return Err(gate_error("history_denied_live_md", "history Load* refused while live ticker/book/time-bar intents are active; use a separate gateway or stop live MD (live XOR heavy ingest)"));
        }
        self.active()
    }
}
impl<V: Venue + ?Sized + 'static> Clone for LocalSession<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<V: Venue + ?Sized + 'static> Drop for LocalInner<V> {
    fn drop(&mut self) {
        self.host
            .state
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
        if self.native.swap(false, Ordering::SeqCst) {
            self.host.state.hub.native_detached();
        }
        if let Some(task) = self
            .delivery
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            task.abort();
        }
        let mut client =
            std::mem::replace(self.client.get_mut(), SessionCtx::new(mpsc::channel(1).0));
        for task in client.forwarders.values() {
            task.abort();
        }
        if !self.detached.swap(true, Ordering::SeqCst) {
            let host = self.host.clone();
            if let Some(runtime) = &self.host.runtime {
                runtime.spawn(async move {
                    client.teardown(&host.state).await;
                    host.detached().await;
                });
            }
        }
    }
}

#[async_trait]
impl<V: Venue + ?Sized + 'static> Session for LocalSession<V> {
    async fn connect(&self) -> Result<()> {
        if self.inner.detached.load(Ordering::SeqCst) {
            return Err(Error::NotSent("session detached".into()));
        }
        self.inner.host.connect().await
    }
    async fn disconnect(&self) -> Result<()> {
        if self.inner.detached.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let _gate = self.inner.order_gate.write().await;
        self.inner.order_closed.store(true, Ordering::SeqCst);
        let mut client = self.inner.client.lock().await;
        let mut detached = std::mem::replace(&mut *client, SessionCtx::new(mpsc::channel(1).0));
        detached.teardown(self.state()).await;
        // Dropping its sender closes delivery after queued records drain.
        drop(detached);
        self.inner.host.detached().await;
        Ok(())
    }
    async fn request_plants(&self, extra: PlantSet) -> Result<()> {
        if self.inner.detached.load(Ordering::SeqCst) {
            return Err(Error::NotSent("session detached".into()));
        }
        self.inner
            .host
            .request_plants(extra)
            .await
            .map_err(|e| coded_error("request_plants_failed", e))
    }
    async fn ensure_order_plant(&self) -> Result<()> {
        self.active()?;
        self.require_gate("ensure_order_plant")?;
        let _gate = self.inner.order_gate.write().await;
        self.active()?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        venue
            .lock()
            .await
            .ensure_order_plant()
            .await
            .map_err(|e| coded_error("ensure_order_failed", e))?;
        self.inner.order_closed.store(false, Ordering::SeqCst);
        Ok(())
    }
    async fn ensure_pnl_plant(&self) -> Result<()> {
        self.active()?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        venue
            .lock()
            .await
            .ensure_pnl_plant()
            .await
            .map_err(|e| coded_error("ensure_pnl_failed", e))
    }
    async fn disconnect_order_plant(&self) -> Result<()> {
        self.active()?;
        struct Closing<'a>(&'a AtomicUsize);
        impl Drop for Closing<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        self.inner
            .disconnecting_order
            .fetch_add(1, Ordering::SeqCst);
        let _closing = Closing(&self.inner.disconnecting_order);
        self.inner.order_closed.store(true, Ordering::SeqCst);
        let _gate = self.inner.order_gate.write().await;
        subscriptions::disconnect_order(self.state(), &mut *self.inner.client.lock().await).await
    }
    async fn disconnect_pnl_plant(&self) -> Result<()> {
        self.active()?;
        subscriptions::disconnect_pnl(self.state(), &mut *self.inner.client.lock().await).await
    }
    async fn open_gates(&self, trading: bool, cancel_all: bool) -> crate::Gates {
        if self.inner.can_open_gates
            && !self.inner.detached.load(Ordering::SeqCst)
            && !self.inner.host.is_terminal()
        {
            self.state().gates.open(trading, cancel_all)
        } else {
            self.state().gates.snapshot()
        }
    }
    async fn resolved_account(&self) -> Option<AccountTriple> {
        if self.active().is_err() {
            return None;
        }
        match &self.state().session {
            Some(s) => s.lock().await.resolved_account().await,
            None => None,
        }
    }
    async fn subscribe(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.subscribe_topic(TopicIntent::Ticker(SubKey {
            symbol: symbol.into(),
            exchange: exchange.into(),
        }))
        .await
    }
    async fn unsubscribe(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.unsubscribe_topic(TopicIntent::Ticker(SubKey {
            symbol: symbol.into(),
            exchange: exchange.into(),
        }))
        .await
    }
    async fn subscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.subscribe_topic(TopicIntent::Book(SubKey {
            symbol: symbol.into(),
            exchange: exchange.into(),
        }))
        .await
    }
    async fn unsubscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.unsubscribe_topic(TopicIntent::Book(SubKey {
            symbol: symbol.into(),
            exchange: exchange.into(),
        }))
        .await
    }
    async fn subscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> Result<()> {
        self.subscribe_topic(TopicIntent::TimeBars(TimeBarIntent {
            symbol: symbol.into(),
            exchange: exchange.into(),
            bar_type,
            period,
        }))
        .await
    }
    async fn unsubscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> Result<()> {
        self.unsubscribe_topic(TopicIntent::TimeBars(TimeBarIntent {
            symbol: symbol.into(),
            exchange: exchange.into(),
            bar_type,
            period,
        }))
        .await
    }
    async fn subscribe_pnl(&self) -> Result<()> {
        self.active()?;
        let mut client = self.inner.client.lock().await;
        self.active()?;
        subscriptions::subscribe_pnl(self.state(), &mut client).await
    }
    async fn subscribe_order_updates(&self) -> Result<()> {
        self.subscribe_orders(false).await
    }
    async fn subscribe_bracket_updates(&self) -> Result<()> {
        self.subscribe_orders(true).await
    }
    async fn get_front_month(&self, symbol: &str, exchange: &str) -> Result<FrontMonthDto> {
        self.require_gate("get_front_month")?;
        self.active()?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.lock().await;
        guard
            .get_front_month(symbol, exchange)
            .await
            .map_err(|e| match e {
                Error::ReconciliationUnavailable(_) => e,
                other => coded_error("get_front_month_failed", other),
            })
    }
    async fn get_reference_data(&self, symbol: &str, exchange: &str) -> Result<ReferenceDataDto> {
        self.require_gate("get_reference_data")?;
        self.active()?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.lock().await;
        guard
            .get_reference_data(symbol, exchange)
            .await
            .map_err(|e| match e {
                Error::ReconciliationUnavailable(_) => e,
                other => coded_error("get_reference_data_failed", other),
            })
    }
    async fn load_ticks(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<HistoryTickDto>> {
        self.require_gate("load_ticks")?;
        let _turn = self.state().history_load_lock.lock().await;
        self.history_admission().await?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.checkout().await;
        guard
            .load_ticks(symbol, exchange, start_time_sec, end_time_sec)
            .await
            .map_err(|e| match e {
                Error::ReconciliationUnavailable(_) => e,
                other => coded_error("load_ticks_failed", other),
            })
    }
    async fn load_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
        bar_type: i32,
        period: i32,
    ) -> Result<Vec<HistoryBarDto>> {
        self.require_gate("load_time_bars")?;
        let _turn = self.state().history_load_lock.lock().await;
        self.history_admission().await?;
        crate::history::parse_time_bar_type(bar_type)
            .map_err(|e| coded_error("invalid_bar_type", e))?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.checkout().await;
        guard
            .load_time_bars(
                symbol,
                exchange,
                start_time_sec,
                end_time_sec,
                bar_type,
                period,
            )
            .await
            .map_err(|e| match e {
                Error::ReconciliationUnavailable(_) => e,
                other => coded_error("load_time_bars_failed", other),
            })
    }
    async fn probe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
        bar_type: i32,
        period: i32,
    ) -> Result<Vec<TimeBarProbeRow>> {
        self.require_gate("probe_time_bars")?;
        let _turn = self.state().history_load_lock.lock().await;
        self.history_admission().await?;
        crate::history::parse_time_bar_type(bar_type)
            .map_err(|e| coded_error("invalid_bar_type", e))?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.checkout().await;
        guard
            .probe_time_bars(
                symbol,
                exchange,
                start_time_sec,
                end_time_sec,
                bar_type,
                period,
            )
            .await
            .map_err(|e| match e {
                Error::ReconciliationUnavailable(_) => e,
                other => coded_error("probe_time_bars_failed", other),
            })
    }
    async fn load_orders(&self, start_sec: i32, end_sec: i32) -> Result<Vec<OrderNotificationDto>> {
        self.require_gate("load_orders")?;
        self.active()?;
        let _recon = self.state().recon_lock.lock().await;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.checkout().await;
        let _ = (start_sec, end_sec);
        guard.load_orders(0, 0).await.map_err(|e| match e {
            Error::ReconciliationUnavailable(_) => e,
            other => coded_error("load_orders_failed", other),
        })
    }
    async fn load_product_rms_info(&self) -> Result<Vec<ProductRmsInfoDto>> {
        self.require_gate("load_product_rms_info")?;
        self.active()?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.checkout().await;
        guard.load_product_rms_info().await.map_err(|e| match e {
            Error::ReconciliationUnavailable(_) => e,
            other => coded_error("fetch_rms_failed", other),
        })
    }
    async fn load_account_rms_info(&self) -> Result<Vec<AccountRmsInfoDto>> {
        self.require_gate("load_account_rms_info")?;
        self.active()?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.checkout().await;
        guard.load_account_rms_info().await.map_err(|e| match e {
            Error::ReconciliationUnavailable(_) => e,
            other => coded_error("fetch_rms_failed", other),
        })
    }
    async fn load_fill_history(&self, start: i32, finish: i32) -> Result<Vec<FillHistoryDto>> {
        self.require_gate("load_fill_history")?;
        self.active()?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.checkout().await;
        guard
            .load_fill_history(start, finish)
            .await
            .map_err(|e| match e {
                Error::ReconciliationUnavailable(_) => e,
                other => coded_error("load_fill_history_failed", other),
            })
    }
    async fn load_order_history(&self, date: &str) -> Result<Vec<OrderNotificationDto>> {
        self.require_gate("load_order_history")?;
        self.active()?;
        let _recon = self.state().recon_lock.lock().await;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.checkout().await;
        guard.load_order_history(date).await.map_err(|e| match e {
            Error::ReconciliationUnavailable(_) => e,
            other => coded_error("load_order_history_failed", other),
        })
    }
    async fn load_order_history_dates(&self) -> Result<Vec<String>> {
        self.require_gate("load_order_history_dates")?;
        self.active()?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.checkout().await;
        guard.load_order_history_dates().await.map_err(|e| match e {
            Error::ReconciliationUnavailable(_) => e,
            other => coded_error("load_order_history_dates_failed", other),
        })
    }
    async fn place_order(&self, order: &PlaceOrder) -> Result<()> {
        let _op = self.command_guard("place_order").await?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.lock().await;
        self.command(guard.place_order(order)).await
    }
    async fn place_bracket_order(&self, order: &PlaceBracketOrder) -> Result<()> {
        let _op = self.command_guard("place_bracket_order").await?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.lock().await;
        self.command(guard.place_bracket_order(order)).await
    }
    async fn adjust_bracket_stop(
        &self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()> {
        let _op = self.command_guard("adjust_bracket_stop").await?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.lock().await;
        self.command(guard.adjust_bracket_stop(basket_id, ticks, level))
            .await
    }
    async fn adjust_bracket_target(
        &self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()> {
        let _op = self.command_guard("adjust_bracket_target").await?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.lock().await;
        self.command(guard.adjust_bracket_target(basket_id, ticks, level))
            .await
    }
    async fn cancel_order(&self, basket_id: &str) -> Result<()> {
        let _op = self.command_guard("cancel_order").await?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.lock().await;
        self.command(guard.cancel_order(basket_id)).await
    }
    async fn modify_order(&self, order: &ModifyOrder) -> Result<()> {
        let _op = self.command_guard("modify_order").await?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.lock().await;
        self.command(guard.modify_order(order)).await
    }
    async fn cancel_all_orders(&self) -> Result<()> {
        let _op = self.command_guard("cancel_all_orders").await?;
        let venue = self.state().session.as_ref().ok_or_else(no_venue)?;
        let guard = venue.lock().await;
        self.command(guard.cancel_all_orders()).await
    }
    async fn poll_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        // A session that has never connected still reports the legacy typed
        // error. A recovering host keeps the consumer's stream alive instead.
        if !self.state().ready.load(Ordering::SeqCst)
            && !self.state().recovering.load(Ordering::SeqCst)
            && !self.inner.host.is_terminal()
        {
            return Err(Error::NotConnected { plant: "ticker" });
        }
        self.ensure_native_delivery();
        self.inner.streams.poll(Stream::Ticker, timeout).await
    }
    async fn poll_history_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.ensure_native_delivery();
        self.inner.streams.poll(Stream::History, timeout).await
    }
    async fn poll_pnl_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.ensure_native_delivery();
        self.inner.streams.poll(Stream::Pnl, timeout).await
    }
    async fn poll_order_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.ensure_native_delivery();
        self.inner.streams.poll(Stream::Order, timeout).await
    }
    fn session_events(&self) -> tokio::sync::broadcast::Receiver<crate::SessionEvent> {
        self.inner.events.subscribe()
    }
    async fn next_events(
        &self,
        stream: Stream,
        max: usize,
        timeout: Duration,
    ) -> Result<Vec<PlantEvent>> {
        self.ensure_native_delivery();
        self.inner.streams.next_events(stream, max, timeout).await
    }
}
