//! The single Rust definition of the plant surface.
//!
//! `Venue` is driven only by the host and implemented by RithmicVenue / FakeVenue.
//! `Session` is the consumer surface implemented by LocalSession / RemoteSession.
//! A temporary blanket forwarding implementation keeps the pre-group-5 direct
//! PyO3 root source-compatible until its registry and Python lock switch together.
//!
//! One-shot commands are never restored; subscription intents are sets on each
//! LocalSession and refcounted across the host. Event waits hold no venue gate.

use std::time::Duration;

use async_trait::async_trait;

use crate::dto::{
    AccountRmsInfoDto, FillHistoryDto, FrontMonthDto, HistoryBarDto, HistoryTickDto,
    OrderNotificationDto, PlantEvent, ProductRmsInfoDto, ReferenceDataDto, TimeBarProbeRow,
};
use crate::error::Result;
use crate::plants::PlantSet;
use crate::streams::Stream;

/// Resolved account triple after override or discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountTriple {
    pub account_id: String,
    pub fcm_id: String,
    pub ib_id: String,
}

/// A new order. `price_type` / `side` / `duration` are the venue strings
/// (`LIMIT`, `BUY`, `DAY`, ...); an empty `duration` means `DAY`.
#[derive(Debug, Clone, PartialEq)]
pub struct PlaceOrder {
    pub symbol: String,
    pub exchange: String,
    pub side: String,
    pub price_type: String,
    pub quantity: i32,
    pub user_tag: String,
    pub price: Option<f64>,
    pub trigger_price: Option<f64>,
    pub duration: String,
    pub trail_by_ticks: Option<i32>,
    pub trail_by_price_id: Option<i32>,
}

/// A plant bracket: entry plus `stop_ticks` and/or `target_ticks` from fill.
#[derive(Debug, Clone, PartialEq)]
pub struct PlaceBracketOrder {
    pub symbol: String,
    pub exchange: String,
    pub side: String,
    pub price_type: String,
    pub quantity: i32,
    pub localid: String,
    pub price: Option<f64>,
    pub trigger_price: Option<f64>,
    pub duration: String,
    pub stop_ticks: Option<i32>,
    pub target_ticks: Option<i32>,
}

/// An order modification addressed by basket id.
#[derive(Debug, Clone, PartialEq)]
pub struct ModifyOrder {
    pub basket_id: String,
    pub symbol: String,
    pub exchange: String,
    pub quantity: i32,
    pub price_type: String,
    pub price: Option<f64>,
    pub trigger_price: Option<f64>,
    pub trail_by_ticks: Option<i32>,
}

/// The plant surface shared by direct and gateway modes.
#[async_trait]
pub trait Session: Send + Sync {
    // --- lifecycle -------------------------------------------------------
    async fn connect(&self) -> Result<()>;
    async fn disconnect(&self) -> Result<()>;
    /// Union extra plants into the set (attach now when already connected).
    async fn request_plants(&self, extra: PlantSet) -> Result<()>;
    async fn ensure_order_plant(&self) -> Result<()>;
    async fn ensure_pnl_plant(&self) -> Result<()>;
    async fn disconnect_order_plant(&self) -> Result<()>;
    async fn disconnect_pnl_plant(&self) -> Result<()>;
    /// Recreate only the ticker plant; sibling plants stay untouched. The
    /// caller re-issues its ticker / book / time-bar intent afterwards.
    async fn reset_ticker(&self) -> Result<()>;
    /// `None` until an override or order-plant discovery resolves it.
    async fn resolved_account(&self) -> Option<AccountTriple>;

    // --- subscription intents --------------------------------------------
    async fn subscribe(&self, symbol: &str, exchange: &str) -> Result<()>;
    async fn unsubscribe(&self, symbol: &str, exchange: &str) -> Result<()>;
    async fn subscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()>;
    async fn unsubscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()>;
    async fn subscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> Result<()>;
    async fn unsubscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> Result<()>;
    async fn subscribe_pnl(&self) -> Result<()>;
    /// Rides the order-plant stream (never a one-shot plant call).
    async fn subscribe_order_updates(&self) -> Result<()>;
    /// Rides the order-plant stream (never a one-shot plant call).
    async fn subscribe_bracket_updates(&self) -> Result<()>;

    // --- reads -----------------------------------------------------------
    async fn get_front_month(&self, symbol: &str, exchange: &str) -> Result<FrontMonthDto>;
    async fn get_reference_data(&self, symbol: &str, exchange: &str) -> Result<ReferenceDataDto>;
    async fn load_ticks(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<HistoryTickDto>>;
    async fn load_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
        bar_type: i32,
        period: i32,
    ) -> Result<Vec<HistoryBarDto>>;
    /// Raw history-plant time-bar replay, including dropped rows (diagnostics).
    async fn probe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
        bar_type: i32,
        period: i32,
    ) -> Result<Vec<TimeBarProbeRow>>;
    async fn load_orders(&self, start_sec: i32, end_sec: i32) -> Result<Vec<OrderNotificationDto>>;
    async fn load_product_rms_info(&self) -> Result<Vec<ProductRmsInfoDto>>;
    async fn load_account_rms_info(&self) -> Result<Vec<AccountRmsInfoDto>>;
    /// Fills for trade dates `start..=finish` (CCYYMMDD). Fails closed with
    /// `ReconciliationUnavailable` at the venue row cap. Read-only.
    async fn load_fill_history(&self, start: i32, finish: i32) -> Result<Vec<FillHistoryDto>>;
    /// Replay one trade date (CCYYMMDD) as order notifications. Read-only.
    async fn load_order_history(&self, date: &str) -> Result<Vec<OrderNotificationDto>>;
    /// Trade dates (CCYYMMDD) the venue keeps order history for. Read-only.
    async fn load_order_history_dates(&self) -> Result<Vec<String>>;

    // --- one-shot commands (never retried; post-send failure is unknown) ---
    async fn place_order(&self, order: &PlaceOrder) -> Result<()>;
    async fn place_bracket_order(&self, order: &PlaceBracketOrder) -> Result<()>;
    async fn adjust_bracket_stop(
        &self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()>;
    async fn adjust_bracket_target(
        &self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()>;
    async fn cancel_order(&self, basket_id: &str) -> Result<()>;
    async fn modify_order(&self, order: &ModifyOrder) -> Result<()>;
    /// Plant-wide; never use it to clean up a single smoke order.
    async fn cancel_all_orders(&self) -> Result<()>;

    // --- event polling (`Duration::ZERO` = non-blocking drain) -------------
    async fn poll_event(&self, timeout: Duration) -> Result<Option<PlantEvent>>;
    async fn poll_history_event(&self, timeout: Duration) -> Result<Option<PlantEvent>>;
    async fn poll_pnl_event(&self, timeout: Duration) -> Result<Option<PlantEvent>>;
    async fn poll_order_event(&self, timeout: Duration) -> Result<Option<PlantEvent>>;

    // --- event-driven delivery ---------------------------------------------
    /// Wait up to `timeout` for the stream's first event (without any session
    /// lock), then return up to `max` already-queued events at once. A lag is
    /// returned in order as `PlantEvent::Gap`; a closed or failed plant is an
    /// error once the events ahead of it are drained. Returns an empty batch on
    /// timeout, and also while the plant is not attached.
    async fn next_events(
        &self,
        stream: Stream,
        max: usize,
        timeout: Duration,
    ) -> Result<Vec<PlantEvent>>;

    // --- lifecycle events ------------------------------------------------------
    /// This session's `Interrupted` / `Resumed` / `RecoveryFailed` events.
    /// `LocalSession` and `RemoteSession` report recovery; a bare venue never does.
    fn session_events(&self) -> tokio::sync::broadcast::Receiver<crate::events::SessionEvent> {
        crate::events::no_session_events()
    }
}

#[async_trait]
pub trait Venue: Send + Sync {
    // --- lifecycle -------------------------------------------------------
    async fn connect(&self) -> Result<()>;
    async fn disconnect(&self) -> Result<()>;
    /// Union extra plants into the set (attach now when already connected).
    async fn request_plants(&self, extra: PlantSet) -> Result<()>;
    async fn ensure_order_plant(&self) -> Result<()>;
    async fn ensure_pnl_plant(&self) -> Result<()>;
    async fn disconnect_order_plant(&self) -> Result<()>;
    async fn disconnect_pnl_plant(&self) -> Result<()>;
    /// Recreate only the ticker plant; sibling plants stay untouched. The
    /// caller re-issues its ticker / book / time-bar intent afterwards.
    async fn reset_ticker(&self) -> Result<()>;
    /// `None` until an override or order-plant discovery resolves it.
    async fn resolved_account(&self) -> Option<AccountTriple>;

    // --- subscription intents --------------------------------------------
    async fn subscribe(&self, symbol: &str, exchange: &str) -> Result<()>;
    async fn unsubscribe(&self, symbol: &str, exchange: &str) -> Result<()>;
    async fn subscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()>;
    async fn unsubscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()>;
    async fn subscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> Result<()>;
    async fn unsubscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> Result<()>;
    async fn subscribe_pnl(&self) -> Result<()>;
    /// Rides the order-plant stream (never a one-shot plant call).
    async fn subscribe_order_updates(&self) -> Result<()>;
    /// Rides the order-plant stream (never a one-shot plant call).
    async fn subscribe_bracket_updates(&self) -> Result<()>;

    // --- reads -----------------------------------------------------------
    async fn get_front_month(&self, symbol: &str, exchange: &str) -> Result<FrontMonthDto>;
    async fn get_reference_data(&self, symbol: &str, exchange: &str) -> Result<ReferenceDataDto>;
    async fn load_ticks(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<HistoryTickDto>>;
    async fn load_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
        bar_type: i32,
        period: i32,
    ) -> Result<Vec<HistoryBarDto>>;
    /// Raw history-plant time-bar replay, including dropped rows (diagnostics).
    async fn probe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
        bar_type: i32,
        period: i32,
    ) -> Result<Vec<TimeBarProbeRow>>;
    async fn load_orders(&self, start_sec: i32, end_sec: i32) -> Result<Vec<OrderNotificationDto>>;
    async fn load_product_rms_info(&self) -> Result<Vec<ProductRmsInfoDto>>;
    async fn load_account_rms_info(&self) -> Result<Vec<AccountRmsInfoDto>>;
    /// Fills for trade dates `start..=finish` (CCYYMMDD). Fails closed with
    /// `ReconciliationUnavailable` at the venue row cap. Read-only.
    async fn load_fill_history(&self, start: i32, finish: i32) -> Result<Vec<FillHistoryDto>>;
    /// Replay one trade date (CCYYMMDD) as order notifications. Read-only.
    async fn load_order_history(&self, date: &str) -> Result<Vec<OrderNotificationDto>>;
    /// Trade dates (CCYYMMDD) the venue keeps order history for. Read-only.
    async fn load_order_history_dates(&self) -> Result<Vec<String>>;

    // --- one-shot commands (never retried; post-send failure is unknown) ---
    async fn place_order(&self, order: &PlaceOrder) -> Result<()>;
    async fn place_bracket_order(&self, order: &PlaceBracketOrder) -> Result<()>;
    async fn adjust_bracket_stop(
        &self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()>;
    async fn adjust_bracket_target(
        &self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()>;
    async fn cancel_order(&self, basket_id: &str) -> Result<()>;
    async fn modify_order(&self, order: &ModifyOrder) -> Result<()>;
    /// Plant-wide; never use it to clean up a single smoke order.
    async fn cancel_all_orders(&self) -> Result<()>;

    // --- event polling (`Duration::ZERO` = non-blocking drain) -------------
    async fn poll_event(&self, timeout: Duration) -> Result<Option<PlantEvent>>;
    async fn poll_history_event(&self, timeout: Duration) -> Result<Option<PlantEvent>>;
    async fn poll_pnl_event(&self, timeout: Duration) -> Result<Option<PlantEvent>>;
    async fn poll_order_event(&self, timeout: Duration) -> Result<Option<PlantEvent>>;

    // --- event-driven delivery ---------------------------------------------
    /// Wait up to `timeout` for the stream's first event (without any session
    /// lock), then return up to `max` already-queued events at once. A lag is
    /// returned in order as `PlantEvent::Gap`; a closed or failed plant is an
    /// error once the events ahead of it are drained. Returns an empty batch on
    /// timeout, and also while the plant is not attached.
    async fn next_events(
        &self,
        stream: Stream,
        max: usize,
        timeout: Duration,
    ) -> Result<Vec<PlantEvent>>;
}

/// Refusal texts shared with [`crate::fake::FakeVenue`], so the two sides of
/// the session contract cannot drift.
pub const DISCONNECT_IN_PROGRESS: &str = "session disconnect in progress; order operation rejected";
pub const ORDER_PLANT_CLOSED: &str =
    "order plant was disconnected; ensure it (or subscribe to order updates) first";

// Transitional direct-root bridge, removed with group 5's atomic registry /
// Python-lock switch. Host code drives Venue exclusively; never this bridge.
#[async_trait]
impl<V: Venue + ?Sized> Session for V {
    async fn connect(&self) -> Result<()> {
        Venue::connect(self).await
    }
    async fn disconnect(&self) -> Result<()> {
        Venue::disconnect(self).await
    }
    async fn request_plants(&self, extra: PlantSet) -> Result<()> {
        Venue::request_plants(self, extra).await
    }
    async fn ensure_order_plant(&self) -> Result<()> {
        Venue::ensure_order_plant(self).await
    }
    async fn ensure_pnl_plant(&self) -> Result<()> {
        Venue::ensure_pnl_plant(self).await
    }
    async fn disconnect_order_plant(&self) -> Result<()> {
        Venue::disconnect_order_plant(self).await
    }
    async fn disconnect_pnl_plant(&self) -> Result<()> {
        Venue::disconnect_pnl_plant(self).await
    }
    async fn reset_ticker(&self) -> Result<()> {
        Venue::reset_ticker(self).await
    }
    async fn resolved_account(&self) -> Option<AccountTriple> {
        Venue::resolved_account(self).await
    }
    async fn subscribe(&self, symbol: &str, exchange: &str) -> Result<()> {
        Venue::subscribe(self, symbol, exchange).await
    }
    async fn unsubscribe(&self, symbol: &str, exchange: &str) -> Result<()> {
        Venue::unsubscribe(self, symbol, exchange).await
    }
    async fn subscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()> {
        Venue::subscribe_order_book_summary(self, symbol, exchange).await
    }
    async fn unsubscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()> {
        Venue::unsubscribe_order_book_summary(self, symbol, exchange).await
    }
    async fn subscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> Result<()> {
        Venue::subscribe_time_bars(self, symbol, exchange, bar_type, period).await
    }
    async fn unsubscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> Result<()> {
        Venue::unsubscribe_time_bars(self, symbol, exchange, bar_type, period).await
    }
    async fn subscribe_pnl(&self) -> Result<()> {
        Venue::subscribe_pnl(self).await
    }
    async fn subscribe_order_updates(&self) -> Result<()> {
        Venue::subscribe_order_updates(self).await
    }
    async fn subscribe_bracket_updates(&self) -> Result<()> {
        Venue::subscribe_bracket_updates(self).await
    }
    async fn get_front_month(&self, symbol: &str, exchange: &str) -> Result<FrontMonthDto> {
        Venue::get_front_month(self, symbol, exchange).await
    }
    async fn get_reference_data(&self, symbol: &str, exchange: &str) -> Result<ReferenceDataDto> {
        Venue::get_reference_data(self, symbol, exchange).await
    }
    async fn load_ticks(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<HistoryTickDto>> {
        Venue::load_ticks(self, symbol, exchange, start_time_sec, end_time_sec).await
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
        Venue::load_time_bars(
            self,
            symbol,
            exchange,
            start_time_sec,
            end_time_sec,
            bar_type,
            period,
        )
        .await
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
        Venue::probe_time_bars(
            self,
            symbol,
            exchange,
            start_time_sec,
            end_time_sec,
            bar_type,
            period,
        )
        .await
    }
    async fn load_orders(&self, start_sec: i32, end_sec: i32) -> Result<Vec<OrderNotificationDto>> {
        Venue::load_orders(self, start_sec, end_sec).await
    }
    async fn load_product_rms_info(&self) -> Result<Vec<ProductRmsInfoDto>> {
        Venue::load_product_rms_info(self).await
    }
    async fn load_account_rms_info(&self) -> Result<Vec<AccountRmsInfoDto>> {
        Venue::load_account_rms_info(self).await
    }
    async fn load_fill_history(&self, start: i32, finish: i32) -> Result<Vec<FillHistoryDto>> {
        Venue::load_fill_history(self, start, finish).await
    }
    async fn load_order_history(&self, date: &str) -> Result<Vec<OrderNotificationDto>> {
        Venue::load_order_history(self, date).await
    }
    async fn load_order_history_dates(&self) -> Result<Vec<String>> {
        Venue::load_order_history_dates(self).await
    }
    async fn place_order(&self, order: &PlaceOrder) -> Result<()> {
        Venue::place_order(self, order).await
    }
    async fn place_bracket_order(&self, order: &PlaceBracketOrder) -> Result<()> {
        Venue::place_bracket_order(self, order).await
    }
    async fn adjust_bracket_stop(
        &self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()> {
        Venue::adjust_bracket_stop(self, basket_id, ticks, level).await
    }
    async fn adjust_bracket_target(
        &self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()> {
        Venue::adjust_bracket_target(self, basket_id, ticks, level).await
    }
    async fn cancel_order(&self, basket_id: &str) -> Result<()> {
        Venue::cancel_order(self, basket_id).await
    }
    async fn modify_order(&self, order: &ModifyOrder) -> Result<()> {
        Venue::modify_order(self, order).await
    }
    async fn cancel_all_orders(&self) -> Result<()> {
        Venue::cancel_all_orders(self).await
    }
    async fn poll_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        Venue::poll_event(self, timeout).await
    }
    async fn poll_history_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        Venue::poll_history_event(self, timeout).await
    }
    async fn poll_pnl_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        Venue::poll_pnl_event(self, timeout).await
    }
    async fn poll_order_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        Venue::poll_order_event(self, timeout).await
    }
    async fn next_events(
        &self,
        stream: Stream,
        max: usize,
        timeout: Duration,
    ) -> Result<Vec<PlantEvent>> {
        Venue::next_events(self, stream, max, timeout).await
    }
}
