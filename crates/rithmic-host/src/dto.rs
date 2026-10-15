//! Venue-independent plant event and request-result data.

/// Events a plant stream dropped because its consumer lagged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GapDto {
    pub stream: crate::streams::Stream,
    pub skipped: u64,
}

/// Dict-friendly plant event for Python consumers (ticker, PnL, orders).
///
/// `OrderNotification` is the largest variant (~500 bytes) because a venue
/// order notification carries many optional scalar fields. Boxing it would
/// churn every construction/match site for a public DTO with no correctness
/// benefit, so the size difference is allowed intentionally.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum PlantEvent {
    /// Last trade update.
    LastTrade(LastTradeDto),
    /// Best bid / offer update.
    Bbo(BboDto),
    /// Aggregated order-book summary levels.
    OrderBook(OrderBookDto),
    /// Account-level PnL update.
    AccountPnl(AccountPnlDto),
    /// Instrument-level PnL / position update.
    InstrumentPnl(InstrumentPnlDto),
    /// Order plant notification (Rithmic or exchange).
    OrderNotification(OrderNotificationDto),
    /// A plant bracket's stop / target levels changed (order plant, template 353).
    BracketUpdate(BracketUpdateDto),
    /// Live or replay time bar (history plant).
    TimeBar(HistoryBarDto),
    /// A stream dropped events (a lagging consumer); carries the count so the
    /// consumer can resynchronize instead of continuing as if nothing was lost.
    Gap(GapDto),
    /// Catch-all with type name for unhandled templates.
    Other {
        /// Discriminator / message variant name.
        type_name: String,
        /// Plant source name.
        source: String,
    },
}

/// Plant bracket state for one basket: stop and target distance in ticks and
/// the quantities resting / released on each leg.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BracketUpdateDto {
    pub account_id: Option<String>,
    pub fcm_id: Option<String>,
    pub ib_id: Option<String>,
    pub basket_id: Option<String>,
    pub stop_ticks: Option<i32>,
    pub stop_quantity: Option<i32>,
    pub stop_quantity_released: Option<i32>,
    pub target_ticks: Option<i32>,
    pub target_quantity: Option<i32>,
    pub target_quantity_released: Option<i32>,
}

/// Product-level RMS (Risk Management System) info — commission fill rate per
/// product code, as published by the order plant. Read-only venue config.
#[derive(Debug, Clone, PartialEq)]
pub struct ProductRmsInfoDto {
    /// Product code (e.g. `"MNQ"`, `"ES"`).
    pub product_code: Option<String>,
    /// Per-contract commission fill rate in the account currency.
    pub commission_fill_rate: Option<f64>,
    /// Field-presence bitmask from the venue (see `ResponseProductRmsInfo`).
    pub presence_bits: Option<u32>,
}

/// Account-level RMS info — default commission rate (fallback when a product
/// has no specific fill rate). Read-only venue config.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountRmsInfoDto {
    pub account_id: Option<String>,
    /// Default per-contract commission rate in the account currency.
    pub default_commission: Option<f64>,
    /// Field-presence bitmask from the venue (see `ResponseAccountRmsInfo`).
    pub presence_bits: Option<u32>,
}

/// One historical fill from the order plant (`RequestShowFillHistory`). Read-only.
#[derive(Debug, Clone, PartialEq)]
pub struct FillHistoryDto {
    pub account_id: Option<String>,
    pub basket_id: Option<String>,
    pub symbol: Option<String>,
    pub exchange: Option<String>,
    /// `BUY` / `SELL`.
    pub transaction_type: Option<String>,
    pub price_type: Option<String>,
    pub duration: Option<String>,
    /// `MANUAL` / `AUTO` — who entered the order.
    pub manual_or_auto: Option<String>,
    pub fill_id: Option<String>,
    pub fill_date: Option<String>,
    pub fill_time: Option<String>,
    pub fill_price: Option<f64>,
    pub fill_size: Option<u64>,
    pub price: Option<f64>,
    pub trigger_price: Option<f64>,
    pub ssboe: Option<i32>,
    pub usecs: Option<i32>,
}

/// Order notification fields from Rithmic or exchange plants.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderNotificationDto {
    /// `"rithmic"` or `"exchange"`.
    pub source: String,
    /// Canonical action kind (`accepted`, `filled`, …) classified at the DTO boundary.
    pub kind: Option<String>,
    pub notify_type: Option<i32>,
    pub notify_type_name: Option<String>,
    pub status: Option<String>,
    pub basket_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub user_tag: Option<String>,
    pub account_id: Option<String>,
    pub symbol: Option<String>,
    pub exchange: Option<String>,
    pub quantity: Option<i32>,
    pub total_fill_size: Option<i32>,
    pub total_unfilled_size: Option<i32>,
    pub fill_size: Option<i32>,
    pub price: Option<f64>,
    pub trigger_price: Option<f64>,
    pub avg_fill_price: Option<f64>,
    pub fill_price: Option<f64>,
    pub transaction_type: Option<i32>,
    pub price_type: Option<i32>,
    pub duration: Option<i32>,
    pub fill_id: Option<String>,
    pub text: Option<String>,
    pub report_text: Option<String>,
    pub completion_reason: Option<String>,
    pub ssboe: Option<i32>,
    pub usecs: Option<i32>,
    pub ts_event_ns: Option<u64>,
    pub is_snapshot: Option<bool>,
}

/// LastTrade fields needed by Python converters.
#[derive(Debug, Clone, PartialEq)]
pub struct LastTradeDto {
    pub symbol: Option<String>,
    pub exchange: Option<String>,
    pub trade_price: Option<f64>,
    pub trade_size: Option<i32>,
    pub aggressor: Option<i32>,
    pub ssboe: Option<i32>,
    pub usecs: Option<i32>,
    pub ts_event_ns: Option<u64>,
    pub is_snapshot: Option<bool>,
}

/// BestBidOffer fields.
#[derive(Debug, Clone, PartialEq)]
pub struct BboDto {
    pub symbol: Option<String>,
    pub exchange: Option<String>,
    pub bid_price: Option<f64>,
    pub bid_size: Option<i32>,
    pub ask_price: Option<f64>,
    pub ask_size: Option<i32>,
    pub ssboe: Option<i32>,
    pub usecs: Option<i32>,
    pub ts_event_ns: Option<u64>,
    pub is_snapshot: Option<bool>,
}

/// Account PnL snapshot / update fields.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountPnlDto {
    pub account_id: Option<String>,
    pub fcm_id: Option<String>,
    pub ib_id: Option<String>,
    pub account_balance: Option<String>,
    pub cash_on_hand: Option<String>,
    pub margin_balance: Option<String>,
    pub day_pnl: Option<String>,
    pub open_position_pnl: Option<String>,
    pub closed_position_pnl: Option<String>,
    pub available_buying_power: Option<String>,
    pub used_buying_power: Option<String>,
    pub is_snapshot: Option<bool>,
    pub ssboe: Option<i32>,
    pub usecs: Option<i32>,
}

/// Instrument PnL / position snapshot fields.
#[derive(Debug, Clone, PartialEq)]
pub struct InstrumentPnlDto {
    pub account_id: Option<String>,
    pub symbol: Option<String>,
    pub exchange: Option<String>,
    pub product_code: Option<String>,
    pub instrument_type: Option<String>,
    pub open_position_pnl: Option<String>,
    pub closed_position_pnl: Option<String>,
    pub mtm_security: Option<String>,
    pub open_position_quantity: Option<i32>,
    pub closed_position_quantity: Option<i32>,
    pub net_quantity: Option<i32>,
    pub avg_open_fill_price: Option<f64>,
    pub is_snapshot: Option<bool>,
    pub ssboe: Option<i32>,
    pub usecs: Option<i32>,
}

/// Aggregated order-book summary (bid/ask level arrays).
#[derive(Debug, Clone, PartialEq)]
pub struct OrderBookDto {
    pub symbol: Option<String>,
    pub exchange: Option<String>,
    pub update_type: Option<i32>,
    pub bid_price: Vec<f64>,
    pub bid_size: Vec<i32>,
    pub ask_price: Vec<f64>,
    pub ask_size: Vec<i32>,
    pub ssboe: Option<i32>,
    pub usecs: Option<i32>,
    pub ts_event_ns: Option<u64>,
}

/// Reference-data fields used to build Nautilus instruments.
#[derive(Debug, Clone, PartialEq)]
pub struct ReferenceDataDto {
    pub symbol: Option<String>,
    pub exchange: Option<String>,
    pub trading_symbol: Option<String>,
    pub trading_exchange: Option<String>,
    pub symbol_name: Option<String>,
    pub product_code: Option<String>,
    pub instrument_type: Option<String>,
    pub underlying: Option<String>,
    pub currency: Option<String>,
    pub expiration_date: Option<String>,
    pub tick_size: Option<f64>,
    pub point_value: Option<f64>,
    pub price_precision: u8,
    pub is_tradable: bool,
}

/// Front-month resolution result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontMonthDto {
    pub symbol: Option<String>,
    pub exchange: Option<String>,
    pub trading_symbol: Option<String>,
    pub trading_exchange: Option<String>,
    pub symbol_name: Option<String>,
    pub is_front_month_symbol: Option<bool>,
}

/// Historical tick / bar row (tick-bar replay with bar_length=1).
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryTickDto {
    pub symbol: Option<String>,
    pub exchange: Option<String>,
    pub open_price: Option<f64>,
    pub high_price: Option<f64>,
    pub low_price: Option<f64>,
    pub close_price: Option<f64>,
    pub volume: Option<u64>,
    pub num_trades: Option<u64>,
    pub ssboe: Option<i32>,
    pub usecs: Option<i32>,
    pub ts_event_ns: Option<u64>,
}

/// One raw history-plant time-bar replay message (including rows we drop).
#[derive(Debug, Clone, PartialEq)]
pub struct TimeBarProbeRow {
    pub variant: String,
    pub source: String,
    pub error: Option<String>,
    pub rp_code: Vec<String>,
    pub parsed: bool,
    pub skip_reason: Option<String>,
    pub symbol: Option<String>,
    pub exchange: Option<String>,
    pub bar_type: Option<i32>,
    pub period: Option<String>,
    pub marker: Option<i32>,
    pub open_price: Option<f64>,
    pub high_price: Option<f64>,
    pub low_price: Option<f64>,
    pub close_price: Option<f64>,
    pub settlement_price: Option<f64>,
    pub has_settlement_price: Option<bool>,
    pub volume: Option<u64>,
    pub num_trades: Option<u64>,
}

/// Historical / replay time bar (OHLCV).
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryBarDto {
    pub symbol: Option<String>,
    pub exchange: Option<String>,
    /// Rithmic bar type enum value (1=second, 2=minute, 3=daily, 4=weekly).
    pub bar_type: Option<i32>,
    /// Period string from the plant (often the period count).
    pub period: Option<String>,
    /// Bar close marker in Unix seconds.
    pub marker: Option<i32>,
    pub open_price: Option<f64>,
    pub high_price: Option<f64>,
    pub low_price: Option<f64>,
    pub close_price: Option<f64>,
    pub volume: Option<u64>,
    pub num_trades: Option<u64>,
    pub bid_volume: Option<u64>,
    pub ask_volume: Option<u64>,
    pub ts_event_ns: Option<u64>,
}
