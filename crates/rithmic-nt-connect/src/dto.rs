//! Venue DTOs converted from rithmic-rs plant messages.

use rithmic_rs::rti::messages::RithmicMessage;
use rithmic_rs::{RithmicResponse, rithmic_to_unix_nanos};

/// Dict-friendly plant event for Python consumers (ticker, PnL, orders).
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
    /// Live or replay time bar (history plant).
    TimeBar(HistoryBarDto),
    /// Catch-all with type name for unhandled templates.
    Other {
        /// Discriminator / message variant name.
        type_name: String,
        /// Plant source name.
        source: String,
    },
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

impl TimeBarProbeRow {
    pub(crate) fn from_response(resp: &RithmicResponse) -> Self {
        let rp_code = resp
            .rp_code()
            .map(|codes| codes.to_vec())
            .unwrap_or_default();
        let error = resp.error.as_ref().map(ToString::to_string);
        match &resp.message {
            RithmicMessage::ResponseTimeBarReplay(m) => {
                let skip_reason = if m.close_price.is_none() && m.settlement_price.is_none() {
                    Some("no close or settlement".into())
                } else if m.marker.is_none() {
                    Some("no marker".into())
                } else {
                    None
                };
                Self {
                    variant: "ResponseTimeBarReplay".into(),
                    source: resp.source.clone(),
                    error,
                    rp_code,
                    parsed: skip_reason.is_none(),
                    skip_reason,
                    symbol: m.symbol.clone(),
                    exchange: m.exchange.clone(),
                    bar_type: m.r#type,
                    period: m.period.clone(),
                    marker: m.marker,
                    open_price: m.open_price,
                    high_price: m.high_price,
                    low_price: m.low_price,
                    close_price: m.close_price,
                    settlement_price: m.settlement_price,
                    has_settlement_price: m.has_settlement_price,
                    volume: m.volume,
                    num_trades: m.num_trades,
                }
            }
            RithmicMessage::TimeBar(m) => {
                let skip_reason = if m.close_price.is_none() {
                    Some("no close".into())
                } else if m.marker.is_none() {
                    Some("no marker".into())
                } else {
                    None
                };
                Self {
                    variant: "TimeBar".into(),
                    source: resp.source.clone(),
                    error,
                    rp_code,
                    parsed: skip_reason.is_none(),
                    skip_reason,
                    symbol: m.symbol.clone(),
                    exchange: m.exchange.clone(),
                    bar_type: m.r#type,
                    period: m.period.clone(),
                    marker: m.marker,
                    open_price: m.open_price,
                    high_price: m.high_price,
                    low_price: m.low_price,
                    close_price: m.close_price,
                    settlement_price: None,
                    has_settlement_price: None,
                    volume: m.volume,
                    num_trades: m.num_trades,
                }
            }
            other => Self {
                variant: format!("{other:?}")
                    .split('(')
                    .next()
                    .unwrap_or("Unknown")
                    .to_string(),
                source: resp.source.clone(),
                error,
                rp_code,
                parsed: false,
                skip_reason: Some("unhandled variant".into()),
                symbol: None,
                exchange: None,
                bar_type: None,
                period: None,
                marker: None,
                open_price: None,
                high_price: None,
                low_price: None,
                close_price: None,
                settlement_price: None,
                has_settlement_price: None,
                volume: None,
                num_trades: None,
            },
        }
    }
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

fn ts_ns(ssboe: Option<i32>, usecs: Option<i32>) -> Option<u64> {
    let ssboe = ssboe?;
    Some(rithmic_to_unix_nanos(ssboe, usecs.unwrap_or(0)))
}

fn order_kind(source: &str, notify_type_name: Option<&str>, status: Option<&str>) -> Option<String> {
    let name = notify_type_name?.to_ascii_uppercase();
    let kind = match (source, name.as_str()) {
        ("rithmic", "OPEN") => "accepted",
        ("rithmic", "MODIFIED") => "updated",
        ("rithmic", "MODIFICATION_FAILED") => "modify_rejected",
        ("rithmic", "CANCELLATION_FAILED") => "cancel_rejected",
        ("rithmic", "COMPLETE") => {
            let status_u = status.unwrap_or("").to_ascii_uppercase();
            if status_u == "CANCELLED" || status_u == "CANCELED" {
                "canceled"
            } else {
                return None;
            }
        }
        ("exchange", "FILL") => "filled",
        ("exchange", "REJECT") => "rejected",
        ("exchange", "CANCEL") => "canceled",
        ("exchange", "TRIGGER") => "triggered",
        ("exchange", "NOT_MODIFIED") => "modify_rejected",
        ("exchange", "NOT_CANCELLED" | "NOT_CANCELED") => "cancel_rejected",
        _ => return None,
    };
    Some(kind.to_string())
}

impl From<&RithmicResponse> for PlantEvent {
    fn from(resp: &RithmicResponse) -> Self {
        match &resp.message {
            RithmicMessage::LastTrade(t) => Self::LastTrade(LastTradeDto {
                symbol: t.symbol.clone(),
                exchange: t.exchange.clone(),
                trade_price: t.trade_price,
                trade_size: t.trade_size,
                aggressor: t.aggressor,
                ssboe: t.ssboe,
                usecs: t.usecs,
                ts_event_ns: ts_ns(t.ssboe, t.usecs),
                is_snapshot: t.is_snapshot,
            }),
            RithmicMessage::BestBidOffer(b) => Self::Bbo(BboDto {
                symbol: b.symbol.clone(),
                exchange: b.exchange.clone(),
                bid_price: b.bid_price,
                bid_size: b.bid_size,
                ask_price: b.ask_price,
                ask_size: b.ask_size,
                ssboe: b.ssboe,
                usecs: b.usecs,
                ts_event_ns: ts_ns(b.ssboe, b.usecs),
                is_snapshot: b.is_snapshot,
            }),
            RithmicMessage::OrderBook(o) => Self::OrderBook(OrderBookDto {
                symbol: o.symbol.clone(),
                exchange: o.exchange.clone(),
                update_type: o.update_type,
                bid_price: o.bid_price.clone(),
                bid_size: o.bid_size.clone(),
                ask_price: o.ask_price.clone(),
                ask_size: o.ask_size.clone(),
                ssboe: o.ssboe,
                usecs: o.usecs,
                ts_event_ns: ts_ns(o.ssboe, o.usecs),
            }),
            RithmicMessage::AccountPnLPositionUpdate(a) => Self::AccountPnl(AccountPnlDto {
                account_id: a.account_id.clone(),
                fcm_id: a.fcm_id.clone(),
                ib_id: a.ib_id.clone(),
                account_balance: a.account_balance.clone(),
                cash_on_hand: a.cash_on_hand.clone(),
                margin_balance: a.margin_balance.clone(),
                day_pnl: a.day_pnl.clone(),
                open_position_pnl: a.open_position_pnl.clone(),
                closed_position_pnl: a.closed_position_pnl.clone(),
                available_buying_power: a.available_buying_power.clone(),
                used_buying_power: a.used_buying_power.clone(),
                is_snapshot: a.is_snapshot,
                ssboe: a.ssboe,
                usecs: a.usecs,
            }),
            RithmicMessage::InstrumentPnLPositionUpdate(i) => Self::InstrumentPnl(InstrumentPnlDto {
                account_id: i.account_id.clone(),
                symbol: i.symbol.clone(),
                exchange: i.exchange.clone(),
                product_code: i.product_code.clone(),
                instrument_type: i.instrument_type.clone(),
                open_position_pnl: i.open_position_pnl.clone(),
                closed_position_pnl: i.closed_position_pnl.clone(),
                mtm_security: i.mtm_security.clone(),
                open_position_quantity: i.open_position_quantity,
                closed_position_quantity: i.closed_position_quantity,
                net_quantity: i.net_quantity,
                avg_open_fill_price: i.avg_open_fill_price,
                is_snapshot: i.is_snapshot,
                ssboe: i.ssboe,
                usecs: i.usecs,
            }),
            RithmicMessage::RithmicOrderNotification(n) => {
                let notify_type_name = n.notify_type.and_then(|v| {
                    rithmic_rs::rti::rithmic_order_notification::NotifyType::try_from(v)
                        .ok()
                        .map(|t| t.as_str_name().to_string())
                });
                let kind = order_kind(
                    "rithmic",
                    notify_type_name.as_deref(),
                    n.status.as_deref(),
                );
                Self::OrderNotification(OrderNotificationDto {
                    source: "rithmic".into(),
                    kind,
                    notify_type: n.notify_type,
                    notify_type_name,
                    status: n.status.clone(),
                    basket_id: n.basket_id.clone(),
                    exchange_order_id: n.exchange_order_id.clone(),
                    user_tag: n.user_tag.clone(),
                    account_id: n.account_id.clone(),
                    symbol: n.symbol.clone(),
                    exchange: n.exchange.clone(),
                    quantity: n.quantity,
                    total_fill_size: n.total_fill_size,
                    total_unfilled_size: n.total_unfilled_size,
                    fill_size: None,
                    price: n.price,
                    trigger_price: n.trigger_price,
                    avg_fill_price: n.avg_fill_price,
                    fill_price: None,
                    transaction_type: n.transaction_type,
                    price_type: n.price_type,
                    fill_id: None,
                    text: n.text.clone(),
                    report_text: n.report_text.clone(),
                    completion_reason: n.completion_reason.clone(),
                    ssboe: n.ssboe,
                    usecs: n.usecs,
                    ts_event_ns: ts_ns(n.ssboe, n.usecs),
                    is_snapshot: n.is_snapshot,
                })
            }
            RithmicMessage::ExchangeOrderNotification(n) => {
                let notify_type_name = n.notify_type.and_then(|v| {
                    rithmic_rs::rti::exchange_order_notification::NotifyType::try_from(v)
                        .ok()
                        .map(|t| t.as_str_name().to_string())
                });
                let kind = order_kind(
                    "exchange",
                    notify_type_name.as_deref(),
                    n.status.as_deref(),
                );
                Self::OrderNotification(OrderNotificationDto {
                    source: "exchange".into(),
                    kind,
                    notify_type: n.notify_type,
                    notify_type_name,
                    status: n.status.clone(),
                    basket_id: n.basket_id.clone(),
                    exchange_order_id: n.exchange_order_id.clone(),
                    user_tag: n.user_tag.clone(),
                    account_id: n.account_id.clone(),
                    symbol: n.symbol.clone(),
                    exchange: n.exchange.clone(),
                    quantity: n.quantity,
                    total_fill_size: n.total_fill_size,
                    total_unfilled_size: n.total_unfilled_size,
                    fill_size: n.fill_size,
                    price: n.price,
                    trigger_price: n.trigger_price,
                    avg_fill_price: n.avg_fill_price,
                    fill_price: n.fill_price,
                    transaction_type: n.transaction_type,
                    price_type: n.price_type,
                    fill_id: n.fill_id.clone(),
                    text: n.text.clone(),
                    report_text: n.report_text.clone(),
                    completion_reason: None,
                    ssboe: n.ssboe,
                    usecs: n.usecs,
                    ts_event_ns: ts_ns(n.ssboe, n.usecs),
                    is_snapshot: n.is_snapshot,
                })
            }
            RithmicMessage::TimeBar(_) => match HistoryBarDto::from_response(resp) {
                Some(bar) => Self::TimeBar(bar),
                None => Self::Other {
                    type_name: "TimeBar".into(),
                    source: resp.source.clone(),
                },
            },
            other => Self::Other {
                type_name: format!("{other:?}")
                    .split('(')
                    .next()
                    .unwrap_or("Unknown")
                    .to_string(),
                source: resp.source.clone(),
            },
        }
    }
}

impl ReferenceDataDto {
    pub(crate) fn from_response(resp: &RithmicResponse) -> Option<Self> {
        use rithmic_rs::InstrumentInfo;
        match &resp.message {
            RithmicMessage::ResponseReferenceData(data) => {
                let info = InstrumentInfo::try_from(data).ok()?;
                let price_precision = info.price_precision();
                Some(Self {
                    symbol: Some(info.symbol),
                    exchange: Some(info.exchange),
                    trading_symbol: data.trading_symbol.clone(),
                    trading_exchange: data.trading_exchange.clone(),
                    symbol_name: info.name,
                    product_code: info.product_code,
                    instrument_type: info.instrument_type,
                    underlying: info.underlying,
                    currency: info.currency,
                    expiration_date: info.expiration_date,
                    tick_size: info.tick_size,
                    point_value: info.point_value,
                    price_precision,
                    is_tradable: info.is_tradable,
                })
            }
            _ => None,
        }
    }
}

impl FrontMonthDto {
    pub(crate) fn from_response(resp: &RithmicResponse) -> Option<Self> {
        match &resp.message {
            RithmicMessage::ResponseFrontMonthContract(m) => Some(Self {
                symbol: m.symbol.clone(),
                exchange: m.exchange.clone(),
                trading_symbol: m.trading_symbol.clone(),
                trading_exchange: m.trading_exchange.clone(),
                symbol_name: m.symbol_name.clone(),
                is_front_month_symbol: m.is_front_month_symbol,
            }),
            RithmicMessage::FrontMonthContractUpdate(m) => Some(Self {
                symbol: m.symbol.clone(),
                exchange: m.exchange.clone(),
                trading_symbol: m.trading_symbol.clone(),
                trading_exchange: m.trading_exchange.clone(),
                symbol_name: m.symbol_name.clone(),
                is_front_month_symbol: m.is_front_month_symbol,
            }),
            _ => None,
        }
    }
}

impl HistoryTickDto {
    pub(crate) fn from_response(resp: &RithmicResponse) -> Option<Self> {
        match &resp.message {
            RithmicMessage::ResponseTickBarReplay(m) => {
                // Incomplete tick bars are dropped (no invented OHLC / size).
                // Prefer close time (index 1): rithmic-rs docs note the first
                // record's open can be the request window start, not trade time.
                let close_price = m.close_price?;
                let num_trades = m.num_trades.filter(|n| *n > 0)?;
                let ssboe = match m.data_bar_ssboe.as_slice() {
                    [_, close, ..] => *close,
                    [only] => *only,
                    [] => return None,
                };
                let usecs = match m.data_bar_usecs.as_slice() {
                    [_, close, ..] => Some(*close),
                    [only] => Some(*only),
                    [] => None,
                };
                Some(Self {
                    symbol: m.symbol.clone(),
                    exchange: m.exchange.clone(),
                    open_price: m.open_price,
                    high_price: m.high_price,
                    low_price: m.low_price,
                    close_price: Some(close_price),
                    volume: m.volume,
                    num_trades: Some(num_trades),
                    ssboe: Some(ssboe),
                    usecs,
                    ts_event_ns: ts_ns(Some(ssboe), usecs),
                })
            }
            _ => None,
        }
    }
}

impl HistoryBarDto {
    fn from_ohlcv(
        symbol: Option<String>,
        exchange: Option<String>,
        bar_type: Option<i32>,
        period: Option<String>,
        marker: i32,
        close_price: f64,
        open_price: Option<f64>,
        high_price: Option<f64>,
        low_price: Option<f64>,
        volume: Option<u64>,
        num_trades: Option<u64>,
        bid_volume: Option<u64>,
        ask_volume: Option<u64>,
    ) -> Self {
        Self {
            symbol,
            exchange,
            bar_type,
            period,
            marker: Some(marker),
            open_price: Some(open_price.unwrap_or(close_price)),
            high_price: Some(high_price.unwrap_or(close_price)),
            low_price: Some(low_price.unwrap_or(close_price)),
            close_price: Some(close_price),
            volume: Some(volume.unwrap_or(0)),
            num_trades,
            bid_volume,
            ask_volume,
            ts_event_ns: Some(rithmic_to_unix_nanos(
                crate::history::marker_to_ssboe(marker),
                0,
            )),
        }
    }

    pub(crate) fn from_response(resp: &RithmicResponse) -> Option<Self> {
        match &resp.message {
            RithmicMessage::ResponseTimeBarReplay(m) => {
                // End-of-replay markers have no price. Daily rows often have
                // settlement and no volume — do not drop those.
                let close_price = m.close_price.or(m.settlement_price)?;
                let marker = m.marker?;
                Some(Self::from_ohlcv(
                    m.symbol.clone(),
                    m.exchange.clone(),
                    m.r#type,
                    m.period.clone(),
                    marker,
                    close_price,
                    m.open_price,
                    m.high_price,
                    m.low_price,
                    m.volume,
                    m.num_trades,
                    m.bid_volume,
                    m.ask_volume,
                ))
            }
            RithmicMessage::TimeBar(m) => {
                let close_price = m.close_price?;
                let marker = m.marker?;
                Some(Self::from_ohlcv(
                    m.symbol.clone(),
                    m.exchange.clone(),
                    m.r#type,
                    m.period.clone(),
                    marker,
                    close_price,
                    m.open_price,
                    m.high_price,
                    m.low_price,
                    m.volume,
                    m.num_trades,
                    m.bid_volume,
                    m.ask_volume,
                ))
            }
            _ => None,
        }
    }
}
