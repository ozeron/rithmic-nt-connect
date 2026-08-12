//! Venue DTOs converted from rithmic-rs plant messages.

use rithmic_rs::rti::messages::RithmicMessage;
use rithmic_rs::{RithmicResponse, rithmic_to_unix_nanos};

/// Dict-friendly ticker / PnL event for Python consumers.
#[derive(Debug, Clone, PartialEq)]
pub enum TickerEvent {
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
    /// Catch-all with type name for unhandled templates.
    Other {
        /// Discriminator / message variant name.
        type_name: String,
        /// Plant source name.
        source: String,
    },
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

fn ts_ns(ssboe: Option<i32>, usecs: Option<i32>) -> Option<u64> {
    let ssboe = ssboe?;
    Some(rithmic_to_unix_nanos(ssboe, usecs.unwrap_or(0)))
}

impl From<&RithmicResponse> for TickerEvent {
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
                let ssboe = m.data_bar_ssboe.first().copied();
                let usecs = m.data_bar_usecs.first().copied();
                Some(Self {
                    symbol: m.symbol.clone(),
                    exchange: m.exchange.clone(),
                    open_price: m.open_price,
                    high_price: m.high_price,
                    low_price: m.low_price,
                    close_price: m.close_price,
                    volume: m.volume,
                    num_trades: m.num_trades,
                    ssboe,
                    usecs,
                    ts_event_ns: ts_ns(ssboe, usecs),
                })
            }
            RithmicMessage::LastTrade(t) => Some(Self {
                symbol: t.symbol.clone(),
                exchange: t.exchange.clone(),
                open_price: t.trade_price,
                high_price: t.trade_price,
                low_price: t.trade_price,
                close_price: t.trade_price,
                volume: t.volume,
                num_trades: Some(1),
                ssboe: t.ssboe,
                usecs: t.usecs,
                ts_event_ns: ts_ns(t.ssboe, t.usecs),
            }),
            _ => None,
        }
    }
}
