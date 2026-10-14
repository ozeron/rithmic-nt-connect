//! pb → DTO: the exact inverse of `crate::convert` (the server's DTO → pb).
//!
//! `GatewaySession` hands out the same `rithmic_plants` DTOs `DirectSession`
//! does, so the extension converts them to dicts in exactly one place for both
//! modes. A round-trip test (`tests/client_convert.rs`) pins `from(to(x)) == x`.

use rithmic_plants::dto::{PlantEvent, ReferenceDataDto};
use rithmic_plants::AccountTriple;

pub use crate::mirror::{
    account_pnl_from_pb, account_rms_info_from_pb, bbo_from_pb, bracket_update_from_pb,
    fill_history_from_pb, front_month_from_pb, history_bar_from_pb, history_tick_from_pb,
    instrument_pnl_from_pb, last_trade_from_pb, order_book_from_pb, order_notification_from_pb,
    product_rms_info_from_pb, time_bar_probe_row_from_pb,
};
use crate::pb;

pub fn reference_data_from_pb(r: pb::ReferenceDataResponse) -> ReferenceDataDto {
    ReferenceDataDto {
        symbol: r.symbol,
        exchange: r.exchange,
        trading_symbol: r.trading_symbol,
        trading_exchange: r.trading_exchange,
        symbol_name: r.symbol_name,
        product_code: r.product_code,
        instrument_type: r.instrument_type,
        underlying: r.underlying,
        currency: r.currency,
        expiration_date: r.expiration_date,
        tick_size: r.tick_size,
        point_value: r.point_value,
        price_precision: r
            .price_precision
            .and_then(|p| u8::try_from(p).ok())
            .unwrap_or(0),
        is_tradable: r.is_tradable.unwrap_or(false),
    }
}

/// `None` when the parent has not resolved an account yet (all fields absent).
pub fn resolved_account_from_pb(r: pb::ResolvedAccountResponse) -> Option<AccountTriple> {
    if r.account_id.is_none() && r.fcm_id.is_none() && r.ib_id.is_none() {
        return None;
    }
    Some(AccountTriple {
        account_id: r.account_id.unwrap_or_default(),
        fcm_id: r.fcm_id.unwrap_or_default(),
        ib_id: r.ib_id.unwrap_or_default(),
    })
}

/// Which client-side queue an event is delivered to (and which `poll_*` reads it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    /// Ticker plant: last trade, BBO, order book, unclassified plant messages.
    Market,
    /// History plant: live time bars.
    History,
    /// PnL plant: account and instrument PnL.
    Pnl,
    /// Order plant: order and bracket notifications.
    Order,
}

impl Channel {
    pub const ALL: [Self; 4] = [Self::Market, Self::History, Self::Pnl, Self::Order];

    /// Plant name used in typed channel errors.
    pub fn stream(self) -> rithmic_plants::Stream {
        use rithmic_plants::Stream;
        match self {
            Self::Market => Stream::Ticker,
            Self::History => Stream::History,
            Self::Pnl => Stream::Pnl,
            Self::Order => Stream::Order,
        }
    }

    pub fn plant(self) -> &'static str {
        match self {
            Self::Market => "ticker",
            Self::History => "history",
            Self::Pnl => "pnl",
            Self::Order => "order",
        }
    }

    pub fn of_stream(stream: rithmic_plants::Stream) -> Self {
        use rithmic_plants::Stream;
        match stream {
            Stream::Ticker => Self::Market,
            Stream::History => Self::History,
            Stream::Pnl => Self::Pnl,
            Stream::Order => Self::Order,
        }
    }

    pub fn of(event: &PlantEvent) -> Self {
        match event {
            PlantEvent::TimeBar(_) => Self::History,
            PlantEvent::Gap(g) => Self::of_stream(g.stream),
            PlantEvent::AccountPnl(_) | PlantEvent::InstrumentPnl(_) => Self::Pnl,
            PlantEvent::OrderNotification(_) | PlantEvent::BracketUpdate(_) => Self::Order,
            PlantEvent::LastTrade(_)
            | PlantEvent::Bbo(_)
            | PlantEvent::OrderBook(_)
            | PlantEvent::Other { .. } => Self::Market,
        }
    }
}

/// Decode a pushed wire event. `None` for an event with no body.
pub fn plant_event_from_pb(event: pb::Event) -> Option<PlantEvent> {
    use pb::event::Body;
    Some(match event.body? {
        Body::LastTrade(t) => PlantEvent::LastTrade(last_trade_from_pb(t)),
        Body::Bbo(b) => PlantEvent::Bbo(bbo_from_pb(b)),
        Body::OrderBook(o) => PlantEvent::OrderBook(order_book_from_pb(o)),
        Body::AccountPnl(a) => PlantEvent::AccountPnl(account_pnl_from_pb(a)),
        Body::InstrumentPnl(i) => PlantEvent::InstrumentPnl(instrument_pnl_from_pb(i)),
        Body::OrderNotification(n) => PlantEvent::OrderNotification(order_notification_from_pb(n)),
        Body::BracketUpdate(b) => PlantEvent::BracketUpdate(bracket_update_from_pb(b)),
        Body::TimeBar(b) => PlantEvent::TimeBar(history_bar_from_pb(b)),
        // Connection-level, not a plant event: the runtime handles it before this.
        Body::TransportResumed(_) => return None,
        Body::Gap(g) => PlantEvent::Gap(rithmic_plants::GapDto {
            stream: rithmic_plants::Stream::from_plant(&g.stream)?,
            skipped: g.skipped,
        }),
        Body::Other(o) => PlantEvent::Other {
            type_name: o.type_name,
            source: o.source,
        },
    })
}
