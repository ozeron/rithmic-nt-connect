//! DTO ↔ protobuf mapping (the one convert boundary between `rithmic-plants`
//! DTOs and the wire `pb` types). No Nautilus types cross this boundary.

use rithmic_plants::dto::{PlantEvent, ReferenceDataDto};
use rithmic_plants::AccountTriple;

pub use crate::mirror::{
    account_pnl_to_pb, account_rms_info_to_pb, bbo_to_pb, bracket_update_to_pb, fill_history_to_pb,
    front_month_to_pb, history_bar_to_pb, history_tick_to_pb, instrument_pnl_to_pb,
    last_trade_to_pb, order_book_to_pb, order_notification_to_pb, product_rms_info_to_pb,
    time_bar_probe_row_to_pb,
};
use crate::pb;
use crate::subscriptions::{Kind, SubKey};

pub fn reference_data_to_pb(r: ReferenceDataDto) -> pb::ReferenceDataResponse {
    pb::ReferenceDataResponse {
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
        price_precision: Some(r.price_precision as i32),
        is_tradable: Some(r.is_tradable),
    }
}

pub fn resolved_account_to_pb(a: AccountTriple) -> pb::ResolvedAccountResponse {
    pb::ResolvedAccountResponse {
        account_id: Some(a.account_id),
        fcm_id: Some(a.fcm_id),
        ib_id: Some(a.ib_id),
    }
}

/// Sentinel routing keys for plant events that are not symbol/exchange scoped
/// (account PnL, order notifications). These share the [`SubKey`] refcount +
/// fan-out machinery used for market data so there is a single hub type.
pub fn pnl_key() -> SubKey {
    SubKey {
        symbol: "__pnl__".into(),
        exchange: String::new(),
    }
}

pub fn order_key() -> SubKey {
    SubKey {
        symbol: "__order__".into(),
        exchange: String::new(),
    }
}

/// Sentinel key for a gap on the ticker or history stream: those streams are
/// not scoped to one topic, so the pump publishes the gap to every symbol topic.
pub const GAP_BROADCAST: &str = "__gap__";

/// Routing key for a gap on `stream`.
pub fn gap_key(stream: rithmic_plants::Stream) -> SubKey {
    use rithmic_plants::Stream;
    match stream {
        Stream::Pnl => pnl_key(),
        Stream::Order => order_key(),
        Stream::Ticker | Stream::History => SubKey {
            symbol: GAP_BROADCAST.into(),
            exchange: String::new(),
        },
    }
}

/// Sentinel key for unclassified plant messages (template types with no
/// symbol/exchange to route on). Routed separately from order notifications
/// so order-update subscribers do not receive unrelated plant chatter.
pub fn other_key() -> SubKey {
    SubKey {
        symbol: "__other__".into(),
        exchange: String::new(),
    }
}

/// Where an event is delivered, described with borrowed strings so deciding it
/// allocates nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route<'a> {
    /// A symbol topic, and the kind of frame it carries.
    Topic {
        symbol: &'a str,
        exchange: &'a str,
        kind: Kind,
    },
    Pnl,
    Order,
    Other,
    /// A gap on `stream`.
    Gap(rithmic_plants::Stream),
}

fn text(field: &Option<String>) -> &str {
    field.as_deref().unwrap_or("")
}

/// The route of `event`, without consuming or copying it.
pub fn route_of(event: &PlantEvent) -> Route<'_> {
    match event {
        PlantEvent::LastTrade(t) => Route::Topic {
            symbol: text(&t.symbol),
            exchange: text(&t.exchange),
            kind: Kind::Ticker,
        },
        PlantEvent::Bbo(b) => Route::Topic {
            symbol: text(&b.symbol),
            exchange: text(&b.exchange),
            kind: Kind::Ticker,
        },
        PlantEvent::OrderBook(o) => Route::Topic {
            symbol: text(&o.symbol),
            exchange: text(&o.exchange),
            kind: Kind::Book,
        },
        PlantEvent::TimeBar(b) => Route::Topic {
            symbol: text(&b.symbol),
            exchange: text(&b.exchange),
            kind: Kind::Bars,
        },
        PlantEvent::AccountPnl(_) | PlantEvent::InstrumentPnl(_) => Route::Pnl,
        // Bracket updates ride the order-plant stream, so they share its topic.
        PlantEvent::OrderNotification(_) | PlantEvent::BracketUpdate(_) => Route::Order,
        PlantEvent::Gap(g) => Route::Gap(g.stream),
        PlantEvent::Other { .. } => Route::Other,
    }
}

/// Convert a plant event to its wire form. Strings move into the message; nothing
/// is copied.
pub fn plant_event_to_pb(event: PlantEvent) -> pb::Event {
    use pb::event::Body;
    let body = match event {
        PlantEvent::LastTrade(t) => Body::LastTrade(last_trade_to_pb(t)),
        PlantEvent::Bbo(b) => Body::Bbo(bbo_to_pb(b)),
        PlantEvent::OrderBook(o) => Body::OrderBook(order_book_to_pb(o)),
        PlantEvent::TimeBar(b) => Body::TimeBar(history_bar_to_pb(b)),
        PlantEvent::AccountPnl(a) => Body::AccountPnl(account_pnl_to_pb(a)),
        PlantEvent::InstrumentPnl(i) => Body::InstrumentPnl(instrument_pnl_to_pb(i)),
        PlantEvent::OrderNotification(n) => Body::OrderNotification(order_notification_to_pb(n)),
        PlantEvent::BracketUpdate(b) => Body::BracketUpdate(bracket_update_to_pb(b)),
        PlantEvent::Gap(g) => Body::Gap(pb::Gap {
            stream: g.stream.plant().into(),
            skipped: g.skipped,
        }),
        PlantEvent::Other { type_name, source } => {
            Body::Other(pb::OtherEvent { type_name, source })
        }
    };
    pb::Event { body: Some(body) }
}

/// Convert a plant event to its wire form, plus the routing key that
/// determines which fan-out subscribers receive it. Symbol/exchange events
/// route by `(symbol, exchange)`; account-level events use the internal
/// `pnl` / `order` / `other` sentinel keys (see [`pnl_key`]). The server hot
/// path uses [`route_of`] and [`plant_event_to_pb`] directly so it can skip an
/// event nobody subscribed to without building a key.
pub fn plant_event_to_routed(event: PlantEvent) -> (SubKey, pb::Event) {
    let key = match route_of(&event) {
        Route::Topic {
            symbol, exchange, ..
        } => SubKey {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
        },
        Route::Pnl => pnl_key(),
        Route::Order => order_key(),
        Route::Other => other_key(),
        Route::Gap(stream) => gap_key(stream),
    };
    (key, plant_event_to_pb(event))
}
