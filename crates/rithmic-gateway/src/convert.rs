//! DTO ↔ protobuf mapping (the one convert boundary between `rithmic-host`
//! DTOs and the wire `pb` types). No Nautilus types cross this boundary.

use rithmic_host::dto::{PlantEvent, ReferenceDataDto};
use rithmic_host::AccountTriple;

pub use crate::mirror::{
    account_pnl_to_pb, account_rms_info_to_pb, bbo_to_pb, bracket_update_to_pb, fill_history_to_pb,
    front_month_to_pb, history_bar_to_pb, history_tick_to_pb, instrument_pnl_to_pb,
    last_trade_to_pb, order_book_to_pb, order_notification_to_pb, product_rms_info_to_pb,
    time_bar_probe_row_to_pb,
};
use crate::pb;
use crate::subscriptions::SubKey;

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

pub use rithmic_host::fanout::{
    gap_key, order_key, other_key, pnl_key, route_of, Route, GAP_BROADCAST,
};

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
