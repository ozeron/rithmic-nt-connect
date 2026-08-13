//! DTO ↔ protobuf mapping (the one convert boundary between `rithmic-plants`
//! DTOs and the wire `pb` types). No Nautilus types cross this boundary.

use rithmic_plants::dto::{
    AccountPnlDto, BboDto, FrontMonthDto, HistoryBarDto, HistoryTickDto, InstrumentPnlDto,
    LastTradeDto, OrderBookDto, OrderNotificationDto, PlantEvent, ReferenceDataDto,
};

use crate::pb;
use crate::subscriptions::SubKey;

pub fn last_trade_to_pb(t: LastTradeDto) -> pb::LastTrade {
    pb::LastTrade {
        symbol: t.symbol,
        exchange: t.exchange,
        trade_price: t.trade_price,
        trade_size: t.trade_size,
        aggressor: t.aggressor,
        ssboe: t.ssboe,
        usecs: t.usecs,
        ts_event_ns: t.ts_event_ns,
        is_snapshot: t.is_snapshot,
    }
}

pub fn bbo_to_pb(b: BboDto) -> pb::Bbo {
    pb::Bbo {
        symbol: b.symbol,
        exchange: b.exchange,
        bid_price: b.bid_price,
        bid_size: b.bid_size,
        ask_price: b.ask_price,
        ask_size: b.ask_size,
        ssboe: b.ssboe,
        usecs: b.usecs,
        ts_event_ns: b.ts_event_ns,
        is_snapshot: b.is_snapshot,
    }
}

pub fn order_book_to_pb(o: OrderBookDto) -> pb::OrderBook {
    pb::OrderBook {
        symbol: o.symbol,
        exchange: o.exchange,
        update_type: o.update_type,
        bid_price: o.bid_price,
        bid_size: o.bid_size,
        ask_price: o.ask_price,
        ask_size: o.ask_size,
        ssboe: o.ssboe,
        usecs: o.usecs,
        ts_event_ns: o.ts_event_ns,
    }
}

pub fn account_pnl_to_pb(a: AccountPnlDto) -> pb::AccountPnl {
    pb::AccountPnl {
        account_id: a.account_id,
        fcm_id: a.fcm_id,
        ib_id: a.ib_id,
        account_balance: a.account_balance,
        cash_on_hand: a.cash_on_hand,
        margin_balance: a.margin_balance,
        day_pnl: a.day_pnl,
        open_position_pnl: a.open_position_pnl,
        closed_position_pnl: a.closed_position_pnl,
        available_buying_power: a.available_buying_power,
        used_buying_power: a.used_buying_power,
        is_snapshot: a.is_snapshot,
        ssboe: a.ssboe,
        usecs: a.usecs,
    }
}

pub fn instrument_pnl_to_pb(i: InstrumentPnlDto) -> pb::InstrumentPnl {
    pb::InstrumentPnl {
        account_id: i.account_id,
        symbol: i.symbol,
        exchange: i.exchange,
        product_code: i.product_code,
        instrument_type: i.instrument_type,
        open_position_pnl: i.open_position_pnl,
        closed_position_pnl: i.closed_position_pnl,
        mtm_security: i.mtm_security,
        open_position_quantity: i.open_position_quantity,
        closed_position_quantity: i.closed_position_quantity,
        net_quantity: i.net_quantity,
        avg_open_fill_price: i.avg_open_fill_price,
        is_snapshot: i.is_snapshot,
        ssboe: i.ssboe,
        usecs: i.usecs,
    }
}

pub fn order_notification_to_pb(n: OrderNotificationDto) -> pb::OrderNotification {
    pb::OrderNotification {
        source: n.source,
        kind: n.kind,
        notify_type: n.notify_type,
        notify_type_name: n.notify_type_name,
        status: n.status,
        basket_id: n.basket_id,
        exchange_order_id: n.exchange_order_id,
        user_tag: n.user_tag,
        account_id: n.account_id,
        symbol: n.symbol,
        exchange: n.exchange,
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
        fill_id: n.fill_id,
        text: n.text,
        report_text: n.report_text,
        completion_reason: n.completion_reason,
        ssboe: n.ssboe,
        usecs: n.usecs,
        ts_event_ns: n.ts_event_ns,
        is_snapshot: n.is_snapshot,
    }
}

pub fn history_bar_to_pb(b: HistoryBarDto) -> pb::HistoryBar {
    pb::HistoryBar {
        symbol: b.symbol,
        exchange: b.exchange,
        bar_type: b.bar_type,
        period: b.period,
        marker: b.marker,
        open_price: b.open_price,
        high_price: b.high_price,
        low_price: b.low_price,
        close_price: b.close_price,
        volume: b.volume,
        num_trades: b.num_trades,
        bid_volume: b.bid_volume,
        ask_volume: b.ask_volume,
        ts_event_ns: b.ts_event_ns,
    }
}

pub fn history_tick_to_pb(t: HistoryTickDto) -> pb::HistoryTick {
    pb::HistoryTick {
        symbol: t.symbol,
        exchange: t.exchange,
        open_price: t.open_price,
        high_price: t.high_price,
        low_price: t.low_price,
        close_price: t.close_price,
        volume: t.volume,
        num_trades: t.num_trades,
        ssboe: t.ssboe,
        usecs: t.usecs,
        ts_event_ns: t.ts_event_ns,
    }
}

pub fn front_month_to_pb(f: FrontMonthDto) -> pb::FrontMonthResponse {
    pb::FrontMonthResponse {
        symbol: f.symbol,
        exchange: f.exchange,
        trading_symbol: f.trading_symbol,
        trading_exchange: f.trading_exchange,
        symbol_name: f.symbol_name,
        is_front_month_symbol: f.is_front_month_symbol,
    }
}

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
        price_precision: r.price_precision as i32,
        is_tradable: r.is_tradable,
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

/// Sentinel key for unclassified plant messages (template types with no
/// symbol/exchange to route on). Routed separately from order notifications
/// so order-update subscribers do not receive unrelated plant chatter.
pub fn other_key() -> SubKey {
    SubKey {
        symbol: "__other__".into(),
        exchange: String::new(),
    }
}

/// Convert a plant event to its wire form, plus the routing key that
/// determines which fan-out subscribers receive it. Symbol/exchange events
/// route by `(symbol, exchange)`; account-level events use the internal
/// `pnl` / `order` / `other` sentinel keys (see [`pnl_key`]).
pub fn plant_event_to_routed(event: PlantEvent) -> (SubKey, pb::Event) {
    use pb::event::Body;
    match event {
        PlantEvent::LastTrade(t) => {
            let key = SubKey {
                symbol: t.symbol.clone().unwrap_or_default(),
                exchange: t.exchange.clone().unwrap_or_default(),
            };
            (
                key,
                pb::Event {
                    body: Some(Body::LastTrade(last_trade_to_pb(t))),
                },
            )
        }
        PlantEvent::Bbo(b) => {
            let key = SubKey {
                symbol: b.symbol.clone().unwrap_or_default(),
                exchange: b.exchange.clone().unwrap_or_default(),
            };
            (
                key,
                pb::Event {
                    body: Some(Body::Bbo(bbo_to_pb(b))),
                },
            )
        }
        PlantEvent::OrderBook(o) => {
            let key = SubKey {
                symbol: o.symbol.clone().unwrap_or_default(),
                exchange: o.exchange.clone().unwrap_or_default(),
            };
            (
                key,
                pb::Event {
                    body: Some(Body::OrderBook(order_book_to_pb(o))),
                },
            )
        }
        PlantEvent::TimeBar(b) => {
            let key = SubKey {
                symbol: b.symbol.clone().unwrap_or_default(),
                exchange: b.exchange.clone().unwrap_or_default(),
            };
            (
                key,
                pb::Event {
                    body: Some(Body::TimeBar(history_bar_to_pb(b))),
                },
            )
        }
        PlantEvent::AccountPnl(a) => (
            pnl_key(),
            pb::Event {
                body: Some(Body::AccountPnl(account_pnl_to_pb(a))),
            },
        ),
        PlantEvent::InstrumentPnl(i) => (
            pnl_key(),
            pb::Event {
                body: Some(Body::InstrumentPnl(instrument_pnl_to_pb(i))),
            },
        ),
        PlantEvent::OrderNotification(n) => (
            order_key(),
            pb::Event {
                body: Some(Body::OrderNotification(order_notification_to_pb(n))),
            },
        ),
        PlantEvent::Other { type_name, source } => (
            other_key(),
            pb::Event {
                body: Some(Body::Other(pb::OtherEvent { type_name, source })),
            },
        ),
    }
}
