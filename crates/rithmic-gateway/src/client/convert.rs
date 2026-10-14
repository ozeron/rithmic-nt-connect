//! pb → DTO: the exact inverse of `crate::convert` (the server's DTO → pb).
//!
//! `GatewaySession` hands out the same `rithmic_plants` DTOs `DirectSession`
//! does, so the extension converts them to dicts in exactly one place for both
//! modes. A round-trip test (`tests/client_convert.rs`) pins `from(to(x)) == x`.

use rithmic_plants::dto::{
    AccountPnlDto, AccountRmsInfoDto, BboDto, FillHistoryDto, FrontMonthDto, HistoryBarDto,
    HistoryTickDto, InstrumentPnlDto, LastTradeDto, OrderBookDto, OrderNotificationDto, PlantEvent,
    ProductRmsInfoDto, ReferenceDataDto, TimeBarProbeRow,
};
use rithmic_plants::AccountTriple;

use crate::pb;

pub fn last_trade_from_pb(t: pb::LastTrade) -> LastTradeDto {
    LastTradeDto {
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

pub fn bbo_from_pb(b: pb::Bbo) -> BboDto {
    BboDto {
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

pub fn order_book_from_pb(o: pb::OrderBook) -> OrderBookDto {
    OrderBookDto {
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

pub fn account_pnl_from_pb(a: pb::AccountPnl) -> AccountPnlDto {
    AccountPnlDto {
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

pub fn instrument_pnl_from_pb(i: pb::InstrumentPnl) -> InstrumentPnlDto {
    InstrumentPnlDto {
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

pub fn order_notification_from_pb(n: pb::OrderNotification) -> OrderNotificationDto {
    OrderNotificationDto {
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
        duration: n.duration,
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

pub fn history_bar_from_pb(b: pb::HistoryBar) -> HistoryBarDto {
    HistoryBarDto {
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

pub fn history_tick_from_pb(t: pb::HistoryTick) -> HistoryTickDto {
    HistoryTickDto {
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

pub fn front_month_from_pb(f: pb::FrontMonthResponse) -> FrontMonthDto {
    FrontMonthDto {
        symbol: f.symbol,
        exchange: f.exchange,
        trading_symbol: f.trading_symbol,
        trading_exchange: f.trading_exchange,
        symbol_name: f.symbol_name,
        is_front_month_symbol: f.is_front_month_symbol,
    }
}

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

pub fn product_rms_info_from_pb(r: pb::ProductRmsInfo) -> ProductRmsInfoDto {
    ProductRmsInfoDto {
        product_code: r.product_code,
        commission_fill_rate: r.commission_fill_rate,
        presence_bits: r.presence_bits,
    }
}

pub fn account_rms_info_from_pb(r: pb::AccountRmsInfo) -> AccountRmsInfoDto {
    AccountRmsInfoDto {
        account_id: r.account_id,
        default_commission: r.default_commission,
        presence_bits: r.presence_bits,
    }
}

pub fn fill_history_from_pb(f: pb::FillHistory) -> FillHistoryDto {
    FillHistoryDto {
        account_id: f.account_id,
        basket_id: f.basket_id,
        symbol: f.symbol,
        exchange: f.exchange,
        transaction_type: f.transaction_type,
        price_type: f.price_type,
        duration: f.duration,
        manual_or_auto: f.manual_or_auto,
        fill_id: f.fill_id,
        fill_date: f.fill_date,
        fill_time: f.fill_time,
        fill_price: f.fill_price,
        fill_size: f.fill_size,
        price: f.price,
        trigger_price: f.trigger_price,
        ssboe: f.ssboe,
        usecs: f.usecs,
    }
}

pub fn time_bar_probe_row_from_pb(r: pb::TimeBarProbeRow) -> TimeBarProbeRow {
    TimeBarProbeRow {
        variant: r.variant,
        source: r.source,
        error: r.error,
        rp_code: r.rp_code,
        parsed: r.parsed,
        skip_reason: r.skip_reason,
        symbol: r.symbol,
        exchange: r.exchange,
        bar_type: r.bar_type,
        period: r.period,
        marker: r.marker,
        open_price: r.open_price,
        high_price: r.high_price,
        low_price: r.low_price,
        close_price: r.close_price,
        settlement_price: r.settlement_price,
        has_settlement_price: r.has_settlement_price,
        volume: r.volume,
        num_trades: r.num_trades,
    }
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
pub fn bracket_update_from_pb(b: pb::BracketUpdate) -> rithmic_plants::BracketUpdateDto {
    rithmic_plants::BracketUpdateDto {
        account_id: b.account_id,
        fcm_id: b.fcm_id,
        ib_id: b.ib_id,
        basket_id: b.basket_id,
        stop_ticks: b.stop_ticks,
        stop_quantity: b.stop_quantity,
        stop_quantity_released: b.stop_quantity_released,
        target_ticks: b.target_ticks,
        target_quantity: b.target_quantity,
        target_quantity_released: b.target_quantity_released,
    }
}

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
