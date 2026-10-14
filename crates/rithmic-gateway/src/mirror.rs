//! DTO ↔ protobuf mappings that are plain field moves, declared once.
//!
//! Each `mirror!` line lists a DTO, its wire message and the shared field names,
//! and generates both directions: `<name>_to_pb` (the server, `crate::convert`)
//! and `<name>_from_pb` (the client, `crate::client::convert`). A field added on
//! one side and not the other is a compile error here instead of a silent drop
//! on the wire. Mappings that convert a value (reference data, the resolved
//! account) stay hand-written next to their callers.

use rithmic_plants::dto::{
    AccountPnlDto, AccountRmsInfoDto, BboDto, BracketUpdateDto, FillHistoryDto, FrontMonthDto,
    HistoryBarDto, HistoryTickDto, InstrumentPnlDto, LastTradeDto, OrderBookDto,
    OrderNotificationDto, ProductRmsInfoDto, TimeBarProbeRow,
};

use crate::pb;

macro_rules! mirror {
    ($to:ident, $from:ident, $dto:ident, $pb:ident { $($field:ident),+ $(,)? }) => {
        pub fn $to(v: $dto) -> pb::$pb {
            pb::$pb { $($field: v.$field),+ }
        }

        pub fn $from(v: pb::$pb) -> $dto {
            $dto { $($field: v.$field),+ }
        }
    };
}

mirror!(
    last_trade_to_pb,
    last_trade_from_pb,
    LastTradeDto,
    LastTrade {
        symbol,
        exchange,
        trade_price,
        trade_size,
        aggressor,
        ssboe,
        usecs,
        ts_event_ns,
        is_snapshot,
    }
);

mirror!(
    bbo_to_pb,
    bbo_from_pb,
    BboDto,
    Bbo {
        symbol,
        exchange,
        bid_price,
        bid_size,
        ask_price,
        ask_size,
        ssboe,
        usecs,
        ts_event_ns,
        is_snapshot,
    }
);

mirror!(
    product_rms_info_to_pb,
    product_rms_info_from_pb,
    ProductRmsInfoDto,
    ProductRmsInfo {
        product_code,
        commission_fill_rate,
        presence_bits,
    }
);

mirror!(
    account_rms_info_to_pb,
    account_rms_info_from_pb,
    AccountRmsInfoDto,
    AccountRmsInfo {
        account_id,
        default_commission,
        presence_bits,
    }
);

mirror!(
    fill_history_to_pb,
    fill_history_from_pb,
    FillHistoryDto,
    FillHistory {
        account_id,
        basket_id,
        symbol,
        exchange,
        transaction_type,
        price_type,
        duration,
        manual_or_auto,
        fill_id,
        fill_date,
        fill_time,
        fill_price,
        fill_size,
        price,
        trigger_price,
        ssboe,
        usecs,
    }
);

mirror!(
    order_book_to_pb,
    order_book_from_pb,
    OrderBookDto,
    OrderBook {
        symbol,
        exchange,
        update_type,
        bid_price,
        bid_size,
        ask_price,
        ask_size,
        ssboe,
        usecs,
        ts_event_ns,
    }
);

mirror!(
    account_pnl_to_pb,
    account_pnl_from_pb,
    AccountPnlDto,
    AccountPnl {
        account_id,
        fcm_id,
        ib_id,
        account_balance,
        cash_on_hand,
        margin_balance,
        day_pnl,
        open_position_pnl,
        closed_position_pnl,
        available_buying_power,
        used_buying_power,
        is_snapshot,
        ssboe,
        usecs,
    }
);

mirror!(
    instrument_pnl_to_pb,
    instrument_pnl_from_pb,
    InstrumentPnlDto,
    InstrumentPnl {
        account_id,
        symbol,
        exchange,
        product_code,
        instrument_type,
        open_position_pnl,
        closed_position_pnl,
        mtm_security,
        open_position_quantity,
        closed_position_quantity,
        net_quantity,
        avg_open_fill_price,
        is_snapshot,
        ssboe,
        usecs,
    }
);

mirror!(
    order_notification_to_pb,
    order_notification_from_pb,
    OrderNotificationDto,
    OrderNotification {
        source,
        kind,
        notify_type,
        notify_type_name,
        status,
        basket_id,
        exchange_order_id,
        user_tag,
        account_id,
        symbol,
        exchange,
        quantity,
        total_fill_size,
        total_unfilled_size,
        fill_size,
        price,
        trigger_price,
        avg_fill_price,
        fill_price,
        transaction_type,
        price_type,
        duration,
        fill_id,
        text,
        report_text,
        completion_reason,
        ssboe,
        usecs,
        ts_event_ns,
        is_snapshot,
    }
);

mirror!(
    history_bar_to_pb,
    history_bar_from_pb,
    HistoryBarDto,
    HistoryBar {
        symbol,
        exchange,
        bar_type,
        period,
        marker,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        num_trades,
        bid_volume,
        ask_volume,
        ts_event_ns,
    }
);

mirror!(
    history_tick_to_pb,
    history_tick_from_pb,
    HistoryTickDto,
    HistoryTick {
        symbol,
        exchange,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        num_trades,
        ssboe,
        usecs,
        ts_event_ns,
    }
);

mirror!(
    front_month_to_pb,
    front_month_from_pb,
    FrontMonthDto,
    FrontMonthResponse {
        symbol,
        exchange,
        trading_symbol,
        trading_exchange,
        symbol_name,
        is_front_month_symbol,
    }
);

mirror!(
    time_bar_probe_row_to_pb,
    time_bar_probe_row_from_pb,
    TimeBarProbeRow,
    TimeBarProbeRow {
        variant,
        source,
        error,
        rp_code,
        parsed,
        skip_reason,
        symbol,
        exchange,
        bar_type,
        period,
        marker,
        open_price,
        high_price,
        low_price,
        close_price,
        settlement_price,
        has_settlement_price,
        volume,
        num_trades,
    }
);

mirror!(
    bracket_update_to_pb,
    bracket_update_from_pb,
    BracketUpdateDto,
    BracketUpdate {
        account_id,
        fcm_id,
        ib_id,
        basket_id,
        stop_ticks,
        stop_quantity,
        stop_quantity_released,
        target_ticks,
        target_quantity,
        target_quantity_released,
    }
);
