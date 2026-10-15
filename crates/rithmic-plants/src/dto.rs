//! Venue DTOs converted from rithmic-rs plant messages.

use rithmic_exec::{exchange_ts_ns, venue_ts_ns};
use rithmic_host::dto::*;
use rithmic_rs::rti::messages::RithmicMessage;
use rithmic_rs::RithmicResponse;

pub(crate) fn product_rms_info_from_response(resp: &RithmicResponse) -> Option<ProductRmsInfoDto> {
    match &resp.message {
        RithmicMessage::ResponseProductRmsInfo(m) => Some(ProductRmsInfoDto {
            product_code: m.product_code.clone(),
            commission_fill_rate: m.commission_fill_rate,
            presence_bits: m.presence_bits,
        }),
        _ => None,
    }
}

pub(crate) fn account_rms_info_from_response(resp: &RithmicResponse) -> Option<AccountRmsInfoDto> {
    match &resp.message {
        RithmicMessage::ResponseAccountRmsInfo(m) => Some(AccountRmsInfoDto {
            account_id: m.account_id.clone(),
            default_commission: m.default_commission,
            presence_bits: m.presence_bits,
        }),
        _ => None,
    }
}

pub(crate) fn fill_history_from_response(resp: &RithmicResponse) -> Option<FillHistoryDto> {
    match &resp.message {
        RithmicMessage::ResponseShowFillHistory(m) => fill_history_from_message(m),
        _ => None,
    }
}

/// The terminal response carries only rp_code; rows without a fill id are skipped.
fn fill_history_from_message(
    m: &rithmic_rs::rti::ResponseShowFillHistory,
) -> Option<FillHistoryDto> {
    m.fill_id.as_ref()?;
    Some(FillHistoryDto {
        account_id: m.account_id.clone(),
        basket_id: m.basket_id.clone(),
        symbol: m.symbol.clone(),
        exchange: m.exchange.clone(),
        transaction_type: m.transaction_type.clone(),
        price_type: m.price_type.clone(),
        duration: m.duration.clone(),
        manual_or_auto: m.manual_or_auto.clone(),
        fill_id: m.fill_id.clone(),
        fill_date: m.fill_date.clone(),
        fill_time: m.fill_time.clone(),
        fill_price: m.fill_price,
        fill_size: m.fill_size,
        price: m.price,
        trigger_price: m.trigger_price,
        ssboe: m.ssboe,
        usecs: m.usecs,
    })
}

/// Why the bar parser drops this row, or `None` when it keeps it. The verdict is
/// the parser's own ([`history_bar_from_response`]), so a probe row reads
/// `parsed` exactly when the load would return the bar; the reason names the
/// first missing piece.
fn probe_skip_reason(
    resp: &RithmicResponse,
    close: Option<f64>,
    marker: Option<i32>,
    open_high_low: [Option<f64>; 3],
) -> Option<String> {
    if history_bar_from_response(resp).is_some() {
        return None;
    }
    Some(
        if close.is_none() {
            "no close or settlement"
        } else if marker.is_none() {
            "no marker"
        } else if open_high_low.iter().any(Option::is_none) {
            "missing open/high/low"
        } else {
            "no volume or unusable marker"
        }
        .into(),
    )
}

pub(crate) fn time_bar_probe_from_response(resp: &RithmicResponse) -> TimeBarProbeRow {
    let rp_code = resp
        .rp_code()
        .map(|codes| codes.to_vec())
        .unwrap_or_default();
    let error = resp.error.as_ref().map(ToString::to_string);
    match &resp.message {
        RithmicMessage::ResponseTimeBarReplay(m) => {
            let skip_reason = probe_skip_reason(
                resp,
                m.close_price.or(m.settlement_price),
                m.marker,
                [m.open_price, m.high_price, m.low_price],
            );
            TimeBarProbeRow {
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
            let skip_reason = probe_skip_reason(
                resp,
                m.close_price,
                m.marker,
                [m.open_price, m.high_price, m.low_price],
            );
            TimeBarProbeRow {
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
        other => TimeBarProbeRow {
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

fn ts_ns(ssboe: Option<i32>, usecs: Option<i32>) -> Option<u64> {
    venue_ts_ns(ssboe, usecs)
}

fn order_kind(
    source: &str,
    notify_type_name: Option<&str>,
    status: Option<&str>,
    text: Option<&str>,
) -> Option<String> {
    // One mapping for plant and adapter: see `rithmic_exec::kind_from_notify`.
    rithmic_exec::kind_from_notify(source, notify_type_name?, status, text).map(str::to_string)
}

/// An order notification DTO from either venue message (Rithmic-side status
/// or exchange report). They share every field but four, which the caller names.
macro_rules! order_notification {
    ($n:ident, $source:literal, $module:ident, $($extra:ident: $value:expr),+ $(,)?) => {{
        let notify_type_name = $n.notify_type.and_then(|v| {
            rithmic_rs::rti::$module::NotifyType::try_from(v)
                .ok()
                .map(|t| t.as_str_name().to_string())
        });
        let kind = order_kind(
            $source,
            notify_type_name.as_deref(),
            $n.status.as_deref(),
            $n.text.as_deref(),
        );
        PlantEvent::OrderNotification(OrderNotificationDto {
            source: $source.into(),
            kind,
            notify_type: $n.notify_type,
            notify_type_name,
            status: $n.status.clone(),
            basket_id: $n.basket_id.clone(),
            exchange_order_id: $n.exchange_order_id.clone(),
            user_tag: $n.user_tag.clone(),
            account_id: $n.account_id.clone(),
            symbol: $n.symbol.clone(),
            exchange: $n.exchange.clone(),
            quantity: $n.quantity,
            total_fill_size: $n.total_fill_size,
            total_unfilled_size: $n.total_unfilled_size,
            price: $n.price,
            trigger_price: $n.trigger_price,
            avg_fill_price: $n.avg_fill_price,
            transaction_type: $n.transaction_type,
            price_type: $n.price_type,
            duration: $n.duration,
            text: $n.text.clone(),
            report_text: $n.report_text.clone(),
            ssboe: $n.ssboe,
            usecs: $n.usecs,
            ts_event_ns: ts_ns($n.ssboe, $n.usecs),
            is_snapshot: $n.is_snapshot,
            $($extra: $value),+
        })
    }};
}

pub(crate) fn plant_event(resp: &RithmicResponse) -> PlantEvent {
    match &resp.message {
        RithmicMessage::LastTrade(t) => PlantEvent::LastTrade(LastTradeDto {
            symbol: t.symbol.clone(),
            exchange: t.exchange.clone(),
            trade_price: t.trade_price,
            trade_size: t.trade_size,
            aggressor: t.aggressor,
            ssboe: t.ssboe,
            usecs: t.usecs,
            // The exchange's own trade time when sent; Rithmic's otherwise.
            ts_event_ns: exchange_ts_ns(t.source_ssboe, t.source_usecs, t.source_nsecs)
                .or_else(|| ts_ns(t.ssboe, t.usecs)),
            is_snapshot: t.is_snapshot,
        }),
        RithmicMessage::BestBidOffer(b) => PlantEvent::Bbo(BboDto {
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
        RithmicMessage::OrderBook(o) => PlantEvent::OrderBook(OrderBookDto {
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
        RithmicMessage::AccountPnLPositionUpdate(a) => PlantEvent::AccountPnl(AccountPnlDto {
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
        RithmicMessage::InstrumentPnLPositionUpdate(i) => {
            PlantEvent::InstrumentPnl(InstrumentPnlDto {
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
            })
        }
        RithmicMessage::RithmicOrderNotification(n) => order_notification!(
            n,
            "rithmic",
            rithmic_order_notification,
            // Rithmic-side status: no fill fields; completion reason is its own.
            fill_size: None,
            fill_price: None,
            fill_id: None,
            completion_reason: n.completion_reason.clone(),
        ),
        RithmicMessage::ExchangeOrderNotification(n) => order_notification!(
            n,
            "exchange",
            exchange_order_notification,
            fill_size: n.fill_size,
            fill_price: n.fill_price,
            fill_id: n.fill_id.clone(),
            completion_reason: None,
        ),
        RithmicMessage::BracketUpdates(b) => PlantEvent::BracketUpdate(BracketUpdateDto {
            account_id: b.account_id.clone(),
            fcm_id: b.fcm_id.clone(),
            ib_id: b.ib_id.clone(),
            basket_id: b.basket_id.clone(),
            stop_ticks: b.stop_ticks,
            stop_quantity: b.stop_quantity,
            stop_quantity_released: b.stop_quantity_released,
            target_ticks: b.target_ticks,
            target_quantity: b.target_quantity,
            target_quantity_released: b.target_quantity_released,
        }),
        RithmicMessage::TimeBar(_) => match history_bar_from_response(resp) {
            Some(bar) => PlantEvent::TimeBar(bar),
            None => PlantEvent::Other {
                type_name: "TimeBar".into(),
                source: resp.source.clone(),
            },
        },
        other => PlantEvent::Other {
            type_name: format!("{other:?}")
                .split('(')
                .next()
                .unwrap_or("Unknown")
                .to_string(),
            source: resp.source.clone(),
        },
    }
}

pub(crate) fn reference_data_from_response(resp: &RithmicResponse) -> Option<ReferenceDataDto> {
    use rithmic_rs::InstrumentInfo;
    match &resp.message {
        RithmicMessage::ResponseReferenceData(data) => {
            let info = InstrumentInfo::try_from(data).ok()?;
            let price_precision = info.price_precision();
            Some(ReferenceDataDto {
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

pub(crate) fn front_month_from_response(resp: &RithmicResponse) -> Option<FrontMonthDto> {
    match &resp.message {
        RithmicMessage::ResponseFrontMonthContract(m) => Some(FrontMonthDto {
            symbol: m.symbol.clone(),
            exchange: m.exchange.clone(),
            trading_symbol: m.trading_symbol.clone(),
            trading_exchange: m.trading_exchange.clone(),
            symbol_name: m.symbol_name.clone(),
            is_front_month_symbol: m.is_front_month_symbol,
        }),
        RithmicMessage::FrontMonthContractUpdate(m) => Some(FrontMonthDto {
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

pub(crate) fn history_tick_from_response(resp: &RithmicResponse) -> Option<HistoryTickDto> {
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
            Some(HistoryTickDto {
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

/// Build a bar only when OHLC are present. Missing open/high/low are dropped
/// (never filled from close). Missing volume is allowed only for settlement
/// replay rows (`allow_missing_volume`); otherwise the row is dropped.
// Flat field mapping from a venue bar record: every arg is a distinct scalar
// field, so a struct wrapper would only relocate the same arity. Allowed.
#[allow(clippy::too_many_arguments)]
fn history_bar_from_ohlcv(
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
    allow_missing_volume: bool,
) -> Option<HistoryBarDto> {
    let open_price = open_price?;
    let high_price = high_price?;
    let low_price = low_price?;
    let volume = match volume {
        Some(v) => v,
        None if allow_missing_volume => 0,
        None => return None,
    };
    let ssboe = crate::history::marker_to_ssboe(marker)?;
    Some(HistoryBarDto {
        symbol,
        exchange,
        bar_type,
        period,
        marker: Some(marker),
        open_price: Some(open_price),
        high_price: Some(high_price),
        low_price: Some(low_price),
        close_price: Some(close_price),
        volume: Some(volume),
        num_trades,
        bid_volume,
        ask_volume,
        ts_event_ns: venue_ts_ns(Some(ssboe), Some(0)),
    })
}

pub(crate) fn history_bar_from_response(resp: &RithmicResponse) -> Option<HistoryBarDto> {
    match &resp.message {
        RithmicMessage::ResponseTimeBarReplay(m) => {
            // End-of-replay markers have no price. Settlement may stand in
            // for close; volume may be omitted only on that settlement path.
            let used_settlement = m.close_price.is_none() && m.settlement_price.is_some();
            let close_price = m.close_price.or(m.settlement_price)?;
            let marker = m.marker?;
            history_bar_from_ohlcv(
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
                used_settlement,
            )
        }
        RithmicMessage::TimeBar(m) => {
            let close_price = m.close_price?;
            let marker = m.marker?;
            history_bar_from_ohlcv(
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
                false,
            )
        }
        _ => None,
    }
}

#[cfg(test)]
mod order_kind_tests {
    use super::order_kind;

    #[test]
    fn trigger_pending_is_stop_acceptance_evidence() {
        // Live-proven (Rithmic Test 2026-08-21): resting STOP_MARKET orders
        // never receive OPEN; TRIGGER_PENDING is their working state.
        assert_eq!(
            order_kind("rithmic", Some("TRIGGER_PENDING"), None, None).as_deref(),
            Some("accepted")
        );
        assert_eq!(
            order_kind("rithmic", Some("OPEN_PENDING"), None, None).as_deref(),
            None
        );
    }

    #[test]
    fn complete_with_text_is_terminal_reject() {
        assert_eq!(
            order_kind(
                "rithmic",
                Some("COMPLETE"),
                Some("complete"),
                Some("No such route exists."),
            )
            .as_deref(),
            Some("rejected")
        );
        assert_eq!(
            order_kind("rithmic", Some("COMPLETE"), Some("complete"), None).as_deref(),
            None
        );
        assert_eq!(
            order_kind("rithmic", Some("COMPLETE"), Some("complete"), Some("  ")).as_deref(),
            None
        );
        assert_eq!(
            order_kind(
                "rithmic",
                Some("COMPLETE"),
                Some("CANCELLED"),
                Some("whatever"),
            )
            .as_deref(),
            Some("canceled")
        );
    }
}

#[cfg(test)]
mod history_bar_tests {
    use super::history_bar_from_ohlcv;

    #[test]
    fn close_only_does_not_invent_ohl() {
        assert!(history_bar_from_ohlcv(
            Some("NQU6".into()),
            Some("CME".into()),
            Some(3),
            Some("1".into()),
            1_700_000_000,
            100.0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
        )
        .is_none());
    }

    #[test]
    fn partial_ohl_is_dropped() {
        assert!(history_bar_from_ohlcv(
            Some("NQU6".into()),
            Some("CME".into()),
            Some(2),
            Some("1".into()),
            1_700_000_000,
            100.0,
            Some(99.0),
            Some(101.0),
            None,
            Some(10),
            None,
            None,
            None,
            false,
        )
        .is_none());
    }

    #[test]
    fn missing_volume_dropped_unless_settlement_path() {
        assert!(history_bar_from_ohlcv(
            Some("NQU6".into()),
            Some("CME".into()),
            Some(2),
            Some("1".into()),
            1_700_000_000,
            100.0,
            Some(99.0),
            Some(101.0),
            Some(98.5),
            None,
            None,
            None,
            None,
            false,
        )
        .is_none());
        let bar = history_bar_from_ohlcv(
            Some("NQU6".into()),
            Some("CME".into()),
            Some(3),
            Some("1".into()),
            1_700_000_000,
            100.0,
            Some(99.0),
            Some(101.0),
            Some(98.5),
            None,
            None,
            None,
            None,
            true,
        )
        .expect("settlement volume default");
        assert_eq!(bar.volume, Some(0));
    }

    #[test]
    fn full_ohlc_parses() {
        let bar = history_bar_from_ohlcv(
            Some("NQU6".into()),
            Some("CME".into()),
            Some(2),
            Some("1".into()),
            1_700_000_000,
            100.0,
            Some(99.0),
            Some(101.0),
            Some(98.5),
            Some(12),
            None,
            None,
            None,
            false,
        )
        .expect("bar");
        assert_eq!(bar.open_price, Some(99.0));
        assert_eq!(bar.high_price, Some(101.0));
        assert_eq!(bar.low_price, Some(98.5));
        assert_eq!(bar.close_price, Some(100.0));
        assert_eq!(bar.volume, Some(12));
    }
}

#[cfg(test)]
mod fill_history_tests {
    use super::fill_history_from_message;
    use rithmic_rs::rti::ResponseShowFillHistory;

    #[test]
    fn fill_row_maps_fields() {
        let mut m = ResponseShowFillHistory::default();
        m.fill_id = Some("F1".into());
        m.symbol = Some("MNQZ6".into());
        m.manual_or_auto = Some("AUTO".into());
        m.fill_price = Some(30666.0);
        m.fill_size = Some(1);
        let row = fill_history_from_message(&m).expect("fill row");
        assert_eq!(row.symbol.as_deref(), Some("MNQZ6"));
        assert_eq!(row.fill_price, Some(30666.0));
        assert_eq!(row.manual_or_auto.as_deref(), Some("AUTO"));
    }

    #[test]
    fn terminal_response_without_fill_is_skipped() {
        assert!(fill_history_from_message(&ResponseShowFillHistory::default()).is_none());
    }
}
