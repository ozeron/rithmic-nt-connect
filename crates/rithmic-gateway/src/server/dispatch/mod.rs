//! Decode → consumer Session → encode. Plant policy belongs to rithmic-host.
use super::{Fingerprint, GatewayState};
use crate::convert::*;
use crate::idle_exit::{IdleExit, IdleExitPolicy};
use crate::pb::{self, frame::Body, Ack, ErrorResponse, Frame};
use crate::subscriptions::{FanoutHub, ParentGates, SharedFanout, SubKey};
use rithmic_host::{
    Error, HistoryPolicy, LocalSession, ModifyOrder, PlaceBracketOrder, PlaceOrder, Session,
    SessionHost,
};
use std::sync::Arc;

pub(super) fn ack_frame(request_id: u64) -> Frame {
    Frame {
        request_id,
        body: Some(Body::Ack(Ack {})),
    }
}
pub(super) fn error_frame(request_id: u64, code: &str, message: &str) -> Frame {
    Frame {
        request_id,
        body: Some(Body::Error(ErrorResponse {
            code: code.into(),
            message: message.into(),
        })),
    }
}
fn reply(request_id: u64, outcome: rithmic_host::Result<Body>, fallback: &str) -> Frame {
    match outcome {
        Ok(body) => Frame {
            request_id,
            body: Some(body),
        },
        Err(Error::Gateway { code, message }) => error_frame(request_id, &code, &message),
        Err(Error::ReconciliationUnavailable(msg)) => {
            error_frame(request_id, "reconciliation_unavailable", &msg)
        }
        Err(e) => {
            let code = if fallback == "command" {
                command_error_code(&e)
            } else {
                fallback.into()
            };
            // The client reconstructs these typed errors from code + message.
            // Display prefixes would become part of their payload on each hop.
            let message = match (&e, code.as_str()) {
                (Error::Rejected(message), "venue_rejected")
                | (Error::NotSent(message), "not_sent")
                | (Error::OutcomeUnknown(message), "venue_unknown") => message.clone(),
                _ => e.to_string(),
            };
            error_frame(request_id, &code, &message)
        }
    }
}
pub(super) fn command_error_code(e: &Error) -> String {
    match e {
        Error::Rejected(_) => "venue_rejected".into(),
        Error::NotSent(_)
        | Error::Config(_)
        | Error::NotConnected { .. }
        | Error::AlreadyConnected => "not_sent".into(),
        Error::Gateway { code, .. } => code.clone(),
        Error::ReconciliationUnavailable(_) => "reconciliation_unavailable".into(),
        _ => "venue_unknown".into(),
    }
}
pub(super) async fn dispatch(
    state: &GatewayState,
    client: &LocalSession,
    id: u64,
    body: Body,
) -> Frame {
    macro_rules! ack {
        ($call:expr) => {
            reply(id, $call.await.map(|()| Body::Ack(Ack {})), "command")
        };
    }
    macro_rules! rows {
        ($call:expr, $variant:ident, $ty:ident, $field:ident, $convert:expr, $err:literal) => {
            reply(
                id,
                $call.await.map(|rows| {
                    Body::$variant(pb::$ty {
                        $field: rows.into_iter().map($convert).collect(),
                    })
                }),
                $err,
            )
        };
    }
    match body {
        Body::Subscribe(r) => ack!(client.subscribe(&r.symbol, &r.exchange)),
        Body::Unsubscribe(r) => ack!(client.unsubscribe(&r.symbol, &r.exchange)),
        Body::SubscribeBook(r) => ack!(client.subscribe_order_book_summary(&r.symbol, &r.exchange)),
        Body::UnsubscribeBook(r) => {
            ack!(client.unsubscribe_order_book_summary(&r.symbol, &r.exchange))
        }
        Body::SubscribeTimeBars(r) => {
            ack!(client.subscribe_time_bars(&r.symbol, &r.exchange, r.bar_type, r.period))
        }
        Body::UnsubscribeTimeBars(r) => {
            ack!(client.unsubscribe_time_bars(&r.symbol, &r.exchange, r.bar_type, r.period))
        }
        Body::SubscribePnl(_) => ack!(client.subscribe_pnl()),
        Body::SubscribeOrderUpdates(_) => ack!(client.subscribe_order_updates()),
        Body::SubscribeBracketUpdates(_) => ack!(client.subscribe_bracket_updates()),
        Body::DisconnectPnl(_) => ack!(client.disconnect_pnl_plant()),
        Body::DisconnectOrder(_) => ack!(client.disconnect_order_plant()),
        Body::EnsureOrder(_) => ack!(client.ensure_order_plant()),
        Body::EnsurePnl(_) => ack!(client.ensure_pnl_plant()),
        Body::ResetTickerPlant(_) => ack!(state.host.reset_ticker_for_legacy_client()),
        Body::RequestPlants(r) => match rithmic_host::PlantSet::parse(&r.plants) {
            Ok(extra) => ack!(client.request_plants(extra)),
            Err(e) => error_frame(id, "invalid_plants", &e.to_string()),
        },
        Body::PlaceOrder(r) => ack!(client.place_order(&PlaceOrder {
            symbol: r.symbol,
            exchange: r.exchange,
            side: r.side,
            price_type: r.price_type,
            quantity: r.quantity,
            user_tag: r.user_tag,
            price: r.price,
            trigger_price: r.trigger_price,
            duration: r.duration,
            trail_by_ticks: r.trail_by_ticks,
            trail_by_price_id: r.trail_by_price_id,
        })),
        Body::PlaceBracketOrder(r) => ack!(client.place_bracket_order(&PlaceBracketOrder {
            symbol: r.symbol,
            exchange: r.exchange,
            side: r.side,
            price_type: r.price_type,
            quantity: r.quantity,
            localid: r.localid,
            price: r.price,
            trigger_price: r.trigger_price,
            duration: r.duration,
            stop_ticks: r.stop_ticks,
            target_ticks: r.target_ticks,
        })),
        Body::AdjustBracketStop(r) => {
            ack!(client.adjust_bracket_stop(&r.basket_id, r.ticks, r.level))
        }
        Body::AdjustBracketTarget(r) => {
            ack!(client.adjust_bracket_target(&r.basket_id, r.ticks, r.level))
        }
        Body::CancelOrder(r) => ack!(client.cancel_order(&r.basket_id)),
        Body::ModifyOrder(r) => ack!(client.modify_order(&ModifyOrder {
            basket_id: r.basket_id,
            symbol: r.symbol,
            exchange: r.exchange,
            quantity: r.quantity,
            price_type: r.price_type,
            price: r.price,
            trigger_price: r.trigger_price,
            trail_by_ticks: r.trail_by_ticks,
        })),
        Body::CancelAllOrders(_) => ack!(client.cancel_all_orders()),
        Body::LoadTicks(r) => rows!(
            client.load_ticks(&r.symbol, &r.exchange, r.start_time_sec, r.end_time_sec),
            LoadTicksResponse,
            LoadTicksResponse,
            ticks,
            history_tick_to_pb,
            "load_ticks_failed"
        ),
        Body::LoadTimeBars(r) => rows!(
            client.load_time_bars(
                &r.symbol,
                &r.exchange,
                r.start_time_sec,
                r.end_time_sec,
                r.bar_type,
                r.period
            ),
            LoadTimeBarsResponse,
            LoadTimeBarsResponse,
            bars,
            history_bar_to_pb,
            "load_time_bars_failed"
        ),
        Body::ProbeTimeBars(r) => rows!(
            client.probe_time_bars(
                &r.symbol,
                &r.exchange,
                r.start_time_sec,
                r.end_time_sec,
                r.bar_type,
                r.period
            ),
            ProbeTimeBarsResponse,
            ProbeTimeBarsResponse,
            rows,
            time_bar_probe_row_to_pb,
            "probe_time_bars_failed"
        ),
        Body::LoadOrders(_) => rows!(
            client.load_orders(0, 0),
            LoadOrdersResponse,
            LoadOrdersResponse,
            events,
            order_notification_to_pb,
            "load_orders_failed"
        ),
        Body::LoadProductRmsInfo(_) => rows!(
            client.load_product_rms_info(),
            LoadProductRmsInfoResponse,
            LoadProductRmsInfoResponse,
            rows,
            product_rms_info_to_pb,
            "fetch_rms_failed"
        ),
        Body::LoadAccountRmsInfo(_) => rows!(
            client.load_account_rms_info(),
            LoadAccountRmsInfoResponse,
            LoadAccountRmsInfoResponse,
            rows,
            account_rms_info_to_pb,
            "fetch_rms_failed"
        ),
        Body::LoadFillHistory(r) => rows!(
            client.load_fill_history(r.start_date, r.finish_date),
            LoadFillHistoryResponse,
            LoadFillHistoryResponse,
            rows,
            fill_history_to_pb,
            "load_fill_history_failed"
        ),
        Body::LoadOrderHistory(r) => rows!(
            client.load_order_history(&r.date),
            LoadOrderHistoryResponse,
            LoadOrderHistoryResponse,
            events,
            order_notification_to_pb,
            "load_order_history_failed"
        ),
        Body::LoadOrderHistoryDates(_) => reply(
            id,
            client.load_order_history_dates().await.map(|dates| {
                Body::LoadOrderHistoryDatesResponse(pb::LoadOrderHistoryDatesResponse { dates })
            }),
            "load_order_history_dates_failed",
        ),
        Body::GetFrontMonth(r) => reply(
            id,
            client
                .get_front_month(&r.symbol, &r.exchange)
                .await
                .map(|dto| Body::FrontMonthResponse(front_month_to_pb(dto))),
            "get_front_month_failed",
        ),
        Body::GetReferenceData(r) => reply(
            id,
            client
                .get_reference_data(&r.symbol, &r.exchange)
                .await
                .map(|dto| Body::ReferenceDataResponse(reference_data_to_pb(dto))),
            "get_reference_data_failed",
        ),
        Body::ResolvedAccount(_) => reply(
            id,
            client.resolved_account_result().await.map(|account| {
                Body::ResolvedAccountResponse(
                    account.map(resolved_account_to_pb).unwrap_or_default(),
                )
            }),
            "resolved_account_failed",
        ),
        Body::GetLiveMdState(_) => {
            let (ticker, book, time_bars) = client.live_md_intent_counts().await;
            Frame {
                request_id: id,
                body: Some(Body::GetLiveMdStateResponse(pb::GetLiveMdStateResponse {
                    live_md: ticker > 0 || book > 0 || time_bars > 0,
                    ticker_intents: ticker,
                    book_intents: book,
                    time_bar_intents: time_bars,
                    ready_peers: state.idle.peer_count().await as u32,
                })),
            }
        }
        Body::Disconnect(_) => ack_frame(id),
        _ => error_frame(id, "unsupported", "unsupported request body"),
    }
}

pub(super) fn test_state(gates: ParentGates, _hub: SharedFanout) -> GatewayState {
    GatewayState {
        host: SessionHost::with_handle(
            None,
            gates,
            Default::default(),
            HistoryPolicy::RefuseUnderLiveMd,
            true,
            8,
        ),
        fingerprint: Fingerprint {
            user: "alice".into(),
            system_name: "LucidTrading".into(),
            url: "wss://example".into(),
            env: "Live".into(),
            account_id: "A1".into(),
            fcm_id: "F1".into(),
            ib_id: "I1".into(),
        },
        expected_auth_token: "secret".into(),
        idle: IdleExit::new(IdleExitPolicy::Never),
        gateway_instance_id: super::boot_gateway_metadata().0,
    }
}
pub fn gate_rpc_for_test(gates: &ParentGates, body: Body) -> Body {
    rpc_sequence_with_gates(*gates, vec![body]).0.remove(0)
}
pub fn rpc_sequence_with_gates(
    gates: ParentGates,
    bodies: Vec<Body>,
) -> (Vec<Body>, crate::reconnect::RestorePlan) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(async {
            let state = test_state(gates, Arc::new(FanoutHub::new(8)));
            let client = state.host.attach();
            let mut out = Vec::new();
            for (i, body) in bodies.into_iter().enumerate() {
                out.push(
                    dispatch(&state, &client, i as u64 + 1, body)
                        .await
                        .body
                        .expect("body"),
                );
            }
            let plan = state.reconnect.restore_plan().await;
            (out, plan)
        })
}
pub fn history_rpc_with_live_ticker_intent_for_test(body: Body) -> Body {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(async {
            let state = test_state(ParentGates::default(), Arc::new(FanoutHub::new(8)));
            state
                .reconnect
                .note_ticker(SubKey {
                    symbol: "MNQU6".into(),
                    exchange: "CME".into(),
                })
                .await;
            dispatch(&state, &state.host.attach(), 1, body)
                .await
                .body
                .expect("body")
        })
}

#[cfg(test)]
mod error_payload_tests {
    use super::*;

    #[test]
    fn typed_errors_keep_inner_payloads_on_the_wire() {
        for (error, code) in [
            (Error::Rejected("x".into()), "venue_rejected"),
            (Error::NotSent("x".into()), "not_sent"),
            (Error::OutcomeUnknown("x".into()), "venue_unknown"),
            (
                Error::ReconciliationUnavailable("x".into()),
                "reconciliation_unavailable",
            ),
        ] {
            let frame = reply(7, Err(error), "command");
            let bytes = crate::codec::encode(&frame).unwrap();
            let (frame, _) = crate::codec::decode::<Frame>(&bytes).unwrap();
            assert_eq!(frame.request_id, 7);
            assert_eq!(
                frame.body,
                Some(Body::Error(ErrorResponse {
                    code: code.into(),
                    message: "x".into()
                }))
            );
        }
    }

    #[test]
    fn untyped_command_failures_keep_their_display_text_and_code() {
        let error = Error::Config("bad input".into());
        let display = error.to_string();
        assert_eq!(
            reply(3, Err(error), "command").body,
            Some(Body::Error(ErrorResponse {
                code: "not_sent".into(),
                message: display
            }))
        );
    }
}
