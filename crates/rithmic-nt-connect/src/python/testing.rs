//! Test-only composition root and plant scripting. Never in release wheels.
use super::*;
use rithmic_host::fake::{FakeFailure, FakeVenue};
use rithmic_host::{Stream, Timing};

#[pyclass(name = "FakeScript")]
pub(super) struct PyFakeScript {
    pub venue: Arc<FakeVenue>,
}
fn stream(name: &str) -> PyResult<Stream> {
    Stream::from_plant(name)
        .ok_or_else(|| PyValueError::new_err(format!("unknown stream {name:?}")))
}
macro_rules! dto {
    ($d:expr, $name:ident, $($field:ident),+ $(,)?) => {
        $name { $( $field: $d.get_item(stringify!($field))?.map(|v| v.extract()).transpose()?.unwrap_or_default(), )+ }
    };
}
#[pymethods]
impl PyFakeScript {
    fn emit(&self, stream_name: &str, event: &Bound<'_, PyDict>) -> PyResult<()> {
        let lane = stream(stream_name)?;
        let kind: String = event
            .get_item("type")?
            .ok_or_else(|| PyValueError::new_err("event needs type"))?
            .extract()?;
        let event = match kind.as_str() {
            "last_trade" => PlantEvent::LastTrade(dto!(
                event,
                LastTradeDto,
                symbol,
                exchange,
                trade_price,
                trade_size,
                aggressor,
                ssboe,
                usecs,
                ts_event_ns,
                is_snapshot
            )),
            "bbo" => PlantEvent::Bbo(dto!(
                event,
                BboDto,
                symbol,
                exchange,
                bid_price,
                bid_size,
                ask_price,
                ask_size,
                ssboe,
                usecs,
                ts_event_ns,
                is_snapshot
            )),
            "order_book" => PlantEvent::OrderBook(dto!(
                event,
                OrderBookDto,
                symbol,
                exchange,
                update_type,
                bid_price,
                bid_size,
                ask_price,
                ask_size,
                ssboe,
                usecs,
                ts_event_ns
            )),
            "account_pnl" => PlantEvent::AccountPnl(dto!(
                event,
                AccountPnlDto,
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
                usecs
            )),
            "instrument_pnl" => PlantEvent::InstrumentPnl(dto!(
                event,
                InstrumentPnlDto,
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
                usecs
            )),
            "order_notification" => PlantEvent::OrderNotification(dto!(
                event,
                OrderNotificationDto,
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
                is_snapshot
            )),
            "bracket_update" => PlantEvent::BracketUpdate(dto!(
                event,
                BracketUpdateDto,
                account_id,
                fcm_id,
                ib_id,
                basket_id,
                stop_ticks,
                stop_quantity,
                stop_quantity_released,
                target_ticks,
                target_quantity,
                target_quantity_released
            )),
            "time_bar" => PlantEvent::TimeBar(dto!(
                event,
                HistoryBarDto,
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
                ts_event_ns
            )),
            "gap" => PlantEvent::Gap(rithmic_host::GapDto {
                stream: lane,
                skipped: event
                    .get_item("skipped")?
                    .map(|v| v.extract())
                    .transpose()?
                    .unwrap_or(1),
            }),
            _ => {
                return Err(PyValueError::new_err(format!(
                    "unknown event type {kind:?}"
                )))
            }
        };
        self.venue.push_event(lane, event);
        Ok(())
    }
    #[pyo3(signature = (stream_name, cause="scripted plant failure"))]
    fn fail_stream(&self, stream_name: &str, cause: &str) -> PyResult<()> {
        self.venue.fail_stream(stream(stream_name)?, cause);
        Ok(())
    }
    fn drop_plants(&self) {
        self.venue.drop_plants();
    }
    #[pyo3(signature = (method, cause="scripted rejection"))]
    fn reject_next(&self, method: &str, cause: &str) {
        self.venue
            .fail_next(method, FakeFailure::Rejected(cause.into()));
    }
    #[pyo3(signature = (method, cause="scripted connection failure"))]
    fn fail_next(&self, method: &str, cause: &str) {
        self.venue
            .fail_next(method, FakeFailure::Rithmic(cause.into()));
    }
    fn set_delay(&self, method: &str, milliseconds: u64) {
        self.venue
            .set_delay(method, Duration::from_millis(milliseconds));
    }
    fn calls(&self) -> Vec<String> {
        self.venue.calls()
    }
    fn clear_calls(&self) {
        self.venue.clear_calls();
    }
}

pub(super) fn new(
    py: Python<'_>,
    key: HostFingerprint,
    lock_dir: Option<String>,
) -> PyResult<PySession> {
    static REGISTRY: OnceLock<HostRegistry<FakeVenue>> = OnceLock::new();
    let lock_dir = lock_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("rithmic-fake-hosts"));
    direct::new(py, PlantSet::MARKET_DATA, move || {
        runtime().block_on(async {
            let account = match (&key.account_id, &key.fcm_id, &key.ib_id) {
                (Some(account_id), Some(fcm_id), Some(ib_id)) => {
                    Some(rithmic_host::AccountTriple {
                        account_id: account_id.clone(),
                        fcm_id: fcm_id.clone(),
                        ib_id: ib_id.clone(),
                    })
                }
                _ => None,
            };
            let registry = REGISTRY.get_or_init(HostRegistry::default);
            loop {
                let host = registry
                    .get_or_start(
                        key.clone(),
                        || {
                            let venue = FakeVenue::new();
                            if let Some(account) = &account {
                                venue.set_account(account.clone());
                            }
                            Arc::new(venue)
                        },
                        &lock_dir,
                        HostOptions {
                            timing: Timing {
                                error_backoff: Duration::from_millis(1),
                                connect_retry: Duration::from_millis(1),
                                restore_backoff: Duration::from_millis(1),
                            },
                            ..HostOptions::default()
                        },
                    )
                    .await
                    .map_err(|e| SessionLockError::new_err(e.to_string()))?;
                let script = host
                    .state
                    .session
                    .as_ref()
                    .expect("fake venue")
                    .checkout()
                    .await;
                if let Ok(session) = host.attach_owned() {
                    let failed_host = host.clone();
                    break Ok(direct::Attachment {
                        session: Arc::new(session),
                        failed: Box::new(move || {
                            failed_host
                                .state
                                .failed
                                .load(std::sync::atomic::Ordering::SeqCst)
                        }),
                        script: Some(script),
                        fault_venue: None,
                    });
                }
            }
        })
    })
}
