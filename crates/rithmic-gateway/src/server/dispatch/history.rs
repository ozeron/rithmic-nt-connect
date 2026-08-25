//! History loads: ticks, time-bar windows (plant slices internally), and
//! time-bar probes. All take the session lock for the duration of the load.
//!
//! While continuous MD intents (ticker / book / live time bars) are live,
//! `Load*` / probe RPCs are refused so the event pump is not starved
//! (2026-08-25 MY043 incident RC2.3 — live XOR heavy ingest).

use rithmic_plants::history::parse_time_bar_type;

use crate::convert::{history_bar_to_pb, history_tick_to_pb, time_bar_probe_row_to_pb};
use crate::pb::{self, frame::Body, Frame};

use super::{err_to_frame, error_frame, no_session_frame, GatewayState};

/// Error code returned to lake / qgw when history would mute live MD.
pub(crate) const HISTORY_DENIED_LIVE_MD: &str = "history_denied_live_md";

const HISTORY_DENIED_LIVE_MD_MSG: &str = "history Load* refused while live ticker/book/time-bar \
intents are active; use a separate gateway or stop live MD (live XOR heavy ingest)";

async fn refuse_if_live_md(state: &GatewayState, request_id: u64) -> Option<Frame> {
    if state.reconnect.has_live_md_intents().await {
        return Some(error_frame(
            request_id,
            HISTORY_DENIED_LIVE_MD,
            HISTORY_DENIED_LIVE_MD_MSG,
        ));
    }
    None
}

pub(super) async fn load_ticks(
    state: &GatewayState,
    request_id: u64,
    req: pb::LoadTicksRequest,
) -> Frame {
    if let Some(denied) = refuse_if_live_md(state, request_id).await {
        return denied;
    }
    let Some(session) = &state.session else {
        return no_session_frame(request_id);
    };
    let guard = session.lock().await;
    match guard
        .load_ticks_all(
            &req.symbol,
            &req.exchange,
            req.start_time_sec,
            req.end_time_sec,
        )
        .await
    {
        Ok(ticks) => Frame {
            request_id,
            body: Some(Body::LoadTicksResponse(pb::LoadTicksResponse {
                ticks: ticks.into_iter().map(history_tick_to_pb).collect(),
            })),
        },
        Err(e) => err_to_frame(request_id, "load_ticks_failed", e),
    }
}

/// One-shot time-bar window RPCs share the no-session / parse / lock /
/// error-frame skeleton; only the session call, response body, and error code
/// differ.
enum TimeBarOp {
    Load(pb::LoadTimeBarsRequest),
    Probe(pb::ProbeTimeBarsRequest),
}

impl TimeBarOp {
    fn raw_bar_type(&self) -> i32 {
        match self {
            Self::Load(req) => req.bar_type,
            Self::Probe(req) => req.bar_type,
        }
    }

    fn failed_code(&self) -> &'static str {
        match self {
            Self::Load(_) => "load_time_bars_failed",
            Self::Probe(_) => "probe_time_bars_failed",
        }
    }
}

pub(super) async fn load_time_bars(
    state: &GatewayState,
    request_id: u64,
    req: pb::LoadTimeBarsRequest,
) -> Frame {
    run_time_bar_op(state, request_id, TimeBarOp::Load(req)).await
}

pub(super) async fn probe_time_bars(
    state: &GatewayState,
    request_id: u64,
    req: pb::ProbeTimeBarsRequest,
) -> Frame {
    run_time_bar_op(state, request_id, TimeBarOp::Probe(req)).await
}

async fn run_time_bar_op(state: &GatewayState, request_id: u64, op: TimeBarOp) -> Frame {
    if let Some(denied) = refuse_if_live_md(state, request_id).await {
        return denied;
    }
    let Some(session) = &state.session else {
        return no_session_frame(request_id);
    };
    let bar_type = match parse_time_bar_type(op.raw_bar_type()) {
        Ok(bt) => bt,
        Err(e) => return err_to_frame(request_id, "invalid_bar_type", e),
    };
    let guard = session.lock().await;
    let outcome = match &op {
        TimeBarOp::Load(req) => guard
            .load_time_bars_all(
                &req.symbol,
                &req.exchange,
                bar_type,
                req.period,
                req.start_time_sec,
                req.end_time_sec,
            )
            .await
            .map(|bars| {
                Body::LoadTimeBarsResponse(pb::LoadTimeBarsResponse {
                    bars: bars.into_iter().map(history_bar_to_pb).collect(),
                })
            }),
        TimeBarOp::Probe(req) => guard
            .probe_time_bars(
                &req.symbol,
                &req.exchange,
                bar_type,
                req.period,
                req.start_time_sec,
                req.end_time_sec,
            )
            .await
            .map(|rows| {
                Body::ProbeTimeBarsResponse(pb::ProbeTimeBarsResponse {
                    rows: rows.into_iter().map(time_bar_probe_row_to_pb).collect(),
                })
            }),
    };
    match outcome {
        Ok(body) => Frame {
            request_id,
            body: Some(body),
        },
        Err(e) => err_to_frame(request_id, op.failed_code(), e),
    }
}
