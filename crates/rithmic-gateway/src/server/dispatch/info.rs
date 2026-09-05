//! Reference-info reads: plant set requests, front month, reference data,
//! resolved account. Read-only; no trading gate.

use rithmic_plants::PlantSet;

use crate::convert::{front_month_to_pb, reference_data_to_pb, resolved_account_to_pb};
use crate::pb::{self, frame::Body, Frame};

use super::{ack_frame, err_to_frame, no_session_frame, GatewayState};

pub(super) async fn request_plants(
    state: &GatewayState,
    request_id: u64,
    req: pb::RequestPlantsRequest,
) -> Frame {
    match PlantSet::parse(&req.plants) {
        Ok(extra) => {
            let Some(session) = &state.session else {
                return no_session_frame(request_id);
            };
            let mut guard = session.lock().await;
            if let Err(e) = guard.request_plants(extra).await {
                return err_to_frame(request_id, "request_plants_failed", e);
            }
            ack_frame(request_id)
        }
        Err(e) => err_to_frame(request_id, "invalid_plants", e),
    }
}

pub(super) async fn get_front_month(
    state: &GatewayState,
    request_id: u64,
    req: pb::GetFrontMonthRequest,
) -> Frame {
    let Some(session) = &state.session else {
        return no_session_frame(request_id);
    };
    let guard = session.lock().await;
    match guard.get_front_month(&req.symbol, &req.exchange).await {
        Ok(dto) => Frame {
            request_id,
            body: Some(Body::FrontMonthResponse(front_month_to_pb(dto))),
        },
        Err(e) => err_to_frame(request_id, "get_front_month_failed", e),
    }
}

pub(super) async fn get_reference_data(
    state: &GatewayState,
    request_id: u64,
    req: pb::GetReferenceDataRequest,
) -> Frame {
    let Some(session) = &state.session else {
        return no_session_frame(request_id);
    };
    let guard = session.lock().await;
    match guard.get_reference_data(&req.symbol, &req.exchange).await {
        Ok(dto) => Frame {
            request_id,
            body: Some(Body::ReferenceDataResponse(reference_data_to_pb(dto))),
        },
        Err(e) => err_to_frame(request_id, "get_reference_data_failed", e),
    }
}

pub(super) async fn resolved_account(state: &GatewayState, request_id: u64) -> Frame {
    let Some(session) = &state.session else {
        return no_session_frame(request_id);
    };
    let guard = session.lock().await;
    // None (not yet resolved) is a valid answer: an empty response lets
    // the client fall back to its configured account id, mirroring the
    // direct path's `resolved_account() -> None`.
    let body = match guard.resolved_account() {
        Some(acct) => Body::ResolvedAccountResponse(resolved_account_to_pb(acct.clone())),
        None => Body::ResolvedAccountResponse(pb::ResolvedAccountResponse::default()),
    };
    Frame {
        request_id,
        body: Some(body),
    }
}

/// RC2.3 barrier snapshot — no plant lock; safe while live MD is active.
pub(super) async fn get_live_md_state(state: &GatewayState, request_id: u64) -> Frame {
    let (ticker, book, time_bars) = state.reconnect.live_md_intent_counts().await;
    let live_md = ticker > 0 || book > 0 || time_bars > 0;
    let ready_peers = state.idle.peer_count().await as u32;
    Frame {
        request_id,
        body: Some(Body::GetLiveMdStateResponse(pb::GetLiveMdStateResponse {
            live_md,
            ticker_intents: ticker,
            book_intents: book,
            time_bar_intents: time_bars,
            ready_peers,
        })),
    }
}
