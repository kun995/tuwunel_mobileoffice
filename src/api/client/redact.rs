use axum::extract::State;
use ruma::{
	MilliSecondsSinceUnixEpoch, api::client::redact::redact_event,
	events::room::redaction::RoomRedactionEventContent,
};
use tuwunel_core::{Err, Result, matrix::pdu::PduBuilder};

use crate::Ruma;

/// Số milliseconds trong 24 giờ
const REDACT_MAX_AGE_MS: u64 = 24 * 60 * 60 * 1_000;

/// # `PUT /_matrix/client/r0/rooms/{roomId}/redact/{eventId}/{txnId}`
///
/// Tries to send a redaction event into the room.
///
/// - Chỉ cho phép thu hồi tin nhắn được gửi trong vòng 24h gần nhất.
/// - TODO: Handle txn id
pub(crate) async fn redact_event_route(
	State(services): State<crate::State>,
	body: Ruma<redact_event::v3::Request>,
) -> Result<redact_event::v3::Response> {
	let sender_user = body.sender_user();
	let body = &body.body;

	// Lấy PDU gốc để kiểm tra thời gian gửi
	let original_pdu = services.timeline.get_pdu(&body.event_id).await?;

	// Tính tuổi sự kiện theo milliseconds (dùng i128 như pdu/unsigned.rs)
	let now_ms: i128 = MilliSecondsSinceUnixEpoch::now().get().into();
	let event_ts_ms: i128 = original_pdu.origin_server_ts.into();
	let age_ms = now_ms.saturating_sub(event_ts_ms);

	if age_ms > REDACT_MAX_AGE_MS as i128 {
		return Err!(Request(Forbidden("Không thể thu hồi tin nhắn đã gửi quá 24h")));
	}

	let state_lock = services.state.mutex.lock(&body.room_id).await;

	let event_id = services
		.timeline
		.build_and_append_pdu(
			PduBuilder {
				redacts: Some(body.event_id.clone()),
				..PduBuilder::timeline(&RoomRedactionEventContent {
					redacts: Some(body.event_id.clone()),
					reason: body.reason.clone(),
				})
			},
			sender_user,
			&body.room_id,
			&state_lock,
		)
		.await?;

	drop(state_lock);

	Ok(redact_event::v3::Response { event_id })
}
