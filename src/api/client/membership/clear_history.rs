use axum::{
	Json,
	extract::{Path, State},
};
use axum_extra::{
	TypedHeader,
	headers::{Authorization, authorization::Bearer},
};
use ruma::{OwnedRoomId, RoomId, UserId, api::error::ErrorKind};
use serde::Serialize;
use tuwunel_core::{Err, Error, Result, matrix::pdu::PduCount};

#[derive(Debug, Serialize)]
pub(crate) struct ClearRoomHistoryResponse {
	pub room_id: OwnedRoomId,
	pub cleared: bool,
	pub hidden: bool,
	pub cleared_before: String,
}

/// # `POST /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/clear_history`
///
/// Clears room history from the caller's view only.
///
/// The caller stays joined to the room. Other members keep their history and
/// continue to see the room normally.
pub(crate) async fn clear_room_history_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path(room_id): Path<ruma::OwnedRoomId>,
) -> Result<Json<ClearRoomHistoryResponse>> {
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken(ruma::api::error::UnknownTokenErrorData {
					soft_logout: false,
				}),
				"Unknown access token.",
			)
		})?;

	let sender_user_ref: &UserId = sender_user.as_ref();
	let room_id_ref: &RoomId = &room_id;

	if !services.metadata.exists(room_id_ref).await {
		return Err!(Request(NotFound("Room does not exist.")));
	}

	if !services
		.state_cache
		.is_joined(sender_user_ref, room_id_ref)
		.await
	{
		return Err!(Request(Forbidden("You are not a member of this room.")));
	}

	let cleared_before = services
		.timeline
		.last_timeline_count(Some(sender_user_ref), room_id_ref, None)
		.await
		.unwrap_or_default();

	let cleared_before = match cleared_before {
		| PduCount::Normal(count) => count,
		| PduCount::Backfilled(_) => 0,
	};

	services
		.deleted_rooms
		.clear_before(sender_user_ref, room_id_ref, cleared_before);
	services
		.pusher
		.reset_room_notification_counts(sender_user_ref, room_id_ref)
		.await;
	services
		.read_receipt
		.private_read_set(tuwunel_service::rooms::read_receipt::PrivateRead {
			room_id: room_id_ref,
			user_id: sender_user_ref,
			count: cleared_before,
			ts: ruma::MilliSecondsSinceUnixEpoch::now(),
			thread: &ruma::events::receipt::ReceiptThread::Unthreaded,
			announce: true,
		})
		.await;

	Ok(Json(ClearRoomHistoryResponse {
		room_id: room_id_ref.to_owned(),
		cleared: true,
		hidden: false,
		cleared_before: cleared_before.to_string(),
	}))
}
