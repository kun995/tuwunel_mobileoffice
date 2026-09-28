use axum::{
	Json,
	extract::{Path, State},
};
use axum_extra::{
	TypedHeader,
	headers::{Authorization, authorization::Bearer},
};
use ruma::{OwnedRoomId, RoomId, api::client::error::ErrorKind};
use serde::Serialize;
use tuwunel_core::{Err, Error, Result, matrix::pdu::PduCount};

#[derive(Debug, Serialize)]
pub(crate) struct DeleteDirectConversationResponse {
	pub room_id: OwnedRoomId,
	pub deleted: bool,
	pub hidden: bool,
	pub deleted_before: String,
}

/// # `POST /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/delete_conversation`
///
/// Deletes a direct conversation from the caller's view only.
///
/// The caller stays joined to the room, so the other DM participant receives no
/// membership event and their room name/state are not affected.
pub(crate) async fn delete_direct_conversation_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path(room_id): Path<Box<RoomId>>,
) -> Result<Json<DeleteDirectConversationResponse>> {
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken { soft_logout: false },
				"Unknown access token.",
			)
		})?;

	if !services.metadata.exists(&room_id).await {
		return Err!(Request(NotFound("Room does not exist.")));
	}

	if !services
		.state_cache
		.is_joined(&sender_user, &room_id)
		.await
	{
		return Err!(Request(Forbidden("You are not a member of this room.")));
	}

	let is_direct = services
		.account_data
		.is_direct(&sender_user, &room_id)
		.await || services
		.state_accessor
		.is_direct(&room_id, &sender_user)
		.await;

	if !is_direct {
		return Err!(Request(Forbidden(
			"delete_conversation is only supported for direct rooms."
		)));
	}

	let deleted_before = services
		.timeline
		.last_timeline_count(Some(&sender_user), &room_id, None)
		.await
		.unwrap_or_default();

	let deleted_before = match deleted_before {
		| PduCount::Normal(count) => count,
		| PduCount::Backfilled(_) => 0,
	};

	let next_count = services.globals.next_count();
	services
		.deleted_rooms
		.delete_before(&sender_user, &room_id, deleted_before);
	services
		.pusher
		.reset_notification_counts(&sender_user, &room_id);
	services
		.read_receipt
		.private_read_set(&room_id, &sender_user, deleted_before);
	drop(next_count);

	let room_id_ref: &RoomId = &room_id;

	Ok(Json(DeleteDirectConversationResponse {
		room_id: room_id_ref.to_owned(),
		deleted: true,
		hidden: true,
		deleted_before: deleted_before.to_string(),
	}))
}
