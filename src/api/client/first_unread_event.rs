use axum::{
	Json,
	extract::{Path, State},
};
use axum_extra::{
	TypedHeader,
	headers::{Authorization, authorization::Bearer},
};
use ruma::{RoomId, api::error::ErrorKind};
use tuwunel_core::{Err, Error, Result};

/// # `GET /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/first_unread_event`
///
/// Returns the event_id of the first unread event in the room for the
/// authenticated user. This allows the client to scroll/jump to the exact
/// position of the first unread message.
///
/// Returns `{"first_unread_event_id": "<event_id>"}` if there are unread
/// events, or `{"first_unread_event_id": null}` if everything is read.
pub(crate) async fn get_first_unread_event_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path(room_id): Path<ruma::OwnedRoomId>,
) -> Result<Json<serde_json::Value>> {
	// Authenticate user from Bearer token
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken(ruma::api::error::UnknownTokenErrorData { soft_logout: false }),
				"Unknown access token.",
			)
		})?;

	// Verify the user is in the room
	if !services.state_cache.is_joined(&sender_user, &room_id).await {
		return Err!(Request(Forbidden("You are not a member of this room.")));
	}

	let event_id = services
		.pusher
		.first_unread_event_id(&sender_user, &room_id)
		.await;

	Ok(Json(serde_json::json!({
		"first_unread_event_id": event_id
	})))
}
