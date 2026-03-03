use axum::{
	Json,
	extract::{Path, State},
};
use axum_extra::{
	TypedHeader,
	headers::{Authorization, authorization::Bearer},
};
use ruma::{EventId, OwnedEventId, RoomId, api::client::error::ErrorKind};
use serde::Deserialize;
use tuwunel_core::{Err, Error, Result, err, matrix::event::Event};

/// # `DELETE /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/events/{eventId}`
///
/// Permanently deletes an event from the calling user's view only.
/// Other users in the room are NOT affected — they still see the event.
/// This action cannot be undone.
pub(crate) async fn delete_event_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path((room_id, event_id)): Path<(Box<RoomId>, Box<EventId>)>,
) -> Result<Json<serde_json::Value>> {
	// Authenticate user from Bearer token
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(ErrorKind::UnknownToken { soft_logout: false }, "Unknown access token.")
		})?;

	// Verify the user is in the room
	if !services.state_cache.is_joined(&sender_user, &room_id).await {
		return Err!(Request(Forbidden("You are not a member of this room.")));
	}

	// Verify the event exists in this room
	let pdu = services
		.timeline
		.get_pdu(&event_id)
		.await
		.map_err(|_| err!(Request(NotFound("Event not found."))))?;

	if *pdu.room_id() != *room_id {
		return Err!(Request(NotFound("Event not found in this room.")));
	}

	services.deleted_events.delete_event(&sender_user, &event_id);

	Ok(Json(serde_json::json!({})))
}

#[derive(Debug, Deserialize)]
pub(crate) struct DeleteEventsBody {
	pub event_ids: Vec<OwnedEventId>,
}

/// # `POST /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/delete_events`
///
/// Permanently deletes multiple events from the calling user's view (batch).
/// Other users in the room are NOT affected — they still see the events.
/// This action cannot be undone.
pub(crate) async fn delete_events_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path(room_id): Path<Box<RoomId>>,
	Json(body): Json<DeleteEventsBody>,
) -> Result<Json<serde_json::Value>> {
	// Authenticate user from Bearer token
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(ErrorKind::UnknownToken { soft_logout: false }, "Unknown access token.")
		})?;

	// Verify the user is in the room
	if !services.state_cache.is_joined(&sender_user, &room_id).await {
		return Err!(Request(Forbidden("You are not a member of this room.")));
	}

	if body.event_ids.is_empty() {
		return Ok(Json(serde_json::json!({})));
	}

	let event_id_refs: Vec<&EventId> = body.event_ids.iter().map(AsRef::as_ref).collect();
	services.deleted_events.delete_events(&sender_user, &event_id_refs);

	Ok(Json(serde_json::json!({})))
}
