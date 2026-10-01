use axum::{
	Json,
	extract::{Path, State},
};
use axum_extra::{
	TypedHeader,
	headers::{Authorization, authorization::Bearer},
};
use ruma::{EventId, OwnedEventId, RoomId, api::error::ErrorKind};
use serde::Deserialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use tuwunel_core::{Err, Error, Result, err, matrix::event::Event, utils};

use tuwunel_service::rooms::message_pairs::MessagePair;

const MESSAGE_BATCH_ID_FIELD: &str = "message_batch_id";
const MESSAGE_BATCH_SEQUENCE_FIELD: &str = "message_batch_sequence";
const MESSAGE_RELATES_TO_FIELD: &str = "m.relates_to";
const MESSAGE_REFERENCE_REL_TYPE: &str = "m.reference";

#[derive(Debug, Deserialize)]
pub(crate) struct SendMessagePairBody {
	#[serde(default)]
	pub batch_id: Option<String>,
	pub target_event_id: OwnedEventId,
	#[serde(default)]
	pub event_type: Option<String>,
	pub content: JsonValue,
	#[serde(default)]
	pub message: Option<String>,
}

/// # `POST /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/send_message_pair`
///
/// Saves an owner-only translation for an existing message in one request.
///
/// - The `event_type` defaults to `m.room.message`.
/// - The caller only needs to provide the target event id; the server links
///   the private record back to the original with `m.relates_to`.
pub(crate) async fn send_message_pair_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path(room_id): Path<ruma::OwnedRoomId>,
	Json(body): Json<SendMessagePairBody>,
) -> Result<Json<serde_json::Value>> {
	let (sender_user, _sender_device, _token) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken(ruma::api::error::UnknownTokenErrorData { soft_logout: false }),
				"Unknown access token.",
			)
		})?;

	if !services.state_cache.is_joined(&sender_user, &room_id).await {
		return Err!(Request(Forbidden("You are not a member of this room.")));
	}

	let batch_id = resolve_batch_id(body.batch_id.as_deref());
	let target_event_id = body.target_event_id.clone();
	let room_id = (*room_id).to_owned();

	// Validate target event exists in this room
	let target_pdu = services
		.timeline
		.get_pdu(&target_event_id)
		.await
		.map_err(|_| err!(Request(NotFound("Target event not found."))))?;

	if target_pdu.room_id() != &*room_id {
		return Err!(Request(NotFound("Target event not found in this room.")));
	}

	if !services
		.state_accessor
		.user_can_see_event(&sender_user, &room_id, &target_event_id)
		.await
	{
		return Err!(Request(NotFound("Target event not found.")));
	}

	let content = normalize_content(body.content, body.message.as_deref(), &batch_id, 0)?;
	let content = add_reference_relation(content, target_event_id.as_ref())?;
	let event_type = body
		.event_type
		.unwrap_or_else(|| "m.room.message".to_owned());

	let event_id = OwnedEventId::parse(format!("${}", utils::random_string(32))).map_err(|e| {
		err!(Request(InvalidParam(
			"Could not create translation id: {e}"
		)))
	})?;
	services.message_pairs.insert(&MessagePair {
		event_id: event_id.clone(),
		room_id: room_id.clone(),
		sender: sender_user.clone(),
		target_event_id: target_event_id.clone(),
		event_type,
		content,
		batch_id: batch_id.clone(),
		origin_server_ts: utils::millis_since_unix_epoch(),
	});

	Ok(Json(serde_json::json!({
		"batch_id": batch_id,
		"target_event_id": target_event_id,
		"event_id": event_id,
	})))
}

/// # `GET /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/send_message_pair`
///
/// Lists only the authenticated user's private translations in this room.
pub(crate) async fn list_message_pairs_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path(room_id): Path<ruma::OwnedRoomId>,
) -> Result<Json<serde_json::Value>> {
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

	if !services.state_cache.is_joined(&sender_user, &room_id).await {
		return Err!(Request(Forbidden("You are not a member of this room.")));
	}

	let translations = services.message_pairs.list(&sender_user, &room_id).await?;
	Ok(Json(serde_json::json!({ "translations": translations })))
}

/// # `DELETE /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/send_message_pair/{eventId}`
///
/// Deletes only the authenticated user's private translation record.
pub(crate) async fn delete_message_pair_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path((room_id, event_id)): Path<(ruma::OwnedRoomId, ruma::OwnedEventId)>,
) -> Result<Json<serde_json::Value>> {
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

	if !services.state_cache.is_joined(&sender_user, &room_id).await {
		return Err!(Request(Forbidden("You are not a member of this room.")));
	}

	let room_id = (*room_id).to_owned();
	let event_id = (*event_id).to_owned();

	if let Ok(pair) = services
		.message_pairs
		.get(&sender_user, &room_id, &event_id)
		.await
	{
		services.message_pairs.remove(&pair);
		return Ok(Json(serde_json::json!({
			"deleted_event_id": event_id,
			"batch_id": pair.batch_id,
		})));
	}

	// Existing public pairs may still be held by clients. Preserve DELETE for
	// the original sender, but only hide the old event for that sender.
	let legacy = services
		.timeline
		.get_pdu(&event_id)
		.await
		.map_err(|_| err!(Request(NotFound("Translation not found."))))?;
	let content = legacy.get_content_as_value();
	let is_pair = content.get(MESSAGE_BATCH_ID_FIELD).is_some()
		&& content
			.get(MESSAGE_RELATES_TO_FIELD)
			.and_then(|relation| relation.get("rel_type"))
			.and_then(JsonValue::as_str)
			== Some(MESSAGE_REFERENCE_REL_TYPE);
	if legacy.room_id() != &*room_id || legacy.sender() != &*sender_user || !is_pair {
		return Err!(Request(NotFound("Translation not found.")));
	}
	services
		.deleted_events
		.delete_event(&sender_user, &event_id);
	Ok(Json(serde_json::json!({
		"deleted_event_id": event_id,
		"batch_id": content.get(MESSAGE_BATCH_ID_FIELD),
	})))
}

fn normalize_content(
	mut content: JsonValue,
	message: Option<&str>,
	batch_id: &str,
	sequence: u64,
) -> Result<JsonValue> {
	let object = content
		.as_object_mut()
		.ok_or_else(|| err!(Request(BadJson("Event content must be a JSON object"))))?;

	if object.get(MESSAGE_BATCH_ID_FIELD).is_none() {
		object.insert(
			MESSAGE_BATCH_ID_FIELD.to_owned(),
			JsonValue::String(batch_id.to_owned()),
		);
	}

	if object.get(MESSAGE_BATCH_SEQUENCE_FIELD).is_none() {
		object.insert(
			MESSAGE_BATCH_SEQUENCE_FIELD.to_owned(),
			JsonValue::from(sequence),
		);
	}

	if let Some(message) = message
		&& !message.is_empty()
		&& is_media_or_file_message(object)
		&& object.get("body").is_none()
	{
		object.insert("body".to_owned(), JsonValue::String(message.to_owned()));
	}

	Ok(content)
}

fn add_reference_relation(
	mut content: JsonValue,
	original_event_id: &EventId,
) -> Result<JsonValue> {
	let object = content
		.as_object_mut()
		.ok_or_else(|| err!(Request(BadJson("Event content must be a JSON object"))))?;

	object.insert(
		MESSAGE_RELATES_TO_FIELD.to_owned(),
		serde_json::json!({
			"rel_type": MESSAGE_REFERENCE_REL_TYPE,
			"event_id": original_event_id,
		}),
	);

	Ok(content)
}

fn resolve_batch_id(provided_batch_id: Option<&str>) -> String {
	if let Some(batch_id) =
		provided_batch_id.filter(|value| !value.is_empty() && value.len() <= 128)
	{
		batch_id.to_owned()
	} else {
		format!("msg_{}", utils::random_string(12))
	}
}

fn is_media_or_file_message(content: &JsonMap<String, JsonValue>) -> bool {
	matches!(
		content.get("msgtype").and_then(JsonValue::as_str),
		Some("m.image" | "m.video" | "m.file")
	)
}

#[cfg(test)]
mod tests {
	use super::{
		MESSAGE_BATCH_ID_FIELD, MESSAGE_BATCH_SEQUENCE_FIELD, MESSAGE_RELATES_TO_FIELD,
		add_reference_relation, normalize_content, resolve_batch_id,
	};
	use ruma::{OwnedEventId, event_id};
	use serde_json::json;

	#[test]
	fn normalizes_content_with_batch_metadata() {
		let content = json!({
			"msgtype": "m.text",
			"body": "hello"
		});

		let content = normalize_content(content, None, "pair-1", 0).unwrap();

		assert_eq!(content[MESSAGE_BATCH_ID_FIELD], "pair-1");
		assert_eq!(content[MESSAGE_BATCH_SEQUENCE_FIELD], 0);
	}

	#[test]
	fn normalize_content_adds_body_for_media_when_missing() {
		let content = json!({
			"msgtype": "m.image",
			"url": "mxc://example.org/media"
		});

		let content = normalize_content(content, Some("preview"), "pair-1", 1).unwrap();

		assert_eq!(content["body"], "preview");
		assert_eq!(content[MESSAGE_BATCH_SEQUENCE_FIELD], 1);
	}

	#[test]
	fn adds_reference_relation() {
		let content = json!({
			"msgtype": "m.text",
			"body": "translated"
		});

		let original_event_id = OwnedEventId::from(event_id!("$original:example.org"));
		let content = add_reference_relation(content, original_event_id.as_ref()).unwrap();

		assert_eq!(content[MESSAGE_RELATES_TO_FIELD]["rel_type"], "m.reference");
		assert_eq!(
			content[MESSAGE_RELATES_TO_FIELD]["event_id"],
			"$original:example.org"
		);
		// m.new_content should NOT be present
		assert!(content.get("m.new_content").is_none());
	}

	#[test]
	fn resolves_empty_or_long_batch_id() {
		assert!(resolve_batch_id(Some("")).starts_with("msg_"));
		assert!(resolve_batch_id(Some(&"a".repeat(129))).starts_with("msg_"));
		assert_eq!(resolve_batch_id(Some("pair-42")), "pair-42");
	}
}
