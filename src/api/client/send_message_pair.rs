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
use serde_json::{Map as JsonMap, Value as JsonValue};
use tuwunel_core::{
	Err, Error, Result, err,
	matrix::{event::Event, pdu::PduBuilder},
	utils,
};

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
/// Sends a new update/replacement event for an existing message in one request.
///
/// - The `event_type` defaults to `m.room.message`.
/// - The caller only needs to provide the target event id; the server links
///   the new event back to the original with `m.relates_to`.
pub(crate) async fn send_message_pair_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path(room_id): Path<Box<RoomId>>,
	Json(body): Json<SendMessagePairBody>,
) -> Result<Json<serde_json::Value>> {
	let (sender_user, sender_device, _token) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken { soft_logout: false },
				"Unknown access token.",
			)
		})?;

	if !services
		.state_cache
		.is_joined(&sender_user, &room_id)
		.await
	{
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

	let state_lock = services.state.mutex.lock(&room_id).await;

	let content = normalize_content(body.content, body.message.as_deref(), &batch_id, 0)?;
	let content = add_reference_relation(content, target_event_id.as_ref())?;
	let content = serde_json::value::to_raw_value(&content)
		.map_err(|e| err!(Request(BadJson("Invalid JSON body: {e}"))))?;
	let event_type = body
		.event_type
		.unwrap_or_else(|| "m.room.message".to_owned());

	let mut unsigned = std::collections::BTreeMap::new();
	unsigned.insert("transaction_id".to_owned(), format!("pair:{}:0", sender_device).into());
	unsigned.insert(MESSAGE_BATCH_ID_FIELD.to_owned(), JsonValue::String(batch_id.clone()));
	unsigned.insert(MESSAGE_BATCH_SEQUENCE_FIELD.to_owned(), JsonValue::from(0_u64));

	let event_id = services
		.timeline
		.build_and_append_pdu(
			PduBuilder {
				event_type: event_type.as_str().into(),
				content,
				unsigned: Some(unsigned),
				..Default::default()
			},
			&sender_user,
			&room_id,
			&state_lock,
		)
		.await?;

	drop(state_lock);

	Ok(Json(serde_json::json!({
		"batch_id": batch_id,
		"target_event_id": target_event_id,
		"event_id": event_id,
	})))
}

/// # `DELETE /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/send_message_pair/{eventId}`
///
/// Permanently deletes the paired/update message from the room timeline and
/// appends a notification event (`org.tuwunel.message_pair_deleted`) so that
/// all clients receive the deletion via /sync.
pub(crate) async fn delete_message_pair_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path((room_id, event_id)): Path<(Box<RoomId>, Box<EventId>)>,
) -> Result<Json<serde_json::Value>> {
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

	if !services
		.state_cache
		.is_joined(&sender_user, &room_id)
		.await
	{
		return Err!(Request(Forbidden("You are not a member of this room.")));
	}

	let room_id = (*room_id).to_owned();
	let event_id = (*event_id).to_owned();

	let original_pdu = services
		.timeline
		.get_pdu(&event_id)
		.await
		.map_err(|_| err!(Request(NotFound("Event not found."))))?;

	if original_pdu.room_id() != &*room_id {
		return Err!(Request(NotFound("Event not found in this room.")));
	}

	let batch_id = original_pdu
		.get_content_as_value()
		.get(MESSAGE_BATCH_ID_FIELD)
		.cloned();

	// Permanently delete the event from storage
	services
		.deleted_events
		.delete_event(&sender_user, &event_id);

	// Append a notification event so all clients learn about the deletion via /sync
	let deletion_content = serde_json::json!({
		"deleted_event_id": event_id,
		"batch_id": batch_id,
	});
	let deletion_content = serde_json::value::to_raw_value(&deletion_content)
		.map_err(|e| err!(Request(BadJson("Failed to serialize deletion event: {e}"))))?;

	let state_lock = services.state.mutex.lock(&room_id).await;
	let notification_event_id = services
		.timeline
		.build_and_append_pdu(
			PduBuilder {
				event_type: "org.tuwunel.message_pair_deleted".into(),
				content: deletion_content,
				..Default::default()
			},
			&sender_user,
			&room_id,
			&state_lock,
		)
		.await?;
	drop(state_lock);

	Ok(Json(serde_json::json!({
		"deleted_event_id": event_id,
		"notification_event_id": notification_event_id,
		"batch_id": batch_id,
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
		object.insert(MESSAGE_BATCH_ID_FIELD.to_owned(), JsonValue::String(batch_id.to_owned()));
	}

	if object.get(MESSAGE_BATCH_SEQUENCE_FIELD).is_none() {
		object.insert(MESSAGE_BATCH_SEQUENCE_FIELD.to_owned(), JsonValue::from(sequence));
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
		assert_eq!(content[MESSAGE_RELATES_TO_FIELD]["event_id"], "$original:example.org");
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
