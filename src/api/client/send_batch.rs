use axum::{
	Json,
	extract::{Path, State},
};
use axum_extra::{
	TypedHeader,
	headers::{Authorization, authorization::Bearer},
};
use ruma::{RoomId, api::error::ErrorKind};
use serde::Deserialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use tuwunel_core::{Err, Error, Result, err, matrix::pdu::PduBuilder, utils};

const MESSAGE_BATCH_ID_FIELD: &str = "message_batch_id";
#[derive(Debug, Deserialize)]
pub(crate) struct SendBatchEventsBody {
	#[serde(default)]
	pub batch_id: Option<String>,
	pub events: Vec<SendBatchEventItem>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SendBatchEventItem {
	#[serde(default)]
	pub event_type: Option<String>,
	pub content: JsonValue,
	#[serde(default)]
	pub message: Option<String>,
}

/// # `POST /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/send_batch`
///
/// Sends multiple timeline events to a room within one request.
///
/// - The `event_type` defaults to `m.room.message` for each item.
/// - If `message` is provided for media/file messages, it is copied into
///   `content.body` when `body` is missing.
pub(crate) async fn send_batch_events_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path(room_id): Path<ruma::OwnedRoomId>,
	Json(body): Json<SendBatchEventsBody>,
) -> Result<Json<serde_json::Value>> {
	let (sender_user, sender_device, _token) = services
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

	if body.events.is_empty() {
		return Ok(Json(serde_json::json!({
			"batch_id": batch_id,
			"event_ids": []
		})));
	}

	let room_id = room_id.as_ref();
	let state_lock = services.state.mutex.lock(room_id).await;
	let mut event_ids = Vec::with_capacity(body.events.len());

	for (index, item) in body.events.into_iter().enumerate() {
		let content = normalize_content(item.content, item.message.as_deref(), &batch_id)?;
		let content = serde_json::value::to_raw_value(&content)
			.map_err(|e| err!(Request(BadJson("Invalid JSON body: {e}"))))?;

		let event_type = item
			.event_type
			.unwrap_or_else(|| "m.room.message".to_owned());

		let mut unsigned = std::collections::BTreeMap::new();
		unsigned.insert(
			"transaction_id".to_owned(),
			format!("batch:{}:{index}", sender_device).into(),
		);
		unsigned.insert(
			MESSAGE_BATCH_ID_FIELD.to_owned(),
			JsonValue::String(batch_id.clone()),
		);

		let event_id = services
			.timeline
			.build_and_append_pdu(
				PduBuilder {
					event_type: event_type.as_str().into(),
					content: content.into(),
					unsigned: Some(unsigned),
					..Default::default()
				},
				&sender_user,
				room_id,
				&state_lock,
			)
			.await?;

		event_ids.push(event_id);
	}

	drop(state_lock);

	Ok(Json(serde_json::json!({
		"batch_id": batch_id,
		"event_ids": event_ids
	})))
}

fn normalize_content(
	mut content: JsonValue,
	message: Option<&str>,
	batch_id: &str,
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

	if let Some(message) = message
		&& !message.is_empty()
		&& is_media_or_file_message(object)
		&& object.get("body").is_none()
	{
		object.insert("body".to_owned(), JsonValue::String(message.to_owned()));
	}

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
