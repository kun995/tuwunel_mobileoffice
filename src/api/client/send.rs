use std::collections::BTreeMap;

use axum::extract::State;
use ruma::{UserId, api::client::message::send_message_event, events::MessageLikeEventType};
use serde_json::{Value as JsonValue, from_str, value::to_raw_value};
use tuwunel_core::{Err, Result, err, info, matrix::pdu::PduBuilder, utils};
use tuwunel_service::Services;

use crate::Ruma;

const CALL_KIND_FIELD: &str = "call_kind";
const EMAIL_PAYLOAD_KEY: &str = "emailPayload";
const EMAIL_PAYLOAD_KIND: &str = "large_payload_ref";
const EMAIL_PAYLOAD_STORAGE_MXC: &str = "mxc";
const CALL_EVENT_PREFIX: &str = "m.call.";

/// Custom event type for system-level activity notifications.
/// Clients should render this as a system message (not a regular chat bubble).
/// - Sender sees `self_text`.
/// - Everyone else sees `other_text` (with `{sender_display}` already expanded by server).
pub(crate) const ACTIVITY_EVENT_TYPE: &str = "im.tuwunel.activity";

/// Placeholder inside `other_text` that the server expands to the sender's display name.
const ACTIVITY_SENDER_PLACEHOLDER: &str = "{sender_display}";

fn derive_call_kind_from_invite(content: &JsonValue) -> Option<&'static str> {
	let sdp = content.get("offer")?.get("sdp")?.as_str()?;

	let mut has_audio = false;
	let mut has_video = false;

	for line in sdp.lines() {
		let media = line.trim_start();

		if media.starts_with("m=audio ") {
			has_audio = true;
		} else if media.starts_with("m=video ") {
			has_video = true;
		}
	}

	if has_video {
		Some("video")
	} else if has_audio {
		Some("voice")
	} else {
		None
	}
}

fn enrich_call_invite_kind(content: &mut JsonValue) {
	if content.get(CALL_KIND_FIELD).is_some() {
		return;
	}

	let Some(kind) = derive_call_kind_from_invite(content) else {
		return;
	};

	if let Some(object) = content.as_object_mut() {
		object.insert(CALL_KIND_FIELD.to_owned(), kind.into());
	}
}

fn is_call_event(event_type: &MessageLikeEventType) -> bool {
	event_type == &MessageLikeEventType::CallInvite
		|| event_type
			.to_string()
			.starts_with(CALL_EVENT_PREFIX)
}

fn call_id_from_content(content: &JsonValue) -> Option<&str> {
	content
		.get("call_id")
		.and_then(JsonValue::as_str)
		.filter(|call_id| !call_id.is_empty())
}

fn string_field<'a>(content: &'a JsonValue, field: &str) -> Option<&'a str> {
	content
		.get(field)
		.and_then(JsonValue::as_str)
		.filter(|value| !value.is_empty())
}

fn nested_string_field<'a>(content: &'a JsonValue, parent: &str, field: &str) -> Option<&'a str> {
	content
		.get(parent)
		.and_then(|parent| parent.get(field))
		.and_then(JsonValue::as_str)
		.filter(|value| !value.is_empty())
}

fn candidates_count(content: &JsonValue) -> usize {
	content
		.get("candidates")
		.and_then(JsonValue::as_array)
		.map_or(0, Vec::len)
}

fn validate_email_payload_stub(content: &JsonValue) -> Result {
	let Some(stub) = content.get(EMAIL_PAYLOAD_KEY) else {
		return Ok(());
	};

	let Some(stub) = stub.as_object() else {
		return Err!(Request(InvalidParam("`emailPayload` must be a JSON object.")));
	};

	let kind = stub
		.get("kind")
		.and_then(JsonValue::as_str)
		.ok_or_else(|| err!(Request(InvalidParam("`emailPayload.kind` is required."))))?;

	if kind != EMAIL_PAYLOAD_KIND {
		return Err!(Request(InvalidParam("`emailPayload.kind` must be `large_payload_ref`.")));
	}

	let storage = stub
		.get("storage")
		.and_then(JsonValue::as_str)
		.ok_or_else(|| err!(Request(InvalidParam("`emailPayload.storage` is required."))))?;

	if storage != EMAIL_PAYLOAD_STORAGE_MXC {
		return Err!(Request(InvalidParam("`emailPayload.storage` must be `mxc`.")));
	}

	let uri = stub
		.get("uri")
		.and_then(JsonValue::as_str)
		.ok_or_else(|| err!(Request(InvalidParam("`emailPayload.uri` is required."))))?;

	if !is_valid_mxc_uri(uri) {
		return Err!(Request(InvalidParam("`emailPayload.uri` must be a valid mxc:// URI.")));
	}

	if let Some(version) = stub.get("v")
		&& version.as_u64().is_none_or(|v| v == 0)
	{
		return Err!(Request(InvalidParam("`emailPayload.v` must be a positive integer.")));
	}

	if let Some(size) = stub.get("size")
		&& size.as_u64().is_none_or(|v| v == 0)
	{
		return Err!(Request(InvalidParam("`emailPayload.size` must be a positive integer.")));
	}

	if let Some(sha256) = stub.get("sha256")
		&& sha256
			.as_str()
			.is_none_or(|value| value.trim().is_empty())
	{
		return Err!(Request(InvalidParam("`emailPayload.sha256` must be a non-empty string.")));
	}

	Ok(())
}

fn is_valid_mxc_uri(uri: &str) -> bool {
	let Some(rest) = uri.strip_prefix("mxc://") else {
		return false;
	};

	let mut parts = rest.split('/');
	let Some(server_name) = parts.next() else {
		return false;
	};
	let Some(media_id) = parts.next() else {
		return false;
	};

	!server_name.is_empty() && !media_id.is_empty() && parts.next().is_none()
}

/// # `PUT /_matrix/client/v3/rooms/{roomId}/send/{eventType}/{txnId}`
///
/// Send a message event into the room.
///
/// - Is a NOOP if the txn id was already used before and returns the same event
///   id again
/// - The only requirement for the content is that it has to be valid json
/// - Tries to send the event into the room, auth rules will determine if it is
///   allowed
pub(crate) async fn send_message_event_route(
	State(services): State<crate::State>,
	body: Ruma<send_message_event::v3::Request>,
) -> Result<send_message_event::v3::Response> {
	let sender_user = body.sender_user();
	let sender_device = body.sender_device.as_deref();
	let appservice_info = body.appservice_info.as_ref();

	// Forbid m.room.encrypted if encryption is disabled
	if MessageLikeEventType::RoomEncrypted == body.event_type && !services.config.allow_encryption
	{
		return Err!(Request(Forbidden("Encryption has been disabled")));
	}

	let state_lock = services.state.mutex.lock(&body.room_id).await;

	if body.event_type == MessageLikeEventType::CallInvite
		&& services
			.directory
			.is_public_room(&body.room_id)
			.await
	{
		return Err!(Request(Forbidden("Room call invites are not allowed in public rooms")));
	}

	// Check if this is a new transaction id
	if let Ok(response) = services
		.transaction_ids
		.existing_txnid(sender_user, sender_device, &body.txn_id)
		.await
	{
		// The client might have sent a txnid of the /sendToDevice endpoint
		// This txnid has no response associated with it
		if response.is_empty() {
			return Err!(Request(InvalidParam(
				"Tried to use txn id already used for an incompatible endpoint."
			)));
		}

		let event_id = utils::string_from_bytes(&response)
			.map(TryInto::try_into)
			.map_err(|e| err!(Database("Invalid event_id in txnid data: {e:?}")))??;

		if is_call_event(&body.event_type) {
			info!(
				event_type = %body.event_type,
				room_id = %body.room_id,
				sender = %sender_user,
				txn_id = %body.txn_id,
				%event_id,
				"Returned existing Matrix call event for duplicate txn"
			);
		}

		return Ok(send_message_event::v3::Response { event_id });
	}

	let mut unsigned = BTreeMap::new();
	unsigned.insert("transaction_id".to_owned(), body.txn_id.to_string().into());

	let mut content = from_str(body.body.body.json().get())
		.map_err(|e| err!(Request(BadJson("Invalid JSON body: {e}"))))?;

	let is_call_event = is_call_event(&body.event_type);
	if is_call_event {
		let call_id = call_id_from_content(&content).unwrap_or("<missing>");
		let party_id = string_field(&content, "party_id").unwrap_or("<missing>");
		let call_kind = string_field(&content, CALL_KIND_FIELD).unwrap_or("<missing>");
		let reason = string_field(&content, "reason").unwrap_or("<none>");
		let version = content.get("version").and_then(JsonValue::as_u64);
		let lifetime = content
			.get("lifetime")
			.and_then(JsonValue::as_u64);
		let has_offer_sdp = nested_string_field(&content, "offer", "sdp").is_some();
		let has_answer_sdp = nested_string_field(&content, "answer", "sdp").is_some();
		let candidates_count = candidates_count(&content);

		info!(
			event_type = %body.event_type,
			room_id = %body.room_id,
			sender = %sender_user,
			txn_id = %body.txn_id,
			call_id,
			party_id,
			call_kind,
			reason,
			?version,
			?lifetime,
			has_offer_sdp,
			has_answer_sdp,
			candidates_count,
			"Received Matrix call event send request"
		);
	}

	if body.event_type == MessageLikeEventType::CallInvite {
		enrich_call_invite_kind(&mut content);
	}

	if body.event_type.to_string() == ACTIVITY_EVENT_TYPE {
		enrich_activity_message(&services, sender_user, &mut content).await;
	}

	validate_email_payload_stub(&content)?;

	let content =
		to_raw_value(&content).map_err(|e| err!(Request(BadJson("Invalid JSON body: {e}"))))?;

	let event_id = services
		.timeline
		.build_and_append_pdu(
			PduBuilder {
				event_type: body.event_type.clone().into(),
				content,
				unsigned: Some(unsigned),
				timestamp: appservice_info.and(body.timestamp),
				..Default::default()
			},
			sender_user,
			&body.room_id,
			&state_lock,
		)
		.await?;

	if is_call_event {
		info!(
			event_type = %body.event_type,
			room_id = %body.room_id,
			sender = %sender_user,
			txn_id = %body.txn_id,
			%event_id,
			"Stored Matrix call event"
		);
	}

	services.transaction_ids.add_txnid(
		sender_user,
		sender_device,
		&body.txn_id,
		event_id.as_bytes(),
	);

	drop(state_lock);

	Ok(send_message_event::v3::Response { event_id })
}

/// Expand the `{sender_display}` placeholder in the `other_text` field of an
/// `im.tuwunel.activity` event with the actual display name (or MXID) of the sender.
///
/// This is done server-side so that any Matrix client — even one that has no knowledge
/// of this custom event type — will display a meaningful, fully-resolved string.
async fn enrich_activity_message(
	services: &Services,
	sender_user: &UserId,
	content: &mut JsonValue,
) {
	// Resolve the sender's display name; fall back gracefully to the full MXID.
	let display_name = services
		.users
		.displayname(sender_user)
		.await
		.unwrap_or_else(|_| sender_user.to_string());

	if let Some(other_text) = content.get_mut("other_text") {
		if let Some(s) = other_text.as_str() {
			let expanded = s.replace(ACTIVITY_SENDER_PLACEHOLDER, &display_name);
			*other_text = JsonValue::String(expanded);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{
		CALL_KIND_FIELD, EMAIL_PAYLOAD_KEY, enrich_call_invite_kind, validate_email_payload_stub,
	};
	use serde_json::json;

	#[test]
	fn adds_voice_call_kind_for_audio_only_invite() {
		let mut content = json!({
			"offer": {
				"type": "offer",
				"sdp": "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n"
			}
		});

		enrich_call_invite_kind(&mut content);

		assert_eq!(content[CALL_KIND_FIELD], "voice");
	}

	#[test]
	fn adds_video_call_kind_when_video_media_exists() {
		let mut content = json!({
			"offer": {
				"type": "offer",
				"sdp": "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n"
			}
		});

		enrich_call_invite_kind(&mut content);

		assert_eq!(content[CALL_KIND_FIELD], "video");
	}

	#[test]
	fn keeps_existing_call_kind() {
		let mut content = json!({
			"offer": {
				"type": "offer",
				"sdp": "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n"
			},
			"call_kind": "voice"
		});

		enrich_call_invite_kind(&mut content);

		assert_eq!(content[CALL_KIND_FIELD], "voice");
	}

	#[test]
	fn accepts_valid_email_payload_stub() {
		let content = json!({
			"msgtype": "m.text",
			"body": "hello",
			"emailPayload": {
				"v": 1,
				"kind": "large_payload_ref",
				"storage": "mxc",
				"uri": "mxc://example.org/abc123",
				"size": 100,
				"sha256": "hash"
			}
		});

		assert!(validate_email_payload_stub(&content).is_ok());
	}

	#[test]
	fn rejects_invalid_email_payload_uri() {
		let content = json!({
			"msgtype": "m.text",
			"body": "hello",
			"emailPayload": {
				"kind": "large_payload_ref",
				"storage": "mxc",
				"uri": "https://example.org/file.json"
			}
		});

		assert!(validate_email_payload_stub(&content).is_err());
	}

	#[test]
	fn accepts_content_without_email_payload_stub() {
		let content = json!({
			"msgtype": "m.text",
			"body": "plain message"
		});

		assert!(content.get(EMAIL_PAYLOAD_KEY).is_none());
		assert!(validate_email_payload_stub(&content).is_ok());
	}
}
