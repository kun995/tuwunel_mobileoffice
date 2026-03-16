use std::collections::BTreeMap;

use axum::extract::State;
use ruma::{api::client::message::send_message_event, events::MessageLikeEventType};
use serde_json::{Value as JsonValue, from_str, value::to_raw_value};
use tuwunel_core::{Err, Result, err, matrix::pdu::PduBuilder, utils};

use crate::Ruma;

const CALL_KIND_FIELD: &str = "call_kind";

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

		return Ok(send_message_event::v3::Response {
			event_id: utils::string_from_bytes(&response)
				.map(TryInto::try_into)
				.map_err(|e| err!(Database("Invalid event_id in txnid data: {e:?}")))??,
		});
	}

	let mut unsigned = BTreeMap::new();
	unsigned.insert("transaction_id".to_owned(), body.txn_id.to_string().into());

	let mut content = from_str(body.body.body.json().get())
		.map_err(|e| err!(Request(BadJson("Invalid JSON body: {e}"))))?;

	if body.event_type == MessageLikeEventType::CallInvite {
		enrich_call_invite_kind(&mut content);
	}

	let content = to_raw_value(&content)
		.map_err(|e| err!(Request(BadJson("Invalid JSON body: {e}"))))?;

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

	services.transaction_ids.add_txnid(
		sender_user,
		sender_device,
		&body.txn_id,
		event_id.as_bytes(),
	);

	drop(state_lock);

	Ok(send_message_event::v3::Response { event_id })
}

#[cfg(test)]
mod tests {
	use super::{CALL_KIND_FIELD, enrich_call_invite_kind};
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
}
