use ruma::{
	EventId, RoomId,
	canonical_json::{RedactedBecause, redact_in_place},
};
use tuwunel_core::{Result, err, implement, matrix::event::Event};

use crate::rooms::{short::ShortRoomId, timeline::RoomMutexGuard};

/// Replace a PDU with the redacted form.
#[implement(super::Service)]
#[tracing::instrument(name = "redact", level = "debug", skip(self))]
pub async fn redact_pdu<Pdu: Event + Send + Sync>(
	&self,
	event_id: &EventId,
	reason: &Pdu,
	shortroomid: ShortRoomId,
	state_lock: &RoomMutexGuard,
) -> Result {
	let Ok(pdu_id) = self.get_pdu_id(event_id).await else {
		// If event does not exist, just noop
		// TODO this is actually wrong!
		return Ok(());
	};

	let mut pdu = self
		.get_pdu_json_from_id(&pdu_id)
		.await
		.map_err(|e| {
			err!(Database(error!(?pdu_id, ?event_id, ?e, "PDU ID points to invalid PDU.")))
		})?;

	self.services
		.retention
		.save_original_pdu(event_id, &pdu, state_lock)
		.await;

	let content = pdu["content"].as_object().unwrap();
	let body = content.get("body").and_then(|body| body.as_str());
	let filename = content
		.get("filename")
		.and_then(|filename| filename.as_str());

	if let Some(body) = body {
		self.services
			.search
			.deindex_pdu(shortroomid, &pdu_id, body);
	}
	if let Some(filename) = filename {
		self.services
			.search
			.deindex_pdu(shortroomid, &pdu_id, filename);
	}

	// Remove from link index; no-op if this PDU had no link
	self.db
		.roomid_haslink_pduid
		.remove(pdu_id.as_bytes());

	let room_id = RoomId::parse(pdu["room_id"].as_str().unwrap()).unwrap();

	let room_version_id = self
		.services
		.state
		.get_room_version(room_id)
		.await?;

	let room_version_rules = room_version_id.rules().ok_or_else(|| {
		err!(Request(UnsupportedRoomVersion(
			"Cannot redact event for unknown room version {room_version_id:?}."
		)))
	})?;

	let message_batch_id = pdu
		.get("content")
		.and_then(|c| c.as_object())
		.and_then(|c| c.get("message_batch_id"))
		.cloned();

	let message_batch_sequence = pdu
		.get("content")
		.and_then(|c| c.as_object())
		.and_then(|c| c.get("message_batch_sequence"))
		.cloned();

	redact_in_place(
		&mut pdu,
		&room_version_rules.redaction,
		Some(RedactedBecause::from_json(reason.to_canonical_object())),
	)
	.map_err(|err| err!("invalid event: {err}"))?;

	if message_batch_id.is_some() || message_batch_sequence.is_some() {
		if let Some(content) = pdu
			.get_mut("content")
			.and_then(|c| c.as_object_mut())
		{
			if let Some(batch_id) = message_batch_id {
				content.insert("message_batch_id".into(), batch_id);
			}
			if let Some(batch_seq) = message_batch_sequence {
				content.insert("message_batch_sequence".into(), batch_seq);
			}
		}
	}

	self.replace_pdu(&pdu_id, &pdu).await
}
