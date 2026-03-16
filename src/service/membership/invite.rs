use std::collections::BTreeMap;

use futures::FutureExt;
use ruma::{
	OwnedServerName, RoomId, UserId,
	api::federation::membership::create_invite,
	events::{StateEventContent, room::member::{MembershipState, RoomMemberEventContent}},
};
use serde_json::Value as JsonValue;
use tuwunel_core::{
	Err, Result, at, err, implement, matrix::event::gen_event_id_canonical_json, pdu::PduBuilder,
	utils,
};

use super::{AUTO_INVITE_BATCH_WINDOW, InviteBatchState, Service};

const INVITE_BATCH_ID_FIELD: &str = "membership_batch_id";

fn invite_unsigned(batch_id: Option<&str>) -> Option<BTreeMap<String, JsonValue>> {
	let batch_id = batch_id.filter(|value| !value.is_empty())?;
	let mut unsigned = BTreeMap::new();
	unsigned.insert(INVITE_BATCH_ID_FIELD.to_owned(), JsonValue::String(batch_id.to_owned()));
	Some(unsigned)
}

#[cfg(test)]
mod tests {
	use super::{INVITE_BATCH_ID_FIELD, invite_unsigned};
	use serde_json::Value as JsonValue;

	#[test]
	fn includes_batch_id_when_present() {
		let unsigned = invite_unsigned(Some("batch-abc")).expect("has unsigned data");
		assert_eq!(
			unsigned.get(INVITE_BATCH_ID_FIELD),
			Some(&JsonValue::String("batch-abc".to_owned()))
		);
	}

	#[test]
	fn returns_none_for_empty_batch_id() {
		assert!(invite_unsigned(Some("")).is_none());
	}
}

#[implement(Service)]
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(%sender_user, %room_id, %user_id)
)]
pub async fn invite(
	&self,
	sender_user: &UserId,
	user_id: &UserId,
	room_id: &RoomId,
	reason: Option<&String>,
	is_direct: bool,
	batch_id: Option<&str>,
) -> Result {
	let resolved_batch_id = self
		.resolve_invite_batch_id(sender_user, room_id, batch_id)
		.await;

	if self.services.globals.user_is_local(user_id) {
		self.local_invite(
			sender_user,
			user_id,
			room_id,
			reason,
			is_direct,
			Some(resolved_batch_id.as_str()),
		)
			.boxed()
			.await?;
	} else {
		self.remote_invite(
			sender_user,
			user_id,
			room_id,
			reason,
			is_direct,
			Some(resolved_batch_id.as_str()),
		)
			.boxed()
			.await?;
	}

	Ok(())
}

#[implement(Service)]
async fn resolve_invite_batch_id(
	&self,
	sender_user: &UserId,
	room_id: &RoomId,
	provided_batch_id: Option<&str>,
) -> String {
	let key = (sender_user.to_owned(), room_id.to_owned());
	let now = std::time::Instant::now();
	let mut batches = self.invite_batches.lock().await;

	batches.retain(|_, state| now.duration_since(state.updated_at) <= AUTO_INVITE_BATCH_WINDOW);

	let batch_id = if let Some(batch_id) = provided_batch_id.filter(|value| !value.is_empty()) {
		batch_id.to_owned()
	} else if let Some(state) = batches.get(&key) {
		state.batch_id.clone()
	} else {
		format!("auto_{}", utils::random_string(12))
	};

	batches.insert(
		key,
		InviteBatchState {
			batch_id: batch_id.clone(),
			updated_at: now,
		},
	);

	batch_id
}

#[implement(Service)]
#[tracing::instrument(name = "remote", level = "debug", skip_all)]
async fn remote_invite(
	&self,
	sender_user: &UserId,
	user_id: &UserId,
	room_id: &RoomId,
	reason: Option<&String>,
	is_direct: bool,
	batch_id: Option<&str>,
) -> Result {
	let (pdu, pdu_json, invite_room_state) = {
		let state_lock = self.services.state.mutex.lock(room_id).await;

		let content = RoomMemberEventContent {
			displayname: self
				.services
				.users
				.displayname(user_id)
				.await
				.ok(),
			avatar_url: self.services.users.avatar_url(user_id).await.ok(),
			is_direct: Some(is_direct),
			reason: reason.cloned(),
			..RoomMemberEventContent::new(MembershipState::Invite)
		};

		let (pdu, pdu_json) = self
			.services
			.timeline
			.create_hash_and_sign_event(
				PduBuilder {
					event_type: content.event_type().into(),
					state_key: Some(user_id.to_string().into()),
					content: serde_json::value::to_raw_value(&content).expect(
						"Invite membership content must serialize to RawValue"
					),
					unsigned: invite_unsigned(batch_id),
					..Default::default()
				},
				sender_user,
				room_id,
				&state_lock,
			)
			.await?;

		let invite_room_state = self.services.state.summary_stripped(&pdu).await;

		drop(state_lock);

		(pdu, pdu_json, invite_room_state)
	};

	let room_version_id = self
		.services
		.state
		.get_room_version(room_id)
		.await?;

	let response = self
		.services
		.federation
		.execute(user_id.server_name(), create_invite::v2::Request {
			room_id: room_id.to_owned(),
			event_id: (*pdu.event_id).to_owned(),
			room_version: room_version_id.clone(),
			event: self
				.services
				.federation
				.format_pdu_into(pdu_json.clone(), Some(&room_version_id))
				.await,
			invite_room_state: invite_room_state
				.into_iter()
				.map(Into::into)
				.collect(),
			via: self
				.services
				.state_cache
				.servers_route_via(room_id)
				.await
				.ok(),
		})
		.await?;

	// We do not add the event_id field to the pdu here because of signature and
	// hashes checks
	let (event_id, value) = gen_event_id_canonical_json(&response.event, &room_version_id)
		.map_err(|e| {
			err!(Request(BadJson(warn!("Could not convert event to canonical JSON: {e}"))))
		})?;

	if pdu.event_id != event_id {
		return Err!(Request(BadJson(warn!(
			%pdu.event_id, %event_id,
			"Server {} sent event with wrong event ID",
			user_id.server_name()
		))));
	}

	let origin: OwnedServerName = serde_json::from_value(serde_json::to_value(
		value
			.get("origin")
			.ok_or_else(|| err!(Request(BadJson("Event missing origin field."))))?,
	)?)
	.map_err(|e| {
		err!(Request(BadJson(warn!("Origin field in event is not a valid server name: {e}"))))
	})?;

	let pdu_id = self
		.services
		.event_handler
		.handle_incoming_pdu(&origin, room_id, &event_id, value, true)
		.await?
		.map(at!(0))
		.ok_or_else(|| {
			err!(Request(InvalidParam("Could not accept incoming PDU as timeline event.")))
		})?;

	self.services
		.sending
		.send_pdu_room(room_id, &pdu_id)
		.await?;

	Ok(())
}

#[implement(Service)]
#[tracing::instrument(name = "local", level = "debug", skip_all)]
async fn local_invite(
	&self,
	sender_user: &UserId,
	user_id: &UserId,
	room_id: &RoomId,
	reason: Option<&String>,
	is_direct: bool,
	batch_id: Option<&str>,
) -> Result {
	if !self
		.services
		.state_cache
		.is_joined(sender_user, room_id)
		.await
	{
		return Err!(Request(Forbidden(
			"You must be joined in the room you are trying to invite from."
		)));
	}

	let state_lock = self.services.state.mutex.lock(room_id).await;

	let content = RoomMemberEventContent {
		displayname: self
			.services
			.users
			.displayname(user_id)
			.await
			.ok(),
		avatar_url: self.services.users.avatar_url(user_id).await.ok(),
		blurhash: self.services.users.blurhash(user_id).await.ok(),
		is_direct: Some(is_direct),
		reason: reason.cloned(),
		..RoomMemberEventContent::new(MembershipState::Invite)
	};

	self.services
		.timeline
		.build_and_append_pdu(
			PduBuilder {
				event_type: content.event_type().into(),
				state_key: Some(user_id.to_string().into()),
				content: serde_json::value::to_raw_value(&content)
					.expect("Invite membership content must serialize to RawValue"),
				unsigned: invite_unsigned(batch_id),
				..Default::default()
			},
			sender_user,
			room_id,
			&state_lock,
		)
		.await?;

	// Release lock before auto-join — join() acquires its own state_lock internally.
	// Holding it here would cause a deadlock.
	drop(state_lock);

	// Auto-join if configured.
	// We already dropped state_lock above — acquire a fresh one for join().
	if self.services.server.config.auto_accept_invites {
		use tuwunel_core::info;

		info!(
			"Auto-accept invites enabled: automatically joining {user_id} to room {room_id} \
			 (invited by {sender_user})"
		);

		let join_lock = self.services.state.mutex.lock(room_id).await;
		match self
			.join(
				user_id, // the invited user becomes the joiner
				room_id,
				None,       // orig_room_id
				None,       // reason
				&[],        // servers — local user, no federation needed
				false,      // is_appservice
				&join_lock,
			)
			.boxed()
			.await
		{
			Ok(()) => {
				info!(
					"Successfully auto-joined {user_id} to room {room_id} after invite from \
					 {sender_user}"
				);
			},
			Err(e) => {
				use tuwunel_core::warn;
				warn!(
					"Failed to auto-join {user_id} to room {room_id} after invite: {e}. \
					 User will need to manually accept the invite."
				);
				// Invite was created successfully — don't propagate error
			},
		}
	}

	Ok(())
}
