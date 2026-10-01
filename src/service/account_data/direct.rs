//! Direct-room account data helpers.
//!
//! These operations read and update a user's `m.direct` mapping. Callers can list direct rooms
//! without selecting a counterparty.

use std::collections::BTreeSet;

use ruma::{
	OwnedRoomId, RoomId, UserId,
	events::{
		GlobalAccountDataEventType,
		direct::{DirectEvent, DirectEventContent, DirectUserIdentifier},
	},
};
use serde_json::{Map as JsonMap, Value as JsonValue};
use tuwunel_core::{Result, at, implement, is_equal_to};

/// The rooms the user's `m.direct` names, whoever the counterparty is.
///
/// The map is keyed by counterparty rather than by room, so answering for one
/// room means flattening all of it. A user with no `m.direct` at all has no
/// direct rooms.
#[implement(super::Service)]
pub async fn direct_rooms(&self, user_id: &UserId) -> BTreeSet<OwnedRoomId> {
	self.direct_content(user_id)
		.await
		.into_iter()
		.flat_map(DirectEventContent::into_iter)
		.map(at!(1))
		.flat_map(Vec::into_iter)
		.collect()
}

/// Record a room as a direct chat with `target` in the user's `m.direct`.
///
/// The room joins the counterparty's list, which is created when this is
/// their first direct room. A list already naming the room is left alone,
/// so repeated calls settle on one entry.
///
/// # Panics
///
/// Panics when storing a changed mapping requires a global sequence number that cannot be allocated
/// or persisted.
#[implement(super::Service)]
pub async fn mark_direct(&self, user_id: &UserId, target: &UserId, room_id: &RoomId) -> Result {
	let mut content = self.direct_content(user_id).await.unwrap_or_default();

	let target: &DirectUserIdentifier = target.into();
	let listed = content
		.get(target)
		.is_some_and(|rooms| rooms.iter().any(is_equal_to!(room_id)));

	if listed {
		return Ok(());
	}

	content
		.entry(target.to_owned())
		.or_default()
		.push(room_id.to_owned());

	let event = serde_json::to_value(DirectEvent { content })?;

	let event_type = GlobalAccountDataEventType::Direct.to_string().into();

	self.services
		.account_data
		.update(None, user_id, event_type, &event)
		.await
}

/// The user's `m.direct` content, absent when they have never had one.
///
/// Account data is stored as the whole event rather than as its content, so
/// the record deserializes as an event and the content comes off it.
#[implement(super::Service)]
async fn direct_content(&self, user_id: &UserId) -> Result<DirectEventContent> {
	self.services
		.account_data
		.get_global(user_id, GlobalAccountDataEventType::Direct)
		.await
		.map(|event: DirectEvent| event.content)
}

#[implement(super::Service)]
pub async fn is_direct(&self, user_id: &UserId, room_id: &RoomId) -> bool {
	// Fast path for well-formed m.direct content.
	if self
		.services
		.account_data
		.get_global(user_id, GlobalAccountDataEventType::Direct)
		.await
		.map(|content: DirectEventContent| content)
		.into_iter()
		.flat_map(DirectEventContent::into_iter)
		.map(at!(1))
		.flat_map(Vec::into_iter)
		.any(is_equal_to!(room_id))
	{
		return true;
	}

	// Fallback for non-canonical historical payloads to avoid false negatives.
	let Ok(mut direct_event) = self
		.services
		.account_data
		.get_global::<JsonValue>(user_id, GlobalAccountDataEventType::Direct)
		.await
	else {
		return false;
	};

	let Some(content) = direct_event
		.as_object_mut()
		.and_then(|event| event.get_mut("content"))
		.and_then(JsonValue::as_object_mut)
	else {
		return false;
	};

	content
		.values_mut()
		.filter_map(JsonValue::as_array_mut)
		.flat_map(|rooms| rooms.iter())
		.filter_map(JsonValue::as_str)
		.any(is_equal_to!(room_id.as_str()))
}

#[implement(super::Service)]
pub async fn direct_rooms_for_user(
	&self,
	user_id: &UserId,
	target_user_id: &UserId,
) -> Vec<OwnedRoomId> {
	let Ok(direct_event) = self
		.services
		.account_data
		.get_global::<JsonValue>(user_id, GlobalAccountDataEventType::Direct)
		.await
	else {
		return Vec::new();
	};

	direct_event
		.get("content")
		.unwrap_or(&direct_event)
		.get(target_user_id.as_str())
		.and_then(JsonValue::as_array)
		.into_iter()
		.flatten()
		.filter_map(JsonValue::as_str)
		.filter_map(|room_id| RoomId::parse(room_id).ok())
		.collect()
}

#[implement(super::Service)]
pub async fn add_direct_room_for_user(
	&self,
	user_id: &UserId,
	target_user_id: &UserId,
	room_id: &RoomId,
) -> Result<bool> {
	let mut direct_event = self
		.services
		.account_data
		.get_global::<JsonValue>(user_id, GlobalAccountDataEventType::Direct)
		.await
		.unwrap_or_else(|_| {
			JsonValue::Object(
				[
					("type".to_owned(), JsonValue::String("m.direct".to_owned())),
					("content".to_owned(), JsonValue::Object(JsonMap::new())),
				]
				.into_iter()
				.collect(),
			)
		});

	let event_obj = ensure_object(&mut direct_event);
	event_obj.insert("type".to_owned(), JsonValue::String("m.direct".to_owned()));

	let content = event_obj
		.entry("content".to_owned())
		.or_insert_with(|| JsonValue::Object(JsonMap::new()));

	if !content.is_object() {
		*content = JsonValue::Object(JsonMap::new());
	}

	let content = content.as_object_mut().expect("validated object above");
	let rooms = content
		.entry(target_user_id.to_string())
		.or_insert_with(|| JsonValue::Array(Vec::new()));

	if !rooms.is_array() {
		*rooms = JsonValue::Array(Vec::new());
	}

	let rooms = rooms.as_array_mut().expect("validated array above");
	let room_id_s = room_id.as_str();

	if rooms
		.iter()
		.filter_map(JsonValue::as_str)
		.any(is_equal_to!(room_id_s))
	{
		return Ok(false);
	}

	rooms.push(JsonValue::String(room_id_s.to_owned()));

	self.services
		.account_data
		.update(
			None,
			user_id,
			GlobalAccountDataEventType::Direct.to_string().into(),
			&direct_event,
		)
		.await?;

	Ok(true)
}

fn ensure_object(value: &mut JsonValue) -> &mut JsonMap<String, JsonValue> {
	if !value.is_object() {
		*value = JsonValue::Object(JsonMap::new());
	}

	value.as_object_mut().expect("validated object above")
}
