use ruma::{
	RoomId, UserId,
	events::{GlobalAccountDataEventType, direct::DirectEventContent},
};
use serde_json::{Map as JsonMap, Value as JsonValue};
use tuwunel_core::{Result, at, implement, is_equal_to};

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

	let content = content
		.as_object_mut()
		.expect("validated object above");
	let rooms = content
		.entry(target_user_id.to_string())
		.or_insert_with(|| JsonValue::Array(Vec::new()));

	if !rooms.is_array() {
		*rooms = JsonValue::Array(Vec::new());
	}

	let rooms = rooms
		.as_array_mut()
		.expect("validated array above");
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

	value
		.as_object_mut()
		.expect("validated object above")
}
