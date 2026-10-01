use std::collections::BTreeMap;

use ruma::{
	OwnedRoomId,
	events::{
		RoomAccountDataEventType,
		tag::{TagEvent, TagEventContent},
	},
};
use tuwunel_core::Result;

use crate::{admin_command, utils::parse_active_local_user_id};

#[admin_command]
pub(super) async fn delete_room_tag(
	&self,
	user_id: String,
	room_id: OwnedRoomId,
	tag: String,
) -> Result {
	let user_id = parse_active_local_user_id(self.services, &user_id).await?;

	let mut tags_event = self
		.services
		.account_data
		.get_room(&room_id, &user_id, RoomAccountDataEventType::Tag)
		.await
		.unwrap_or(TagEvent {
			content: TagEventContent { tags: BTreeMap::new() },
		});

	tags_event
		.content
		.tags
		.remove(&tag.clone().into());

	self.services
		.account_data
		.update(
			Some(&room_id),
			&user_id,
			RoomAccountDataEventType::Tag,
			&serde_json::to_value(tags_event).expect("to json value always works"),
		)
		.await?;

	write!(
		self,
		"Successfully updated room account data for {user_id} and room {room_id}, deleting room \
		 tag {tag}"
	)
	.await
}
