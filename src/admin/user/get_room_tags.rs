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
pub(super) async fn get_room_tags(&self, user_id: String, room_id: OwnedRoomId) -> Result {
	let user_id = parse_active_local_user_id(self.services, &user_id).await?;

	let tags_event = self
		.services
		.account_data
		.get_room(&room_id, &user_id, RoomAccountDataEventType::Tag)
		.await
		.unwrap_or(TagEvent {
			content: TagEventContent { tags: BTreeMap::new() },
		});

	write!(self, "```\n{:#?}\n```", tags_event.content.tags).await
}
