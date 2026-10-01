use ruma::{OwnedRoomId, RoomOrAliasId};
use tuwunel_core::{Err, Result, is_equal_to, warn};

use super::do_ban_room;
use crate::admin_command;

#[admin_command]
pub(super) async fn ban_list_of_rooms(&self) -> Result {
	if self.body.len() < 2
		|| !self.body[0].trim().starts_with("```")
		|| self.body.last().unwrap_or(&"").trim() != "```"
	{
		return Err!("Expected code block in command body. Add --help for details.",);
	}

	let rooms_s = self
		.body
		.to_vec()
		.drain(1..self.body.len().saturating_sub(1))
		.collect::<Vec<_>>();

	let admin_room_id = self.services.admin.get_admin_room().await.ok();

	let mut room_ids: Vec<OwnedRoomId> = Vec::with_capacity(rooms_s.len());

	for room in rooms_s {
		let room_alias_or_id = match <&RoomOrAliasId>::try_from(room) {
			| Ok(room_alias_or_id) => room_alias_or_id,
			| Err(e) => {
				warn!("Error parsing room {room} during bulk room banning, ignoring: {e}");
				continue;
			},
		};

		let room_id = match self
			.services
			.alias
			.maybe_resolve(room_alias_or_id)
			.await
		{
			| Ok(room_id) => room_id,
			| Err(e) => {
				warn!("Failed to resolve room alias {room_alias_or_id} to a room ID: {e}");
				continue;
			},
		};

		if admin_room_id
			.as_ref()
			.is_some_and(is_equal_to!(&room_id))
		{
			warn!("User specified admin room in bulk ban list, ignoring");
			continue;
		}

		room_ids.push(room_id);
	}

	let rooms_len = room_ids.len();

	for room_id in room_ids {
		do_ban_room(self.services, &room_id).await;
	}

	write!(
		self,
		"Finished bulk room ban, banned {rooms_len} total rooms, evicted all users, and \
		 disabled incoming federation with the room."
	)
	.await
}
