use ruma::OwnedRoomOrAliasId;
use tuwunel_core::{Err, Result, debug};

use super::do_ban_room;
use crate::admin_command;

#[admin_command]
pub(super) async fn ban_room(&self, room: OwnedRoomOrAliasId) -> Result {
	debug!("Got room alias or ID: {}", room);

	let admin_room_alias = &self.services.admin.admin_alias;

	if let Ok(admin_room_id) = self.services.admin.get_admin_room().await
		&& (room.to_string().eq(&admin_room_id) || room.to_string().eq(admin_room_alias))
	{
		return Err!("Not allowed to ban the admin room.");
	}

	let room_id = self.services.alias.maybe_resolve(&room).await?;

	do_ban_room(self.services, &room_id).await;

	self.write_str(
		"Room banned, removed all our local users, and disabled incoming federation with room.",
	)
	.await
}
