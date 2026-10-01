use ruma::OwnedRoomId;
use tuwunel_core::{Err, Result};

use crate::admin_command;

#[admin_command]
pub(super) async fn room_delete(&self, room_id: OwnedRoomId, force: bool) -> Result {
	if self.services.admin.is_admin_room(&room_id).await {
		return Err!("Cannot delete admin room");
	}

	let state_lock = self.services.state.mutex.lock(&room_id).await;

	self.services
		.delete
		.delete_room(&room_id, force, state_lock)
		.await?;

	self.write_str("Successfully deleted the room from our database.")
		.await?;

	Ok(())
}
