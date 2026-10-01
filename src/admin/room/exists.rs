use ruma::OwnedRoomId;
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn room_exists(&self, room_id: OwnedRoomId) -> Result {
	let result = self.services.metadata.exists(&room_id).await;

	write!(self, "{result}").await
}
