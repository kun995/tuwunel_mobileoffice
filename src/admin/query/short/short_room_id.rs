use ruma::OwnedRoomOrAliasId;
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn short_room_id(&self, room_id: OwnedRoomOrAliasId) -> Result {
	let room_id = self
		.services
		.alias
		.maybe_resolve(&room_id)
		.await?;

	let shortid = self
		.services
		.short
		.get_shortroomid(&room_id)
		.await?;

	write!(self, "{shortid:#?}").await
}
