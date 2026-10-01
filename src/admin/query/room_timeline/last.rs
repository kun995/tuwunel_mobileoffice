use ruma::OwnedRoomOrAliasId;
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn last(&self, room_id: OwnedRoomOrAliasId) -> Result {
	let room_id = self
		.services
		.alias
		.maybe_resolve(&room_id)
		.await?;

	let result = self
		.services
		.timeline
		.last_timeline_count(None, &room_id, None)
		.await?;

	write!(self, "{result:#?}").await
}
