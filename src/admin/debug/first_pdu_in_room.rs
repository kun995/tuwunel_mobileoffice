use ruma::OwnedRoomId;
use tuwunel_core::{Err, Result, err};

use crate::admin_command;

#[admin_command]
#[tracing::instrument(level = "debug", skip(self))]
pub(super) async fn first_pdu_in_room(&self, room_id: OwnedRoomId) -> Result {
	if !self
		.services
		.state_cache
		.server_in_room(&self.services.server.name, &room_id)
		.await
	{
		return Err!("We are not participating in the room / we don't know about the room ID.",);
	}

	let first_pdu = self
		.services
		.timeline
		.first_pdu_in_room(&room_id)
		.await
		.map_err(|_| err!(Database("Failed to find the first PDU in database")))?;

	let out = format!("{first_pdu:?}");
	self.write_str(&out).await
}
