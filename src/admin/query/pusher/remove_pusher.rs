use ruma::OwnedUserId;
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn remove_pusher(&self, user_id: OwnedUserId, pushkey: String) -> Result {
	let exists = self
		.services
		.pusher
		.get_pusher(&user_id, &pushkey)
		.await
		.is_ok();

	self.services
		.pusher
		.delete_pusher(&user_id, &pushkey)
		.await;

	let message = if exists {
		"Pusher deleted."
	} else {
		"Pusher was not found but deletion was still attempted."
	};

	self.write_str(message).await
}
