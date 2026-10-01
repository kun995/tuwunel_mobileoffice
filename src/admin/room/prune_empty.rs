use futures::{FutureExt, StreamExt};
use tuwunel_core::{Result, utils::FutureBoolExt};

use crate::admin_command;

#[admin_command]
pub(super) async fn room_prune_empty(&self, force: bool) -> Result {
	let rooms = self
		.services
		.metadata
		.iter_ids()
		.filter(|room_id| {
			let has_no_local_users = self
				.services
				.state_cache
				.local_users_in_room(room_id)
				.boxed()
				.into_future()
				.map(|(next, ..)| next.is_none())
				.boxed();

			let has_no_local_invites = self
				.services
				.state_cache
				.local_users_invited_to_room(room_id)
				.boxed()
				.into_future()
				.map(|(next, ..)| next.is_none())
				.boxed();

			has_no_local_users.and(has_no_local_invites)
		})
		.map(ToOwned::to_owned)
		.collect::<Vec<_>>()
		.await;

	for room_id in &rooms {
		let state_lock = self.services.state.mutex.lock(room_id).await;

		self.services
			.delete
			.delete_room(room_id, force, state_lock)
			.await?;
	}

	let rooms_len = rooms.len();

	write!(self, "Successfully deleted {rooms_len} rooms from our database.").await?;

	Ok(())
}
