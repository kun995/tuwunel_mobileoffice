use futures::{Stream, StreamExt, TryStreamExt};
use regex::Regex;
use ruma::{OwnedRoomId, RoomId};
use tuwunel_core::{Result, utils::stream::ReadyExt};
use tuwunel_service::Services;

use crate::{Context, admin_command, get_room_info, utils::parse_user_id};

#[admin_command]
pub(super) async fn room_purge_user(
	&self,
	user_id: String,
	regex: bool,
	sole_member: bool,
	dry_run: bool,
) -> Result {
	let services = self.services;

	if dry_run {
		self.write_str("Matching rooms:\n```\n").await?;
	}

	let count = if regex {
		let pattern = &Regex::new(&user_id)?;
		let rooms = services
			.metadata
			.iter_ids()
			.map(ToOwned::to_owned)
			.filter_map(async |room_id| {
				(!services.admin.is_admin_room(&room_id).await
					&& room_has_matching_member(services, &room_id, pattern, sole_member).await)
					.then_some(room_id)
			});

		purge_stream(self, rooms, dry_run).await?
	} else {
		let user_id = parse_user_id(services, &user_id)?;
		let rooms = services
			.state_cache
			.rooms_joined(&user_id)
			.map(ToOwned::to_owned)
			.filter_map(async |room_id| {
				(!services.admin.is_admin_room(&room_id).await
					&& (!sole_member || is_sole_joined_member(services, &room_id).await))
					.then_some(room_id)
			});

		purge_stream(self, rooms, dry_run).await?
	};

	match (dry_run, count) {
		| (true, _) => write!(self, "```\nMatched {count} rooms."),
		| (false, 0) => write!(self, "No rooms matched."),
		| (false, _) => write!(self, "Deleted {count} rooms from our database."),
	}
	.await
}

async fn room_has_matching_member(
	services: &Services,
	room_id: &RoomId,
	pattern: &Regex,
	sole_member: bool,
) -> bool {
	let sole_ok = !sole_member || is_sole_joined_member(services, room_id).await;

	sole_ok
		&& services
			.state_cache
			.room_members(room_id)
			.ready_any(|user| pattern.is_match(user.as_str()))
			.await
}

async fn is_sole_joined_member(services: &Services, room_id: &RoomId) -> bool {
	services
		.state_cache
		.room_joined_count(room_id)
		.await
		.is_ok_and(|count| count == 1)
}

/// Lists (dry run) or deletes each matched room, returning the count.
async fn purge_stream<S>(context: &Context<'_>, rooms: S, dry_run: bool) -> Result<usize>
where
	S: Stream<Item = OwnedRoomId> + Send,
{
	let services = context.services;

	rooms
		.map(Ok)
		.try_fold(0_usize, async |count, room_id: OwnedRoomId| {
			if dry_run {
				let (id, members, name) = get_room_info(services, &room_id).await;

				writeln!(context, "{id}\tMembers: {members}\tName: {name}").await?;
			} else {
				let state_lock = services.state.mutex.lock(&room_id).await;

				// Non-forced: preserves local users' left-membership records.
				services
					.delete
					.delete_room(&room_id, false, state_lock)
					.await?;
			}

			Ok(count.saturating_add(1))
		})
		.await
}
