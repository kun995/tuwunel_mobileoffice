use axum::extract::State;
use futures::{FutureExt, StreamExt};
use rand::seq::SliceRandom;
use ruma::{
	Int, RoomId, UserId,
	api::client::membership::leave_room,
	events::{StateEventType, room::power_levels::RoomPowerLevelsEventContent},
};
use tuwunel_core::{Result, matrix::pdu::PduBuilder};
use tuwunel_service::{Services, rooms::timeline::RoomMutexGuard};

use crate::Ruma;

/// # `POST /_matrix/client/v3/rooms/{roomId}/leave`
///
/// Tries to leave the sender user from a room.
///
/// - This should always work if the user is currently joined.
pub(crate) async fn leave_room_route(
	State(services): State<crate::State>,
	body: Ruma<leave_room::v3::Request>,
) -> Result<leave_room::v3::Response> {
	let state_lock = services.state.mutex.lock(&body.room_id).await;
	auto_assign_admin_before_leave(&services, body.sender_user(), &body.room_id, &state_lock)
		.boxed()
		.await?;

	services
		.membership
		.leave(body.sender_user(), &body.room_id, body.reason.clone(), false, &state_lock)
		.boxed()
		.await?;

	if services.config.delete_rooms_after_leave {
		services
			.delete
			.delete_if_empty_local(&body.room_id, state_lock)
			.boxed()
			.await;
	}

	Ok(leave_room::v3::Response {})
}

async fn auto_assign_admin_before_leave(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	state_lock: &RoomMutexGuard,
) -> Result {
	if services.admin.is_admin_room(room_id).await {
		return Ok(());
	}

	let Ok(power_levels) = services
		.state_accessor
		.get_power_levels(room_id)
		.await
	else {
		return Ok(());
	};

	let admin_power = Int::from(100);
	let sender_power = power_levels.for_user(sender_user);
	if sender_power < admin_power {
		return Ok(());
	}

	let mut candidates = services
		.state_cache
		.room_members(room_id)
		.map(ToOwned::to_owned)
		.collect::<Vec<_>>()
		.await;

	candidates.retain(|user_id| {
		let user_id: &UserId = user_id.as_ref();
		user_id != sender_user
	});

	if candidates.iter().any(|user_id| {
		let user_id: &UserId = user_id.as_ref();
		power_levels.for_user(user_id) >= admin_power
	}) {
		return Ok(());
	}

	candidates.shuffle(&mut rand::thread_rng());
	let Some(next_admin) = candidates.into_iter().next() else {
		return Ok(());
	};

	let mut power_levels_content = services
		.state_accessor
		.room_state_get_content::<RoomPowerLevelsEventContent>(
			room_id,
			&StateEventType::RoomPowerLevels,
			"",
		)
		.await
		.unwrap_or_default();

	power_levels_content.users.remove(sender_user);
	power_levels_content
		.users
		.insert(next_admin, admin_power);

	services
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &power_levels_content),
			sender_user,
			room_id,
			state_lock,
		)
		.boxed()
		.await?;

	Ok(())
}
