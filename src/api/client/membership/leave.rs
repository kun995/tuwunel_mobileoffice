use axum::extract::State;
use futures::{FutureExt, StreamExt};
use rand::seq::SliceRandom;
use ruma::api::client::membership::leave_room::v3::{Request, Response};
use ruma::{
	Int, RoomId, UserId,
	events::{StateEventType, room::power_levels::RoomPowerLevelsEventContent},
};
use tuwunel_core::matrix::pdu::PduBuilder;
use tuwunel_core::{Err, Result};
use tuwunel_service::{Services, rooms::timeline::RoomMutexGuard};

use crate::{Ruma, client::admin::misc::is_notice_room};

/// Leaves a room through `POST /_matrix/client/v3/rooms/{roomId}/leave`.
///
/// Server-notice invitations cannot be rejected, but joined recipients can leave.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) async fn leave_room_route(
	State(services): State<crate::State>,
	Ruma {
		body, sender_user, ..
	}: Ruma<Request>,
) -> Result<Response> {
	let state_lock = services.state.mutex.lock(&body.room_id).await;
	let sender = sender_user
		.as_deref()
		.expect("user must be authenticated for this handler");

	if services.state_cache.is_invited(sender, &body.room_id).await
		&& is_notice_room(&services, sender, &body.room_id).await?
	{
		return Err!(Request(CannotLeaveServerNoticeRoom(
			"You cannot reject this invite"
		)));
	}

	// Reuse the room guard: taking this mutex again would deadlock the leave request.
	auto_assign_admin_before_leave(&services, sender, &body.room_id, &state_lock)
		.boxed()
		.await?;
	services
		.membership
		.leave(sender, &body.room_id, body.reason, false, &state_lock)
		.await?;

	if services.config.delete_rooms_after_leave {
		services
			.delete
			.delete_if_empty_local(&body.room_id, state_lock)
			.boxed()
			.await;
	}

	Ok(Response {})
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

	let Ok(power_levels) = services.state_accessor.get_power_levels(room_id).await else {
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

	candidates.shuffle(&mut rand::rng());
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
	power_levels_content.users.insert(next_admin, admin_power);

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
