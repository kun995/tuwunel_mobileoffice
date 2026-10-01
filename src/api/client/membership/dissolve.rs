use axum::{
	Json,
	extract::{Path, State},
};
use axum_extra::{
	TypedHeader,
	headers::{Authorization, authorization::Bearer},
};
use futures::{FutureExt, StreamExt, stream};
use ruma::{OwnedUserId, RoomId, UserId, api::error::ErrorKind};
use serde::{Deserialize, Serialize};
use tuwunel_core::{Err, Error, Result};

#[derive(Debug, Default, Deserialize)]
pub(crate) struct DissolveRoomBody {
	#[serde(default)]
	pub reason: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct DissolveRoomResponse {
	pub removed_user_ids: Vec<OwnedUserId>,
	pub left_user_id: OwnedUserId,
}

/// # `POST /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/dissolve`
///
/// Dissolves a room from the caller's perspective by removing all other current
/// members and then leaving the caller from the room.
pub(crate) async fn dissolve_room_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path(room_id): Path<ruma::OwnedRoomId>,
	Json(body): Json<DissolveRoomBody>,
) -> Result<Json<DissolveRoomResponse>> {
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken(ruma::api::error::UnknownTokenErrorData { soft_logout: false }),
				"Unknown access token.",
			)
		})?;
	let sender_user_ref: &UserId = sender_user.as_ref();
	let room_id_ref: &RoomId = &room_id;

	if !services
		.state_cache
		.is_joined(sender_user_ref, room_id_ref)
		.await
	{
		return Err!(Request(Forbidden("You are not a member of this room.")));
	}

	if services.admin.is_admin_room(room_id_ref).await {
		return Err!(Request(Forbidden("The admin room cannot be dissolved.")));
	}

	let state_lock = services.state.mutex.lock(room_id_ref).await;
	let target_user_ids: Vec<_> = stream::select_all([
		services.state_cache.room_members(room_id_ref).boxed(),
		services
			.state_cache
			.room_members_invited(room_id_ref)
			.boxed(),
		services
			.state_cache
			.room_members_knocked(room_id_ref)
			.boxed(),
	])
	.map(ToOwned::to_owned)
	.collect()
	.await;

	let target_user_ids = dedup_user_ids(target_user_ids);

	let power_levels = services
		.state_accessor
		.get_power_levels(room_id_ref)
		.await?;
	let sender_power = power_levels.for_user(sender_user_ref);
	if sender_power < power_levels.kick {
		return Err!(Request(Forbidden(
			"You do not have permission to dissolve this room."
		)));
	}

	let blocking_user_ids: Vec<_> = target_user_ids
		.iter()
		.filter(|user_id| {
			let user_id_ref: &UserId = user_id.as_ref();
			user_id_ref != sender_user_ref
		})
		.filter(|user_id| {
			let user_id_ref: &UserId = user_id.as_ref();
			power_levels.for_user(user_id_ref) >= sender_power
		})
		.cloned()
		.collect();

	if !blocking_user_ids.is_empty() {
		return Err!(Request(Forbidden(
			"Cannot dissolve room because one or more members have equal or higher power level."
		)));
	}

	let reason = body
		.reason
		.unwrap_or_else(|| "Room dissolved by administrator.".to_owned());

	let mut removed_user_ids = Vec::new();
	for user_id in target_user_ids {
		if user_id == sender_user {
			continue;
		}

		services
			.membership
			.kick(
				room_id_ref,
				&user_id,
				Some(&reason),
				sender_user_ref,
				&state_lock,
			)
			.boxed()
			.await?;
		removed_user_ids.push(user_id);
	}

	services
		.membership
		.leave(
			sender_user_ref,
			room_id_ref,
			Some(reason),
			false,
			&state_lock,
		)
		.boxed()
		.await?;

	if services.config.delete_rooms_after_leave {
		services
			.delete
			.delete_if_empty_local(room_id_ref, state_lock)
			.boxed()
			.await;
	}

	Ok(Json(DissolveRoomResponse {
		removed_user_ids,
		left_user_id: sender_user,
	}))
}

fn dedup_user_ids(mut user_ids: Vec<OwnedUserId>) -> Vec<OwnedUserId> {
	user_ids.sort();
	user_ids.dedup();
	user_ids
}
