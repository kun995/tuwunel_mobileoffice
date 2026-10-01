use axum::extract::State;
use futures::{StreamExt, stream};
use itertools::Itertools;
use ruma::{OwnedRoomId, api::client::keys::get_key_changes};
use tuwunel_core::{Result, at, err, utils::stream::BroadbandExt};

use crate::Ruma;

/// # `GET /_matrix/client/r0/keys/changes`
///
/// Gets a list of users who have updated their device identity keys since the
/// previous sync token.
///
/// - TODO: left users
pub(crate) async fn get_key_changes_route(
	State(services): State<crate::State>,
	body: Ruma<get_key_changes::v3::Request>,
) -> Result<get_key_changes::v3::Response> {
	let sender_user = body.sender_user();

	let from = body
		.from
		.parse()
		.map_err(|_| err!(Request(InvalidParam("Invalid `from`."))))?;

	let to = body
		.to
		.parse()
		.map_err(|_| err!(Request(InvalidParam("Invalid `to`."))))?;

	let user_changes = services
		.users
		.keys_changed(sender_user, from, Some(to))
		.map(ToOwned::to_owned);

	let room_changes = services
		.state_cache
		.rooms_joined(sender_user)
		.map(ToOwned::to_owned)
		.broad_then(async |room_id: OwnedRoomId| {
			services
				.users
				.room_keys_changed(&room_id, from, Some(to))
				.map(at!(0))
				.map(ToOwned::to_owned)
				.collect::<Vec<_>>()
				.await
		})
		.flat_map(stream::iter);

	let changed = user_changes
		.chain(room_changes)
		.collect::<Vec<_>>()
		.await
		.into_iter()
		.sorted_unstable()
		.dedup()
		.collect();

	Ok(get_key_changes::v3::Response {
		left: Vec::new(), // TODO
		changed,
	})
}
