use axum::extract::State;
use ruma::api::client::backup::{
	add_backup_keys::{self, v3::Response as AddResponse},
	delete_backup_keys::{self, v3::Response as DeleteResponse},
	get_backup_keys::{self, v3::Response as GetResponse},
};
use tuwunel_core::{Result, utils::stream::IterStream};

use super::format_count_etag;
use crate::Ruma;

/// # `PUT /_matrix/client/r0/room_keys/keys`
///
/// Add the received backup keys to the database.
///
/// - Only manipulating the most recently created version of the backup is
///   allowed
/// - Adds the keys to the backup
/// - Returns the new number of keys in this backup and the etag
pub(crate) async fn add_backup_keys_route(
	State(services): State<crate::State>,
	body: Ruma<add_backup_keys::v3::Request>,
) -> Result<add_backup_keys::v3::Response> {
	let keys = body
		.rooms
		.iter()
		.flat_map(|(room_id, room)| {
			room.sessions
				.iter()
				.map(move |(session_id, key_data)| (room_id, session_id, key_data))
		})
		.map(|(room_id, session_id, key_data)| (room_id.as_ref(), session_id.as_str(), key_data))
		.stream();

	let metadata = services
		.key_backups
		.add_keys(body.sender_user(), &body.version, keys)
		.await?;

	let (count, etag) = format_count_etag(metadata)?;

	Ok(AddResponse { count, etag })
}

/// # `GET /_matrix/client/r0/room_keys/keys`
///
/// Retrieves all keys from the backup.
pub(crate) async fn get_backup_keys_route(
	State(services): State<crate::State>,
	body: Ruma<get_backup_keys::v3::Request>,
) -> Result<get_backup_keys::v3::Response> {
	let rooms = services
		.key_backups
		.get_all(body.sender_user(), &body.version)
		.await;

	Ok(GetResponse { rooms })
}

/// # `DELETE /_matrix/client/r0/room_keys/keys`
///
/// Delete the keys from the backup.
pub(crate) async fn delete_backup_keys_route(
	State(services): State<crate::State>,
	body: Ruma<delete_backup_keys::v3::Request>,
) -> Result<delete_backup_keys::v3::Response> {
	let metadata = services
		.key_backups
		.delete_all_keys(body.sender_user(), &body.version)
		.await?;

	let (count, etag) = format_count_etag(metadata)?;

	Ok(DeleteResponse { count, etag })
}
