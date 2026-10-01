use axum::extract::State;
use ruma::api::client::backup::{
	add_backup_keys_for_room::{self, v3::Response as AddResponse},
	delete_backup_keys_for_room::{self, v3::Response as DeleteResponse},
	get_backup_keys_for_room::{self, v3::Response as GetResponse},
};
use tuwunel_core::{Result, utils::stream::IterStream};

use super::format_count_etag;
use crate::Ruma;

/// # `PUT /_matrix/client/r0/room_keys/keys/{roomId}`
///
/// Add the received backup keys to the database.
///
/// - Only manipulating the most recently created version of the backup is
///   allowed
/// - Adds the keys to the backup
/// - Returns the new number of keys in this backup and the etag
pub(crate) async fn add_backup_keys_for_room_route(
	State(services): State<crate::State>,
	body: Ruma<add_backup_keys_for_room::v3::Request>,
) -> Result<add_backup_keys_for_room::v3::Response> {
	let keys = body
		.sessions
		.iter()
		.map(|(session_id, key_data)| (body.room_id.as_ref(), session_id.as_str(), key_data))
		.stream();

	let metadata = services
		.key_backups
		.add_keys(body.sender_user(), &body.version, keys)
		.await?;

	let (count, etag) = format_count_etag(metadata)?;

	Ok(AddResponse { count, etag })
}

/// # `GET /_matrix/client/r0/room_keys/keys/{roomId}`
///
/// Retrieves all keys from the backup for a given room.
pub(crate) async fn get_backup_keys_for_room_route(
	State(services): State<crate::State>,
	body: Ruma<get_backup_keys_for_room::v3::Request>,
) -> Result<get_backup_keys_for_room::v3::Response> {
	let sessions = services
		.key_backups
		.get_room(body.sender_user(), &body.version, &body.room_id)
		.await;

	Ok(GetResponse { sessions })
}

/// # `DELETE /_matrix/client/r0/room_keys/keys/{roomId}`
///
/// Delete the keys from the backup for a given room.
pub(crate) async fn delete_backup_keys_for_room_route(
	State(services): State<crate::State>,
	body: Ruma<delete_backup_keys_for_room::v3::Request>,
) -> Result<delete_backup_keys_for_room::v3::Response> {
	let metadata = services
		.key_backups
		.delete_room_keys(body.sender_user(), &body.version, &body.room_id)
		.await?;

	let (count, etag) = format_count_etag(metadata)?;

	Ok(DeleteResponse { count, etag })
}
