use std::iter::once;

use axum::extract::State;
use ruma::api::client::backup::{
	add_backup_keys_for_session::{self, v3::Response as AddResponse},
	delete_backup_keys_for_session::{self, v3::Response as DeleteResponse},
	get_backup_keys_for_session::{self, v3::Response as GetResponse},
};
use tuwunel_core::{Result, err, utils::stream::IterStream};

use super::format_count_etag;
use crate::Ruma;

/// # `PUT /_matrix/client/r0/room_keys/keys/{roomId}/{sessionId}`
///
/// Add the received backup key to the database.
///
/// - Only manipulating the most recently created version of the backup is
///   allowed
/// - Adds the keys to the backup
/// - Returns the new number of keys in this backup and the etag
pub(crate) async fn add_backup_keys_for_session_route(
	State(services): State<crate::State>,
	body: Ruma<add_backup_keys_for_session::v3::Request>,
) -> Result<add_backup_keys_for_session::v3::Response> {
	let keys =
		once((body.room_id.as_ref(), body.session_id.as_str(), &body.session_data)).stream();

	let metadata = services
		.key_backups
		.add_keys(body.sender_user(), &body.version, keys)
		.await?;

	let (count, etag) = format_count_etag(metadata)?;

	Ok(AddResponse { count, etag })
}

/// # `GET /_matrix/client/r0/room_keys/keys/{roomId}/{sessionId}`
///
/// Retrieves a key from the backup.
pub(crate) async fn get_backup_keys_for_session_route(
	State(services): State<crate::State>,
	body: Ruma<get_backup_keys_for_session::v3::Request>,
) -> Result<get_backup_keys_for_session::v3::Response> {
	let key_data = services
		.key_backups
		.get_session(body.sender_user(), &body.version, &body.room_id, &body.session_id)
		.await
		.map_err(|error| {
			if error.is_not_found() {
				err!(Request(NotFound(debug_error!(
					"Backup key not found for this user's session."
				))))
			} else {
				error
			}
		})?;

	Ok(GetResponse { key_data })
}

/// # `DELETE /_matrix/client/r0/room_keys/keys/{roomId}/{sessionId}`
///
/// Delete a key from the backup.
pub(crate) async fn delete_backup_keys_for_session_route(
	State(services): State<crate::State>,
	body: Ruma<delete_backup_keys_for_session::v3::Request>,
) -> Result<delete_backup_keys_for_session::v3::Response> {
	let metadata = services
		.key_backups
		.delete_room_key(body.sender_user(), &body.version, &body.room_id, &body.session_id)
		.await?;

	let (count, etag) = format_count_etag(metadata)?;

	Ok(DeleteResponse { count, etag })
}
