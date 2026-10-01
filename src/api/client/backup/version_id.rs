use axum::extract::State;
use ruma::api::client::backup::{delete_backup_version, get_backup_info, update_backup_version};
use tuwunel_core::{Result, err};

use super::{get_count_etag, validate_algorithm_shape};
use crate::Ruma;

/// # `PUT /_matrix/client/r0/room_keys/version/{version}`
///
/// Update information about an existing backup. Only `auth_data` can be
/// modified.
pub(crate) async fn update_backup_version_route(
	State(services): State<crate::State>,
	body: Ruma<update_backup_version::v3::Request>,
) -> Result<update_backup_version::v3::Response> {
	validate_algorithm_shape(&body.algorithm)
		.map_err(|e| err!(Request(BadJson("Invalid backup metadata: {e}"))))?;

	services
		.key_backups
		.update_backup(body.sender_user(), &body.version, &body.algorithm)
		.await?;

	Ok(update_backup_version::v3::Response {})
}

/// # `GET /_matrix/client/v3/room_keys/version/{version}`
///
/// Get information about an existing backup.
pub(crate) async fn get_backup_info_route(
	State(services): State<crate::State>,
	body: Ruma<get_backup_info::v3::Request>,
) -> Result<get_backup_info::v3::Response> {
	let algorithm = services
		.key_backups
		.get_backup(body.sender_user(), &body.version)
		.await
		.map_err(|_| {
			err!(Request(NotFound("Key backup does not exist at version {:?}", body.version)))
		})?;

	validate_algorithm_shape(&algorithm).map_err(|e| {
		err!(Request(NotFound(
			"Key backup does not exist at version {:?}: {e}",
			body.version
		)))
	})?;

	let (count, etag) = get_count_etag(&services, body.sender_user(), &body.version).await?;

	Ok(get_backup_info::v3::Response {
		algorithm,
		count,
		etag,
		version: body.version.clone(),
	})
}

/// # `DELETE /_matrix/client/r0/room_keys/version/{version}`
///
/// Delete an existing key backup.
///
/// - Deletes both information about the backup, as well as all key data related
///   to the backup
pub(crate) async fn delete_backup_version_route(
	State(services): State<crate::State>,
	body: Ruma<delete_backup_version::v3::Request>,
) -> Result<delete_backup_version::v3::Response> {
	services
		.key_backups
		.delete_backup(body.sender_user(), &body.version)
		.await;

	Ok(delete_backup_version::v3::Response {})
}
