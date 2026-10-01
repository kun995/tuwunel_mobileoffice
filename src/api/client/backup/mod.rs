mod keys;
mod keys_room;
mod keys_session;
mod version;
mod version_id;

use ruma::{CanonicalJsonValue, UInt, UserId, serde::Raw};
use serde::Deserialize;
use tuwunel_core::{Error, Result};
use tuwunel_service::Services;

pub(crate) use self::{
	keys::{add_backup_keys_route, delete_backup_keys_route, get_backup_keys_route},
	keys_room::{
		add_backup_keys_for_room_route, delete_backup_keys_for_room_route,
		get_backup_keys_for_room_route,
	},
	keys_session::{
		add_backup_keys_for_session_route, delete_backup_keys_for_session_route,
		get_backup_keys_for_session_route,
	},
	version::{create_backup_version_route, get_latest_backup_info_route},
	version_id::{
		delete_backup_version_route, get_backup_info_route, update_backup_version_route,
	},
};

/// Overrides ruma's internal `AlgorithmWithData` shape required by the GET
/// `/room_keys/version[/{version}]` response serializer. Validating against
/// this will not raise a serialization error (HTTP 500) when responding.
#[derive(Deserialize)]
#[expect(unused)]
struct AlgorithmShape {
	algorithm: Raw<CanonicalJsonValue>,
	auth_data: Raw<CanonicalJsonValue>,
}

pub(super) fn validate_algorithm_shape<T>(raw: &Raw<T>) -> Result {
	raw.deserialize_as_unchecked::<AlgorithmShape>()
		.map_err(Into::into)
		.map(drop)
}

pub(super) async fn get_count_etag(
	services: &Services,
	sender_user: &UserId,
	version: &str,
) -> Result<(UInt, String)> {
	let metadata = services
		.key_backups
		.get_count_etag(sender_user, version)
		.await?;

	format_count_etag(metadata)
}

pub(super) fn format_count_etag((count, etag): (usize, u64)) -> Result<(UInt, String)> {
	let count = count.try_into().map_err(Error::from)?;

	Ok((count, etag.to_string()))
}
