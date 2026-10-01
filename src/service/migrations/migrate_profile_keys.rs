use ruma::{UserId, profile::ProfileFieldName};
use serde::de::IgnoredAny;
use tuwunel_core::{
	Result, info,
	utils::{ReadyExt, stream::TryExpect},
	warn,
};
use tuwunel_database::Json;

use crate::Services;

/// Relocates the per-user displayname and avatar_url out of their dedicated
/// columns into the unified useridprofilekey_value store keyed by MSC4133 field
/// name, where the profile service now reads them.
///
/// The dedicated columns are left intact, so an older binary opening the same
/// database still resolves.
pub(super) async fn migrate_profile_keys(services: &Services) -> Result {
	let db = &services.db;
	let cork = db.cork_and_sync();

	let userid_displayname = db["userid_displayname"].clone();
	let userid_avatarurl = db["userid_avatarurl"].clone();
	let userid_blurhash = db["userid_blurhash"].clone();
	let useridprofilekey_value = db["useridprofilekey_value"].clone();

	warn!(
		"Relocating displaynames, avatar_urls and blurhashes into the unified profile-key store"
	);

	let displaynames = userid_displayname
		.stream()
		.expect_ok()
		.ready_fold(0_usize, |count, (user_id, displayname): (&UserId, &str)| {
			let key = (user_id, ProfileFieldName::DisplayName.as_str());
			let value = displayname.to_owned();

			useridprofilekey_value.put(key, Json(value));

			count.saturating_add(1)
		})
		.await;

	let avatar_urls = userid_avatarurl
		.stream()
		.expect_ok()
		.ready_fold(0_usize, |count, (user_id, avatar_url): (&UserId, &str)| {
			let key = (user_id, ProfileFieldName::AvatarUrl.as_str());
			let value = avatar_url.to_owned();

			useridprofilekey_value.put(key, Json(value));

			count.saturating_add(1)
		})
		.await;

	let blurhashes = userid_blurhash
		.stream()
		.expect_ok()
		.ready_fold(0_usize, |count, (user_id, blurhash): (&UserId, &str)| {
			let key = (user_id, "xyz.amorgan.blurhash");
			let value = blurhash.to_owned();

			useridprofilekey_value.put(key, Json(value));

			count.saturating_add(1)
		})
		.await;

	let fixed_strings = useridprofilekey_value
		.raw_stream()
		.expect_ok()
		.ready_fold(0_usize, |count, (key, value)| {
			if serde_json::from_slice::<IgnoredAny>(value).is_err() {
				let Ok(string) = str::from_utf8(value) else {
					warn!("Non-UTF8 data in profile value: {key:?} => {value:?}");
					useridprofilekey_value.remove(key);
					return count;
				};
				useridprofilekey_value.raw_put(key, Json(string));
				return count.saturating_add(1);
			}

			count
		})
		.await;

	drop(cork);
	info!(%displaynames, %avatar_urls, %blurhashes, %fixed_strings, "Relocated profile keys into useridprofilekey_value");

	db["global"].insert(b"migrate_profile_keys_to_useridprofilekey", []);
	useridprofilekey_value.sort()
}
