use ruma::UserId;
use tuwunel_core::{
	Result, info,
	utils::{ReadyExt, stream::TryIgnore},
	warn,
};
use tuwunel_database::SEP;

use crate::Services;

pub(super) async fn upgrade_legacy_mediaid_user(services: &Services) -> Result {
	let db = &services.db;
	let cork = db.cork_and_sync();
	let mediaid_user = db["mediaid_user"].clone();

	warn!("Upgrading legacy mediaid_user keys to composite (mxc, user_id) layout");

	let (checked, upgraded, removed_invalid) = mediaid_user
		.raw_stream()
		.ignore_err()
		.ready_fold(
			(0_usize, 0_usize, 0_usize),
			|(mut checked, mut upgraded, mut removed_invalid), (raw_key, raw_val)| {
				checked = checked.saturating_add(1);

				let has_sep = raw_key.contains(&SEP);
				let user_id = str::from_utf8(raw_val)
					.ok()
					.and_then(|s| <&UserId>::try_from(s).ok());

				match (has_sep, user_id) {
					| (true, _) => {},
					| (false, None) => {
						warn!(
							?raw_key,
							?raw_val,
							"Legacy entry has unparsable user_id, removing"
						);

						mediaid_user.remove(raw_key);
						removed_invalid = removed_invalid.saturating_add(1);
					},
					| (false, Some(user_id)) => {
						let mut new_key = raw_key.to_vec();

						new_key.push(SEP);
						new_key.extend_from_slice(user_id.as_bytes());

						mediaid_user.put_raw(new_key, user_id.as_str());
						mediaid_user.remove(raw_key);

						upgraded = upgraded.saturating_add(1);
					},
				}

				(checked, upgraded, removed_invalid)
			},
		)
		.await;

	drop(cork);
	info!(
		%checked,
		%upgraded,
		%removed_invalid,
		"Upgraded legacy mediaid_user keys"
	);

	db["global"].insert(b"upgrade_legacy_mediaid_user", []);
	mediaid_user.sort()
}
