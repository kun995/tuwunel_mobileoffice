use ruma::{MxcUri, UserId};
use tuwunel_core::{
	Result, info,
	utils::{ReadyExt, stream::TryExpect},
	warn,
};

use crate::Services;

pub(super) async fn remove_remote_media_userid(services: &Services) -> Result {
	let db = &services.db;
	let cork = db.cork_and_sync();
	let mediaid_user = db["mediaid_user"].clone();

	warn!("Removing stored user id for remote media");

	let (checked, removed_remote, removed_invalid) = mediaid_user
		.keys()
		.expect_ok()
		.ready_fold(
			(0, 0, 0),
			|(mut checked, mut removed_remote, mut removed_invalid): (usize, usize, usize),
			 (mxc_uri, user_id): (&MxcUri, &UserId)| {
				checked = checked.saturating_add(1);

				let Ok(mxc) = mxc_uri.parts() else {
					warn!(?mxc_uri, "Invalid MXC URL, removing it");

					mediaid_user.del((mxc_uri, user_id));

					removed_invalid = removed_invalid.saturating_add(1);

					return (checked, removed_remote, removed_invalid);
				};

				if !services.globals.server_is_ours(mxc.server_name) {
					mediaid_user.del((mxc_uri, user_id));

					removed_remote = removed_remote.saturating_add(1);

					return (checked, removed_remote, removed_invalid);
				}

				(checked, removed_remote, removed_invalid)
			},
		)
		.await;

	drop(cork);
	info!(
		%checked,
		%removed_remote,
		%removed_invalid,
		"Removed stored user id for remote media"
	);

	db["global"].insert(b"remove_remote_media_userid", []);
	mediaid_user.sort()
}
