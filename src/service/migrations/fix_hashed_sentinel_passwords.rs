use tuwunel_core::{
	Result, debug, info,
	utils::{ReadyExt, hash::verify_password, stream::TryExpect},
	warn,
};

use crate::Services;

pub(super) async fn fix_hashed_sentinel_passwords(services: &Services) -> Result {
	const PASSWORD_SENTINEL: &str = "*";

	if services.config.identity_provider.is_empty() {
		debug!("Skipping sentinel password migration since no SSO IdP configured.");
		return Ok(());
	}

	let db = &services.db;
	let cork = db.cork_and_sync();
	let userid_password = db["userid_password"].clone();

	warn!(sentinel = %PASSWORD_SENTINEL, "Fixing occurrences of hashed sentinel passwords");

	let (checked, good, bad) = userid_password
		.stream()
		.expect_ok()
		.ready_fold(
			(0, 0, 0),
			|(mut checked, mut good, mut bad): (usize, usize, usize),
			 (key, val): (&str, &str)| {
				let good_sentinel = val == PASSWORD_SENTINEL;
				let bad_sentinel = !val.is_empty()
					&& !good_sentinel
					&& verify_password(PASSWORD_SENTINEL, val).is_ok();

				checked = checked.saturating_add(usize::from(true));
				good = good.saturating_add(usize::from(good_sentinel));
				bad = bad.saturating_add(usize::from(bad_sentinel));

				if bad_sentinel {
					userid_password.insert(key, PASSWORD_SENTINEL);
				}

				(checked, good, bad)
			},
		)
		.await;

	drop(cork);
	info!(?checked, ?good, ?bad, "Fixed any occurrences of hashed sentinel passwords");

	db["global"].insert(b"fix_hashed_sentinel_passwords", []);
	userid_password.sort()
}
