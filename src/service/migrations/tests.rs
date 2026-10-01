use ruma::server_name;

use super::local_user_id;

#[test]
fn localpart_becomes_a_local_user_id() {
	let user_id = local_user_id("alice", server_name!("example.org"))
		.expect("a plain localpart composes a user id");

	assert_eq!(user_id.as_str(), "@alice:example.org");
}

#[test]
fn localpart_past_the_inline_budget_survives() {
	let localpart = "a".repeat(64);
	let user_id = local_user_id(&localpart, server_name!("example.org"))
		.expect("a spilled buffer still composes a user id");

	assert_eq!(user_id.localpart(), localpart);
}

#[test]
fn unusable_localpart_is_skipped() {
	assert!(local_user_id("alice\0bob", server_name!("example.org")).is_none());
}
