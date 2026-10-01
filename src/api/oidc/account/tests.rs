use super::{ACCOUNT_MANAGEMENT_ACTIONS_SUPPORTED, normalize_account_action};

#[test]
fn stable_names_map_to_aliases() {
	assert_eq!(normalize_account_action("org.matrix.devices_list"), "org.matrix.sessions_list");
	assert_eq!(normalize_account_action("org.matrix.device_view"), "org.matrix.session_view");
	assert_eq!(normalize_account_action("org.matrix.device_delete"), "org.matrix.session_end");
}

#[test]
fn aliases_and_others_pass_through() {
	for action in [
		"org.matrix.sessions_list",
		"org.matrix.session_view",
		"org.matrix.session_end",
		"org.matrix.profile",
		"org.matrix.account_deactivate",
		"org.matrix.cross_signing_reset",
	] {
		assert_eq!(normalize_account_action(action), action);
	}
}

#[test]
fn stable_actions_are_advertised() {
	for action in [
		"org.matrix.profile",
		"org.matrix.devices_list",
		"org.matrix.device_view",
		"org.matrix.device_delete",
		"org.matrix.account_deactivate",
		"org.matrix.cross_signing_reset",
	] {
		assert!(ACCOUNT_MANAGEMENT_ACTIONS_SUPPORTED.contains(&action));
	}
}
