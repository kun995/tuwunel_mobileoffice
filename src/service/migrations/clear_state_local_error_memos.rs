use tuwunel_core::{Result, warn};

use super::CLEAR_STATE_LOCAL_ERROR_MEMOS;
use crate::Services;

/// Clears local state-resolution memo rows produced by older auth semantics.
///
/// Authoritative state and compressed state groups remain intact. The marker
/// is written only after the derived memo map has been cleared successfully.
#[tracing::instrument(level = "debug", skip_all)]
pub(super) async fn clear_state_local_error_memos(services: &Services) -> Result {
	let db = &services.db;
	let memos = db["eventid_resolvedstate"].clone();

	warn!("Clearing local state-resolution memo rows");
	memos.clear().await;

	db["global"].insert(CLEAR_STATE_LOCAL_ERROR_MEMOS, []);
	memos.sort()
}
