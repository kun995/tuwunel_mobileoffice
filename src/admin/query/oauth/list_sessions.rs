use futures::{StreamExt, TryStreamExt};
use ruma::OwnedUserId;
use tuwunel_core::{Result, utils::stream::ReadyExt};

use crate::admin_command;

#[admin_command]
pub(super) async fn oauth_list_sessions(&self, user_id: Option<OwnedUserId>) -> Result {
	if let Some(user_id) = user_id.as_deref() {
		return self
			.services
			.oauth
			.sessions
			.get_sess_id_by_user(user_id)
			.map_ok(|id| format!("{id}\n"))
			.try_for_each(async |id: String| self.write_str(&id).await)
			.await;
	}

	self.services
		.oauth
		.sessions
		.stream()
		.ready_filter_map(|sess| sess.sess_id)
		.map(|sess_id| format!("{sess_id:?}\n"))
		.for_each(async |id: String| {
			self.write_str(&id).await.ok();
		})
		.await;

	Ok(())
}
