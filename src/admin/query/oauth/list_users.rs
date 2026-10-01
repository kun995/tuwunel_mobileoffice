use futures::{StreamExt, TryStreamExt};
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn oauth_list_users(&self) -> Result {
	self.services
		.oauth
		.sessions
		.users()
		.map(|id| format!("{id}\n"))
		.map(Ok)
		.try_for_each(async |id: String| self.write_str(&id).await)
		.await
}
