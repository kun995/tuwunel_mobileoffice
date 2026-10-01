use futures::TryStreamExt;
use ruma::OwnedUserId;
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn oauth_show_user(&self, user_id: OwnedUserId) -> Result {
	self.services
		.oauth
		.sessions
		.get_sess_id_by_user(&user_id)
		.try_for_each(async |id| {
			let session = self.services.oauth.sessions.get(&id).await?;

			write!(self, "{session:#?}\n").await
		})
		.await
}
