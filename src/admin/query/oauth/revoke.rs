use tuwunel_core::{
	Result,
	either::{Left, Right},
};

use super::SessionOrUserId;
use crate::admin_command;

#[admin_command]
pub(super) async fn oauth_revoke(&self, id: SessionOrUserId) -> Result {
	match id {
		| Left(sess_id) => {
			let session = self.services.oauth.sessions.get(&sess_id).await?;

			let provider = self
				.services
				.oauth
				.sessions
				.provider(&session)
				.await?;

			self.services
				.oauth
				.revoke_token((&provider, &session))
				.await
				.ok();
		},
		| Right(user_id) =>
			self.services
				.oauth
				.revoke_user_tokens(&user_id)
				.await,
	}

	self.write_str("revoked").await
}
