use tuwunel_core::Result;
use tuwunel_service::oauth::SessionId;

use crate::admin_command;

#[admin_command]
pub(super) async fn oauth_token_info(&self, id: SessionId) -> Result {
	let session = self.services.oauth.sessions.get(&id).await?;

	let provider = self
		.services
		.oauth
		.sessions
		.provider(&session)
		.await?;

	let tokeninfo = self
		.services
		.oauth
		.request_tokeninfo((&provider, &session))
		.await?;

	write!(self, "{tokeninfo:#?}\n").await
}
