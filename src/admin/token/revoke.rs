use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn revoke(&self, token: String) -> Result {
	self.services
		.registration_tokens
		.revoke_token(&token)
		.await?;

	self.write_str("Token revoked successfully.")
		.await
}
