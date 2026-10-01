use tuwunel_core::Result;
use tuwunel_service::oauth::ProviderId;

use crate::admin_command;

#[admin_command]
pub(super) async fn oauth_show_provider(&self, id: ProviderId, config: bool) -> Result {
	if config {
		let config = self.services.oauth.providers.get_config(&id)?;

		write!(self, "{config:#?}\n").await?;
		return Ok(());
	}

	let provider = self.services.oauth.providers.get(&id).await?;

	write!(self, "{provider:#?}\n").await
}
