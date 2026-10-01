use futures::TryStreamExt;
use tuwunel_core::{Result, utils::stream::IterStream};
use tuwunel_service::oauth::Provider;

use crate::admin_command;

#[admin_command]
pub(super) async fn oauth_list_providers(&self) -> Result {
	self.services
		.config
		.identity_provider
		.values()
		.try_stream()
		.map_ok(Provider::id)
		.map_ok(|id| format!("{id}\n"))
		.try_for_each(async |id| self.write_str(&id).await)
		.await
}
