use tuwunel_core::Result;
use tuwunel_service::storage::CopyMode;

use crate::admin_command;

#[admin_command]
pub(super) async fn query_storage_copy(
	&self,
	provider: Option<String>,
	force: bool,
	src: String,
	dst: String,
) -> Result {
	let id = provider.as_deref().unwrap_or_default();
	let provider = self.services.storage.provider(id)?;
	let overwrite = force
		.then_some(CopyMode::Overwrite)
		.unwrap_or(CopyMode::Create);

	let result = provider.copy(&src, &dst, overwrite).await;

	write!(self, "{result:#?}\n").await
}
