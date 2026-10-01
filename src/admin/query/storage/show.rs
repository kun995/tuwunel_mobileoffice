use tuwunel_core::{Result, utils::string::SplitInfallible};

use crate::admin_command;

#[admin_command]
pub(super) async fn query_storage_show(&self, provider: Option<String>, src: String) -> Result {
	let (prefix, src) = src.as_str().split_once_infallible("//");
	let id = provider.as_deref().unwrap_or(prefix);

	let provider = self.services.storage.provider(id)?;
	let meta = provider.head(src).await?;

	write!(self, "{meta:#?}\n").await
}
