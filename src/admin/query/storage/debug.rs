use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn query_storage_debug(&self, provider: String) -> Result {
	let provider = self.services.storage.provider(&provider)?;

	write!(self, "{provider:#?}\n").await
}
