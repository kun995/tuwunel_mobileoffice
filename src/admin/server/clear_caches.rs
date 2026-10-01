use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn clear_caches(&self) -> Result {
	self.services.clear_cache().await;

	self.write_str("Done.").await
}
