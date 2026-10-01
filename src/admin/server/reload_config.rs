use std::path::PathBuf;

use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn reload_config(&self, path: Option<PathBuf>) -> Result {
	let path = path.as_deref().into_iter();
	self.services.config.reload(path)?;

	self.write_str("Successfully reconfigured.").await
}
