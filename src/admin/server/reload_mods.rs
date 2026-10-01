use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn reload_mods(&self) -> Result {
	self.services.server.reload()?;

	self.write_str("Reloading server...").await
}
