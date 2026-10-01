use tuwunel_core::{Result, warn};

use crate::admin_command;

#[admin_command]
pub(super) async fn shutdown(&self) -> Result {
	warn!("shutdown command");
	self.services.server.shutdown()?;

	self.write_str("Shutting down server...").await
}
