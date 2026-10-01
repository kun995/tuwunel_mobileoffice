use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn show_config(&self) -> Result {
	write!(self, "{}", *self.services.server.config).await
}
