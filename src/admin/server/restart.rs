use tuwunel_core::{Err, Result, utils::sys::current_exe_deleted};

use crate::admin_command;

#[admin_command]
pub(super) async fn restart(&self, force: bool) -> Result {
	if !force && current_exe_deleted() {
		return Err!(
			"The server cannot be restarted because the executable changed. If this is expected \
			 use --force to override."
		);
	}

	self.services.server.restart()?;

	self.write_str("Restarting server...").await
}
