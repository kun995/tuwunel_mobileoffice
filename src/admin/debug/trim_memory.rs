use tuwunel_core::{Result, alloc::trim, err};

use crate::admin_command;

#[admin_command]
pub(super) async fn trim_memory(&self) -> Result {
	trim().map_err(|error| err!("mallctl: {error}"))?;

	writeln!(self, "done").await
}
