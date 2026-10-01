use std::time::SystemTime;

use tuwunel_core::{Result, utils};

use crate::admin_command;

#[admin_command]
pub(super) async fn time(&self) -> Result {
	let now = SystemTime::now();
	let now = utils::time::format(now, "%+");

	self.write_str(&now).await
}
