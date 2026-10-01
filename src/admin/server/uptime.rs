use tuwunel_core::{Result, utils::time};

use crate::admin_command;

#[admin_command]
pub(super) async fn uptime(&self) -> Result {
	let elapsed = self
		.services
		.server
		.started
		.elapsed()
		.expect("standard duration");

	let result = time::pretty(elapsed);
	write!(self, "{result}.").await
}
