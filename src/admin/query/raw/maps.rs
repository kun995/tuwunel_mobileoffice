use tuwunel_core::{Result, at};

use crate::admin_command;

#[admin_command]
pub(super) async fn raw_maps(&self) -> Result {
	let list: Vec<_> = self
		.services
		.db
		.iter()
		.map(at!(0))
		.copied()
		.collect();

	write!(self, "{list:#?}").await
}
