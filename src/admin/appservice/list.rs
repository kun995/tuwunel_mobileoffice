use futures::StreamExt;
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn appservice_list(&self) -> Result {
	let appservices: Vec<_> = self
		.services
		.appservice
		.iter_ids()
		.collect()
		.await;

	let len = appservices.len();
	let list = appservices.join(", ");
	write!(self, "Appservices ({len}): {list}").await
}
