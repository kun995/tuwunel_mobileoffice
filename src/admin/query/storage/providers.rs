use futures::TryStreamExt;
use tuwunel_core::{Result, utils::stream::IterStream};

use crate::admin_command;

#[admin_command]
pub(super) async fn query_storage_providers(&self) -> Result {
	self.services
		.storage
		.providers()
		.try_stream()
		.try_for_each(|conf| writeln!(&self, "\n`{:?}`\n{conf:#?}", conf.name))
		.await
}
