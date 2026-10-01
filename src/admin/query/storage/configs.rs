use futures::TryStreamExt;
use tuwunel_core::{Result, utils::stream::IterStream};

use crate::admin_command;

#[admin_command]
pub(super) async fn query_storage_configs(&self) -> Result {
	self.services
		.storage
		.configs(None)
		.try_stream()
		.try_for_each(|(id, conf)| writeln!(&self, "\n`{id:?}`\n{conf:#?}"))
		.await
}
