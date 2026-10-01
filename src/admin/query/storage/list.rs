use futures::{FutureExt, TryStreamExt};
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn query_storage_list(
	&self,
	provider: Option<String>,
	prefix: Option<String>,
) -> Result {
	let id = provider.as_deref().unwrap_or_default();
	let provider = self.services.storage.provider(id)?;

	provider
		.list(prefix.as_deref())
		.try_for_each(|meta| writeln!(&self, "{meta:?}"))
		.boxed()
		.await
}
