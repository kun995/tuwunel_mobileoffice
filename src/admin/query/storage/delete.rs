use futures::{FutureExt, StreamExt};
use tuwunel_core::{
	Result,
	utils::{result::LogErr, stream::IterStream},
};

use crate::admin_command;

#[admin_command]
pub(super) async fn query_storage_delete(
	&self,
	provider: Option<String>,
	src: Vec<String>,
	verbose: bool,
) -> Result {
	let id = provider.as_deref().unwrap_or_default();
	let provider = self.services.storage.provider(id)?;

	provider
		.delete(src.into_iter().stream())
		.for_each(async |result| {
			match result {
				| Ok(_) if !verbose => Ok(()),
				| Ok(path) => write!(self, "deleted {path}\n").await,
				| Err(e) => write!(self, "failed: {e:?}\n").await,
			}
			.log_err()
			.ok();
		})
		.map(Ok)
		.await
}
