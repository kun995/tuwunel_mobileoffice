use std::collections::HashSet;

use futures::{TryStreamExt, future::try_join};
use tuwunel_core::{Result, utils::stream::IterStream};

use crate::admin_command;

#[admin_command]
pub(super) async fn query_storage_duplicates(
	&self,
	provider_a: String,
	provider_b: String,
) -> Result {
	let a = self
		.services
		.storage
		.provider(&provider_a)?
		.list(None)
		.map_ok(|meta| meta.location)
		.try_collect::<HashSet<_>>();

	let b = self
		.services
		.storage
		.provider(&provider_b)?
		.list(None)
		.map_ok(|meta| meta.location)
		.try_collect::<HashSet<_>>();

	let (a, b) = try_join(a, b).await?;
	a.intersection(&b)
		.try_stream()
		.try_for_each(|item| writeln!(&self, "{item}"))
		.await
}
