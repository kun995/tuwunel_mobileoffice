use std::collections::HashSet;

use futures::{TryStreamExt, future::try_join};
use tuwunel_core::{
	Result,
	utils::stream::{IterStream, TryBroadbandExt},
};

use crate::admin_command;

#[admin_command]
pub(super) async fn query_storage_sync(&self, src: String, dst: String) -> Result {
	let src_p = self.services.storage.provider(&src)?;

	let dst_p = self.services.storage.provider(&dst)?;

	let src_objects = src_p
		.list(None)
		.map_ok(|meta| meta.location)
		.try_collect::<HashSet<_>>();

	let dst_objects = dst_p
		.list(None)
		.map_ok(|meta| meta.location)
		.try_collect::<HashSet<_>>();

	let (src_objects, dst_objects) = try_join(src_objects, dst_objects).await?;

	let copied = src_objects
		.difference(&dst_objects)
		.try_stream()
		.broadn_and_then(2, async |item| {
			let data = src_p.get(item.as_ref()).await?;
			dst_p.put_one(item.as_ref(), data).await
		})
		.try_fold(0_usize, async |count, _| Ok(count.saturating_add(1)))
		.await?;

	writeln!(self, "Copied {copied} objects from {src:?} to {dst:?}.").await
}
