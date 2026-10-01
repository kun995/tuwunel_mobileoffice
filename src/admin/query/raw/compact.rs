use futures::StreamExt;
use tokio::time::Instant;
use tuwunel_core::{
	Result, is_zero,
	utils::stream::{IterStream, TryParallelExt},
};
use tuwunel_database::compact::Options;

use super::with_maps_or;
use crate::admin_command;

#[admin_command]
pub(super) async fn raw_compact(
	&self,
	maps: Option<Vec<String>>,
	start: Option<String>,
	stop: Option<String>,
	from: Option<usize>,
	into: Option<usize>,
	parallelism: Option<usize>,
	exhaustive: bool,
) -> Result {
	let maps = with_maps_or(maps.as_deref(), self.services)?;

	let range = (
		start
			.as_ref()
			.map(String::as_bytes)
			.map(Into::into),
		stop.as_ref()
			.map(String::as_bytes)
			.map(Into::into),
	);

	let options = Options {
		range,
		level: (from, into),
		exclusive: parallelism.is_some_and(is_zero!()),
		exhaustive,
	};

	let runtime = self.services.server.runtime().clone();
	let parallelism = parallelism.unwrap_or(1);
	let results = maps
		.into_iter()
		.try_stream()
		.paralleln_and_then(runtime, parallelism, move |map| {
			map.compact_blocking(options.clone())?;
			Ok(map.name().to_owned())
		})
		.collect::<Vec<_>>();

	let timer = Instant::now();
	let results = results.await;
	let query_time = timer.elapsed();
	write!(self, "Jobs completed in {query_time:?}:\n\n```rs\n{results:#?}\n```").await
}
