use tuwunel_core::Result;

use crate::admin_command;

#[cfg(tokio_unstable)]
#[admin_command]
pub(super) async fn runtime_metrics(&self) -> Result {
	let out = self
		.services
		.server
		.metrics
		.runtime_metrics()
		.map_or_else(
			|| "Runtime metrics are not available.".to_owned(),
			|metrics| {
				format!(
					"```rs\nnum_workers: {}\nnum_alive_tasks: {}\nglobal_queue_depth: {}\n```",
					metrics.num_workers(),
					metrics.num_alive_tasks(),
					metrics.global_queue_depth()
				)
			},
		);

	self.write_str(&out).await
}

#[cfg(not(tokio_unstable))]
#[admin_command]
pub(super) async fn runtime_metrics(&self) -> Result {
	self.write_str("Runtime metrics require building with `tokio_unstable`.")
		.await
}
