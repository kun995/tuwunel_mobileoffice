use tuwunel_core::{Result, tokio_metrics::TaskMonitor};

use crate::admin_command;

#[admin_command]
pub(super) async fn task_metrics(&self) -> Result {
	let out = self
		.services
		.server
		.metrics
		.task_metrics()
		.map(TaskMonitor::cumulative)
		.map_or_else(
			|| "Task metrics are not available.".to_owned(),
			|metrics| format!("```rs\n{metrics:#?}\n```"),
		);

	self.write_str(&out).await
}
