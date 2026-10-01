use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn task_interval(&self) -> Result {
	let out = self
		.services
		.server
		.metrics
		.task_interval()
		.map_or_else(
			|| "Task metrics are not available.".to_owned(),
			|metrics| format!("```rs\n{metrics:#?}\n```"),
		);

	self.write_str(&out).await
}
