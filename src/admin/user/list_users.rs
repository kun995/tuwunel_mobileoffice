use futures::StreamExt;
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn list_users(&self) -> Result {
	let users: Vec<_> = self
		.services
		.users
		.list_local_users()
		.map(ToString::to_string)
		.collect()
		.await;

	write!(self, "Found {} local user account(s):\n```\n", users.len()).await?;
	for user in &users {
		writeln!(self, "{user}").await?;
	}
	write!(self, "```").await
}
