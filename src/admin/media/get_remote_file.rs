use std::time::Duration;

use ruma::{Mxc, OwnedMxcUri, OwnedServerName};
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn get_remote_file(
	&self,
	mxc: OwnedMxcUri,
	server: Option<OwnedServerName>,
	timeout: u32,
) -> Result {
	let mxc: Mxc<'_> = mxc.as_str().try_into()?;
	let timeout = Duration::from_millis(timeout.into());
	let mut result = self
		.services
		.media
		.fetch_remote_content(&mxc, server.as_deref(), timeout)
		.await?
		.media;

	let len = result.content.len();
	result.content.clear();

	write!(self, "```\n{result:#?}\nreceived {len} bytes for file content.\n```").await
}
