use std::time::Duration;

use ruma::{Mxc, OwnedMxcUri, OwnedServerName};
use tuwunel_core::Result;
use tuwunel_service::media::{Animate, Dim};

use crate::admin_command;

#[admin_command]
pub(super) async fn get_remote_thumbnail(
	&self,
	mxc: OwnedMxcUri,
	server: Option<OwnedServerName>,
	timeout: u32,
	width: u32,
	height: u32,
) -> Result {
	let mxc: Mxc<'_> = mxc.as_str().try_into()?;
	let timeout = Duration::from_millis(timeout.into());
	let dim = Dim::new(width, height, None);
	let mut result = self
		.services
		.media
		.fetch_remote_thumbnail(&mxc, server.as_deref(), timeout, &dim, Animate::Allowed)
		.await?
		.media;

	let len = result.content.len();
	result.content.clear();

	write!(self, "```\n{result:#?}\nreceived {len} bytes for file content.\n```").await
}
