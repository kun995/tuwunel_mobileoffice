use ruma::{Mxc, OwnedMxcUri};
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn get_file_info(&self, mxc: OwnedMxcUri) -> Result {
	let mxc: Mxc<'_> = mxc.as_str().try_into()?;
	let metadata = self.services.media.get_metadata(&mxc).await;

	write!(self, "```\n{metadata:#?}\n```").await
}
