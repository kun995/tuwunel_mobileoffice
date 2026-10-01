use ruma::{Mxc, OwnedServerName};
use tuwunel_core::{Err, Result, error, trace, warn};

use crate::admin_command;

#[admin_command]
pub(super) async fn delete_all_from_server(
	&self,
	server_name: OwnedServerName,
	yes_i_want_to_delete_local_media: bool,
) -> Result {
	if server_name == self.services.globals.server_name() && !yes_i_want_to_delete_local_media {
		return Err!("This command only works for remote media by default.",);
	}

	let Ok(all_mxcs) = self
		.services
		.media
		.get_all_mxcs()
		.await
		.inspect_err(|e| error!("Failed to get MXC URIs from our database: {e}"))
	else {
		return Err!("Failed to get MXC URIs from our database",);
	};

	let mut deleted_count: usize = 0;

	for mxc in all_mxcs {
		let Ok(mxc_server_name) = mxc.server_name().inspect_err(|e| {
			warn!(
				"Failed to parse MXC {mxc} server name from database, ignoring error and \
				 skipping: {e}"
			);
		}) else {
			continue;
		};

		if mxc_server_name != server_name {
			trace!("skipping MXC URI {mxc}");
			continue;
		}

		let mxc: Mxc<'_> = mxc.as_str().try_into()?;

		match self.services.media.delete(&mxc).await {
			| Ok(()) => {
				deleted_count = deleted_count.saturating_add(1);
			},
			| Err(e) => {
				warn!("Failed to delete {mxc}, ignoring error and skipping: {e}");
			},
		}
	}

	write!(self, "Deleted {deleted_count} total files.").await
}
