mod current_count;
mod database_version;
mod signing_keys_for;

use clap::Subcommand;
use ruma::OwnedServerName;
use tuwunel_core::Result;

use crate::admin_command_dispatch;

#[admin_command_dispatch(handler_prefix = "globals")]
#[derive(Debug, Subcommand)]
/// All the getters and iterators from src/service/globals/
pub(crate) enum GlobalsCommand {
	DatabaseVersion,

	CurrentCount,

	/// - This returns an empty `Ok(BTreeMap<..>)` when there are no keys found
	///   for the server.
	SigningKeysFor {
		origin: OwnedServerName,
	},
}
