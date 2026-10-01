mod account_data_get;
mod changes_since;

use clap::Subcommand;
use ruma::{OwnedRoomId, OwnedUserId};
use tuwunel_core::Result;

use crate::admin_command_dispatch;

#[admin_command_dispatch]
#[derive(Debug, Subcommand)]
/// All the getters and iterators from src/service/account_data/
pub(crate) enum AccountDataCommand {
	/// - Returns all changes to the account data that happened after `since`.
	ChangesSince {
		/// Full user ID
		user_id: OwnedUserId,
		/// UNIX timestamp since (u64)
		since: u64,
		/// Optional room ID of the account data
		room_id: Option<OwnedRoomId>,
	},

	/// - Searches the account data for a specific kind.
	AccountDataGet {
		/// Full user ID
		user_id: OwnedUserId,
		/// Account data event type
		kind: String,
		/// Optional room ID of the account data
		room_id: Option<OwnedRoomId>,
	},
}
