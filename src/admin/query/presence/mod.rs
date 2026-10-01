mod get_presence;
mod presence_since;

use clap::Subcommand;
use ruma::OwnedUserId;
use tuwunel_core::Result;

use crate::admin_command_dispatch;

#[admin_command_dispatch(handler_prefix = "presence")]
#[derive(Debug, Subcommand)]
/// All the getters and iterators from src/service/presence/
pub(crate) enum PresenceCommand {
	/// - Returns the latest presence event for the given user.
	GetPresence {
		/// Full user ID
		user_id: OwnedUserId,
	},

	/// - Iterator of the most recent presence updates that happened after the
	///   event with id `since`.
	PresenceSince {
		/// UNIX timestamp since (u64)
		since: u64,

		/// Upper-bound of since
		to: Option<u64>,
	},
}
