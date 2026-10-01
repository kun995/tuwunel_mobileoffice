mod get_pushers;
mod remove_pusher;

use clap::Subcommand;
use ruma::OwnedUserId;
use tuwunel_core::Result;

use crate::admin_command_dispatch;

#[admin_command_dispatch]
#[derive(Debug, Subcommand)]
pub(crate) enum PusherCommand {
	/// - Returns all the pushers for the user.
	GetPushers {
		/// Full user ID
		user_id: OwnedUserId,
	},

	/// - Manually delete a pusher for a user.
	RemovePusher {
		/// Full user ID
		user_id: OwnedUserId,

		/// Pushkey
		pushkey: String,
	},
}
