mod address_in_use;
mod get_bindings;
mod user_id_for_email;

use clap::Subcommand;
use ruma::OwnedUserId;
use tuwunel_core::Result;

use crate::admin_command_dispatch;

#[admin_command_dispatch]
#[derive(Debug, Subcommand)]
/// All the getters from src/service/threepid/
pub(crate) enum ThreepidCommand {
	GetBindings {
		user_id: OwnedUserId,
	},

	UserIdForEmail {
		address: String,
	},

	AddressInUse {
		address: String,
	},
}
