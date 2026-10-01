mod list;
mod remove;
mod set;
mod which;

use clap::Subcommand;
use ruma::{OwnedRoomAliasId, OwnedRoomId};
use tuwunel_core::Result;
use tuwunel_service::Services;

use crate::admin_command_dispatch;

#[admin_command_dispatch(handler_prefix = "alias")]
#[derive(Debug, Subcommand)]
pub(crate) enum RoomAliasCommand {
	/// - Make an alias point to a room.
	Set {
		#[arg(short, long)]
		/// Set the alias even if a room is already using it
		force: bool,

		/// The room id to set the alias on
		room_id: OwnedRoomId,

		/// The alias localpart to use (`alias`, not `#alias:servername.tld`)
		room_alias_localpart: String,
	},

	/// - Remove a local alias
	Remove {
		/// The alias localpart to remove (`alias`, not `#alias:servername.tld`)
		room_alias_localpart: String,
	},

	/// - Show which room is using an alias
	Which {
		/// The alias localpart to look up (`alias`, not
		/// `#alias:servername.tld`)
		room_alias_localpart: String,
	},

	/// - List aliases currently being used
	List {
		/// If set, only list the aliases for this room
		room_id: Option<OwnedRoomId>,
	},
}

fn parse_alias_from_localpart(
	services: &Services,
	room_alias_localpart: &String,
) -> Result<OwnedRoomAliasId> {
	let room_alias_str = format!("#{}:{}", room_alias_localpart, services.globals.server_name());

	Ok(OwnedRoomAliasId::try_from(room_alias_str)?)
}
