mod all_local_aliases;
mod local_aliases_for_room;
mod resolve_alias;
mod resolve_local_alias;

use clap::Subcommand;
use ruma::{OwnedRoomAliasId, OwnedRoomId};
use tuwunel_core::Result;

use crate::admin_command_dispatch;

#[admin_command_dispatch]
#[derive(Debug, Subcommand)]
/// All the getters and iterators from src/service/rooms/alias/
pub(crate) enum RoomAliasCommand {
	/// - Resolve any local or remote alias.
	ResolveAlias {
		/// Full room alias
		alias: OwnedRoomAliasId,
	},

	/// - Resolve an alias on this server.
	ResolveLocalAlias {
		/// Full room alias
		alias: OwnedRoomAliasId,
	},

	/// - Iterator of all our local room aliases for the room ID
	LocalAliasesForRoom {
		/// Full room ID
		room_id: OwnedRoomId,
	},

	/// - Iterator of all our local aliases in our database with their room IDs
	AllLocalAliases,
}
