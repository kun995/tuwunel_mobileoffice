mod short_event_id;
mod short_room_id;

use clap::Subcommand;
use ruma::{OwnedEventId, OwnedRoomOrAliasId};
use tuwunel_core::Result;

use crate::admin_command_dispatch;

#[admin_command_dispatch]
#[derive(Debug, Subcommand)]
/// Query tables from database
pub(crate) enum ShortCommand {
	ShortEventId {
		event_id: OwnedEventId,
	},

	ShortRoomId {
		room_id: OwnedRoomOrAliasId,
	},
}
