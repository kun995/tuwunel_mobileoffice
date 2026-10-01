mod last;
mod pdus;

use clap::Subcommand;
use ruma::OwnedRoomOrAliasId;
use tuwunel_core::Result;

use crate::admin_command_dispatch;

#[admin_command_dispatch]
#[derive(Debug, Subcommand)]
/// Query tables from database
pub(crate) enum RoomTimelineCommand {
	Pdus {
		room_id: OwnedRoomOrAliasId,

		from: Option<String>,

		#[arg(short, long)]
		limit: Option<usize>,
	},

	Last {
		room_id: OwnedRoomOrAliasId,
	},
}
