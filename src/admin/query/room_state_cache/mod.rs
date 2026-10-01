mod active_local_users_in_room;
mod get_invite_count;
mod get_left_count;
mod invite_state;
mod local_users_in_room;
mod room_invited_count;
mod room_joined_count;
mod room_members;
mod room_members_invited;
mod room_servers;
mod room_user_once_joined;
mod rooms_invited;
mod rooms_joined;
mod rooms_left;
mod server_in_room;
mod server_rooms;
mod user_memberships;

use clap::Subcommand;
use ruma::{OwnedRoomId, OwnedServerName, OwnedUserId};
use tuwunel_core::Result;

use crate::admin_command_dispatch;

#[admin_command_dispatch]
#[derive(Debug, Subcommand)]
pub(crate) enum RoomStateCacheCommand {
	ServerInRoom {
		server: OwnedServerName,
		room_id: OwnedRoomId,
	},

	RoomServers {
		room_id: OwnedRoomId,
	},

	ServerRooms {
		server: OwnedServerName,
	},

	RoomMembers {
		room_id: OwnedRoomId,
	},

	LocalUsersInRoom {
		room_id: OwnedRoomId,
	},

	ActiveLocalUsersInRoom {
		room_id: OwnedRoomId,
	},

	RoomJoinedCount {
		room_id: OwnedRoomId,
	},

	RoomInvitedCount {
		room_id: OwnedRoomId,
	},

	RoomUserOnceJoined {
		room_id: OwnedRoomId,
	},

	RoomMembersInvited {
		room_id: OwnedRoomId,
	},

	GetInviteCount {
		room_id: OwnedRoomId,
		user_id: OwnedUserId,
	},

	GetLeftCount {
		room_id: OwnedRoomId,
		user_id: OwnedUserId,
	},

	RoomsJoined {
		user_id: OwnedUserId,
	},

	RoomsLeft {
		user_id: OwnedUserId,
	},

	RoomsInvited {
		user_id: OwnedUserId,
	},

	InviteState {
		user_id: OwnedUserId,
		room_id: OwnedRoomId,
	},

	UserMemberships {
		user_id: OwnedUserId,
	},
}
