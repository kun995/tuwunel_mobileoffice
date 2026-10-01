use std::sync::Arc;

use futures::{Stream, StreamExt};
use ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId};
use tuwunel_core::utils::stream::ReadyExt;
use tuwunel_core::{Result, err, implement, utils::stream::TryIgnore};
use tuwunel_database::Map;

pub struct Service {
	db: Data,
}

struct Data {
	/// workspaceId bytes → spaceRoomId bytes
	workspaceid_spaceroomid: Arc<Map>,
	/// roomId bytes → workspaceId bytes
	roomid_workspaceid: Arc<Map>,
	/// composite key: "{workspaceId}\xFF{roomId}" → [] (set membership index)
	workspaceid_roomids: Arc<Map>,
	/// composite key: "{userId}\xFF{workspaceId}" → [] (set membership index)
	userid_workspaceids: Arc<Map>,
	/// composite key: "{workspaceId}\xFF{userId}" → [] (set membership index)
	workspaceid_userids: Arc<Map>,
}

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				workspaceid_spaceroomid: args.db["workspaceid_spaceroomid"].clone(),
				roomid_workspaceid: args.db["roomid_workspaceid"].clone(),
				workspaceid_roomids: args.db["workspaceid_roomids"].clone(),
				userid_workspaceids: args.db["userid_workspaceids"].clone(),
				workspaceid_userids: args.db["workspaceid_userids"].clone(),
			},
		}))
	}

	fn name(&self) -> &str {
		crate::service::make_name(std::module_path!())
	}
}

/// Build composite key: workspace_id bytes + 0xFF separator + room_id bytes
fn composite_key(workspace_id: &str, room_id: &RoomId) -> Vec<u8> {
	let mut key = Vec::with_capacity(workspace_id.len() + 1 + room_id.as_bytes().len());
	key.extend_from_slice(workspace_id.as_bytes());
	key.push(0xFF);
	key.extend_from_slice(room_id.as_bytes());
	key
}

/// Prefix for scanning all rooms of a workspace: workspace_id bytes + 0xFF
fn workspace_prefix(workspace_id: &str) -> Vec<u8> {
	let mut prefix = Vec::with_capacity(workspace_id.len() + 1);
	prefix.extend_from_slice(workspace_id.as_bytes());
	prefix.push(0xFF);
	prefix
}

fn user_workspace_key(user_id: &UserId, workspace_id: &str) -> Vec<u8> {
	let mut key = Vec::with_capacity(user_id.as_bytes().len() + 1 + workspace_id.len());
	key.extend_from_slice(user_id.as_bytes());
	key.push(0xFF);
	key.extend_from_slice(workspace_id.as_bytes());
	key
}

fn user_workspace_prefix(user_id: &UserId) -> Vec<u8> {
	let mut prefix = Vec::with_capacity(user_id.as_bytes().len() + 1);
	prefix.extend_from_slice(user_id.as_bytes());
	prefix.push(0xFF);
	prefix
}

fn workspace_user_key(workspace_id: &str, user_id: &UserId) -> Vec<u8> {
	let mut key = Vec::with_capacity(workspace_id.len() + 1 + user_id.as_bytes().len());
	key.extend_from_slice(workspace_id.as_bytes());
	key.push(0xFF);
	key.extend_from_slice(user_id.as_bytes());
	key
}

fn workspace_user_prefix(workspace_id: &str) -> Vec<u8> {
	let mut prefix = Vec::with_capacity(workspace_id.len() + 1);
	prefix.extend_from_slice(workspace_id.as_bytes());
	prefix.push(0xFF);
	prefix
}

// ─── Space / Workspace mapping ────────────────────────────────────────────────

/// Lưu mapping workspaceId → spaceRoomId (khi tạo Space/workspace).
#[implement(Service)]
pub fn set_space_room_id(&self, workspace_id: &str, space_room_id: &RoomId) {
	self.db
		.workspaceid_spaceroomid
		.raw_put(workspace_id.as_bytes(), space_room_id.as_bytes());
	self.db
		.roomid_workspaceid
		.raw_put(space_room_id.as_bytes(), workspace_id.as_bytes());
}

/// Lookup: workspaceId → spaceRoomId
#[implement(Service)]
pub async fn get_space_room_id(&self, workspace_id: &str) -> Result<OwnedRoomId> {
	let bytes = self
		.db
		.workspaceid_spaceroomid
		.get(workspace_id.as_bytes())
		.await?;

	let s = std::str::from_utf8(&bytes)
		.map_err(|e| err!(Database(error!("Invalid UTF-8 for space room id: {e}"))))?;
	OwnedRoomId::parse(s)
		.map_err(|e| err!(Database(error!("Invalid RoomId for space room id: {e}"))))
}

// ─── Room / Workspace mapping ─────────────────────────────────────────────────

/// Lưu mapping roomId → workspaceId + ghi vào set index.
/// Atomic với RocksDB (source of truth).
#[implement(Service)]
pub fn set_workspace_id(&self, room_id: &RoomId, workspace_id: &str) {
	self.db
		.roomid_workspaceid
		.raw_put(room_id.as_bytes(), workspace_id.as_bytes());
	let key = composite_key(workspace_id, room_id);
	let empty: &[u8] = &[];
	self.db.workspaceid_roomids.raw_put(&key, empty);
}

/// Lookup: roomId → workspaceId
#[implement(Service)]
pub async fn get_workspace_id(&self, room_id: &RoomId) -> Result<String> {
	let bytes = self.db.roomid_workspaceid.get(room_id.as_bytes()).await?;

	std::str::from_utf8(&bytes)
		.map(|s| s.to_owned())
		.map_err(|e| err!(Database(error!("Invalid UTF-8 for workspace_id: {e}"))))
}

/// Stream tất cả roomId thuộc workspace từ RocksDB index (source of truth).
/// Không phụ thuộc m.space.child events.
#[implement(Service)]
pub fn rooms_by_workspace(
	&self,
	workspace_id: &str,
) -> impl Stream<Item = OwnedRoomId> + Send + '_ {
	let prefix = workspace_prefix(workspace_id);
	let prefix_len = prefix.len();

	self.db
		.workspaceid_roomids
		.keys_prefix_raw(&prefix)
		.ignore_err()
		.ready_filter_map(move |key_bytes: &[u8]| {
			// key_bytes = "{workspace_id}\xFF{room_id}"
			// room_id portion starts at prefix_len
			if key_bytes.len() <= prefix_len {
				return None;
			}
			let room_bytes = &key_bytes[prefix_len..];
			std::str::from_utf8(room_bytes)
				.ok()
				.and_then(|s| OwnedRoomId::parse(s).ok())
		})
}

#[implement(Service)]
pub async fn add_user_workspace(&self, user_id: &UserId, workspace_id: &str) -> Vec<String> {
	let workspace_id = workspace_id.trim();
	if workspace_id.is_empty() {
		return self.user_workspaces(user_id).await;
	}

	let empty: &[u8] = &[];
	let user_workspace_key = user_workspace_key(user_id, workspace_id);
	let workspace_user_key = workspace_user_key(workspace_id, user_id);
	self.db
		.userid_workspaceids
		.raw_put(&user_workspace_key, empty);
	self.db
		.workspaceid_userids
		.raw_put(&workspace_user_key, empty);

	self.user_workspaces(user_id).await
}

#[implement(Service)]
pub async fn user_in_workspace(&self, user_id: &UserId, workspace_id: &str) -> bool {
	let workspace_id = workspace_id.trim();
	if workspace_id.is_empty() {
		return false;
	}

	let key = user_workspace_key(user_id, workspace_id);
	self.db.userid_workspaceids.get(&key).await.is_ok()
}

#[implement(Service)]
pub async fn set_user_workspaces(&self, user_id: &UserId, workspace_ids: &[String]) -> Vec<String> {
	let existing = self.user_workspaces(user_id).await;
	for workspace_id in existing {
		let user_workspace_key = user_workspace_key(user_id, &workspace_id);
		let workspace_user_key = workspace_user_key(&workspace_id, user_id);
		self.db.userid_workspaceids.remove(&user_workspace_key);
		self.db.workspaceid_userids.remove(&workspace_user_key);
	}

	let mut workspace_ids = workspace_ids
		.iter()
		.map(String::as_str)
		.map(str::trim)
		.filter(|workspace_id| !workspace_id.is_empty())
		.map(ToOwned::to_owned)
		.collect::<Vec<_>>();
	workspace_ids.sort();
	workspace_ids.dedup();

	let empty: &[u8] = &[];
	for workspace_id in &workspace_ids {
		let user_workspace_key = user_workspace_key(user_id, workspace_id);
		let workspace_user_key = workspace_user_key(workspace_id, user_id);
		self.db
			.userid_workspaceids
			.raw_put(&user_workspace_key, empty);
		self.db
			.workspaceid_userids
			.raw_put(&workspace_user_key, empty);
	}

	workspace_ids
}

#[implement(Service)]
pub async fn user_workspaces(&self, user_id: &UserId) -> Vec<String> {
	let prefix = user_workspace_prefix(user_id);
	let prefix_len = prefix.len();

	let mut workspace_ids = self
		.db
		.userid_workspaceids
		.keys_prefix_raw(&prefix)
		.ignore_err()
		.ready_filter_map(move |key_bytes: &[u8]| {
			if key_bytes.len() <= prefix_len {
				return None;
			}

			std::str::from_utf8(&key_bytes[prefix_len..])
				.ok()
				.map(ToOwned::to_owned)
		})
		.collect::<Vec<_>>()
		.await;
	workspace_ids.sort();
	workspace_ids
}

#[implement(Service)]
pub fn users_by_workspace(
	&self,
	workspace_id: &str,
) -> impl Stream<Item = OwnedUserId> + Send + '_ {
	let prefix = workspace_user_prefix(workspace_id);
	let prefix_len = prefix.len();

	self.db
		.workspaceid_userids
		.keys_prefix_raw(&prefix)
		.ignore_err()
		.ready_filter_map(move |key_bytes: &[u8]| {
			if key_bytes.len() <= prefix_len {
				return None;
			}

			std::str::from_utf8(&key_bytes[prefix_len..])
				.ok()
				.and_then(|s| OwnedUserId::parse(s).ok())
		})
}
