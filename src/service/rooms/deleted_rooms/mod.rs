use std::sync::Arc;

use ruma::{RoomId, UserId};
use tuwunel_core::{Result, matrix::pdu::PduCount};
use tuwunel_database::{Deserialized, Map};

pub struct Service {
	services: Arc<crate::services::OnceServices>,
	db: Data,
}

struct Data {
	userroomid_deletedbefore: Arc<Map>,
	userroomid_clearedbefore: Arc<Map>,
}

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			services: args.services.clone(),
			db: Data {
				userroomid_deletedbefore: args.db["userroomid_deletedbefore"].clone(),
				userroomid_clearedbefore: args.db["userroomid_clearedbefore"].clone(),
			},
		}))
	}

	fn name(&self) -> &str {
		crate::service::make_name(std::module_path!())
	}
}

impl Service {
	/// Stores the timeline checkpoint for a per-user conversation delete.
	/// Events at or before this checkpoint stay visible to other users, but are
	/// hidden from this user through sync/history filters.
	pub fn delete_before(&self, user_id: &UserId, room_id: &RoomId, count: u64) {
		let key = (user_id, room_id);
		self.db.userroomid_deletedbefore.put(key, count);
	}

	/// Stores the timeline checkpoint for a per-user history clear.
	/// Unlike conversation delete, this does not hide the room from room lists.
	pub fn clear_before(&self, user_id: &UserId, room_id: &RoomId, count: u64) {
		let key = (user_id, room_id);
		self.db.userroomid_clearedbefore.put(key, count);
	}

	pub async fn deleted_before(&self, user_id: &UserId, room_id: &RoomId) -> Option<u64> {
		let deleted_before = self.room_hidden_before(user_id, room_id).await;
		let cleared_before = self.room_cleared_before(user_id, room_id).await;

		deleted_before.max(cleared_before)
	}

	async fn room_hidden_before(&self, user_id: &UserId, room_id: &RoomId) -> Option<u64> {
		let key = (user_id, room_id);
		self.db
			.userroomid_deletedbefore
			.qry(&key)
			.await
			.deserialized()
			.ok()
	}

	async fn room_cleared_before(&self, user_id: &UserId, room_id: &RoomId) -> Option<u64> {
		let key = (user_id, room_id);
		self.db
			.userroomid_clearedbefore
			.qry(&key)
			.await
			.deserialized()
			.ok()
	}

	pub async fn should_hide_room(&self, user_id: &UserId, room_id: &RoomId) -> bool {
		let Some(deleted_before) = self.room_hidden_before(user_id, room_id).await else {
			return false;
		};

		let Ok(last_count) = self
			.services
			.timeline
			.last_timeline_count(Some(user_id), room_id, None)
			.await
		else {
			return true;
		};

		!is_after_deleted_before(last_count, Some(deleted_before))
	}

	pub async fn is_deleted_pdu(
		&self,
		user_id: &UserId,
		room_id: &RoomId,
		count: PduCount,
	) -> bool {
		!is_after_deleted_before(count, self.deleted_before(user_id, room_id).await)
	}
}

#[inline]
pub fn is_after_deleted_before(count: PduCount, deleted_before: Option<u64>) -> bool {
	let Some(deleted_before) = deleted_before else {
		return true;
	};

	match count {
		| PduCount::Normal(count) => count > deleted_before,
		| PduCount::Backfilled(_) => false,
	}
}
