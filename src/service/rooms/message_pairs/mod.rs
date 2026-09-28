use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use ruma::{EventId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tuwunel_core::Result;
use tuwunel_database::{Deserialized, Json, Map};

/// A translation owned by one account. This is never a room PDU.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MessagePair {
	pub event_id: OwnedEventId,
	pub room_id: OwnedRoomId,
	pub sender: OwnedUserId,
	pub target_event_id: OwnedEventId,
	pub event_type: String,
	pub content: Value,
	pub batch_id: String,
	pub origin_server_ts: u64,
}

pub struct Service {
	userroomid_messagepairs: Arc<Map>,
}

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			userroomid_messagepairs: args.db["userroomid_messagepairs"].clone(),
		}))
	}

	fn name(&self) -> &str {
		crate::service::make_name(std::module_path!())
	}
}

impl Service {
	pub fn insert(&self, pair: &MessagePair) {
		let key = pair_key(&pair.sender, &pair.room_id, &pair.event_id);
		self.userroomid_messagepairs
			.raw_put(key, Json(pair));
	}

	pub async fn get(
		&self,
		owner: &UserId,
		room_id: &RoomId,
		event_id: &EventId,
	) -> Result<MessagePair> {
		let key = pair_key(owner, room_id, event_id);
		self.userroomid_messagepairs
			.get(&key)
			.await
			.deserialized()
	}

	pub async fn list(&self, owner: &UserId, room_id: &RoomId) -> Result<Vec<MessagePair>> {
		let prefix = pair_prefix(owner, room_id);
		let mut pairs: Vec<MessagePair> = self
			.userroomid_messagepairs
			.raw_stream_prefix(&prefix)
			.map(|entry| entry.and_then(|(_, value)| Ok(serde_json::from_slice(value.as_ref())?)))
			.try_collect()
			.await?;
		pairs.sort_by_key(|pair| pair.origin_server_ts);
		Ok(pairs)
	}

	pub fn remove(&self, pair: &MessagePair) {
		let key = pair_key(&pair.sender, &pair.room_id, &pair.event_id);
		self.userroomid_messagepairs.remove(&key);
	}
}

fn pair_key(owner: &UserId, room_id: &RoomId, event_id: &EventId) -> Vec<u8> {
	let mut key = pair_prefix(owner, room_id);
	key.extend_from_slice(event_id.as_bytes());
	key
}

fn pair_prefix(owner: &UserId, room_id: &RoomId) -> Vec<u8> {
	let mut prefix = Vec::with_capacity(owner.as_bytes().len() + room_id.as_bytes().len() + 2);
	prefix.extend_from_slice(owner.as_bytes());
	prefix.push(0xFF);
	prefix.extend_from_slice(room_id.as_bytes());
	prefix.push(0xFF);
	prefix
}

#[cfg(test)]
mod tests {
	use ruma::{event_id, room_id, user_id};

	use super::{pair_key, pair_prefix};

	#[test]
	fn pair_keys_are_scoped_to_owner_and_room() {
		let alice = user_id!("@alice:example.org");
		let bob = user_id!("@bob:example.org");
		let first_room = room_id!("!first:example.org");
		let second_room = room_id!("!second:example.org");
		let event_id = event_id!("$private:example.org");
		let key = pair_key(alice, first_room, event_id);

		assert!(key.starts_with(&pair_prefix(alice, first_room)));
		assert!(!key.starts_with(&pair_prefix(bob, first_room)));
		assert!(!key.starts_with(&pair_prefix(alice, second_room)));
	}
}
