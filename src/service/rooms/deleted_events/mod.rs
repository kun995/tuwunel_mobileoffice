use std::sync::Arc;

use ruma::{EventId, UserId};
use tuwunel_core::Result;
use tuwunel_database::Map;

pub struct Service {
	db: Data,
}

struct Data {
	usereventsid_deleted: Arc<Map>,
}

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				usereventsid_deleted: args.db["usereventsid_deleted"].clone(),
			},
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	/// Permanently deletes an event on behalf of a user (only hidden from
	/// that user). Cannot be restored.
	pub fn delete_event(&self, user_id: &UserId, event_id: &EventId) {
		let key = (user_id, event_id);
		self.db.usereventsid_deleted.put_raw(key, b"");
	}

	/// Permanently deletes multiple events on behalf of a user (batch).
	pub fn delete_events(&self, user_id: &UserId, event_ids: &[&EventId]) {
		for event_id in event_ids {
			self.delete_event(user_id, event_id);
		}
	}

	/// Returns `true` if the given event has been deleted by the given user.
	pub async fn is_deleted(&self, user_id: &UserId, event_id: &EventId) -> bool {
		let key = (user_id, event_id);
		self.db.usereventsid_deleted.qry(&key).await.is_ok()
	}
}
