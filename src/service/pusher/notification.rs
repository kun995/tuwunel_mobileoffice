use futures::{StreamExt, TryStreamExt, pin_mut};
use ruma::{EventId, OwnedEventId, RoomId, UserId, push::Action};
use tuwunel_core::{
	Result, implement,
	matrix::pdu::{Count, PduCount, PduId},
	trace,
	utils::stream::{ReadyExt, TryIgnore},
};
use tuwunel_database::{Deserialized, Interfix};

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub fn reset_notification_counts(&self, user_id: &UserId, room_id: &RoomId) {
	let count = self.services.globals.next_count();

	let userroom_id = (user_id, room_id);
	self.db
		.userroomid_highlightcount
		.put(userroom_id, 0_u64);
	self.db
		.userroomid_notificationcount
		.put(userroom_id, 0_u64);

	let roomuser_id = (room_id, user_id);
	self.db
		.roomuserid_lastnotificationread
		.put(roomuser_id, *count);

	let removed = self.clear_suppressed_room(user_id, room_id);
	if removed > 0 {
		trace!(?user_id, ?room_id, removed, "Cleared suppressed push events after read");
	}
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self), ret(level = "trace"))]
pub async fn notification_count(&self, user_id: &UserId, room_id: &RoomId) -> u64 {
	let key = (user_id, room_id);
	self.db
		.userroomid_notificationcount
		.qry(&key)
		.await
		.deserialized()
		.unwrap_or(0)
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self), ret(level = "trace"))]
pub async fn highlight_count(&self, user_id: &UserId, room_id: &RoomId) -> u64 {
	let key = (user_id, room_id);
	self.db
		.userroomid_highlightcount
		.qry(&key)
		.await
		.deserialized()
		.unwrap_or(0)
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self), ret(level = "trace"))]
pub async fn last_notification_read(&self, user_id: &UserId, room_id: &RoomId) -> Result<u64> {
	let key = (room_id, user_id);
	self.db
		.roomuserid_lastnotificationread
		.qry(&key)
		.await
		.deserialized()
}

/// Removes the notification entry for an event that has just been redacted.
///
/// Redaction removes the event from the user's visible timeline, but the
/// notification counters are maintained separately from the timeline. Keep
/// both stores in sync so unread badges do not retain deleted messages.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn remove_redacted_event_notifications(&self, room_id: &RoomId, event_id: &EventId) {
	let users = self
		.services
		.state_cache
		.local_users_in_room(room_id)
		.map(ToOwned::to_owned)
		.collect::<Vec<_>>()
		.await;

	for user in users {
		self.remove_notification_for_redacted_event(&user, room_id, event_id)
			.await;
	}
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn remove_notification_for_redacted_event(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	event_id: &EventId,
) {
	let Ok(raw_pdu_id) = self.services.timeline.get_pdu_id(event_id).await else {
		return;
	};

	let pdu_id: PduId = raw_pdu_id.into();
	let Count::Normal(count) = pdu_id.count else {
		return;
	};

	let key = (user_id, count);
	let Some(notified) = self
		.db
		.useridcount_notification
		.qry(&key)
		.await
		.deserialized::<super::Notified>()
		.ok()
	else {
		return;
	};

	// The count is part of the notification key, while the short room id
	// disambiguates the event and prevents removing a record from another room.
	if notified.sroomid != pdu_id.shortroomid {
		return;
	}

	self.db.useridcount_notification.del(key);

	// A read/reset operation may already have cleared the aggregate counter.
	// In that case remove only the stale event record, not another unread item.
	let unread = self
		.last_notification_read(user_id, room_id)
		.await
		.map_or(true, |last_read| count > last_read);
	if !unread {
		return;
	}

	if notified
		.actions
		.iter()
		.any(|action| matches!(action, Action::Notify))
	{
		self.decrement_notificationcount(room_id, user_id)
			.await;
	}

	if notified.actions.iter().any(Action::is_highlight) {
		self.decrement_highlightcount(room_id, user_id)
			.await;
	}
}

#[implement(super::Service)]
async fn decrement_notificationcount(&self, room_id: &RoomId, user_id: &UserId) {
	let key = (room_id.to_owned(), user_id.to_owned());
	let _lock = self.notification_increment_mutex.lock(&key).await;
	let count: u64 = self
		.db
		.userroomid_notificationcount
		.qry(&(user_id, room_id))
		.await
		.deserialized()
		.unwrap_or(0);
	self.db
		.userroomid_notificationcount
		.put((user_id, room_id), count.saturating_sub(1));
}

#[implement(super::Service)]
async fn decrement_highlightcount(&self, room_id: &RoomId, user_id: &UserId) {
	let key = (room_id.to_owned(), user_id.to_owned());
	let _lock = self.highlight_increment_mutex.lock(&key).await;
	let count: u64 = self
		.db
		.userroomid_highlightcount
		.qry(&(user_id, room_id))
		.await
		.deserialized()
		.unwrap_or(0);
	self.db
		.userroomid_highlightcount
		.put((user_id, room_id), count.saturating_sub(1));
}

#[implement(super::Service)]
pub async fn delete_room_notification_read(&self, room_id: &RoomId) -> Result {
	let key = (room_id, Interfix);
	self.db
		.roomuserid_lastnotificationread
		.keys_prefix_raw(&key)
		.ignore_err()
		.ready_for_each(|key| {
			trace!("Removing key: {key:?}");
			self.db
				.roomuserid_lastnotificationread
				.remove(key);
		})
		.await;

	Ok(())
}

/// Returns the event_id of the first unread event in the given room for the user.
///
/// Looks at the user's last private read receipt (PDU count) and finds the
/// next PDU in the room's timeline — that is the first unread message.
/// Returns `None` if there are no unread events or no read receipt exists.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self), ret(level = "trace"))]
pub async fn first_unread_event_id(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
) -> Option<OwnedEventId> {
	// Get the PDU count of the user's last private read receipt in this room
	let last_read = self
		.services
		.read_receipt
		.private_read_get_count(room_id, user_id)
		.await
		.ok()?;

	// Stream PDUs forward from last_read in this room; first item is the first unread
	let pdus = self
		.services
		.timeline
		.pdus(Some(user_id), room_id, Some(PduCount::Normal(last_read)));

	pin_mut!(pdus);
	let (_, pdu) = pdus.try_next().await.ok()??;

	Some(pdu.event_id)
}
