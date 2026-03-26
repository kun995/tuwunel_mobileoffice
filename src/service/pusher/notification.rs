use futures::{TryStreamExt, pin_mut};
use ruma::{OwnedEventId, RoomId, UserId};
use tuwunel_core::{
	Result, implement, trace,
	matrix::pdu::PduCount,
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
