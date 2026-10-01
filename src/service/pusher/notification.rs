use futures::{TryStreamExt, pin_mut};
use std::{collections::BTreeMap, fmt::Debug};

use futures::{StreamExt, future::join3, stream::select};
use ruma::{EventId, OwnedEventId, RoomId, UserId, events::receipt::ReceiptThread, push::Action};
use serde::Serialize;
use tuwunel_core::{
	Result, implement,
	matrix::pdu::{Count, PduCount, PduId},
	trace,
	utils::{
		stream::{BroadbandExt, ReadyExt, TryIgnore},
		u64_from_u8,
	},
};
use tuwunel_database::{
	Deserialized, Ignore, IgnoreAll, Interfix, KeyBuf, deserialize_from_slice as deserialize_key,
};

/// Per-thread unread counts: `(notification, highlight)` keyed by thread root.
type ThreadCounts = BTreeMap<OwnedEventId, (u64, u64)>;

/// Per-thread last-read counts keyed by thread root. Used by sync v3 to
/// gate emission of `unread_thread_notifications` to threads whose read
/// cursor advanced within the sync window.
type ThreadLastReads = BTreeMap<OwnedEventId, u64>;

/// Reset the room's main-timeline notification counts.
///
/// The last-read stamp gates sync output; callers dispatch the badge refresh
/// after every reset.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn reset_notification_counts(&self, user_id: &UserId, room_id: &RoomId) {
	let count = self.services.globals.next_count();

	let userroom_id = (user_id, room_id);

	self.reset_notification_count(room_id, user_id, userroom_id)
		.await;

	self.db
		.userroomid_highlightcount
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
async fn reset_notification_count<K>(&self, room_id: &RoomId, user_id: &UserId, key: K)
where
	K: Serialize + Debug + Send + Sync,
{
	// The increment path is a read-modify-write under this lock; an unlocked
	// zero could land inside it and be overwritten by the stale sum.
	let _lock = self
		.notification_increment_mutex
		.lock(&(room_id.to_owned(), user_id.to_owned()))
		.await;

	self.db
		.userroomid_notificationcount
		.put(key, 0_u64);
}

/// Reset counts for a single thread within a room.
///
/// The last-read stamp gates sync output.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn reset_thread_notification_counts(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	thread_root: &EventId,
) {
	let count = self.services.globals.next_count();

	let userroom_thread = (user_id, room_id, thread_root);

	self.reset_notification_count(room_id, user_id, userroom_thread)
		.await;

	self.db
		.userroomid_highlightcount
		.put(userroom_thread, 0_u64);

	let roomuser_thread = (room_id, user_id, thread_root);
	self.db
		.roomuserid_lastnotificationread
		.put(roomuser_thread, *count);
}

/// Clear all per-thread notification state for this user and room.
///
/// The `Interfix` prefix excludes the main row. The notification-count sweep
/// runs under the increment mutex so a concurrent read-modify-write cannot
/// resurrect a cleared row.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn clear_all_thread_notification_counts(&self, user_id: &UserId, room_id: &RoomId) {
	let userroom_prefix = (user_id, room_id, Interfix);
	let roomuser_prefix = (room_id, user_id, Interfix);

	let highlights = self
		.db
		.userroomid_highlightcount
		.del_prefix(&userroom_prefix);

	let last_reads = self
		.db
		.roomuserid_lastnotificationread
		.del_prefix(&roomuser_prefix);

	let notifications = async {
		let _lock = self
			.notification_increment_mutex
			.lock(&(room_id.to_owned(), user_id.to_owned()))
			.await;

		self.db
			.userroomid_notificationcount
			.del_prefix(&userroom_prefix)
			.await;
	};

	join3(notifications, highlights, last_reads).await;
}

/// Dispatcher: route a receipt's `ReceiptThread` to the matching reset path.
///
/// `Unthreaded` clears all room and thread counts; `Main` clears only the
/// main-timeline counts; `Thread(id)` clears just that thread unless the
/// acknowledged event is the thread root. `None` denotes a non-receipt reset.
#[implement(super::Service)]
pub async fn reset_notification_counts_for_thread(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	acknowledged: Option<&EventId>,
	thread: &ReceiptThread,
) {
	match thread {
		| ReceiptThread::Thread(root) if acknowledged == Some(root) => {},
		| ReceiptThread::Main => {
			self.reset_notification_counts(user_id, room_id)
				.await
		},
		| ReceiptThread::Thread(root) => {
			self.reset_thread_notification_counts(user_id, room_id, root)
				.await
		},
		| _ => {
			self.reset_notification_counts(user_id, room_id)
				.await;

			self.clear_all_thread_notification_counts(user_id, room_id)
				.await;
		},
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

/// Return the user's account-wide unread notification count.
///
/// Joined main and thread rows contribute to a saturating total.
#[implement(super::Service)]
#[tracing::instrument(level = "trace", skip(self), ret)]
pub async fn global_notification_count(&self, user_id: &UserId) -> u64 {
	self.db
		.userroomid_notificationcount
		.stream_prefix_raw(&(user_id, Interfix))
		.ignore_err()
		.ready_filter_map(|(key, count)| {
			let count = u64_from_u8(count);

			(count > 0).then(|| (KeyBuf::from(key), count))
		})
		.broad_filter_map(async |(key, count)| {
			let (_, room_id, _): (Ignore, &RoomId, IgnoreAll) =
				deserialize_key(&key).expect("notification count key");

			self.services
				.state_cache
				.is_joined(user_id, room_id)
				.await
				.then_some(count)
		})
		.ready_fold(0_u64, u64::saturating_add)
		.await
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

/// Per-thread `(notification, highlight)` counts for one room and user.
/// `Interfix` excludes the legacy 2-tuple main row from the scan; only
/// 3-tuple `(user, room, root)` rows match.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn thread_notification_counts(&self, user_id: &UserId, room_id: &RoomId) -> ThreadCounts {
	let prefix = (user_id, room_id, Interfix);
	let notifications = self
		.db
		.userroomid_notificationcount
		.stream_prefix(&prefix)
		.ignore_err()
		.map(notification_kv);

	let highlights = self
		.db
		.userroomid_highlightcount
		.stream_prefix(&prefix)
		.ignore_err()
		.map(highlight_kv);

	select(notifications, highlights)
		.ready_fold(ThreadCounts::default(), merge_thread_count)
		.await
}

fn notification_kv(
	(key, notifications): ((&UserId, &RoomId, OwnedEventId), u64),
) -> (OwnedEventId, (u64, u64)) {
	(key.2, (notifications, 0))
}

fn highlight_kv(
	(key, highlights): ((&UserId, &RoomId, OwnedEventId), u64),
) -> (OwnedEventId, (u64, u64)) {
	(key.2, (0, highlights))
}

fn merge_thread_count(
	mut counts: ThreadCounts,
	(root, (notifications, highlights)): (OwnedEventId, (u64, u64)),
) -> ThreadCounts {
	let entry = counts.entry(root).or_default();
	entry.0 = entry.0.saturating_add(notifications);
	entry.1 = entry.1.saturating_add(highlights);
	counts
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
	let last_read = match notified.thread_root.as_deref() {
		| Some(root) => self
			.thread_last_notification_reads(user_id, room_id)
			.await
			.get(root)
			.copied(),
		| None => self
			.last_notification_read(user_id, room_id)
			.await
			.ok(),
	};
	let unread = last_read.is_none_or(|last_read| count > last_read);
	if !unread {
		return;
	}

	if notified
		.actions
		.iter()
		.any(|action| matches!(action, Action::Notify))
	{
		self.decrement_notificationcount(room_id, user_id, notified.thread_root.as_deref())
			.await;
	}

	if notified.actions.iter().any(Action::is_highlight) {
		self.decrement_highlightcount(room_id, user_id, notified.thread_root.as_deref())
			.await;
	}
}

#[implement(super::Service)]
async fn decrement_notificationcount(
	&self,
	room_id: &RoomId,
	user_id: &UserId,
	thread: Option<&EventId>,
) {
	let key = (room_id.to_owned(), user_id.to_owned());
	let _lock = self.notification_increment_mutex.lock(&key).await;
	let key = match thread {
		| Some(root) => tuwunel_database::serialize_to_vec((user_id, room_id, root)),
		| None => tuwunel_database::serialize_to_vec((user_id, room_id)),
	}
	.expect("notification key");
	let count: u64 = self
		.db
		.userroomid_notificationcount
		.get(&key)
		.await
		.deserialized()
		.unwrap_or(0);
	self.db
		.userroomid_notificationcount
		.insert(&key, count.saturating_sub(1).to_be_bytes());
}

#[implement(super::Service)]
async fn decrement_highlightcount(
	&self,
	room_id: &RoomId,
	user_id: &UserId,
	thread: Option<&EventId>,
) {
	let key = (room_id.to_owned(), user_id.to_owned());
	let _lock = self.highlight_increment_mutex.lock(&key).await;
	let key = match thread {
		| Some(root) => tuwunel_database::serialize_to_vec((user_id, room_id, root)),
		| None => tuwunel_database::serialize_to_vec((user_id, room_id)),
	}
	.expect("notification key");
	let count: u64 = self
		.db
		.userroomid_highlightcount
		.get(&key)
		.await
		.deserialized()
		.unwrap_or(0);
	self.db
		.userroomid_highlightcount
		.insert(&key, count.saturating_sub(1).to_be_bytes());
}

/// Per-thread last-read counts for one room and user. `Interfix` keeps the
/// scan to 3-tuple `(room, user, root)` rows; the legacy 2-tuple main row
/// is excluded by construction and lives behind `last_notification_read`.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn thread_last_notification_reads(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
) -> ThreadLastReads {
	let prefix = (room_id, user_id, Interfix);
	self.db
		.roomuserid_lastnotificationread
		.stream_prefix(&prefix)
		.ignore_err()
		.map(|((_, _, root), count): ((Ignore, Ignore, OwnedEventId), u64)| (root, count))
		.collect()
		.await
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
	let pdus =
		self.services
			.timeline
			.pdus(Some(user_id), room_id, Some(PduCount::Normal(last_read.0)));

	pin_mut!(pdus);
	let (_, pdu) = pdus.try_next().await.ok()??;

	Some(pdu.event_id)
}

#[implement(super::Service)]
pub async fn reset_room_notification_counts(&self, user: &UserId, room: &RoomId) {
	self.reset_notification_counts(user, room).await;
	self.clear_all_thread_notification_counts(user, room)
		.await;
}

#[implement(super::Service)]
pub(super) async fn room_notification_count_for_push(&self, user: &UserId, room: &RoomId) -> u64 {
	let main = self.notification_count(user, room).await;
	self.thread_notification_counts(user, room)
		.await
		.values()
		.fold(main, |sum, (count, _)| sum.saturating_add(*count))
}
