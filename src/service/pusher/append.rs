use std::{collections::HashSet, sync::Arc};

use futures::{FutureExt, StreamExt, future::join};
use ruma::{
	RoomId, UserId,
	api::client::push::ProfileTag,
	events::{GlobalAccountDataEventType, TimelineEventType, push_rules::PushRulesEvent},
	push::{Action, Actions, Ruleset, Tweak},
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue as RawJsonValue;
use tuwunel_core::{
	Result, implement,
	matrix::{
		event::Event,
		pdu::{Count, Pdu, PduId, RawPduId},
	},
	utils::{BoolExt, ReadyExt, future::TryExtExt, time::now_millis},
};
use tuwunel_database::{Deserialized, Json, Map};

use crate::rooms::short::ShortRoomId;

const REMINDER_CREATED_EVENT_TYPE: &str = "im.tuwunel.reminder.created";
const REMINDER_DUE_EVENT_TYPE: &str = "im.tuwunel.reminder.due";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReminderPushKind {
	Created,
	Due,
}

#[derive(Debug, Deserialize)]
struct ReminderPushContent {
	scope: Option<String>,
}

#[derive(Clone, Copy)]
enum PushTargetPolicy {
	ExcludeSender,
	IncludeSender,
	SenderOnly,
}

/// Succinct version of Ruma's Notification. Appended to the database when the
/// user is notified. The PduCount is part of the database key so only the
/// shortroomid is included. Together they  make the PduId.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Notified {
	/// Milliseconds time at which the event notification was sent.
	pub ts: u64,

	/// ShortRoomId
	pub sroomid: ShortRoomId,

	/// The profile tag of the rule that matched this event.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub tag: Option<ProfileTag>,

	/// Actions vector
	pub actions: Actions,
}

/// Called by timeline append_pdu.
#[implement(super::Service)]
#[tracing::instrument(name = "append", level = "debug", skip_all)]
pub(crate) async fn append_pdu(&self, pdu_id: RawPduId, pdu: &Pdu) -> Result {
	// Don't notify the sender of their own events. Ignore lists only suppress DM
	// messages; group rooms should remain visible and notify normally.
	let reminder_push_kind = reminder_push_kind(pdu.kind());
	let push_target_policy = reminder_push_target_policy(pdu, reminder_push_kind);
	let sender = pdu.sender().to_owned();

	let push_targets = self
		.services
		.state_cache
		.active_local_users_in_room(pdu.room_id())
		.map(ToOwned::to_owned)
		.ready_filter(move |user| match push_target_policy {
			| PushTargetPolicy::ExcludeSender => *user != sender,
			| PushTargetPolicy::IncludeSender => true,
			| PushTargetPolicy::SenderOnly => *user == sender,
		})
		.filter_map(async |recipient_user| {
			let ignored = self
				.services
				.users
				.user_is_ignored(pdu.sender(), &recipient_user)
				.await;

			if ignored {
				let is_direct = self
					.services
					.account_data
					.is_direct(&recipient_user, pdu.room_id())
					.await || self
					.services
					.state_accessor
					.is_direct(pdu.room_id(), &recipient_user)
					.await;

				if is_direct {
					return Some((None, Some(recipient_user)));
				}
			}

			Some((Some(recipient_user), None))
		})
		.fold(
			(HashSet::new(), Vec::new()),
			|(mut push_target, mut ignored_recipients), (push_user, ignored_user)| async move {
				if let Some(push_user) = push_user {
					push_target.insert(push_user);
				}

				if let Some(ignored_user) = ignored_user {
					ignored_recipients.push(ignored_user);
				}

				(push_target, ignored_recipients)
			},
		);

	let power_levels = self
		.services
		.state_accessor
		.get_power_levels(pdu.room_id())
		.ok();

	let ((mut push_target, ignored_recipients), power_levels) =
		join(push_targets, power_levels).boxed().await;

	for recipient_user in ignored_recipients {
		self.services
			.deleted_events
			.delete_event(&recipient_user, pdu.event_id());
	}

	if *pdu.kind() == TimelineEventType::RoomMember
		&& let Some(Ok(target_user_id)) = pdu.state_key().map(UserId::parse)
		&& self
			.services
			.users
			.is_active_local(target_user_id)
			.await
	{
		push_target.insert(target_user_id.to_owned());
	}

	let serialized = pdu.to_format();
	let _cork = self.db.db.cork();
	for user in &push_target {
		let rules_for_user = self
			.services
			.account_data
			.get_global(user, GlobalAccountDataEventType::PushRules)
			.await
			.map_or_else(
				|_| Ruleset::server_default(user),
				|ev: PushRulesEvent| ev.content.global,
			);

		let actions = self
			.services
			.pusher
			.get_actions(user, &rules_for_user, power_levels.as_ref(), &serialized, pdu.room_id())
			.await;

		let notify_by_rule = actions
			.iter()
			.any(|action| matches!(action, Action::Notify));

		let notify = notify_by_rule || reminder_push_kind.is_some();

		let highlight = actions
			.iter()
			.any(|action| matches!(action, Action::SetTweak(Tweak::Highlight(true))));

		let increment_notify =
			notify.then_async(|| self.increment_notificationcount(pdu.room_id(), user));

		let increment_highlight =
			highlight.then_async(|| self.increment_highlightcount(pdu.room_id(), user));

		join(increment_notify, increment_highlight).await;

		if notify || highlight || self.services.config.push_everything {
			let id: PduId = pdu_id.into();
			let notified = Notified {
				ts: now_millis(),
				sroomid: id.shortroomid,
				tag: None,
				actions: notification_actions(actions, notify_by_rule, reminder_push_kind),
			};

			if matches!(id.count, Count::Normal(_)) {
				self.db
					.useridcount_notification
					.put((user, id.count.into_unsigned()), Json(notified));
			}
		}

		let is_reaction = *pdu.kind() == TimelineEventType::Reaction;
		if notify || highlight || is_reaction || self.services.config.push_everything {
			self.services
				.pusher
				.get_pushkeys(user)
				.map(ToOwned::to_owned)
				.ready_for_each(|push_key| {
					self.services
						.sending
						.send_pdu_push(&pdu_id, user, push_key)
						.expect("TODO: replace with future");
				})
				.await;
		}
	}

	Ok(())
}

fn reminder_push_kind(kind: &TimelineEventType) -> Option<ReminderPushKind> {
	match kind.to_cow_str().as_ref() {
		| REMINDER_CREATED_EVENT_TYPE => Some(ReminderPushKind::Created),
		| REMINDER_DUE_EVENT_TYPE => Some(ReminderPushKind::Due),
		| _ => None,
	}
}

fn reminder_push_target_policy(
	pdu: &Pdu,
	reminder_push_kind: Option<ReminderPushKind>,
) -> PushTargetPolicy {
	match reminder_push_kind {
		| Some(ReminderPushKind::Created) if reminder_scope_is_me(pdu.content()) => {
			PushTargetPolicy::SenderOnly
		},
		| Some(ReminderPushKind::Due) => PushTargetPolicy::IncludeSender,
		| _ => PushTargetPolicy::ExcludeSender,
	}
}

fn reminder_scope_is_me(content: &RawJsonValue) -> bool {
	serde_json::from_str::<ReminderPushContent>(content.get())
		.ok()
		.and_then(|content| content.scope)
		.is_some_and(|scope| scope == "me")
}

fn notification_actions(
	actions: &[Action],
	notify_by_rule: bool,
	reminder_push_kind: Option<ReminderPushKind>,
) -> Actions {
	let mut actions: Actions = actions.into();
	if reminder_push_kind.is_some() && !notify_by_rule {
		actions.push(Action::Notify);
	}

	actions
}

#[implement(super::Service)]
async fn increment_notificationcount(&self, room_id: &RoomId, user_id: &UserId) {
	let db = &self.db.userroomid_notificationcount;
	let key = (room_id.to_owned(), user_id.to_owned());
	let _lock = self.notification_increment_mutex.lock(&key).await;

	increment(db, (user_id, room_id)).await;
}

#[implement(super::Service)]
async fn increment_highlightcount(&self, room_id: &RoomId, user_id: &UserId) {
	let db = &self.db.userroomid_highlightcount;
	let key = (room_id.to_owned(), user_id.to_owned());
	let _lock = self.highlight_increment_mutex.lock(&key).await;

	increment(db, (user_id, room_id)).await;
}

async fn increment(db: &Arc<Map>, key: (&UserId, &RoomId)) {
	let old: u64 = db.qry(&key).await.deserialized().unwrap_or(0);
	let new = old.saturating_add(1);
	db.put(key, new);
}
