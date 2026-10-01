use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Datelike, Days, FixedOffset, Months, TimeZone};
use futures::{StreamExt, stream};
use loole::{Receiver, Sender};
use ruma::{
	OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId, api::client::push::PusherKind,
	events::TimelineEventType,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tuwunel_core::{
	Result, err, implement, info,
	matrix::pdu::PduBuilder,
	result::LogErr,
	utils::{
		millis_since_unix_epoch, rand,
		stream::{ReadyExt, TryIgnore},
	},
	warn,
};
use tuwunel_database::{Deserialized, Ignore, Json, Map};

pub const REMINDER_CREATED_EVENT_TYPE: &str = "im.tuwunel.reminder.created";
pub const REMINDER_UPDATED_EVENT_TYPE: &str = "im.tuwunel.reminder.updated";
pub const REMINDER_DUE_EVENT_TYPE: &str = "im.tuwunel.reminder.due";
pub const REMINDER_CANCELLED_EVENT_TYPE: &str = "im.tuwunel.reminder.cancelled";

const REMINDER_WORKER_INTERVAL: Duration = Duration::from_secs(30);
const MAX_DUE_PER_TICK: usize = 100;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Reminder {
	pub reminder_id: String,
	pub room_id: OwnedRoomId,
	pub title: String,
	pub remind_at: String,
	pub remind_at_ms: u64,
	pub repeat: ReminderRepeat,
	pub scope: ReminderScope,
	pub status: ReminderStatus,
	pub created_by: OwnedUserId,
	pub created_at_ms: u64,
	pub last_triggered_at_ms: Option<u64>,
	pub event_id: Option<OwnedEventId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ReminderRepeat {
	None,
	Daily,
	Weekly,
	WeeklyDays { days: Vec<u8> },
	Monthly,
	Yearly,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReminderScope {
	Me,
	Room,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReminderStatus {
	Active,
	Cancelled,
	Completed,
}

pub struct NewReminder {
	pub room_id: OwnedRoomId,
	pub title: String,
	pub remind_at: String,
	pub remind_at_ms: u64,
	pub repeat: ReminderRepeat,
	pub scope: ReminderScope,
	pub created_by: OwnedUserId,
}

pub struct UpdateReminder {
	pub title: String,
	pub remind_at: String,
	pub remind_at_ms: u64,
	pub repeat: ReminderRepeat,
	pub scope: ReminderScope,
}

pub struct Service {
	db: Data,
	services: Arc<crate::services::OnceServices>,
	interrupt: (Sender<()>, Receiver<()>),
}

struct Data {
	reminderid_reminder: Arc<Map>,
	remindertime_reminderid: Arc<Map>,
	roomid_reminderids: Arc<Map>,
}

#[async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				reminderid_reminder: args.db["reminderid_reminder"].clone(),
				remindertime_reminderid: args.db["remindertime_reminderid"].clone(),
				roomid_reminderids: args.db["roomid_reminderids"].clone(),
			},
			services: args.services.clone(),
			interrupt: loole::unbounded(),
		}))
	}

	async fn worker(self: Arc<Self>) -> Result {
		let receiver = self.interrupt.1.clone();
		while !receiver.is_closed() {
			self.process_due_reminders().await.log_err().ok();

			tokio::select! {
				_ = tokio::time::sleep(REMINDER_WORKER_INTERVAL) => {},
				result = receiver.recv_async() => {
					if result.is_err() {
						break;
					}
				},
			}
		}

		Ok(())
	}

	async fn interrupt(&self) {
		self.interrupt.0.close();
	}

	fn name(&self) -> &str {
		crate::service::make_name(std::module_path!())
	}
}

#[implement(Service)]
pub async fn create_reminder(&self, input: NewReminder) -> Result<Reminder> {
	let reminder_id = format!("rem_{}", rand::string(24));
	let created_at_ms = millis_since_unix_epoch();

	let mut reminder = Reminder {
		reminder_id,
		room_id: input.room_id,
		title: input.title,
		remind_at: input.remind_at,
		remind_at_ms: input.remind_at_ms,
		repeat: input.repeat,
		scope: input.scope,
		status: ReminderStatus::Active,
		created_by: input.created_by,
		created_at_ms,
		last_triggered_at_ms: None,
		event_id: None,
	};

	let event_id = self
		.append_reminder_event(&reminder, REMINDER_CREATED_EVENT_TYPE)
		.await?;
	reminder.event_id = Some(event_id);
	self.store_reminder(&reminder);
	self.wake_worker();

	Ok(reminder)
}

#[implement(Service)]
pub async fn get_reminder(&self, reminder_id: &str) -> Result<Reminder> {
	self.db
		.reminderid_reminder
		.qry(reminder_id)
		.await
		.deserialized()
}

#[implement(Service)]
pub async fn reminders_for_room(&self, room_id: &RoomId) -> Vec<Reminder> {
	let prefix = room_reminder_prefix(room_id);
	let reminder_ids = self
		.db
		.roomid_reminderids
		.raw_keys_prefix(&prefix)
		.ignore_err()
		.ready_filter_map(|key| reminder_id_from_room_key(&prefix, key).map(ToOwned::to_owned))
		.collect::<Vec<_>>()
		.await;

	stream::iter(reminder_ids)
		.then(|reminder_id| async move { self.get_reminder(&reminder_id).await.ok() })
		.filter_map(|reminder| async move { reminder })
		.collect()
		.await
}

#[implement(Service)]
pub async fn update_reminder(
	&self,
	reminder_id: &str,
	sender: &UserId,
	input: UpdateReminder,
) -> Result<Reminder> {
	let mut reminder = self.get_reminder(reminder_id).await?;
	if reminder.created_by.as_str() != sender.as_str() {
		return Err(err!(Request(Forbidden(
			"Only the reminder creator can update this reminder."
		))));
	}

	if reminder.status != ReminderStatus::Active {
		return Err(err!(Request(InvalidParam(
			"Only active reminders can be updated."
		))));
	}

	let old_remind_at_ms = reminder.remind_at_ms;
	reminder.title = input.title;
	reminder.remind_at = input.remind_at;
	reminder.remind_at_ms = input.remind_at_ms;
	reminder.repeat = input.repeat;
	reminder.scope = input.scope;
	reminder.last_triggered_at_ms = None;

	let event_id = self
		.append_reminder_event(&reminder, REMINDER_UPDATED_EVENT_TYPE)
		.await?;
	reminder.event_id = Some(event_id);

	self.remove_due_index_at(old_remind_at_ms, &reminder.reminder_id);
	self.store_reminder(&reminder);
	self.wake_worker();

	Ok(reminder)
}

#[implement(Service)]
pub async fn cancel_reminder(&self, reminder_id: &str, sender: &UserId) -> Result<Reminder> {
	let mut reminder = self.get_reminder(reminder_id).await?;
	if reminder.created_by.as_str() != sender.as_str() {
		return Err(err!(Request(Forbidden(
			"Only the reminder creator can cancel this reminder."
		))));
	}

	if reminder.status != ReminderStatus::Active {
		return Ok(reminder);
	}

	reminder.status = ReminderStatus::Cancelled;
	self.remove_due_index(&reminder);
	self.remove_room_index(&reminder);
	self.store_reminder_only(&reminder);
	self.append_reminder_event(&reminder, REMINDER_CANCELLED_EVENT_TYPE)
		.await?;

	Ok(reminder)
}

#[implement(Service)]
pub async fn purge_user_reminders(&self, user_id: &UserId) -> usize {
	let reminders = self
		.db
		.reminderid_reminder
		.raw_stream()
		.ignore_err()
		.ready_filter_map(|(_, value): (&[u8], &[u8])| {
			serde_json::from_slice::<Reminder>(value)
				.ok()
				.filter(|reminder| reminder.created_by.as_str() == user_id.as_str())
		})
		.collect::<Vec<_>>()
		.await;

	for reminder in &reminders {
		self.remove_due_index(reminder);
		self.remove_room_index(reminder);
		self.db
			.reminderid_reminder
			.remove(reminder.reminder_id.as_bytes());
	}

	if !reminders.is_empty() {
		self.wake_worker();
	}

	reminders.len()
}

#[implement(Service)]
async fn process_due_reminders(&self) -> Result {
	let now = millis_since_unix_epoch();
	let due = self
		.db
		.remindertime_reminderid
		.stream::<(u64, String), Ignore>()
		.ignore_err()
		.ready_take_while(move |((remind_at_ms, _), _)| *remind_at_ms <= now)
		.take(MAX_DUE_PER_TICK)
		.map(|((remind_at_ms, reminder_id), _)| (remind_at_ms, reminder_id))
		.collect::<Vec<_>>()
		.await;

	for (due_at_ms, reminder_id) in due {
		self.process_due_reminder(&reminder_id, due_at_ms, now)
			.await
			.log_err()
			.ok();
	}

	Ok(())
}

#[implement(Service)]
async fn process_due_reminder(&self, reminder_id: &str, due_at_ms: u64, now: u64) -> Result {
	let mut reminder = match self.get_reminder(reminder_id).await {
		Ok(reminder) => reminder,
		Err(error) => {
			warn!(%reminder_id, %due_at_ms, %error, "Removing stale reminder due index");
			self.remove_due_index_at(due_at_ms, reminder_id);
			return Ok(());
		}
	};

	if reminder.remind_at_ms != due_at_ms {
		warn!(
			%reminder_id,
			index_due_at_ms = due_at_ms,
			current_due_at_ms = reminder.remind_at_ms,
			"Removing stale reminder due index with mismatched due time"
		);
		self.remove_due_index_at(due_at_ms, reminder_id);
		return Ok(());
	}

	if reminder.status != ReminderStatus::Active {
		self.remove_due_index_at(due_at_ms, reminder_id);
		return Ok(());
	}

	if reminder.remind_at_ms > now {
		return Ok(());
	}

	reminder.last_triggered_at_ms = Some(now);

	match reminder.scope {
		ReminderScope::Room => {
			self.append_reminder_event(&reminder, REMINDER_DUE_EVENT_TYPE)
				.await?;
		}
		ReminderScope::Me => {
			self.send_personal_due_push(&reminder).await?;
		}
	}

	self.remove_due_index_at(due_at_ms, reminder_id);

	if let Some((next_at, next_at_ms)) = next_occurrence(&reminder)? {
		reminder.remind_at = next_at;
		reminder.remind_at_ms = next_at_ms;
		self.store_reminder(&reminder);
		self.wake_worker();
	} else {
		reminder.status = ReminderStatus::Completed;
		self.store_reminder_only(&reminder);
	}

	Ok(())
}

#[implement(Service)]
async fn append_reminder_event(
	&self,
	reminder: &Reminder,
	event_type: &str,
) -> Result<OwnedEventId> {
	let state_lock = self.services.state.mutex.lock(&reminder.room_id).await;
	let content = reminder_event_content(reminder, event_type);
	let content = serde_json::value::to_raw_value(&content)
		.map_err(|e| err!(Request(BadJson("Failed to serialize reminder event: {e}"))))?;

	let event_id = self
		.services
		.timeline
		.build_and_append_pdu(
			PduBuilder {
				event_type: TimelineEventType::from(event_type),
				content: content.into(),
				..Default::default()
			},
			&reminder.created_by,
			&reminder.room_id,
			&state_lock,
		)
		.await?;
	drop(state_lock);

	Ok(event_id)
}

#[implement(Service)]
async fn send_personal_due_push(&self, reminder: &Reminder) -> Result {
	let room_name = self
		.services
		.state_accessor
		.get_name(&reminder.room_id)
		.await
		.ok();
	let workspace_id = self
		.services
		.workspace
		.get_workspace_id(&reminder.room_id)
		.await
		.ok();
	let is_direct_chat = self
		.services
		.account_data
		.is_direct(&reminder.created_by, &reminder.room_id)
		.await || self
		.services
		.state_accessor
		.is_direct(&reminder.room_id, &reminder.created_by)
		.await;
	let sender_avatar_url = self
		.services
		.profile
		.avatar_url(&reminder.created_by)
		.await
		.ok()
		.map(|avatar_url| avatar_url.to_string());
	let room_avatar_url = self
		.services
		.state_accessor
		.get_avatar(&reminder.room_id)
		.await
		.ok()
		.and_then(|content| content.url)
		.map(|avatar_url| avatar_url.to_string());

	let pushers = self.services.pusher.get_pushers(&reminder.created_by).await;
	let mut attempted = false;
	let mut delivered = false;
	for pusher in pushers {
		let PusherKind::Http(http) = &pusher.kind else {
			continue;
		};
		attempted = true;
		let mut device_data = http.data.clone();
		if let Some(format) = http.format.as_ref()
			&& let Ok(format) = serde_json::to_value(format)
		{
			device_data.insert("format".to_owned(), format);
		}
		device_data.insert(
			"lang".to_owned(),
			JsonValue::String(crate::pusher::normalized_language(&pusher.lang).to_owned()),
		);
		let unread = self
			.services
			.pusher
			.notification_count(&reminder.created_by, &reminder.room_id)
			.await;

		let url = push_gateway_notify_url(&http.url, &self.services.config.notification_push_path);
		let payload = json!({
			"notification": {
				"event_id": reminder.event_id.clone(),
				"room_id": reminder.room_id.to_string(),
				"event_type": REMINDER_DUE_EVENT_TYPE,
				"type": REMINDER_DUE_EVENT_TYPE,
				"sender": reminder.created_by.to_string(),
				"sender_display_name": "Reminder",
				"room_name": room_name,
				"sender_avatar_url": sender_avatar_url,
				"room_avatar_url": room_avatar_url,
				"is_direct_chat": is_direct_chat,
				"workspaceId": workspace_id,
				"prio": "high",
				"counts": {
					"unread": unread,
					"missed_calls": 0
				},
				"content": reminder_event_content(reminder, REMINDER_DUE_EVENT_TYPE),
				"devices": [{
					"app_id": pusher.ids.app_id.clone(),
					"pushkey": pusher.ids.pushkey.clone(),
					"data": device_data,
					"format": http.format.clone(),
				}]
			}
		});

		let response = self
			.services
			.client
			.pusher
			.post(&url)
			.json(&payload)
			.send()
			.await;

		match response {
			Ok(response) if response.status().is_success() => {
				delivered = true;
			}
			Ok(response) => {
				info!(%url, status = %response.status(), "Reminder push gateway returned non-success status");
			}
			Err(error) => {
				info!(%url, %error, "Failed to send reminder push");
			}
		}
	}

	if !attempted {
		warn!(
			reminder_id = %reminder.reminder_id,
			user_id = %reminder.created_by,
			"No HTTP pusher registered for personal reminder due notification"
		);
	}

	if attempted && !delivered {
		return Err(err!(BadServerResponse(
			"Reminder due push failed for all registered HTTP pushers."
		)));
	}

	Ok(())
}

fn reminder_event_content(reminder: &Reminder, event_type: &str) -> JsonValue {
	let body = match event_type {
		REMINDER_CREATED_EVENT_TYPE => format!("Reminder created: {}", reminder.title),
		REMINDER_UPDATED_EVENT_TYPE => format!("Reminder updated: {}", reminder.title),
		REMINDER_CANCELLED_EVENT_TYPE => format!("Reminder cancelled: {}", reminder.title),
		_ => format!("Reminder due: {}", reminder.title),
	};

	json!({
		"reminder_id": reminder.reminder_id.clone(),
		"title": reminder.title.clone(),
		"remind_at": reminder.remind_at.clone(),
		"remind_at_ms": reminder.remind_at_ms,
		"repeat": reminder.repeat.clone(),
		"scope": reminder.scope,
		"status": reminder.status,
		"created_by": reminder.created_by.to_string(),
		"created_at_ms": reminder.created_at_ms,
		"last_triggered_at_ms": reminder.last_triggered_at_ms,
		"body": body,
	})
}

fn next_occurrence(reminder: &Reminder) -> Result<Option<(String, u64)>> {
	match &reminder.repeat {
		ReminderRepeat::None => Ok(None),
		ReminderRepeat::Daily => add_days(&reminder.remind_at, 1).map(Some),
		ReminderRepeat::Weekly => add_days(&reminder.remind_at, 7).map(Some),
		ReminderRepeat::WeeklyDays { days } => next_weekly_day(&reminder.remind_at, days).map(Some),
		ReminderRepeat::Monthly => add_months(&reminder.remind_at, 1).map(Some),
		ReminderRepeat::Yearly => add_months(&reminder.remind_at, 12).map(Some),
	}
}

fn add_days(remind_at: &str, days: u64) -> Result<(String, u64)> {
	let next = parse_remind_at(remind_at)?
		.checked_add_days(Days::new(days))
		.ok_or_else(|| err!(Request(InvalidParam("Reminder next occurrence overflow."))))?;
	Ok(format_remind_at(next))
}

fn next_weekly_day(remind_at: &str, days: &[u8]) -> Result<(String, u64)> {
	let current = parse_remind_at(remind_at)?;
	let mut normalized = days.to_vec();
	normalized.sort_unstable();
	normalized.dedup();

	for offset in 1..=7 {
		let candidate = current
			.checked_add_days(Days::new(offset))
			.ok_or_else(|| err!(Request(InvalidParam("Reminder next occurrence overflow."))))?;
		let weekday = candidate.weekday().number_from_monday() as u8;
		if normalized.contains(&weekday) {
			return Ok(format_remind_at(candidate));
		}
	}

	add_days(remind_at, 7)
}

fn add_months(remind_at: &str, months: u32) -> Result<(String, u64)> {
	let current = parse_remind_at(remind_at)?;
	if let Some(next) = current.checked_add_months(Months::new(months)) {
		return Ok(format_remind_at(next));
	}

	let target_month_start = current
		.with_day(1)
		.and_then(|date| date.checked_add_months(Months::new(months)))
		.ok_or_else(|| err!(Request(InvalidParam("Reminder next occurrence overflow."))))?;
	let last_day = last_day_of_month(target_month_start.year(), target_month_start.month());
	let next = target_month_start
		.with_day(last_day)
		.ok_or_else(|| err!(Request(InvalidParam("Invalid reminder month day."))))?;
	Ok(format_remind_at(next))
}

fn parse_remind_at(remind_at: &str) -> Result<DateTime<FixedOffset>> {
	DateTime::parse_from_rfc3339(remind_at)
		.map_err(|_| err!(Request(InvalidParam("remind_at must be RFC3339 datetime."))))
}

fn format_remind_at(dt: DateTime<FixedOffset>) -> (String, u64) {
	(dt.to_rfc3339(), dt.timestamp_millis().max(0) as u64)
}

fn last_day_of_month(year: i32, month: u32) -> u32 {
	let offset = FixedOffset::east_opt(0).expect("zero offset is valid");
	let first_next_month = if month == 12 {
		offset.with_ymd_and_hms(year + 1, 1, 1, 0, 0, 0)
	} else {
		offset.with_ymd_and_hms(year, month + 1, 1, 0, 0, 0)
	}
	.single()
	.expect("first day of month is valid");
	first_next_month
		.checked_sub_days(Days::new(1))
		.expect("previous day is valid")
		.day()
}

fn push_gateway_notify_url(base_url: &str, notification_path: &str) -> String {
	if base_url.ends_with(notification_path) {
		return base_url.to_owned();
	}

	format!(
		"{}/{}",
		base_url.trim_end_matches('/'),
		notification_path.trim_start_matches('/')
	)
}

fn room_reminder_key(room_id: &RoomId, reminder_id: &str) -> Vec<u8> {
	let mut key = room_reminder_prefix(room_id);
	key.extend_from_slice(reminder_id.as_bytes());
	key
}

fn room_reminder_prefix(room_id: &RoomId) -> Vec<u8> {
	let mut prefix = Vec::with_capacity(room_id.as_bytes().len() + 1);
	prefix.extend_from_slice(room_id.as_bytes());
	prefix.push(0xFF);
	prefix
}

fn reminder_id_from_room_key<'a>(prefix: &[u8], key: &'a [u8]) -> Option<&'a str> {
	if key.len() <= prefix.len() {
		return None;
	}
	std::str::from_utf8(&key[prefix.len()..]).ok()
}

impl Service {
	fn store_reminder(&self, reminder: &Reminder) {
		self.store_reminder_only(reminder);
		self.db
			.remindertime_reminderid
			.put((reminder.remind_at_ms, reminder.reminder_id.as_str()), 0_u8);
		self.db.roomid_reminderids.raw_put(
			room_reminder_key(&reminder.room_id, &reminder.reminder_id),
			0_u8,
		);
	}

	fn store_reminder_only(&self, reminder: &Reminder) {
		self.db
			.reminderid_reminder
			.put(reminder.reminder_id.as_str(), Json(reminder));
	}

	fn remove_due_index(&self, reminder: &Reminder) {
		self.remove_due_index_at(reminder.remind_at_ms, &reminder.reminder_id);
	}

	fn remove_due_index_at(&self, remind_at_ms: u64, reminder_id: &str) {
		self.db
			.remindertime_reminderid
			.del((remind_at_ms, reminder_id));
	}

	fn remove_room_index(&self, reminder: &Reminder) {
		self.db
			.roomid_reminderids
			.remove(&room_reminder_key(&reminder.room_id, &reminder.reminder_id));
	}

	fn wake_worker(&self) {
		self.interrupt.0.send(()).ok();
	}
}
