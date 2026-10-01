use axum::{
	Json,
	extract::{Path, State},
};
use axum_extra::{
	TypedHeader,
	headers::{Authorization, authorization::Bearer},
};
use chrono::DateTime;
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, api::error::ErrorKind};
use serde::{Deserialize, Serialize};
use tuwunel_core::{Err, Error, Result, err, utils::millis_since_unix_epoch};
use tuwunel_service::rooms::reminders::{
	NewReminder, Reminder, ReminderRepeat, ReminderScope, ReminderStatus, UpdateReminder,
};

#[derive(Debug, Deserialize)]
pub(crate) struct CreateReminderBody {
	pub title: String,
	pub remind_at: String,
	#[serde(default = "default_repeat")]
	pub repeat: ReminderRepeat,
	#[serde(default = "default_scope")]
	pub scope: ReminderScope,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateReminderBody {
	pub title: String,
	pub remind_at: String,
	#[serde(default = "default_repeat")]
	pub repeat: ReminderRepeat,
	#[serde(default = "default_scope")]
	pub scope: ReminderScope,
}

#[derive(Debug, Serialize)]
pub(crate) struct ReminderResponse {
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

#[derive(Debug, Serialize)]
pub(crate) struct ReminderListResponse {
	pub reminders: Vec<ReminderResponse>,
}

/// # `POST /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/reminders`
///
/// Creates a persistent chat reminder and appends a reminder-created system event.
pub(crate) async fn create_reminder_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path(room_id): Path<ruma::OwnedRoomId>,
	Json(body): Json<CreateReminderBody>,
) -> Result<Json<ReminderResponse>> {
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken(ruma::api::error::UnknownTokenErrorData { soft_logout: false }),
				"Unknown access token.",
			)
		})?;

	if !services.state_cache.is_joined(&sender_user, &room_id).await {
		return Err!(Request(Forbidden("You are not a member of this room.")));
	}

	let title = validate_title(&body.title)?;
	validate_repeat(&body.repeat)?;
	let remind_at_ms = parse_remind_at_ms(&body.remind_at)?;
	if remind_at_ms <= millis_since_unix_epoch() {
		return Err!(Request(InvalidParam("remind_at must be in the future.")));
	}

	let reminder = services
		.reminders
		.create_reminder(NewReminder {
			room_id: (*room_id).to_owned(),
			title,
			remind_at: body.remind_at,
			remind_at_ms,
			repeat: body.repeat,
			scope: body.scope,
			created_by: sender_user,
		})
		.await?;

	Ok(Json(reminder.into()))
}

/// # `PUT /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/reminders/{reminderId}`
///
/// Updates an active reminder created by the authenticated user.
pub(crate) async fn update_reminder_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path((room_id, reminder_id)): Path<(ruma::OwnedRoomId, String)>,
	Json(body): Json<UpdateReminderBody>,
) -> Result<Json<ReminderResponse>> {
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken(ruma::api::error::UnknownTokenErrorData { soft_logout: false }),
				"Unknown access token.",
			)
		})?;

	if !services.state_cache.is_joined(&sender_user, &room_id).await {
		return Err!(Request(Forbidden("You are not a member of this room.")));
	}

	let existing = services
		.reminders
		.get_reminder(&reminder_id)
		.await
		.map_err(|_| err!(Request(NotFound("Reminder not found."))))?;

	if existing.room_id.as_str() != room_id.as_str()
		|| !reminder_visible_to(&existing, &sender_user)
	{
		return Err!(Request(NotFound("Reminder not found.")));
	}

	let title = validate_title(&body.title)?;
	validate_repeat(&body.repeat)?;
	let remind_at_ms = parse_remind_at_ms(&body.remind_at)?;
	if remind_at_ms <= millis_since_unix_epoch() {
		return Err!(Request(InvalidParam("remind_at must be in the future.")));
	}

	let reminder = services
		.reminders
		.update_reminder(
			&reminder_id,
			&sender_user,
			UpdateReminder {
				title,
				remind_at: body.remind_at,
				remind_at_ms,
				repeat: body.repeat,
				scope: body.scope,
			},
		)
		.await?;

	Ok(Json(reminder.into()))
}

/// # `GET /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/reminders`
///
/// Lists active reminders visible to the authenticated user in a room.
pub(crate) async fn list_reminders_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path(room_id): Path<ruma::OwnedRoomId>,
) -> Result<Json<ReminderListResponse>> {
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken(ruma::api::error::UnknownTokenErrorData { soft_logout: false }),
				"Unknown access token.",
			)
		})?;

	if !services.state_cache.is_joined(&sender_user, &room_id).await {
		return Err!(Request(Forbidden("You are not a member of this room.")));
	}

	let reminders = services
		.reminders
		.reminders_for_room(&room_id)
		.await
		.into_iter()
		.filter(|reminder| reminder_visible_to(reminder, &sender_user))
		.map(ReminderResponse::from)
		.collect();

	Ok(Json(ReminderListResponse { reminders }))
}

/// # `GET /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/reminders/{reminderId}`
pub(crate) async fn get_reminder_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path((room_id, reminder_id)): Path<(ruma::OwnedRoomId, String)>,
) -> Result<Json<ReminderResponse>> {
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken(ruma::api::error::UnknownTokenErrorData { soft_logout: false }),
				"Unknown access token.",
			)
		})?;

	if !services.state_cache.is_joined(&sender_user, &room_id).await {
		return Err!(Request(Forbidden("You are not a member of this room.")));
	}

	let reminder = services
		.reminders
		.get_reminder(&reminder_id)
		.await
		.map_err(|_| err!(Request(NotFound("Reminder not found."))))?;

	if reminder.room_id.as_str() != room_id.as_str()
		|| !reminder_visible_to(&reminder, &sender_user)
	{
		return Err!(Request(NotFound("Reminder not found.")));
	}

	Ok(Json(reminder.into()))
}

/// # `DELETE /_matrix/client/unstable/org.tuwunel/rooms/{roomId}/reminders/{reminderId}`
pub(crate) async fn cancel_reminder_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path((room_id, reminder_id)): Path<(ruma::OwnedRoomId, String)>,
) -> Result<Json<ReminderResponse>> {
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken(ruma::api::error::UnknownTokenErrorData { soft_logout: false }),
				"Unknown access token.",
			)
		})?;

	if !services.state_cache.is_joined(&sender_user, &room_id).await {
		return Err!(Request(Forbidden("You are not a member of this room.")));
	}

	let reminder = services
		.reminders
		.get_reminder(&reminder_id)
		.await
		.map_err(|_| err!(Request(NotFound("Reminder not found."))))?;

	if reminder.room_id.as_str() != room_id.as_str() {
		return Err!(Request(NotFound("Reminder not found.")));
	}

	let reminder = services
		.reminders
		.cancel_reminder(&reminder_id, &sender_user)
		.await?;

	Ok(Json(reminder.into()))
}

fn default_repeat() -> ReminderRepeat {
	ReminderRepeat::None
}

fn default_scope() -> ReminderScope {
	ReminderScope::Room
}

fn validate_title(title: &str) -> Result<String> {
	let title = title.trim();
	if title.is_empty() {
		return Err!(Request(InvalidParam("title must not be empty.")));
	}

	if title.chars().count() > 100 {
		return Err!(Request(InvalidParam(
			"title must not exceed 100 characters."
		)));
	}

	if title.chars().any(char::is_control) {
		return Err!(Request(InvalidParam(
			"title must not contain control characters."
		)));
	}

	Ok(title.to_owned())
}

fn validate_repeat(repeat: &ReminderRepeat) -> Result {
	if let ReminderRepeat::WeeklyDays { days } = repeat {
		if days.is_empty() {
			return Err!(Request(InvalidParam("repeat.days must not be empty.")));
		}

		if days.iter().any(|day| !(1..=7).contains(day)) {
			return Err!(Request(InvalidParam(
				"repeat.days values must be from 1 to 7."
			)));
		}
	}

	Ok(())
}

fn parse_remind_at_ms(remind_at: &str) -> Result<u64> {
	let remind_at = DateTime::parse_from_rfc3339(remind_at)
		.map_err(|_| err!(Request(InvalidParam("remind_at must be RFC3339 datetime."))))?;
	let timestamp = remind_at.timestamp_millis();
	if timestamp < 0 {
		return Err!(Request(InvalidParam("remind_at must be after Unix epoch.")));
	}

	Ok(timestamp as u64)
}

fn reminder_visible_to(reminder: &Reminder, user_id: &ruma::UserId) -> bool {
	reminder.status == ReminderStatus::Active
		&& (reminder.scope == ReminderScope::Room
			|| reminder.created_by.as_str() == user_id.as_str())
}

impl From<Reminder> for ReminderResponse {
	fn from(reminder: Reminder) -> Self {
		Self {
			reminder_id: reminder.reminder_id,
			room_id: reminder.room_id,
			title: reminder.title,
			remind_at: reminder.remind_at,
			remind_at_ms: reminder.remind_at_ms,
			repeat: reminder.repeat,
			scope: reminder.scope,
			status: reminder.status,
			created_by: reminder.created_by,
			created_at_ms: reminder.created_at_ms,
			last_triggered_at_ms: reminder.last_triggered_at_ms,
			event_id: reminder.event_id,
		}
	}
}
