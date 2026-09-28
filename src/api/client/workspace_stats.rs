use axum::{
	Json,
	extract::{Path, State},
};
use axum_extra::{
	TypedHeader,
	headers::{Authorization, authorization::Bearer},
};
use chrono::{DateTime, SecondsFormat, Utc};
use futures::{StreamExt, TryStreamExt, pin_mut, stream};
use ruma::{OwnedRoomId, UserId, api::client::error::ErrorKind, events::TimelineEventType};
use serde::Serialize;
use tuwunel_core::{Err, Error, Result};
use tuwunel_service::Services;

const SCAN_CONCURRENCY: usize = 8;

#[derive(Debug, Serialize)]
pub(crate) struct WorkspaceStatsResponse {
	pub workspace_id: String,
	/// True when the workspace has at least one indexed user or room, or a timeline event.
	pub has_data: bool,
	pub has_message_data: bool,
	pub indexed_user_count: usize,
	pub active_user_count: usize,
	pub stale_user_count: usize,
	pub room_count: usize,
	pub active_room_count: usize,
	pub stale_room_count: usize,
	pub rooms_with_messages: usize,
	pub timeline_event_count: usize,
	pub message_count: usize,
	pub last_data_ts: Option<u64>,
	pub last_data_at: Option<String>,
	pub last_message_ts: Option<u64>,
	pub last_message_at: Option<String>,
	pub complete: bool,
	pub scan_errors: Vec<WorkspaceStatsScanError>,
}

#[derive(Debug, Serialize)]
pub(crate) struct WorkspaceStatsScanError {
	pub room_id: OwnedRoomId,
	pub error: String,
}

struct RoomStats {
	room_id: OwnedRoomId,
	active: bool,
	timeline_event_count: usize,
	message_count: usize,
	last_data_ts: Option<u64>,
	last_message_ts: Option<u64>,
	scan_error: Option<String>,
}

/// # `GET /_matrix/client/unstable/org.tuwunel/workspaces/{workspace_id}/stats`
///
/// Returns workspace-scoped data statistics. This endpoint is intentionally
/// admin-only because it can inspect room and user mappings outside the caller's
/// own account.
pub(crate) async fn workspace_stats_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path(workspace_id): Path<String>,
) -> Result<Json<WorkspaceStatsResponse>> {
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken { soft_logout: false },
				"Unknown access token.",
			)
		})?;

	let sender_user_ref: &UserId = sender_user.as_ref();
	if !services.users.is_admin(sender_user_ref).await {
		return Err!(Request(Forbidden(
			"You do not have permission to view workspace statistics.",
		)));
	}

	let workspace_id = workspace_id.trim().to_owned();
	if workspace_id.is_empty() {
		return Err!(Request(InvalidParam("workspace_id must not be empty.")));
	}

	let indexed_user_ids = services
		.workspace
		.users_by_workspace(&workspace_id)
		.collect::<Vec<_>>()
		.await;
	let indexed_user_count = indexed_user_ids.len();
	let user_statuses = stream::iter(indexed_user_ids)
		.map(|user_id| async move {
			let user_id_ref: &UserId = user_id.as_ref();
			services.users.exists(user_id_ref).await
		})
		.buffer_unordered(SCAN_CONCURRENCY)
		.collect::<Vec<_>>()
		.await;
	let active_user_count = user_statuses
		.iter()
		.filter(|exists| **exists)
		.count();

	let mut room_ids = services
		.workspace
		.rooms_by_workspace(&workspace_id)
		.collect::<Vec<_>>()
		.await;
	room_ids.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
	room_ids.dedup();

	let room_stats = stream::iter(room_ids)
		.map(|room_id| inspect_room(&services, room_id))
		.buffer_unordered(SCAN_CONCURRENCY)
		.collect::<Vec<_>>()
		.await;

	let room_count = room_stats.len();
	let active_room_count = room_stats
		.iter()
		.filter(|room| room.active)
		.count();
	let stale_room_count = room_count.saturating_sub(active_room_count);
	let rooms_with_messages = room_stats
		.iter()
		.filter(|room| room.message_count > 0)
		.count();
	let timeline_event_count = room_stats
		.iter()
		.fold(0usize, |total, room| total.saturating_add(room.timeline_event_count));
	let message_count = room_stats
		.iter()
		.fold(0usize, |total, room| total.saturating_add(room.message_count));

	let last_data_ts = room_stats
		.iter()
		.filter_map(|room| room.last_data_ts)
		.max();
	let last_message_ts = room_stats
		.iter()
		.filter_map(|room| room.last_message_ts)
		.max();
	let scan_errors = room_stats
		.into_iter()
		.filter_map(|room| {
			room.scan_error
				.map(|error| WorkspaceStatsScanError { room_id: room.room_id, error })
		})
		.collect::<Vec<_>>();
	let mut scan_errors = scan_errors;
	scan_errors.sort_unstable_by(|left, right| left.room_id.as_str().cmp(right.room_id.as_str()));

	Ok(Json(WorkspaceStatsResponse {
		workspace_id,
		has_data: indexed_user_count > 0 || room_count > 0 || timeline_event_count > 0,
		has_message_data: message_count > 0,
		indexed_user_count,
		active_user_count,
		stale_user_count: indexed_user_count.saturating_sub(active_user_count),
		room_count,
		active_room_count,
		stale_room_count,
		rooms_with_messages,
		timeline_event_count,
		message_count,
		last_data_ts,
		last_data_at: last_data_ts.and_then(format_timestamp),
		last_message_ts,
		last_message_at: last_message_ts.and_then(format_timestamp),
		complete: scan_errors.is_empty(),
		scan_errors,
	}))
}

async fn inspect_room(services: &Services, room_id: OwnedRoomId) -> RoomStats {
	if !services.metadata.exists(&room_id).await {
		return RoomStats {
			room_id,
			active: false,
			timeline_event_count: 0,
			message_count: 0,
			last_data_ts: None,
			last_message_ts: None,
			scan_error: None,
		};
	}

	let room_id_for_scan = room_id.clone();
	let pdus = services
		.timeline
		.pdus(None, &room_id_for_scan, None);
	pin_mut!(pdus);

	let mut timeline_event_count = 0usize;
	let mut message_count = 0usize;
	let mut last_data_ts = None;
	let mut last_message_ts = None;
	let mut scan_error = None;

	loop {
		let (_, pdu) = match pdus.try_next().await {
			| Ok(Some(item)) => item,
			| Ok(None) => break,
			| Err(error) => {
				scan_error = Some(error.to_string());
				break;
			},
		};

		timeline_event_count = timeline_event_count.saturating_add(1);
		let timestamp = u64::from(pdu.origin_server_ts);
		update_max(&mut last_data_ts, timestamp);

		if matches!(pdu.kind, TimelineEventType::RoomMessage | TimelineEventType::RoomEncrypted) {
			message_count = message_count.saturating_add(1);
			update_max(&mut last_message_ts, timestamp);
		}
	}

	RoomStats {
		room_id,
		active: true,
		timeline_event_count,
		message_count,
		last_data_ts,
		last_message_ts,
		scan_error,
	}
}

fn update_max(current: &mut Option<u64>, candidate: u64) {
	if current.is_none_or(|value| candidate > value) {
		*current = Some(candidate);
	}
}

fn format_timestamp(timestamp_ms: u64) -> Option<String> {
	let timestamp_ms = i64::try_from(timestamp_ms).ok()?;
	DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
		.map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::Millis, true))
}

#[cfg(test)]
mod tests {
	use super::format_timestamp;

	#[test]
	fn formats_timestamp_as_utc_rfc3339_with_milliseconds() {
		assert_eq!(
			format_timestamp(1_783_940_520_123),
			Some("2026-07-13T12:20:00.123Z".to_owned())
		);
	}

	#[test]
	fn rejects_timestamps_that_do_not_fit_chrono() {
		assert_eq!(format_timestamp(u64::MAX), None);
	}
}
