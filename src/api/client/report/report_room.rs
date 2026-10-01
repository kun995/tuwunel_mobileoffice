use axum::extract::State;
use ruma::api::client::room::report_room;
use tuwunel_core::{Err, Result, info};

use super::REASON_MAX_LEN;
use crate::{ClientIp, Ruma};

/// # `POST /_matrix/client/v3/rooms/{roomId}/report`
///
/// Reports an abusive room to homeserver admins
#[tracing::instrument(skip_all, fields(%client), name = "report_room")]
pub(crate) async fn report_room_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<report_room::v3::Request>,
) -> Result<report_room::v3::Response> {
	let sender_user = body.sender_user();

	info!(
		"Received room report by user {sender_user} for room {} with reason: \"{}\"",
		body.room_id, body.reason,
	);

	if body.reason.len() > REASON_MAX_LEN {
		return Err!(Request(InvalidParam(
			"Reason too long, should be {REASON_MAX_LEN} characters or fewer"
		)));
	}

	super::delay_response().await;

	if !services
		.state_cache
		.server_in_room(&services.server.name, &body.room_id)
		.await
	{
		return Err!(Request(NotFound(
			"Room does not exist to us, no local users have joined at all"
		)));
	}

	services
		.admin
		.send_report(&format!(
			"@room Room report received from {}\nReport Reason: {}\n\nRoom ID: {}",
			sender_user, body.reason, body.room_id,
		))
		.await;

	Ok(report_room::v3::Response {})
}
