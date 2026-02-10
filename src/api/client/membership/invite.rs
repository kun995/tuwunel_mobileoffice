use axum::extract::State;
use axum_client_ip::InsecureClientIp;
use futures::{FutureExt, join};
use ruma::{api::client::membership::invite_user, events::room::member::MembershipState};
use tuwunel_core::{Err, Result};

use super::banned_room_check;
use crate::{Ruma, client::utils::invite_check};

/// # `POST /_matrix/client/r0/rooms/{roomId}/invite`
///
/// Tries to send an invite event into the room.
#[tracing::instrument(skip_all, fields(%client), name = "invite")]
pub(crate) async fn invite_user_route(
	State(services): State<crate::State>,
	InsecureClientIp(client): InsecureClientIp,
	body: Ruma<invite_user::v3::Request>,
) -> Result<invite_user::v3::Response> {
	let sender_user = body.sender_user();

	let room_id = &body.room_id;

	invite_check(&services, sender_user, room_id).await?;

	banned_room_check(&services, sender_user, room_id, None, client).await?;

	// Check room member limit (count invited + joined)
	if let Ok(joined_count) = services.state_cache.room_joined_count(room_id).await {
		if let Ok(invited_count) = services.state_cache.room_invited_count(room_id).await {
			// Try to get member limit from room state
			if let Ok(limit_data) = services
				.state_accessor
				.room_state_get(room_id, &"im.tuwunel.room.member_limit".into(), "")
				.await
			{
				// Parse the limit from state event content
				if let Ok(limit_event) = serde_json::from_str::<serde_json::Value>(limit_data.content.get()) {
					if let Some(limit) = limit_event.get("limit").and_then(|v| v.as_u64()) {
						let total = joined_count + invited_count;
						if total >= limit {
						use ruma::api::client::error::ErrorKind;
					return Err(tuwunel_core::Error::Request(
						ErrorKind::LimitExceeded {
							retry_after: None,
						},
						"Room has reached maximum member limit (including pending invites)".into(),
						http::StatusCode::BAD_REQUEST,
					));
					}
					}
				}
			}
		}
	}

	let invite_user::v3::InvitationRecipient::UserId { user_id } = &body.recipient else {
		return Err!(Request(ThreepidDenied("Third party identifiers are not implemented")));
	};

	let sender_ignored_recipient = services
		.users
		.user_is_ignored(sender_user, user_id);

	let recipient_ignored_by_sender = services
		.users
		.user_is_ignored(user_id, sender_user);

	let (sender_ignored_recipient, recipient_ignored_by_sender) =
		join!(sender_ignored_recipient, recipient_ignored_by_sender);

	if sender_ignored_recipient {
		return Ok(invite_user::v3::Response {});
	}

	// TODO: this should be in the service, but moving it from here would
	// trigger the recipient_ignored_by_sender check before the banned check,
	// revealing the ignore state to the sending user
	if let Ok(target_user_membership) = services
		.state_accessor
		.get_member(room_id, user_id)
		.await && target_user_membership.membership == MembershipState::Ban
	{
		return Err!(Request(Forbidden("User is banned from this room.")));
	}

	if recipient_ignored_by_sender {
		// silently drop the invite to the recipient if they've been ignored by the
		// sender, pretend it worked
		return Ok(invite_user::v3::Response {});
	}

	services
		.membership
		.invite(sender_user, user_id, room_id, body.reason.as_ref(), false)
		.boxed()
		.await?;

	Ok(invite_user::v3::Response {})
}
