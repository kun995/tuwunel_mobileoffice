use axum::{extract::State, http::Uri};
use futures::{FutureExt, join};
use ruma::{
	api::client::membership::invite_user::{
		self,
		v3::{InvitationRecipient, InviteUserId},
	},
	events::room::member::MembershipState,
};
use tuwunel_core::{Err, Result};

use super::banned_room_check;
use crate::{ClientIp, Ruma, client::utils::invite_check};

fn parse_invite_batch_id(uri: &Uri) -> Option<String> {
	let batch_id = uri
		.query()
		.and_then(|q| q.split('&').find_map(|part| part.strip_prefix("batch_id=")))
		.filter(|value| !value.is_empty() && value.len() <= 128)?;

	Some(batch_id.to_owned())
}

#[cfg(test)]
mod tests {
	use super::parse_invite_batch_id;
	use axum::http::Uri;

	#[test]
	fn parses_batch_id_from_query() {
		let uri: Uri = "/_matrix/client/v3/rooms/!r:hs/invite?batch_id=batch-1&x=1"
			.parse()
			.expect("valid uri");

		assert_eq!(parse_invite_batch_id(&uri), Some("batch-1".to_owned()));
	}

	#[test]
	fn rejects_empty_batch_id() {
		let uri: Uri = "/_matrix/client/v3/rooms/!r:hs/invite?batch_id="
			.parse()
			.expect("valid uri");

		assert_eq!(parse_invite_batch_id(&uri), None);
	}
}

/// # `POST /_matrix/client/r0/rooms/{roomId}/invite`
///
/// Tries to send an invite event into the room.
#[tracing::instrument(skip_all, fields(%client), name = "invite")]
pub(crate) async fn invite_user_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	uri: Uri,
	body: Ruma<invite_user::v3::Request>,
) -> Result<invite_user::v3::Response> {
	let sender_user = body.sender_user();
	let invite_batch_id = parse_invite_batch_id(&uri);

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
				if let Ok(limit_event) =
					serde_json::from_str::<serde_json::Value>(limit_data.content.json().get())
				{
					if let Some(limit) = limit_event.get("limit").and_then(|v| v.as_u64()) {
						let total = joined_count + invited_count;
						if total >= limit {
							use ruma::api::error::ErrorKind;
							return Err(tuwunel_core::Error::Request(
								ErrorKind::LimitExceeded(ruma::api::error::LimitExceededErrorData { retry_after: None }),
								"Room has reached maximum member limit (including pending invites)"
									.into(),
								http::StatusCode::BAD_REQUEST,
							));
						}
					}
				}
			}
		}
	}

	let InvitationRecipient::UserId(InviteUserId { user_id, reason }) = &body.recipient else {
		return Err!(Request(ThreepidDenied(
			"Third party identifiers are not implemented"
		)));
	};

	// TODO: this should be in the service, but moving it from here would run the
	// sender's ignore-list check before the banned check, revealing the ignore
	// state to the sending user if the recipient is banned
	let member = services.state_accessor.get_member(room_id, user_id);

	let ignored = services.users.user_is_ignored(user_id, sender_user);

	let (member, ignored) = join!(member, ignored);

	if member.is_ok_and(|member| member.membership == MembershipState::Ban) {
		return Err!(Request(Forbidden("User is banned from this room.")));
	}

	if ignored {
		// silently drop the invite to the recipient if they've been ignored by the
		// sender, pretend it worked
		return Ok(invite_user::v3::Response {});
	}

	services
		.membership
		.invite(
			sender_user,
			user_id,
			room_id,
			reason.as_ref(),
			false,
			invite_batch_id.as_deref(),
		)
		.boxed()
		.await?;

	Ok(invite_user::v3::Response {})
}
