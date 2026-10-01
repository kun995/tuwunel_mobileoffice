use axum::extract::State;
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, RoomId,
	api::client::membership::{join_room_by_id, join_room_by_id_or_alias},
};
use tuwunel_core::{Result, warn};
use tuwunel_service::membership::Join;

use super::banned_room_check;
use crate::{ClientIp, Ruma};

/// # `POST /_matrix/client/r0/rooms/{roomId}/join`
///
/// Tries to join the sender user into a room.
///
/// - If the server knowns about this room: creates the join event and does auth
///   rules locally
/// - If the server does not know about the room: asks other servers over
///   federation
#[tracing::instrument(skip_all, fields(%client), name = "join")]
pub(crate) async fn join_room_by_id_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<join_room_by_id::v3::Request>,
) -> Result<join_room_by_id::v3::Response> {
	let sender_user = body.sender_user();

	let room_id: &RoomId = &body.room_id;

	banned_room_check(&services, sender_user, room_id, None, client).await?;

	// Check room member limit
	if let Ok(current_count) = services.state_cache.room_joined_count(room_id).await {
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
					if current_count >= limit {
						use ruma::api::error::ErrorKind;
						return Err(tuwunel_core::Error::Request(
							ErrorKind::LimitExceeded(ruma::api::error::LimitExceededErrorData { retry_after: None }),
							"Room has reached maximum member limit".into(),
							http::StatusCode::BAD_REQUEST,
						));
					}
				}
			}
		}
	}

	let extra_content = extra_member_content(body.json_body.as_ref());

	let mut errors = 0_usize;
	while let Err(e) = services
		.membership
		.join(Join {
			sender_user,
			room_id,
			orig_room_id: None,
			reason: body.reason.clone(),
			servers: &[],
			is_appservice: body.appservice_info.is_some(),
			extra_content: extra_content.clone(),
		})
		.await
	{
		errors = errors.saturating_add(1);
		if errors >= services.config.max_join_attempts_per_join_request {
			warn!(
				"Several servers failed. Giving up for this request. Try again for different \
				 server selection."
			);
			return Err(e);
		}
	}

	Ok(join_room_by_id::v3::Response {
		room_id: room_id.to_owned(),
	})
}

/// # `POST /_matrix/client/r0/join/{roomIdOrAlias}`
///
/// Tries to join the sender user into a room.
///
/// - If the server knowns about this room: creates the join event and does auth
///   rules locally
/// - If the server does not know about the room: use the server name query
///   param if specified. if not specified, asks other servers over federation
///   via room alias server name and room ID server name
#[tracing::instrument(skip_all, fields(%client), name = "join")]
pub(crate) async fn join_room_by_id_or_alias_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<join_room_by_id_or_alias::v3::Request>,
) -> Result<join_room_by_id_or_alias::v3::Response> {
	let sender_user = body.sender_user();
	let appservice_info = &body.appservice_info;

	let (room_id, servers) = services
		.alias
		.maybe_resolve_with_servers(&body.room_id_or_alias, Some(&body.via))
		.await?;

	banned_room_check(
		&services,
		sender_user,
		&room_id,
		Some(&body.room_id_or_alias),
		client,
	)
	.await?;

	// Check room member limit
	if let Ok(current_count) = services.state_cache.room_joined_count(&room_id).await {
		// Try to get member limit from room state
		if let Ok(limit_data) = services
			.state_accessor
			.room_state_get(&room_id, &"im.tuwunel.room.member_limit".into(), "")
			.await
		{
			// Parse the limit from state event content
			if let Ok(limit_event) =
				serde_json::from_str::<serde_json::Value>(limit_data.content.json().get())
			{
				if let Some(limit) = limit_event.get("limit").and_then(|v| v.as_u64()) {
					if current_count >= limit {
						use ruma::api::error::ErrorKind;
						return Err(tuwunel_core::Error::Request(
							ErrorKind::LimitExceeded(ruma::api::error::LimitExceededErrorData { retry_after: None }),
							"Room has reached maximum member limit".into(),
							http::StatusCode::BAD_REQUEST,
						));
					}
				}
			}
		}
	}

	let extra_content = extra_member_content(body.json_body.as_ref());

	let mut errors = 0_usize;
	while let Err(e) = services
		.membership
		.join(Join {
			sender_user,
			room_id: &room_id,
			orig_room_id: Some(&body.room_id_or_alias),
			reason: body.reason.clone(),
			servers: &servers,
			is_appservice: appservice_info.is_some(),
			extra_content: extra_content.clone(),
		})
		.await
	{
		errors = errors.saturating_add(1);
		if errors >= services.config.max_join_attempts_per_join_request {
			warn!(
				"Several servers failed. Giving up for this request. Try again for different \
				 server selection."
			);
			return Err(e);
		}
	}

	Ok(join_room_by_id_or_alias::v3::Response {
		room_id: room_id.clone(),
	})
}

const RESERVED_JOIN_KEYS: [&str; 3] = [
	"reason",
	"third_party_signed",
	"join_authorised_via_users_server",
];

// Drop recognized and server-owned keys the client must not set.
fn extra_member_content(json_body: Option<&CanonicalJsonValue>) -> Option<CanonicalJsonObject> {
	let CanonicalJsonValue::Object(object) = json_body? else {
		return None;
	};

	let extra: CanonicalJsonObject = object
		.iter()
		.filter(|(key, _)| !RESERVED_JOIN_KEYS.contains(&key.as_str()))
		.map(|(key, value)| (key.clone(), value.clone()))
		.collect();

	(!extra.is_empty()).then_some(extra)
}
