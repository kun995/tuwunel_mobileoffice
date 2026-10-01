use std::collections::BTreeMap;

use axum::extract::State;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use ruma::{
	CanonicalJsonObject, EventEncryptionAlgorithm, Int, OwnedRoomAliasId, OwnedRoomId, OwnedUserId,
	RoomAliasId, RoomId, RoomVersionId, UserId,
	api::client::room::{
		self,
		create_room::{
			self, RoomPowerLevelsContentOverride,
			v3::{CreationContent, RoomPreset},
		},
	},
	events::{
		StateEventType, TimelineEventType,
		ignored_user_list::IgnoredUserListEventContent,
		room::{
			canonical_alias::RoomCanonicalAliasEventContent,
			create::{PreviousRoom, RoomCreateEventContent},
			encryption::RoomEncryptionEventContent,
			guest_access::{GuestAccess, RoomGuestAccessEventContent},
			history_visibility::{HistoryVisibility, RoomHistoryVisibilityEventContent},
			join_rules::{JoinRule, RoomJoinRulesEventContent},
			member::{MembershipState, RoomMemberEventContent},
			name::RoomNameEventContent,
			power_levels::RoomPowerLevelsEventContent,
			topic::RoomTopicEventContent,
		},
		space::child::SpaceChildEventContent,
	},
	int,
	room_version_rules::{RoomIdFormatVersion, RoomVersionRules},
	serde::{JsonObject, Raw},
};
use serde::Deserialize;
use serde_json::{
	Value as JsonValue, json,
	value::{RawValue as RawJsonValue, to_raw_value},
};
use tuwunel_core::{
	Err, Result, debug_info, debug_warn, err, info,
	matrix::{
		StateKey,
		pdu::{Content, PduBuilder},
		room_version,
	},
	utils::{BoolExt, IterStream, ReadyExt, option::OptionExt, result::FlatOk},
	warn,
};
use tuwunel_service::{
	Services, appservice::RegistrationInfo, membership::INITIAL_ROOM_CREATION_FIELD,
	rooms::state::RoomMutexGuard,
};

use crate::{Ruma, client::utils::invite_check};

fn initial_room_creation_unsigned() -> BTreeMap<String, serde_json::Value> {
	BTreeMap::from([(
		INITIAL_ROOM_CREATION_FIELD.to_owned(),
		serde_json::Value::Bool(true),
	)])
}

/// # `POST /_matrix/client/v3/createRoom`
///
/// Creates a new room.
///
/// - Room ID is randomly generated
/// - Create alias if `room_alias_name` is set
/// - Send create event
/// - Join sender user
/// - Send power levels event
/// - Send canonical room alias
/// - Send join rules
/// - Send history visibility
/// - Send guest access
/// - Send events listed in initial state
/// - Send events implied by `name` and `topic`
/// - Send invite events
pub(crate) async fn create_room_route(
	State(services): State<crate::State>,
	body: Ruma<create_room::v3::Request>,
) -> Result<create_room::v3::Response> {
	can_create_room_check(&services, &body).await?;
	can_publish_directory_check(&services, &body).await?;

	// Figure out preset. We need it for preset specific events
	let preset = body.preset.clone().unwrap_or(match &body.visibility {
		room::Visibility::Public => RoomPreset::PublicChat,
		_ => RoomPreset::PrivateChat, // Room visibility should not be custom
	});

	// Determine room version
	let (room_version, version_rules) = body
		.room_version
		.as_ref()
		.map_or(
			Ok(&services.server.config.default_room_version),
			|version| {
				services
					.config
					.supported_room_version(version)
					.then_ok_or_else(version, || {
						err!(Request(UnsupportedRoomVersion(
							"This server does not support room version {version:?}"
						)))
					})
			},
		)
		.and_then(|version| Ok((version, room_version::rules(version)?)))?;

	// Error on existing alias before committing to creation.
	let alias = body
		.room_alias_name
		.as_ref()
		.map_async(|alias| room_alias_check(&services, alias, body.appservice_info.as_ref()))
		.await
		.transpose()?;

	let sender_user = body.sender_user();
	let can_reuse_direct_room = body.is_direct
		&& body.invite.len() == 1
		&& body.invite_3pid.is_empty()
		&& body.name.is_none()
		&& body.topic.is_none()
		&& body.room_alias_name.is_none()
		&& body.room_id.is_none()
		&& body.initial_state.is_empty()
		&& body.power_level_content_override.is_none()
		&& body.invite[0].as_str() != sender_user.as_str();
	if can_reuse_direct_room {
		let workspace_id = body
			.json_body
			.as_ref()
			.and_then(|value| value.as_object())
			.and_then(|object| object.get("workspace_id"))
			.and_then(|value| value.as_str());

		if let Some(room_id) =
			find_existing_direct_room(&services, sender_user, &body.invite[0], workspace_id).await
		{
			info!(%sender_user, %room_id, target_user = %body.invite[0], "Reusing existing direct room");
			return Ok(create_room::v3::Response::new(room_id));
		}
	}

	// Increment and hold the counter; the room will sync atomically to clients
	// which is preferable.
	let next_count = services.globals.next_count();

	// 1. Create the create event.
	let (room_id, state_lock) = match version_rules.room_id_format {
		RoomIdFormatVersion::V1 => {
			create_create_event_legacy(&services, &body, room_version, &version_rules).await?
		}
		RoomIdFormatVersion::V2 => {
			create_create_event(&services, &body, &preset, room_version, &version_rules)
				.await
				.map_err(|e| {
					err!(Request(InvalidParam(
						"Error while creating m.room.create event: {e}"
					)))
				})?
		}
	};

	let sender_user = body.sender_user();

	// 2. Let the room creator join
	apply_creator_join_pdu(&services, &body, sender_user, &room_id, &state_lock)
		.boxed()
		.await?;

	// 3. Power levels
	apply_power_levels_pdu(
		&services,
		&body,
		&preset,
		&version_rules,
		sender_user,
		&room_id,
		&state_lock,
	)
	.boxed()
	.await?;

	// 4. Canonical room alias
	if let Some(room_alias_id) = &alias {
		apply_canonical_alias_pdu(&services, room_alias_id, sender_user, &room_id, &state_lock)
			.boxed()
			.await?;
	}

	// 5. Events set by preset
	let initial_state = apply_preset_state_pdus(
		&services,
		&body,
		&preset,
		sender_user,
		&room_id,
		&state_lock,
	)
	.boxed()
	.await?;

	// 6. Events listed in initial_state
	apply_initial_state_pdus(
		&services,
		initial_state,
		&preset,
		sender_user,
		&room_id,
		&state_lock,
	)
	.boxed()
	.await?;

	// 7. Events implied by name and topic
	apply_name_and_topic_pdus(&services, &body, sender_user, &room_id, &state_lock)
		.boxed()
		.await?;

	// 7.5 Default member limit (if configured)
	if services.config.default_room_member_limit > 0 {
		services
			.timeline
			.build_and_append_pdu(
				PduBuilder {
					event_type: TimelineEventType::from("im.tuwunel.room.member_limit"),
					content: to_raw_value(&json!({
						"limit": services.config.default_room_member_limit
					}))?.into(),
					state_key: Some(StateKey::new()),
					..Default::default()
				},
				sender_user,
				&room_id,
				&state_lock,
			)
			.boxed()
			.await?;
	}

	// 7.9 Send a non-state "room created" timeline event so the client has a
	// fresh origin_server_ts to sort this room to the top of the room list.
	// All previous creation events are state events and get moved to the `state`
	// section on subsequent syncs — leaving new rooms without messages at the
	// bottom of the list. This timeline event fixes that.
	{
		let creator_display = services
			.profile
			.displayname(sender_user)
			.await
			.unwrap_or_else(|_| sender_user.to_string());
		let creator_message = format!("{creator_display} đã tạo nhóm");
		let mut created_content = serde_json::Map::new();
		created_content.insert(
			"activity_type".to_owned(),
			serde_json::Value::String("room_created".to_owned()),
		);
		created_content.insert(
			"body".to_owned(),
			serde_json::Value::String(creator_message.clone()),
		);
		created_content.insert(
			"msgtype".to_owned(),
			serde_json::Value::String("m.notice".to_owned()),
		);
		created_content.insert(
			"self_text".to_owned(),
			serde_json::Value::String("Bạn đã tạo nhóm".to_owned()),
		);
		created_content.insert(
			"other_text".to_owned(),
			serde_json::Value::String(creator_message),
		);
		created_content.insert(
			"creator_user_id".to_owned(),
			serde_json::Value::String(sender_user.to_string()),
		);
		created_content.insert(
			INITIAL_ROOM_CREATION_FIELD.to_owned(),
			serde_json::Value::Bool(true),
		);
		if let Some(ref name) = body.name {
			created_content.insert(
				"room_name".to_owned(),
				serde_json::Value::String(name.clone()),
			);
		}
		if let Err(e) = services
			.timeline
			.build_and_append_pdu(
				PduBuilder {
					event_type: TimelineEventType::from("im.tuwunel.room.created"),
					content: to_raw_value(&created_content)?.into(),
					state_key: None, // non-state: stays in timeline across syncs
					..Default::default()
				},
				sender_user,
				&room_id,
				&state_lock,
			)
			.boxed()
			.await
		{
			warn!(%e, %room_id, "Failed to send im.tuwunel.room.created event (non-fatal)");
		}
	}

	drop(next_count);
	drop(state_lock);

	// if inviting anyone with room creation and invite check passes
	if (!body.invite.is_empty() || !body.invite_3pid.is_empty())
		&& invite_check(&services, sender_user, &room_id).await.is_ok()
	{
		process_invites(&services, &body, sender_user, &room_id)
			.boxed()
			.await;
	}

	finalize_alias_and_directory(&services, &body, alias.as_deref(), sender_user, &room_id).await?;

	copy_creator_predecessor_push_rule(
		&services,
		body.creation_content.as_ref(),
		sender_user,
		&room_id,
	)
	.await;

	info!("{sender_user} created a room with room ID {room_id}");

	// Handle workspaceId: link room to Matrix Space
	let workspace_id = body
		.json_body
		.as_ref()
		.and_then(|v| v.as_object())
		.and_then(|obj| obj.get("workspace_id"))
		.and_then(|v| v.as_str())
		.map(ToOwned::to_owned);

	if let Some(ref wid) = workspace_id {
		// Check if this is a Space (workspace container)
		let is_space = body
			.json_body
			.as_ref()
			.and_then(|v| v.as_object())
			.and_then(|obj| obj.get("creation_content"))
			.and_then(|v| v.as_object())
			.and_then(|obj| obj.get("type"))
			.and_then(|v| v.as_str())
			== Some("m.space");

		if is_space {
			// Case 1: Tạo Space/workspace → lưu mapping workspaceId → spaceRoomId
			// Đây là thao tác atomic với RocksDB, luôn thành công trừ khi disk lỗi.
			services.workspace.set_space_room_id(wid, &room_id);
			info!("Workspace {wid:?} mapped to Space room {room_id}");
		} else {
			// Case 2: Tạo room thường trong workspace
			// Bước A: Ghi vào RocksDB index (source of truth, reliable)
			services.workspace.set_workspace_id(&room_id, wid);
			info!("Room {room_id} linked to workspace {wid:?} in DB index");

			// Bước A2: Ghi workspace_id vào room state event để sync tự deliver đến client.
			// Client đọc event type "im.tuwunel.workspace" → biết room thuộc workspace nào,
			// không cần API call thêm.
			let room_lock = services.state.mutex.lock(&room_id).await;
			if let Err(e) = services
				.timeline
				.build_and_append_pdu(
					PduBuilder {
						event_type: TimelineEventType::from("im.tuwunel.workspace"),
						content: to_raw_value(&json!({ "id": wid }))?.into(),
						state_key: Some(StateKey::new()),
						..Default::default()
					},
					sender_user,
					&room_id,
					&room_lock,
				)
				.boxed()
				.await
			{
				warn!(%e, %room_id, workspace_id = %wid, "Failed to write im.tuwunel.workspace state event (non-fatal)");
			}
			drop(room_lock);

			// Bước B: Gửi m.space.child event vào Space (best-effort, Matrix spec compliance)
			// Nếu lỗi → chỉ warn, không ảnh hưởng tính đúng đắn vì filter dùng RocksDB index.
			// Bỏ qua nếu là phòng 1-1 để tránh client nhầm lẫn là group chat
			if !body.is_direct {
				if let Ok(space_room_id) = services.workspace.get_space_room_id(wid).await {
					let server_name = services.globals.server_name().to_owned();
					let space_lock = services.state.mutex.lock(&space_room_id).await;
					let child_content = SpaceChildEventContent {
						via: vec![server_name],
						suggested: false,
						order: None,
					};
					if let Err(e) = services
						.timeline
						.build_and_append_pdu(
							PduBuilder {
								event_type: TimelineEventType::SpaceChild,
								content: to_raw_value(&child_content)?.into(),
								state_key: Some(room_id.as_str().into()),
								..Default::default()
							},
							sender_user,
							&space_room_id,
							&space_lock,
						)
						.boxed()
						.await
					{
						// Best-effort: room đã được ghi vào RocksDB index rồi,
						// filter vẫn hoạt động đúng. Space child chỉ phục vụ Matrix clients khác.
						warn!(%e, %room_id, %space_room_id, "Failed to send m.space.child event (best-effort); workspace DB index is intact");
					}
				} else {
					warn!(%room_id, workspace_id = %wid, "workspace_id provided but no Space room found for this workspace; skipping m.space.child");
				}
			}
		}
	}

	if body.is_direct {
		// m.direct is global account data. Merge room IDs into existing content
		// instead of replacing the whole event to avoid dropping prior DM rooms.
		for user_id in &body.invite {
			if let Err(e) = services
				.account_data
				.add_direct_room_for_user(sender_user, user_id, &room_id)
				.await
			{
				warn!(
					%e,
					%room_id,
					%user_id,
					"Failed to merge m.direct global account data for room creator"
				);
			}
		}
	}

	Ok(create_room::v3::Response::new(room_id))
}

async fn apply_creator_join_pdu(
	services: &Services,
	body: &Ruma<create_room::v3::Request>,
	sender_user: &UserId,
	room_id: &RoomId,
	state_lock: &RoomMutexGuard,
) -> Result {
	let content = services
		.profile
		.fill_content(
			sender_user,
			RoomMemberEventContent {
				is_direct: body.is_direct,
				..RoomMemberEventContent::new(MembershipState::Join)
			},
		)
		.await;

	services
		.timeline
		.build_and_append_pdu(
			PduBuilder {
				unsigned: Some(initial_room_creation_unsigned()),
				..PduBuilder::state(sender_user.to_string(), &content)
			},
			sender_user,
			room_id,
			state_lock,
		)
		.await
		.map(|_| ())
}

async fn apply_power_levels_pdu(
	services: &Services,
	body: &Ruma<create_room::v3::Request>,
	preset: &RoomPreset,
	version_rules: &RoomVersionRules,
	sender_user: &UserId,
	room_id: &RoomId,
	state_lock: &RoomMutexGuard,
) -> Result {
	let users = build_power_levels_users(services, body, preset, version_rules, sender_user).await;

	let default_override = services
		.config
		.default_power_level_content_override
		.as_ref();

	let power_levels_content = default_power_levels_content(
		version_rules,
		default_override,
		body.power_level_content_override.as_ref(),
		preset,
		users,
		sender_user,
	)?;

	services
		.timeline
		.build_and_append_pdu(
			PduBuilder {
				event_type: TimelineEventType::RoomPowerLevels,
				content: to_raw_value(&power_levels_content)?.into(),
				state_key: Some(StateKey::new()),
				..Default::default()
			},
			sender_user,
			room_id,
			state_lock,
		)
		.await
		.map(|_| ())
}

async fn build_power_levels_users(
	services: &Services,
	body: &Ruma<create_room::v3::Request>,
	preset: &RoomPreset,
	version_rules: &RoomVersionRules,
	sender_user: &UserId,
) -> BTreeMap<OwnedUserId, Int> {
	let seed = version_rules
		.authorization
		.explicitly_privilege_room_creators
		.or(|| (sender_user.to_owned(), int!(100)))
		.into_iter()
		.collect::<BTreeMap<_, _>>();

	let trusted_invitees = *preset == RoomPreset::TrustedPrivateChat
		&& !version_rules.authorization.additional_room_creators;

	if !trusted_invitees {
		return seed;
	}

	let ignored = services.users.ignored_users(sender_user).await;

	body.invite
		.iter()
		.filter(|invite| invite_allowed(ignored.as_ref(), invite))
		.fold(seed, |mut users, invite| {
			users.insert(invite.clone(), int!(100));
			users
		})
}

async fn apply_canonical_alias_pdu(
	services: &Services,
	room_alias_id: &RoomAliasId,
	sender_user: &UserId,
	room_id: &RoomId,
	state_lock: &RoomMutexGuard,
) -> Result {
	services
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(
				String::new(),
				&RoomCanonicalAliasEventContent {
					alias: Some(room_alias_id.to_owned()),
					alt_aliases: vec![],
				},
			),
			sender_user,
			room_id,
			state_lock,
		)
		.await
		.map(|_| ())
}

async fn apply_preset_state_pdus(
	services: &Services,
	body: &Ruma<create_room::v3::Request>,
	preset: &RoomPreset,
	sender_user: &UserId,
	room_id: &RoomId,
	state_lock: &RoomMutexGuard,
) -> Result<Vec<InitialEvent>> {
	let mut initial_state = body
		.initial_state
		.iter()
		.map(|state| Ok(state.deserialize_as_unchecked::<InitialEvent>()?))
		.filter_ok(|event| {
			services.config.allow_encryption || event.event_type != StateEventType::RoomEncryption
		})
		.filter_ok(|event| {
			// client/appservice workaround: if a user sends an initial_state event with a
			// state event in there with the content of literally `{}` (not null or empty
			// string), let's just skip it over and warn.
			if event.content.json().get() == "{}" {
				debug_warn!(
					"skipping empty initial state event of type {}",
					event.event_type
				);
				false
			} else {
				true
			}
		})
		.filter_ok(|event| body.name.is_none() || event.event_type != StateEventType::RoomName)
		.filter_ok(|event| body.topic.is_none() || event.event_type != StateEventType::RoomTopic)
		.collect::<Result<Vec<_>>>()?;

	let join_rule_pdubuilder = take_initial(&mut initial_state, &StateEventType::RoomJoinRules, "")
		.map(Into::into)
		.unwrap_or_else(|| {
			PduBuilder::state(
				String::new(),
				&RoomJoinRulesEventContent::new(match preset {
					RoomPreset::PublicChat => JoinRule::Public,
					// according to spec "invite" is the default
					_ => JoinRule::Invite,
				}),
			)
		});

	let history_visibility_pdubuilder = take_initial(
		&mut initial_state,
		&StateEventType::RoomHistoryVisibility,
		"",
	)
	.map(Into::into)
	.unwrap_or_else(|| {
		PduBuilder::state(
			String::new(),
			&RoomHistoryVisibilityEventContent::new(HistoryVisibility::Shared),
		)
	});

	let guest_access = guest_access_pdu(
		take_initial(&mut initial_state, &StateEventType::RoomGuestAccess, "").map(Into::into),
		preset,
	);

	// 5.1 Join Rules
	services
		.timeline
		.build_and_append_pdu(join_rule_pdubuilder, sender_user, room_id, state_lock)
		.boxed()
		.await?;

	// 5.2 History Visibility
	services
		.timeline
		.build_and_append_pdu(
			history_visibility_pdubuilder,
			sender_user,
			room_id,
			state_lock,
		)
		.boxed()
		.await?;

	// 5.3 Guest Access
	if let Some(guest_access) = guest_access {
		services
			.timeline
			.build_and_append_pdu(guest_access, sender_user, room_id, state_lock)
			.boxed()
			.await?;
	}

	Ok(initial_state)
}

fn guest_access_pdu(initial: Option<PduBuilder>, preset: &RoomPreset) -> Option<PduBuilder> {
	let can_join = || {
		PduBuilder::state(
			String::new(),
			&RoomGuestAccessEventContent::new(GuestAccess::CanJoin),
		)
	};

	initial.or_else(|| preset.ne(&RoomPreset::PublicChat).then(can_join))
}

async fn apply_initial_state_pdus(
	services: &Services,
	initial_state: Vec<InitialEvent>,
	preset: &RoomPreset,
	sender_user: &UserId,
	room_id: &RoomId,
	state_lock: &RoomMutexGuard,
) -> Result {
	let is_encrypted = encrypts_room(&initial_state);

	for event in initial_state {
		services
			.timeline
			.build_and_append_pdu(event.into(), sender_user, room_id, state_lock)
			.boxed()
			.await?;
	}

	if !services.config.allow_encryption || is_encrypted {
		return Ok(());
	}

	let config = services
		.config
		.encryption_enabled_by_default_for_room_type
		.as_deref();

	let should_encrypt = match config {
		Some("all") => true,
		Some("invite") => matches!(
			preset,
			RoomPreset::PrivateChat | RoomPreset::TrustedPrivateChat
		),
		_ => false,
	};

	if !should_encrypt {
		return Ok(());
	}

	let algorithm = EventEncryptionAlgorithm::MegolmV1AesSha2;
	let content = RoomEncryptionEventContent::new(algorithm);
	services
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &content),
			sender_user,
			room_id,
			state_lock,
		)
		.boxed()
		.await?;

	Ok(())
}

/// Whether `initial_state` already configures the room's encryption.
///
/// The last entry at the empty state key is the one that survives state
/// resolution, so it alone decides whether the server's forced default is
/// displaced, and only by naming a string `algorithm`. The raw field is read
/// rather than deserialized so an escaped string still counts as one.
fn encrypts_room(initial_state: &[InitialEvent]) -> bool {
	initial_state
		.iter()
		.rfind(|event| {
			event.event_type == StateEventType::RoomEncryption && event.state_key.is_empty()
		})
		.and_then(|event| event.content.get_field("algorithm").flat_ok())
		.is_some_and(|algorithm: &RawJsonValue| algorithm.get().starts_with('"'))
}

async fn apply_name_and_topic_pdus(
	services: &Services,
	body: &Ruma<create_room::v3::Request>,
	sender_user: &UserId,
	room_id: &RoomId,
	state_lock: &RoomMutexGuard,
) -> Result {
	if let Some(name) = &body.name {
		services
			.timeline
			.build_and_append_pdu(
				PduBuilder {
					unsigned: Some(initial_room_creation_unsigned()),
					..PduBuilder::state(String::new(), &RoomNameEventContent::new(name.clone()))
				},
				sender_user,
				room_id,
				state_lock,
			)
			.boxed()
			.await?;
	}

	if let Some(topic) = &body.topic {
		services
			.timeline
			.build_and_append_pdu(
				PduBuilder::state(String::new(), &RoomTopicEventContent::new(topic.clone())),
				sender_user,
				room_id,
				state_lock,
			)
			.boxed()
			.await?;
	}

	Ok(())
}

async fn process_invites(
	services: &Services,
	body: &Ruma<create_room::v3::Request>,
	sender_user: &UserId,
	room_id: &RoomId,
) {
	// 8. Events implied by invite (and TODO: invite_3pid)
	let ignored = services.users.ignored_users(sender_user).await;

	body.invite
		.iter()
		.stream()
		.ready_filter(|user_id| invite_allowed(ignored.as_ref(), user_id))
		.for_each(async |user_id| {
			if let Err(e) = services
				.membership
				.invite_for_room_creation(sender_user, user_id, room_id, None, body.is_direct, None)
				.boxed()
				.await
			{
				warn!(%e, "Failed to send invite");
			}
		})
		.await;
}

/// Gate an invitee against the sender's own ignore list.
///
/// The invitee's own invite permission is not consulted here: `local_invite`
/// is the authoritative check and refuses a blocked invite the same way any
/// other failed invite is handled, while an ignored one proceeds and is
/// withheld from the invitee afterwards.
fn invite_allowed(ignored: Option<&IgnoredUserListEventContent>, invitee: &UserId) -> bool {
	ignored.is_none_or(|content| !content.ignored_users.contains_key(invitee))
}

async fn finalize_alias_and_directory(
	services: &Services,
	body: &Ruma<create_room::v3::Request>,
	alias: Option<&RoomAliasId>,
	sender_user: &UserId,
	room_id: &RoomId,
) -> Result {
	if let Some(alias) = alias {
		services.alias.set_alias_by(alias, room_id, sender_user)?;
	}

	if body.visibility == room::Visibility::Public {
		services.directory.set_public(room_id, alias);

		services
			.admin
			.notify_loud(&format!(
				"{sender_user} made {room_id} public to the room directory"
			))
			.await;

		info!(
			"{sender_user} made {0} public to the room directory",
			room_id
		);
	}

	Ok(())
}

async fn copy_creator_predecessor_push_rule(
	services: &Services,
	creation_content: Option<&Raw<CreationContent>>,
	sender_user: &UserId,
	room_id: &RoomId,
) {
	let Some(from_room) = creation_content
		.and_then(|content| {
			content
				.get_field::<PreviousRoom>("predecessor")
				.ok()
				.flatten()
		})
		.map(|predecessor| predecessor.room_id)
	else {
		return;
	};

	services
		.account_data
		.copy_room_push_rule(sender_user, &from_room, room_id)
		.await
		.inspect_err(|e| warn!(%e, "Failed to copy predecessor push rules"))
		.ok();
}

async fn create_create_event(
	services: &Services,
	body: &Ruma<create_room::v3::Request>,
	preset: &RoomPreset,
	room_version: &RoomVersionId,
	version_rules: &RoomVersionRules,
) -> Result<(OwnedRoomId, RoomMutexGuard)> {
	let _sender_user = body.sender_user();

	let mut create_content = match &body.creation_content {
		Some(content) => {
			let mut content = content
				.deserialize_as_unchecked::<CanonicalJsonObject>()
				.map_err(|e| {
					err!(Request(BadJson(error!(
						"Failed to deserialise content as canonical JSON: {e}"
					))))
				})?;

			if !services.config.federate_created_rooms
				&& (!services.config.allow_federation || !content.contains_key("m.federate"))
			{
				content.insert("m.federate".into(), json!(false).try_into()?);
			}

			content.insert(
				"room_version".into(),
				json!(room_version.as_str())
					.try_into()
					.map_err(|e| err!(Request(BadJson("Invalid creation content: {e}"))))?,
			);

			content
		}
		None => {
			let content = RoomCreateEventContent::new_v11();

			let mut content =
				serde_json::from_str::<CanonicalJsonObject>(to_raw_value(&content)?.get())?;

			if !services.config.federate_created_rooms {
				content.insert("m.federate".into(), json!(false).try_into()?);
			}

			content.insert(
				"room_version".into(),
				json!(room_version.as_str()).try_into()?,
			);
			content
		}
	};

	if version_rules.authorization.additional_room_creators {
		let mut additional_creators = body
			.creation_content
			.as_ref()
			.and_then(|c| c.deserialize_as_unchecked::<CreationContent>().ok())
			.unwrap_or_default()
			.additional_creators;

		if *preset == RoomPreset::TrustedPrivateChat {
			additional_creators.extend(body.invite.clone());
		}

		let additional_creators = additional_creators
			.into_iter()
			.sorted()
			.dedup()
			.collect_vec();

		if !additional_creators.is_empty() {
			create_content.insert(
				"additional_creators".into(),
				json!(additional_creators).try_into()?,
			);
		}
	}

	// Embed room name into the create event so clients can display
	// "UserA created the group «Room name»" as a single system message.
	if let Some(ref name) = body.name {
		create_content.insert("room_name".into(), json!(name).try_into()?);
	}

	// 1. The room create event, using a placeholder room_id
	let room_id = ruma::room_id!("!thiswillbereplaced").to_owned();
	let state_lock = services.state.mutex.lock(&room_id).await;
	let create_event_id = services
		.timeline
		.build_and_append_pdu(
			PduBuilder {
				event_type: TimelineEventType::RoomCreate,
				content: to_raw_value(&create_content)?.into(),
				state_key: Some(StateKey::new()),
				..Default::default()
			},
			body.sender_user(),
			&room_id,
			&state_lock,
		)
		.boxed()
		.await?;

	drop(state_lock);

	// The real room_id is now the event_id.
	let room_id = OwnedRoomId::from_parts('!', create_event_id.localpart(), None)?;
	let state_lock = services.state.mutex.lock(&room_id).await;

	Ok((room_id, state_lock))
}

async fn create_create_event_legacy(
	services: &Services,
	body: &Ruma<create_room::v3::Request>,
	room_version: &RoomVersionId,
	version_rules: &RoomVersionRules,
) -> Result<(OwnedRoomId, RoomMutexGuard)> {
	let room_id: OwnedRoomId = match &body.room_id {
		None => RoomId::new_v1(&services.server.name),
		Some(custom_id) => custom_room_id_check(services, custom_id).await?,
	};

	let state_lock = services.state.mutex.lock(&room_id).await;

	let _short_id = services.short.get_or_create_shortroomid(&room_id).await;

	let mut create_content = match &body.creation_content {
		Some(content) => {
			let mut content = content
				.deserialize_as_unchecked::<CanonicalJsonObject>()
				.map_err(|e| {
					err!(Request(BadJson(error!(
						"Failed to deserialise content as canonical JSON: {e}"
					))))
				})?;

			if !version_rules.authorization.use_room_create_sender {
				content.insert(
					"creator".into(),
					json!(body.sender_user()).try_into().map_err(|e| {
						err!(Request(BadJson(debug_error!(
							"Invalid creation content: {e}"
						))))
					})?,
				);
			}

			if !services.config.federate_created_rooms
				&& (!services.config.allow_federation || !content.contains_key("m.federate"))
			{
				content.insert("m.federate".into(), json!(false).try_into()?);
			}

			content.insert(
				"room_version".into(),
				json!(room_version.as_str())
					.try_into()
					.map_err(|e| err!(Request(BadJson("Invalid creation content: {e}"))))?,
			);

			content
		}
		None => {
			let content = if !version_rules.authorization.use_room_create_sender {
				RoomCreateEventContent::new_v1(body.sender_user().to_owned())
			} else {
				RoomCreateEventContent::new_v11()
			};

			let mut content =
				serde_json::from_str::<CanonicalJsonObject>(to_raw_value(&content)?.get())?;

			if !services.config.federate_created_rooms {
				content.insert("m.federate".into(), json!(false).try_into()?);
			}

			content.insert(
				"room_version".into(),
				json!(room_version.as_str()).try_into()?,
			);
			content
		}
	};

	// Embed room name into the create event so clients can display
	// "UserA created the group «Room name»" as a single system message.
	if let Some(ref name) = body.name {
		create_content.insert("room_name".into(), json!(name).try_into()?);
	}

	// 1. The room create event
	services
		.timeline
		.build_and_append_pdu(
			PduBuilder {
				event_type: TimelineEventType::RoomCreate,
				content: to_raw_value(&create_content)?.into(),
				state_key: Some(StateKey::new()),
				..Default::default()
			},
			body.sender_user(),
			&room_id,
			&state_lock,
		)
		.boxed()
		.await?;

	Ok((room_id, state_lock))
}

/// creates the power_levels_content for the PDU builder
fn default_power_levels_content(
	version_rules: &RoomVersionRules,
	default_power_level_content_override: Option<&JsonValue>,
	power_level_content_override: Option<&Raw<RoomPowerLevelsContentOverride>>,
	preset: &RoomPreset,
	users: BTreeMap<OwnedUserId, Int>,
	creator_user_id: &UserId,
) -> Result<JsonValue> {
	use serde_json::to_value;

	let mut power_levels_content = RoomPowerLevelsEventContent::new(&version_rules.authorization);
	power_levels_content.users = users;

	let mut power_levels_content = to_value(power_levels_content)?;

	// secure proper defaults of sensitive/dangerous permissions that moderators
	// (power level 50) should not have easy access to
	power_levels_content["events"]["m.room.power_levels"] = json!(100);
	power_levels_content["events"]["m.room.server_acl"] = json!(100);
	power_levels_content["events"]["m.room.encryption"] = json!(100);
	power_levels_content["events"]["m.room.history_visibility"] = json!(100);

	if version_rules
		.authorization
		.explicitly_privilege_room_creators
	{
		power_levels_content["events"]["m.room.tombstone"] = json!(150);
	} else {
		power_levels_content["events"]["m.room.tombstone"] = json!(100);
	}

	// always allow users to respond (not post new) to polls. this is primarily
	// useful in read-only announcement rooms that post a public poll.
	power_levels_content["events"]["org.matrix.msc3381.poll.response"] = json!(0);
	power_levels_content["events"]["m.poll.response"] = json!(0);

	// public_chat: pin invite and call-setup events at PL 50. Synapse pins
	// invite and m.call.invite here; the MSC3401 entries are tuwunel-only.
	if *preset == RoomPreset::PublicChat {
		power_levels_content["invite"] = json!(50);
		power_levels_content["events"]["m.call.invite"] = json!(50);
		power_levels_content["events"]["m.call"] = json!(50);
		power_levels_content["events"]["m.call.member"] = json!(50);
		power_levels_content["events"]["org.matrix.msc3401.call"] = json!(50);
		power_levels_content["events"]["org.matrix.msc3401.call.member"] = json!(50);
	}

	if let Some(default_power_level_content_override) = default_power_level_content_override {
		let overrides = default_power_level_content_override
			.as_object()
			.expect("default_power_level_content_override is validated at startup")
			.iter()
			.map(|(key, value)| (key.clone(), value.clone()));

		merge_power_level_content_override(&mut power_levels_content, overrides);
	}

	if let Some(power_level_content_override) = power_level_content_override {
		let overrides: JsonObject = serde_json::from_str(power_level_content_override.json().get())
			.map_err(|e| {
				err!(Request(BadJson(
					"Invalid power_level_content_override: {e:?}"
				)))
			})?;

		for (key, value) in overrides {
			// Deep-merge "users" and "events" sub-objects instead of replacing them
			// wholesale. A shallow replace would wipe out the creator's power level
			// (set above) and the security-hardened event defaults.
			if (key == "users" || key == "events") && value.is_object() {
				if let Some(existing) = power_levels_content[key.as_str()].as_object_mut() {
					for (sub_key, sub_val) in value.as_object().expect("checked above") {
						existing.insert(sub_key.clone(), sub_val.clone());
					}
					continue;
				}
			}
			power_levels_content[key] = value;
		}
	}

	// Bảo vệ: đảm bảo creator luôn giữ quyền ít nhất 100 dù có power_level_content_override
	if !version_rules
		.authorization
		.explicitly_privilege_room_creators
	{
		let creator_key = creator_user_id.to_string();
		let current = power_levels_content["users"][&creator_key]
			.as_i64()
			.unwrap_or(0);
		if current < 100 {
			power_levels_content["users"][creator_key] = serde_json::json!(100);
		}
	}

	Ok(power_levels_content)
}

/// Replace each top-level power-levels key wholesale; no deep merge.
fn merge_power_level_content_override(
	power_levels_content: &mut JsonValue,
	overrides: impl IntoIterator<Item = (String, JsonValue)>,
) {
	power_levels_content
		.as_object_mut()
		.expect("power levels content must serialize to an object")
		.extend(overrides);
}

/// if a room is being created with a room alias, run our checks
async fn room_alias_check(
	services: &Services,
	room_alias_name: &str,
	appservice_info: Option<&RegistrationInfo>,
) -> Result<OwnedRoomAliasId> {
	// Basic checks on the room alias validity
	if room_alias_name.contains(':') {
		return Err!(Request(InvalidParam(
			"Room alias contained `:` which is not allowed. Please note that this expects a \
			 localpart, not the full room alias.",
		)));
	} else if room_alias_name.contains(char::is_whitespace) {
		return Err!(Request(InvalidParam(
			"Room alias contained spaces which is not a valid room alias.",
		)));
	}

	// check if room alias is forbidden
	if services
		.config
		.forbidden_alias_names
		.is_match(room_alias_name)
	{
		return Err!(Request(Unknown("Room alias name is forbidden.")));
	}

	let server_name = services.globals.server_name();
	let full_room_alias = OwnedRoomAliasId::parse(format!("#{room_alias_name}:{server_name}"))
		.map_err(|e| {
			err!(Request(InvalidParam(debug_error!(
				?e,
				?room_alias_name,
				"Failed to parse room alias.",
			))))
		})?;

	if services
		.alias
		.resolve_local_alias(&full_room_alias)
		.await
		.is_ok()
	{
		return Err!(Request(RoomInUse("Room alias already exists.")));
	}

	if let Some(info) = appservice_info {
		if !info.aliases.is_match(full_room_alias.as_str()) {
			return Err!(Request(Exclusive("Room alias is not in namespace.")));
		}
	} else if services
		.appservice
		.is_exclusive_alias(&full_room_alias)
		.await
	{
		return Err!(Request(Exclusive("Room alias reserved by appservice.",)));
	}

	debug_info!("Full room alias: {full_room_alias}");

	Ok(full_room_alias)
}

/// Finds an existing one-to-one room before creating a new direct room.
///
/// A user's `m.direct` account data may contain stale rooms or multiple rooms
/// for the same target. Only a room which still exists, has exactly two joined
/// members, is marked direct for both members, and matches the requested
/// workspace is eligible for reuse.
async fn find_existing_direct_room(
	services: &Services,
	sender_user: &ruma::UserId,
	target_user: &ruma::UserId,
	workspace_id: Option<&str>,
) -> Option<OwnedRoomId> {
	let room_ids = services
		.account_data
		.direct_rooms_for_user(sender_user, target_user)
		.await;

	for room_id in room_ids.into_iter().rev() {
		if !services.metadata.exists(&room_id).await
			|| !services.state_cache.is_joined(sender_user, &room_id).await
			|| !services.state_cache.is_joined(target_user, &room_id).await
		{
			continue;
		}

		if services
			.state_cache
			.room_joined_count(&room_id)
			.await
			.ok()
			.ne(&Some(2))
		{
			continue;
		}

		let sender_is_direct = services.account_data.is_direct(sender_user, &room_id).await
			|| services
				.state_accessor
				.is_direct(&room_id, sender_user)
				.await;
		let target_is_direct = services.account_data.is_direct(target_user, &room_id).await
			|| services
				.state_accessor
				.is_direct(&room_id, target_user)
				.await;

		if !sender_is_direct || !target_is_direct {
			continue;
		}

		if let Some(workspace_id) = workspace_id {
			let Ok(existing_workspace_id) = services.workspace.get_workspace_id(&room_id).await
			else {
				continue;
			};
			if existing_workspace_id != workspace_id {
				continue;
			}
		}

		return Some(room_id);
	}

	None
}

/// if a room is being created with a custom room ID, run our checks against it
async fn custom_room_id_check(services: &Services, custom_room_id: &str) -> Result<OwnedRoomId> {
	// apply forbidden room alias checks to custom room IDs too
	if services
		.config
		.forbidden_alias_names
		.is_match(custom_room_id)
	{
		return Err!(Request(Unknown("Custom room ID is forbidden.")));
	}

	if custom_room_id.contains(':') {
		return Err!(Request(InvalidParam(
			"Custom room ID contained `:` which is not allowed. Please note that this expects a \
			 localpart, not the full room ID.",
		)));
	} else if custom_room_id.contains(char::is_whitespace) {
		return Err!(Request(InvalidParam(
			"Custom room ID contained spaces which is not valid."
		)));
	}

	let server_name = services.globals.server_name();
	let full_room_id = format!("!{custom_room_id}:{server_name}");

	let room_id = OwnedRoomId::parse(full_room_id)
		.inspect(|full_room_id| debug_info!(?full_room_id, "Full custom room ID"))
		.inspect_err(|e| {
			warn!(
				?e,
				?custom_room_id,
				"Failed to create room with custom room ID"
			);
		})?;

	// check if room ID doesn't already exist instead of erroring on auth check
	if services.short.get_shortroomid(&room_id).await.is_ok() {
		return Err!(Request(RoomInUse(
			"Room with that custom room ID already exists",
		)));
	}

	Ok(room_id)
}

async fn can_publish_directory_check(
	services: &Services,
	body: &Ruma<create_room::v3::Request>,
) -> Result {
	if !services.server.config.lockdown_public_room_directory
		|| body.appservice_info.is_some()
		|| body.visibility != room::Visibility::Public
		|| services.admin.user_is_admin(body.sender_user()).await
	{
		return Ok(());
	}

	let msg = format!(
		"Non-admin user {} tried to publish new to the directory while \
		 lockdown_public_room_directory is enabled",
		body.sender_user(),
	);

	warn!("{msg}");
	services.admin.notify(&msg).await;

	Err!(Request(Forbidden(
		"Publishing rooms to the room directory is not allowed"
	)))
}

async fn can_create_room_check(
	services: &Services,
	body: &Ruma<create_room::v3::Request>,
) -> Result {
	if !services.config.allow_room_creation
		&& body.appservice_info.is_none()
		&& !services.admin.user_is_admin(body.sender_user()).await
	{
		return Err!(Request(Forbidden("Room creation has been disabled.",)));
	}

	Ok(())
}

#[derive(Deserialize)]
struct InitialEvent {
	#[serde(rename = "type")]
	event_type: StateEventType,

	#[serde(default = "StateKey::new")]
	state_key: StateKey,

	content: Content,
}

impl From<InitialEvent> for PduBuilder {
	fn from(value: InitialEvent) -> Self {
		Self {
			event_type: value.event_type.into(),
			content: value.content,
			unsigned: None,
			state_key: Some(value.state_key),
			redacts: None,
			timestamp: None,
		}
	}
}

fn take_initial(
	initial_state: &mut Vec<InitialEvent>,
	event_type: &StateEventType,
	state_key: &str,
) -> Option<InitialEvent> {
	initial_state
		.extract_if(.., |event| {
			&event.event_type == event_type && event.state_key == state_key
		})
		.next()
}

#[cfg(test)]
mod tests {
	use tuwunel_core::matrix::room_version::rules;

	use super::*;

	fn guest_access(pdu: &PduBuilder) -> GuestAccess {
		pdu.content
			.deserialize_as_unchecked::<RoomGuestAccessEventContent>()
			.expect("guest access content")
			.guest_access
	}

	#[test]
	fn default_power_levels_content_applies_server_default_override() {
		let version_rules = rules(&RoomVersionId::V11).expect("supported room version");

		let content = default_power_levels_content(
			&version_rules,
			Some(&json!({ "users_default": 50 })),
			None,
			&RoomPreset::PrivateChat,
			BTreeMap::new(),
			ruma::user_id!("@alice:example.com"),
		)
		.expect("power levels content");

		assert_eq!(content["users_default"], json!(50));
	}

	#[test]
	fn request_override_wins_over_server_default_override() {
		let version_rules = rules(&RoomVersionId::V11).expect("supported room version");
		let request_override =
			Raw::from_json(to_raw_value(&json!({ "users_default": 75 })).expect("raw json"));

		let content = default_power_levels_content(
			&version_rules,
			Some(&json!({ "users_default": 50 })),
			Some(&request_override),
			&RoomPreset::PrivateChat,
			BTreeMap::new(),
			ruma::user_id!("@alice:example.com"),
		)
		.expect("power levels content");

		assert_eq!(content["users_default"], json!(75));
	}

	#[test]
	fn default_override_preserves_explicit_user_power_levels() {
		let version_rules = rules(&RoomVersionId::V11).expect("supported room version");
		let creator = OwnedUserId::try_from("@alice:example.com").expect("valid user id");
		let users = BTreeMap::from([(creator.clone(), int!(100))]);

		let content = default_power_levels_content(
			&version_rules,
			Some(&json!({ "users_default": 50 })),
			None,
			&RoomPreset::PrivateChat,
			users,
			&creator,
		)
		.expect("power levels content");

		assert_eq!(content["users_default"], json!(50));
		assert_eq!(content["users"][creator.as_str()], json!(100));
	}

	#[test]
	fn ioffice_request_override_deep_merges_and_preserves_creator_power() {
		let creator = ruma::user_id!("@alice:example.com");
		let request = Raw::from_json(
			to_raw_value(&json!({
				"users": { "@bob:example.com": 50, "@alice:example.com": 150 },
				"events": { "m.room.message": 25 }
			}))
			.unwrap(),
		);
		let content = default_power_levels_content(
			&rules(&RoomVersionId::V11).unwrap(),
			None,
			Some(&request),
			&RoomPreset::PrivateChat,
			BTreeMap::from([(creator.to_owned(), int!(100))]),
			creator,
		)
		.unwrap();
		assert_eq!(content["users"][creator.as_str()], json!(150));
		assert_eq!(content["users"]["@bob:example.com"], json!(50));
		assert_eq!(content["events"]["m.room.message"], json!(25));
		assert_eq!(content["events"]["m.room.power_levels"], json!(100));
	}

	#[test]
	fn ioffice_override_cannot_demote_creator_below_100() {
		let creator = ruma::user_id!("@alice:example.com");
		let request =
			Raw::from_json(to_raw_value(&json!({"users": {"@alice:example.com": 0}})).unwrap());
		let content = default_power_levels_content(
			&rules(&RoomVersionId::V11).unwrap(),
			None,
			Some(&request),
			&RoomPreset::PrivateChat,
			BTreeMap::new(),
			creator,
		)
		.unwrap();
		assert_eq!(content["users"][creator.as_str()], json!(100));
	}

	#[test]
	fn public_chat_omits_default_guest_access() {
		assert!(guest_access_pdu(None, &RoomPreset::PublicChat).is_none());
	}

	#[test]
	fn private_presets_default_to_guest_access() {
		for preset in [RoomPreset::PrivateChat, RoomPreset::TrustedPrivateChat] {
			let pdu = guest_access_pdu(None, &preset).expect("guest access pdu");

			assert_eq!(pdu.event_type, TimelineEventType::RoomGuestAccess);
			assert_eq!(pdu.state_key.as_deref(), Some(""));
			assert_eq!(guest_access(&pdu), GuestAccess::CanJoin);
		}
	}

	#[test]
	fn explicit_guest_access_survives_public_preset() {
		let explicit = PduBuilder::state(
			String::new(),
			&RoomGuestAccessEventContent::new(GuestAccess::Forbidden),
		);

		let pdu = guest_access_pdu(Some(explicit), &RoomPreset::PublicChat)
			.expect("explicit guest access pdu");

		assert_eq!(guest_access(&pdu), GuestAccess::Forbidden);
	}

	#[test]
	fn encryption_needs_a_string_algorithm() {
		for content in [
			r#"{"algorithm":"m.megolm.v1.aes-sha2"}"#,
			r#"{"algorithm":"\u006d.megolm.v1.aes-sha2"}"#,
			r#"{"algorithm":"whatever","rotation_period_ms":604800000}"#,
			r#"{"algorithm" : "m.megolm.v1.aes-sha2"}"#,
		] {
			assert!(encrypts_room(&[initial_state_event(
				"m.room.encryption",
				"",
				content
			)]));
		}
	}

	fn initial_state_event(event_type: &str, state_key: &str, content: &str) -> InitialEvent {
		let json =
			format!(r#"{{"type":"{event_type}","state_key":"{state_key}","content":{content}}}"#);

		serde_json::from_str(&json).expect("initial state event")
	}

	#[test]
	fn contentless_encryption_leaves_the_forced_default() {
		for content in [
			"{}",
			"{ }",
			r#"{"x":1}"#,
			r#"{"algorithm":1}"#,
			r#"{"algorithm":null}"#,
		] {
			assert!(!encrypts_room(&[initial_state_event(
				"m.room.encryption",
				"",
				content
			)]));
		}
	}

	#[test]
	fn the_last_entry_at_the_empty_state_key_decides() {
		let valid = r#"{"algorithm":"m.megolm.v1.aes-sha2"}"#;
		let junk = r#"{"x":1}"#;
		let event = |content| initial_state_event("m.room.encryption", "", content);

		assert!(!encrypts_room(&[event(valid), event(junk)]));
		assert!(encrypts_room(&[event(junk), event(valid)]));
		assert!(encrypts_room(&[
			event(valid),
			initial_state_event("m.room.encryption", "x", junk)
		]));
	}

	#[test]
	fn a_foreign_state_key_never_encrypts() {
		let content = r#"{"algorithm":"m.megolm.v1.aes-sha2"}"#;
		let event = initial_state_event("m.room.encryption", "x", content);

		assert!(!encrypts_room(&[event]));
	}

	#[test]
	fn other_event_types_never_encrypt() {
		let event =
			initial_state_event("m.room.name", "", r#"{"algorithm":"m.megolm.v1.aes-sha2"}"#);

		assert!(!encrypts_room(&[event]));
	}
}
