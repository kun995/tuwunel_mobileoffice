use std::{
	collections::{BTreeMap, BTreeSet},
	io::Write,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures::StreamExt;
use serde::Serialize;
use serde_json::{Map as JsonMap, Value};
use tuwunel_core::{
	matrix::event::gen_event_id,
	ruma::{CanonicalJsonObject, OwnedEventId, OwnedRoomId, RoomVersionId},
};
use tuwunel_database::Database;

use super::{MAP_GROUPS, SigningKeyId};

#[derive(Serialize)]
struct BusinessRecord {
	format: &'static str,
	version: u8,
	group: &'static str,
	source_map: &'static str,
	#[serde(skip_serializing_if = "Option::is_none")]
	record_type: Option<&'static str>,
	#[serde(skip_serializing_if = "Option::is_none")]
	identity: Option<BTreeMap<String, Value>>,
	#[serde(skip_serializing_if = "Option::is_none")]
	key: Option<Vec<Value>>,
	#[serde(skip_serializing_if = "Option::is_none")]
	value: Option<Value>,
	#[serde(skip_serializing_if = "Option::is_none")]
	value_b64: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	user_id: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	displayname: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	avatar_url: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	deactivated: Option<bool>,
}

#[derive(Serialize)]
struct EventRecord {
	format: &'static str,
	version: u8,
	group: &'static str,
	source_map: &'static str,
	room_id: String,
	event_id: String,
	depth: u64,
	prev_events: Vec<String>,
	redacted: bool,
	event_id_hash_valid: bool,
	event: Value,
}

struct Pdu {
	room_id: OwnedRoomId,
	event_id: OwnedEventId,
	depth: u64,
	prev_events: Vec<OwnedEventId>,
	redacted: bool,
	json: Value,
}

pub(crate) async fn export_business_records<W: Write>(
	database: &Database,
	mut output: Option<&mut std::io::BufWriter<W>>,
) -> Result<BTreeMap<&'static str, (u64, u64)>, Box<dyn std::error::Error>> {
	let mut stats = BTreeMap::new();
	let mut records = Vec::new();
	let mut users: BTreeMap<String, JsonMap<String, Value>> = BTreeMap::new();
	let mut redacted_ids = BTreeSet::new();

	// Password values are never emitted. The source encodes deactivation as an
	// empty password, so this map is used only to derive the boolean state.
	let passwords = database.get("userid_password")?;
	let rows = passwords.raw_stream();
	futures::pin_mut!(rows);
	while let Some(row) = rows.next().await {
		let (key, value) = row?;
		if let Some(user_id) = single_text_key(key) {
			users
				.entry(user_id)
				.or_default()
				.insert("deactivated".into(), Value::Bool(value.is_empty()));
		}
	}

	let original = database.get("eventid_originalpdu")?;
	let rows = original.raw_stream();
	futures::pin_mut!(rows);
	while let Some(row) = rows.next().await {
		let (key, _) = row?;
		if let Some(event_id) = single_text_key(key) {
			redacted_ids.insert(event_id);
		}
	}

	// The signing-key identifier is safe to migrate; private key bytes remain
	// intentionally excluded in both raw and decoded output.
	let signing_keys = database.get("keyid_key")?;
	let rows = signing_keys.raw_stream();
	futures::pin_mut!(rows);
	while let Some(row) = rows.next().await {
		let (key_id, _) = row?;
		records.push(serde_json::to_value(SigningKeyId {
			format: "mo-chat-export",
			version: 2,
			group: "signing_key_ids",
			source_map: "keyid_key",
			key_id_b64: STANDARD.encode(key_id),
		})?);
		increment(&mut stats, "signing_key_ids", key_id.len())?;
	}

	let mut pdus = Vec::new();
	for &(map_name, group) in MAP_GROUPS {
		if map_name == "eventid_originalpdu" {
			continue;
		}
		let map = database.get(map_name)?;
		let rows = map.raw_stream();
		futures::pin_mut!(rows);
		while let Some(row) = rows.next().await {
			let (key, value) = row?;
			let row_bytes = key.len().saturating_add(value.len());

			if map_name == "pduid_pdu" || map_name == "eventid_outlierpdu" {
				let json: Value = serde_json::from_slice(value)?;
				pdus.push(parse_pdu(json, &redacted_ids)?);
				increment(&mut stats, group, row_bytes)?;
				continue;
			}

			if matches!(map_name, "userid_displayname" | "userid_avatarurl") {
				if let Some(user_id) = single_text_key(key) {
					let field = if map_name == "userid_displayname" {
						"displayname"
					} else {
						"avatar_url"
					};
					let decoded = decode_value(value);
					users
						.entry(user_id)
						.or_default()
						.insert(field.into(), decoded);
				}
				continue;
			}

			increment(&mut stats, group, row_bytes)?;
			let decoded_key = split_key(key);
			let identity = identity_fields(map_name, &decoded_key);
			let (value, value_b64) = match serde_json::from_slice::<Value>(value) {
				| Ok(json) => (Some(json), None),
				| Err(_) if std::str::from_utf8(value).is_ok() => {
					(Some(Value::String(String::from_utf8_lossy(value).into_owned())), None)
				},
				| Err(_) => (None, Some(STANDARD.encode(value))),
			};
			records.push(serde_json::to_value(BusinessRecord {
				format: "mo-chat-export",
				version: 2,
				group,
				source_map: map_name,
				record_type: Some(record_type(map_name)),
				identity,
				key: Some(decoded_key),
				value,
				value_b64,
				user_id: None,
				displayname: None,
				avatar_url: None,
				deactivated: None,
			})?);
		}
	}

	for (user_id, mut fields) in users {
		increment(&mut stats, "users", 0)?;
		let record = BusinessRecord {
			format: "mo-chat-export",
			version: 2,
			group: "users",
			source_map: "user_profile",
			record_type: Some("profile"),
			identity: None,
			key: None,
			value: Some(Value::Object(std::mem::take(&mut fields))),
			value_b64: None,
			user_id: Some(user_id),
			displayname: None,
			avatar_url: None,
			deactivated: None,
		};
		records.push(serde_json::to_value(record)?);
	}

	for (room_id, room_pdus) in topological_rooms(pdus)? {
		let versions = room_versions(&room_pdus)?;
		for pdu in room_pdus {
			let mut canonical: CanonicalJsonObject = serde_json::from_value(pdu.json.clone())?;
			let version = versions
				.get(&room_id)
				.unwrap_or(&RoomVersionId::V1);
			if version != &RoomVersionId::V1 && version != &RoomVersionId::V2 {
				canonical.remove("event_id");
			}
			let generated_id = gen_event_id(&canonical, version)?;
			if generated_id != pdu.event_id {
				return Err(format!("event ID hash mismatch for {}", pdu.event_id).into());
			}
			let record = EventRecord {
				format: "mo-chat-export",
				version: 2,
				group: "events",
				source_map: "pduid_pdu",
				room_id: pdu.room_id.to_string(),
				event_id: pdu.event_id.to_string(),
				depth: pdu.depth,
				prev_events: pdu
					.prev_events
					.iter()
					.map(ToString::to_string)
					.collect(),
				redacted: pdu.redacted,
				event_id_hash_valid: true,
				event: pdu.json,
			};
			records.push(serde_json::to_value(record)?);
		}
	}

	for record in records {
		if let Some(out) = output.as_deref_mut() {
			serde_json::to_writer(&mut *out, &record)?;
			out.write_all(b"\n")?;
		}
	}
	Ok(stats)
}

fn parse_pdu(
	json: Value,
	redacted_ids: &BTreeSet<String>,
) -> Result<Pdu, Box<dyn std::error::Error>> {
	let room_id: OwnedRoomId = json
		.get("room_id")
		.and_then(Value::as_str)
		.ok_or("PDU has no room_id")?
		.try_into()?;
	let event_id: OwnedEventId = json
		.get("event_id")
		.and_then(Value::as_str)
		.ok_or("PDU has no event_id")?
		.try_into()?;
	let depth = json
		.get("depth")
		.and_then(Value::as_u64)
		.unwrap_or_default();
	let prev_events = json
		.get("prev_events")
		.and_then(Value::as_array)
		.into_iter()
		.flatten()
		.filter_map(|prev| {
			prev.as_str()
				.or_else(|| prev.as_array()?.first()?.as_str())
		})
		.map(TryInto::try_into)
		.collect::<Result<Vec<OwnedEventId>, _>>()?;
	let redacted = redacted_ids.contains(event_id.as_str());
	Ok(Pdu {
		room_id,
		event_id,
		depth,
		prev_events,
		redacted,
		json,
	})
}

fn room_versions(
	pdus: &[Pdu],
) -> Result<BTreeMap<OwnedRoomId, RoomVersionId>, Box<dyn std::error::Error>> {
	let mut versions = BTreeMap::new();
	for pdu in pdus {
		if pdu.json.get("type").and_then(Value::as_str) != Some("m.room.create") {
			continue;
		}
		let version = pdu
			.json
			.get("content")
			.and_then(|content| content.get("room_version"))
			.and_then(Value::as_str)
			.map(RoomVersionId::try_from)
			.transpose()?
			.unwrap_or(RoomVersionId::V1);
		versions.insert(pdu.room_id.clone(), version);
	}
	Ok(versions)
}

fn topological_rooms(
	pdus: Vec<Pdu>,
) -> Result<BTreeMap<OwnedRoomId, Vec<Pdu>>, Box<dyn std::error::Error>> {
	let mut rooms: BTreeMap<OwnedRoomId, Vec<Pdu>> = BTreeMap::new();
	for pdu in pdus {
		rooms
			.entry(pdu.room_id.clone())
			.or_default()
			.push(pdu);
	}
	for (room_id, events) in &mut rooms {
		let all: BTreeSet<_> = events
			.iter()
			.map(|pdu| pdu.event_id.clone())
			.collect();
		let mut emitted = BTreeSet::new();
		let mut ordered = Vec::with_capacity(events.len());
		while !events.is_empty() {
			let next = events
				.iter()
				.enumerate()
				.filter(|(_, pdu)| {
					pdu.prev_events
						.iter()
						.all(|prev| !all.contains(prev) || emitted.contains(prev))
				})
				.min_by_key(|(_, pdu)| (pdu.depth, pdu.event_id.clone()))
				.map(|(i, _)| i);
			let Some(index) = next else {
				return Err(format!("cycle in PDU DAG for room {room_id}").into());
			};
			let pdu = events.remove(index);
			emitted.insert(pdu.event_id.clone());
			ordered.push(pdu);
		}
		*events = ordered;
	}
	Ok(rooms)
}

fn split_key(key: &[u8]) -> Vec<Value> {
	key.split(|byte| *byte == 0xff)
		.map(|part| {
			std::str::from_utf8(part)
				.map(|text| Value::String(text.to_owned()))
				.unwrap_or_else(|_| serde_json::json!({"bytes_b64": STANDARD.encode(part)}))
		})
		.collect()
}

/// Stable importer-facing row kinds. The decoded key/value stay alongside
/// this classification as provenance because Tuwunel's internal key codecs
/// are not a public schema.
fn record_type(map: &str) -> &'static str {
	match map {
		| "userid_origin" | "userid_oauthid" => "sso_identity_link",
		| "roomid_shortstatehash"
		| "shortstatehash_statediff"
		| "shortstatekey_statekey"
		| "shorteventid_shortstatehash"
		| "statehash_shortstatehash"
		| "statekey_shortstatekey"
		| "roomusertype_roomuserdataid"
		| "roomuserid_joined"
		| "userroomid_leftstate" => "room_state_index",
		| "roomuserdataid_accountdata" => "account_data_item",
		| "senderkey_pusher" | "pushkey_deviceid" => "pusher",
		| "readreceiptid_readreceipt" => "receipt",
		| "roomuserid_lastprivatereadupdate"
		| "roomuserid_lastnotificationread"
		| "roomuserid_privateread" => "read_marker",
		| "userdeviceid_metadata" => "device",
		| "userdeviceid_token" => "device_access_token",
		| "userdeviceid_refresh" => "device_refresh_token",
		| "token_userdeviceid" => "access_token_device_index",
		| "mediaid_file" | "mediaid_user" | "mediaid_sha256" | "media_sha256_refs" => {
			"media_metadata_item"
		},
		| "workspaceid_spaceroomid" => "workspace_space",
		| "roomid_workspaceid" | "workspaceid_roomids" => "room_workspace",
		| "userid_workspaceids" | "workspaceid_userids" => "user_workspace",
		| "reminderid_reminder" | "remindertime_reminderid" | "roomid_reminderids" => {
			"reminder_item"
		},
		| "userroomid_messagepairs" => "message_pair",
		| "usereventsid_deleted" | "userroomid_deletedbefore" | "userroomid_clearedbefore" => {
			"hidden_dm_checkpoint"
		},
		| "roomid_haslink_pduid" => "room_link_index",
		| "roomid_hasusermessage" => "room_user_message_flag",
		| _ => "decoded_kv",
	}
}

fn identity_fields(map: &str, key: &[Value]) -> Option<BTreeMap<String, Value>> {
	let names: &[&str] = match map {
		| "userid_origin" | "userid_oauthid" => &["user_id", "external_id"],
		| "roomuserdataid_accountdata" => {
			&["room_id", "user_id", "stream_position", "account_data_type"]
		},
		| "senderkey_pusher" => &["sender_key", "pusher_key"],
		| "pushkey_deviceid" => &["push_key", "device_id"],
		| "readreceiptid_readreceipt" => &["receipt_id"],
		| "roomuserid_lastprivatereadupdate"
		| "roomuserid_lastnotificationread"
		| "roomuserid_privateread" => &["room_id", "user_id"],
		| "userdeviceid_metadata" | "userdeviceid_token" | "userdeviceid_refresh" => {
			&["user_id", "device_id"]
		},
		| "token_userdeviceid" => &["token", "user_id", "device_id"],
		| "mediaid_file" | "mediaid_user" | "mediaid_sha256" => &["media_id"],
		| "media_sha256_refs" => &["sha256", "reference"],
		| "workspaceid_spaceroomid" => &["workspace_id"],
		| "roomid_workspaceid" | "roomid_hasusermessage" => &["room_id"],
		| "workspaceid_roomids" => &["workspace_id", "room_id"],
		| "userid_workspaceids" => &["user_id", "workspace_id"],
		| "workspaceid_userids" => &["workspace_id", "user_id"],
		| "reminderid_reminder" => &["reminder_id"],
		| "remindertime_reminderid" => &["reminder_time", "reminder_id"],
		| "roomid_reminderids" => &["room_id", "reminder_id"],
		| "userroomid_messagepairs" | "userroomid_deletedbefore" | "userroomid_clearedbefore" => {
			&["user_id", "room_id"]
		},
		| "usereventsid_deleted" => &["user_id", "event_id"],
		| "roomid_haslink_pduid" => &["room_id", "event_id"],
		| _ => return None,
	};
	if key.len() != names.len() {
		return None;
	}
	Some(
		names
			.iter()
			.zip(key)
			.map(|(name, value)| ((*name).to_owned(), value.clone()))
			.collect(),
	)
}

fn single_text_key(key: &[u8]) -> Option<String> {
	let parts = split_key(key);
	(parts.len() == 1)
		.then(|| parts[0].as_str().map(ToOwned::to_owned))
		.flatten()
}

fn decode_value(value: &[u8]) -> Value {
	serde_json::from_slice(value)
		.unwrap_or_else(|_| Value::String(String::from_utf8_lossy(value).into_owned()))
}

fn increment(
	stats: &mut BTreeMap<&'static str, (u64, u64)>,
	group: &'static str,
	bytes: usize,
) -> Result<(), Box<dyn std::error::Error>> {
	let stat = stats.entry(group).or_insert((0, 0));
	stat.0 = stat
		.0
		.checked_add(1)
		.ok_or("record count overflow")?;
	stat.1 = stat
		.1
		.checked_add(u64::try_from(bytes)?)
		.ok_or("byte count overflow")?;
	Ok(())
}
