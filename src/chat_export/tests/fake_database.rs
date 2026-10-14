use std::{
	collections::BTreeSet,
	fs,
	io::BufWriter,
	time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;
use tuwunel::{Args, Server, runtime};
use tuwunel_core::{
	matrix::{event::gen_event_id, pdu::PduBuilder},
	ruma::{
		CanonicalJsonObject, CanonicalJsonValue, RoomId, RoomVersionId, UserId,
		events::room::{
			create::RoomCreateEventContent,
			member::{MembershipState, RoomMemberEventContent},
		},
	},
};
use tuwunel_database::Json;
use tuwunel_service::Services;

#[path = "../main.rs"]
#[allow(dead_code)]
mod exporter;
use exporter::{business::current_state_snapshot, export_records, export_records_mode};

#[test]
fn exports_fake_service_database_records_by_group_without_private_key_bytes()
-> Result<(), Box<dyn std::error::Error>> {
	let nonce = SystemTime::now()
		.duration_since(UNIX_EPOCH)?
		.as_nanos();
	let db_path = std::env::temp_dir()
		.join(format!("tuwunel-chat-export-fixture-{}-{nonce}", std::process::id()));
	fs::create_dir(&db_path)?;
	let mut args = Args::default_test(&["chat-export-fixture"]);
	args.maintenance = true;
	args.option
		.push(format!("database_path={:?}", db_path.to_string_lossy()));
	let runtime = runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime.handle()))?;
	let services = runtime.block_on(Services::build(server.server.clone()))?;

	let alice = UserId::parse("@alice:localhost")?;
	let bob = UserId::parse("@bob:localhost")?;
	let room = RoomId::parse("!room-fixture:localhost")?;
	let indexed_room = RoomId::parse("!real-state-index:localhost")?;
	runtime.block_on(async {
		services
			.users
			.create(&alice, None, Some("sso"))
			.await?;
		services
			.users
			.set_displayname(&alice, Some("Fixture Alice"));
		services
			.users
			.create(&bob, None, Some("sso"))
			.await?;
		services
			.users
			.set_displayname(&bob, Some("Fixture Bob"));
		services
			.workspace
			.add_user_workspace(&alice, "workspace-fixture")
			.await;
		services
			.workspace
			.set_workspace_id(&room, "workspace-fixture");
		Ok::<(), tuwunel_core::Error>(())
	})?;

	// Exercise the production room timeline/state path as well as raw fixture
	// maps: this creates the short state hash and current-state index used by
	// the business exporter, rather than merely testing snapshot serialization.
	runtime.block_on(async {
		services
			.short
			.get_or_create_shortroomid(&indexed_room)
			.await;
		let state_lock = services.state.mutex.lock(indexed_room).await;
		services
			.timeline
			.build_and_append_pdu(
				PduBuilder::state(String::new(), &RoomCreateEventContent::new_v11()),
				&alice,
				&indexed_room,
				&state_lock,
			)
			.await?;
		services
			.timeline
			.build_and_append_pdu(
				PduBuilder::state(
					alice.to_string(),
					&RoomMemberEventContent::new(MembershipState::Join),
				),
				&alice,
				&indexed_room,
				&state_lock,
			)
			.await?;
		Ok::<(), tuwunel_core::Error>(())
	})?;

	// Populate actual PDU rows with fake canonical events in a two-event DAG.
	// Event IDs are calculated with the same room-version hashing function as
	// Tuwunel; no live or backup database is involved.
	let create = fake_pdu(
		serde_json::json!({"room_id":room.as_str(),"type":"m.room.create","sender":alice.as_str(),"state_key":"","origin_server_ts":1,"depth":1,"prev_events":[],"auth_events":[],"content":{"creator":alice.as_str(),"room_version":"11"},"hashes":{"sha256":"fixture"},"signatures":{}}),
		RoomVersionId::V11,
	)?;
	let create_id = create["event_id"].as_str().unwrap().to_owned();
	let message = fake_pdu(
		serde_json::json!({"room_id":room.as_str(),"type":"m.room.message","sender":alice.as_str(),"origin_server_ts":2,"depth":2,"prev_events":[create_id],"auth_events":[],"content":{"msgtype":"m.text","body":"fixture"},"hashes":{"sha256":"fixture"},"signatures":{}}),
		RoomVersionId::V11,
	)?;
	let message_id = message["event_id"].as_str().unwrap().to_owned();
	services
		.db
		.get("pduid_pdu")?
		.raw_put(b"fixture-pdu-create", Json(create));
	services
		.db
		.get("pduid_pdu")?
		.raw_put(b"fixture-pdu-message", Json(message));
	services
		.db
		.get("eventid_originalpdu")?
		.insert(message_id.as_bytes(), b"original");
	services
		.db
		.get("userid_password")?
		.insert(alice.as_bytes(), b"fixture-password-hash");
	services
		.db
		.get("userid_password")?
		.insert(b"@deactivated:localhost", b"");
	services
		.db
		.get("userid_avatarurl")?
		.insert(alice.as_bytes(), b"mxc://localhost/avatar");
	services
		.db
		.get("userid_origin")?
		.insert(b"@alice:localhost\xffkeycloak", b"mobile-office");
	services
		.db
		.get("roomuserdataid_accountdata")?
		.insert(
			b"\xff@alice:localhost\xff1\xffm.direct",
			br#"{"type":"m.direct","content":{"@bob:localhost":["!room-fixture:localhost"]}}"#,
		);
	services
		.db
		.get("senderkey_pusher")?
		.insert(b"@alice:localhost\xffpush-key", br#"{"kind":"http","app_id":"fixture"}"#);
	services
		.db
		.get("readreceiptid_readreceipt")?
		.insert(
			b"!room-fixture:localhost\xff@alice:localhost\xff$fixture-event",
			b"fixture-receipt",
		);
	services
		.db
		.get("userdeviceid_metadata")?
		.insert(b"@alice:localhost\xffdevice-fixture", br#"{"display_name":"Fixture device"}"#);
	let media_key = [
		b"mxc://localhost/media-fixture".as_slice(),
		b"\xff",
		&0u32.to_be_bytes(),
		b"\xff",
		&0u32.to_be_bytes(),
		b"\xffattachment; filename=fixture.png\xffimage/png",
	]
	.concat();
	services
		.db
		.get("mediaid_file")?
		.insert(&media_key, b"");
	services
		.db
		.get("mediaid_user")?
		.insert(b"mxc://localhost/media-fixture\xff@alice:localhost", b"@alice:localhost");
	services
		.db
		.get("mediaid_sha256")?
		.insert(&media_key, &[0x11; 32]);
	services
		.db
		.get("media_sha256_refs")?
		.insert(&[0x11; 32], &1u32.to_be_bytes());
	services
		.db
		.get("roomid_hasusermessage")?
		.insert(b"!room-fixture:localhost", b"1");
	services.db.get("reminderid_reminder")?.insert(
		b"rem_fixture",
		br#"{"room_id":"!room-fixture:localhost","created_by":"@alice:localhost","title":"fixture reminder"}"#,
	);
	services
		.db
		.get("userroomid_messagepairs")?
		.insert(
			b"fixture-user-room-pair",
			br#"{"target_event_id":"$fixture-event:localhost","owner":"@alice:localhost"}"#,
		);
	services
		.db
		.get("usereventsid_deleted")?
		.insert(b"fixture-user-event-deleted", b"1");
	services
		.db
		.get("keyid_key")?
		.insert(b"ed25519:fixture", b"DO_NOT_EXPORT_PRIVATE_KEY");

	let mut writer = BufWriter::new(Vec::new());
	let stats = runtime.block_on(export_records(&services.db, &services, Some(&mut writer)))?;
	use std::io::Write as _;
	writer.flush()?;
	let jsonl = String::from_utf8(writer.into_inner()?)?;
	let rows: Vec<Value> = jsonl
		.lines()
		.map(serde_json::from_str)
		.collect::<Result<_, _>>()?;
	let groups: BTreeSet<&str> = rows
		.iter()
		.filter_map(|row| row["group"].as_str())
		.collect();
	for group in [
		"users",
		"sso_links",
		"workspaces",
		"events",
		"account_data",
		"pushers",
		"receipts",
		"devices",
		"media_metadata",
		"reminders",
		"message_pairs",
		"hidden_dm",
		"custom_state",
		"signing_key_ids",
	] {
		assert!(groups.contains(group), "missing group {group}");
		assert!(stats[group].0 > 0, "group {group} has no counted rows");
	}
	assert!(!jsonl.contains("DO_NOT_EXPORT_PRIVATE_KEY"));
	assert!(stats["events"].0 >= 4);
	assert_eq!(stats["reminders"].0, 1);
	assert_eq!(stats["users"].0, 3);
	assert!(!jsonl.contains("fixture-password-hash"));
	let events: Vec<_> = rows
		.iter()
		.filter(|row| row["group"] == "events" && row["room_id"] == room.as_str())
		.collect();
	assert_eq!(events.len(), 2);
	assert_eq!(events[0]["event"]["type"], "m.room.create");
	assert_eq!(events[1]["event_id"], message_id);
	assert_eq!(events[1]["prev_events"][0], create_id);
	assert_eq!(events[1]["redacted"], true);
	assert_eq!(events[1]["event_id_hash_valid"], true);
	let snapshot = current_state_snapshot(room.as_str(), "m.room.create", "", &create_id);
	assert_eq!(snapshot["group"], "current_state");
	assert_eq!(snapshot["record_type"], "current_state_snapshot");
	assert_eq!(snapshot["room_id"], room.as_str());
	assert_eq!(snapshot["type"], "m.room.create");
	assert_eq!(snapshot["state_key"], "");
	assert_eq!(snapshot["event_id"], create_id);
	let profiles: Vec<_> = rows
		.iter()
		.filter(|row| row["source_map"] == "user_profile")
		.collect();
	assert!(
		profiles
			.iter()
			.any(|row| row["user_id"] == "@alice:localhost"
				&& row["value"]["avatar_url"] == "mxc://localhost/avatar")
	);
	let indexed_snapshots: Vec<_> = rows
		.iter()
		.filter(|row| row["group"] == "current_state" && row["room_id"] == indexed_room.as_str())
		.collect();
	assert!(
		indexed_snapshots
			.iter()
			.any(|row| row["type"] == "m.room.create" && row["state_key"] == ""),
		"create state was not read back from the live Tuwunel state index: {indexed_snapshots:?}"
	);
	assert!(
		indexed_snapshots
			.iter()
			.any(|row| row["type"] == "m.room.member" && row["state_key"] == "@alice:localhost"),
		"creator membership was not read back from the live Tuwunel state index: {indexed_snapshots:?}"
	);
	assert!(
		rows.iter()
			.any(|row| row["source_map"] == "roomuserdataid_accountdata"
				&& row["record_type"] == "account_data_item"
				&& row["identity"]["account_data_type"] == "m.direct"
				&& row["value"]["content"]["@bob:localhost"][0] == "!room-fixture:localhost")
	);
	assert!(
		rows.iter()
			.any(|row| row["source_map"] == "senderkey_pusher"
				&& row["record_type"] == "pusher"
				&& row["value"]["kind"] == "http")
	);
	let media = rows
		.iter()
		.find(|row| row["record_type"] == "media_file")
		.unwrap();
	assert_eq!(media["value"]["media_id"], "media-fixture");
	assert_eq!(media["value"]["server_name"], "localhost");
	assert_eq!(media["value"]["uploader"], "@alice:localhost");
	assert_eq!(media["value"]["content_type"], "image/png");
	assert_eq!(media["value"]["dimensions"]["width"], 0);
	assert_eq!(media["value"]["filename"], "fixture.png");
	assert_eq!(media["value"]["sha256"], "11".repeat(32));
	assert_eq!(media["value"]["reference_count"], 1);
	for (source_map, expected_kind) in [
		("userid_origin", "sso_identity_provider"),
		("readreceiptid_readreceipt", "receipt"),
		("userdeviceid_metadata", "device"),
		("workspaceid_roomids", "room_workspace"),
		("reminderid_reminder", "reminder_item"),
		("userroomid_messagepairs", "message_pair"),
		("usereventsid_deleted", "hidden_dm_checkpoint"),
		("roomid_hasusermessage", "room_user_message_flag"),
	] {
		assert!(
			rows.iter()
				.any(|row| row["source_map"] == source_map && row["record_type"] == expected_kind),
			"missing {source_map} classification"
		);
	}
	assert!(
		profiles
			.iter()
			.any(|row| row["user_id"] == "@deactivated:localhost"
				&& row["value"]["deactivated"] == true)
	);

	let mut raw_writer = BufWriter::new(Vec::new());
	runtime.block_on(export_records_mode(&services.db, Some(&mut raw_writer), true))?;
	let raw_output = String::from_utf8(raw_writer.into_inner()?)?;
	assert!(
		raw_output
			.lines()
			.all(|line| serde_json::from_str::<Value>(line).unwrap()["version"] == 1)
	);

	drop(services);
	drop(server);
	drop(runtime);
	fs::remove_dir_all(db_path)?;
	Ok(())
}

fn fake_pdu(
	value: Value,
	room_version: RoomVersionId,
) -> Result<Value, Box<dyn std::error::Error>> {
	let canonical: CanonicalJsonObject = serde_json::from_value(value)?;
	let event_id = gen_event_id(&canonical, &room_version)?;
	let mut pdu = canonical;
	pdu.insert("event_id".into(), CanonicalJsonValue::String(event_id.to_string()));
	Ok(serde_json::to_value(pdu)?)
}
