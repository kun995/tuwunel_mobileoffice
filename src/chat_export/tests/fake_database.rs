use std::{
	collections::BTreeSet,
	fs,
	io::BufWriter,
	time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;
use tuwunel::{Args, Server, runtime};
use tuwunel_core::ruma::{RoomId, UserId};
use tuwunel_service::Services;

#[path = "../main.rs"]
#[allow(dead_code)]
mod exporter;
use exporter::export_records;

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

	// Populate source families that require a full room/event API setup with
	// synthetic Tuwunel-codec values; no live or backup database is involved.
	services.db.get("eventid_originalpdu")?.insert(
		b"$fixture-event:localhost",
		br#"{"event_id":"$fixture-event:localhost","room_id":"!room-fixture:localhost","type":"m.room.message","sender":"@alice:localhost","content":{"body":"fixture"}}"#,
	);
	services
		.db
		.get("timeredacted_eventid")?
		.insert(b"1", b"$fixture-redacted:localhost");
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
	let stats = runtime.block_on(export_records(&services.db, Some(&mut writer)))?;
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
	assert!(groups.contains("users"));
	assert!(groups.contains("workspaces"));
	assert!(groups.contains("events"));
	assert!(groups.contains("reminders"));
	assert!(groups.contains("message_pairs"));
	assert!(groups.contains("hidden_dm"));
	assert!(groups.contains("signing_key_ids"));
	assert!(!jsonl.contains("DO_NOT_EXPORT_PRIVATE_KEY"));
	assert!(stats["users"].0 >= 2);
	assert!(stats["events"].0 >= 2);
	assert!(stats["reminders"].0 >= 1);

	drop(services);
	drop(server);
	drop(runtime);
	fs::remove_dir_all(db_path)?;
	Ok(())
}
