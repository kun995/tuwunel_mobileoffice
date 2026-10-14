use std::{
	collections::{BTreeMap, BTreeSet},
	fs::OpenOptions,
	io::{self, BufWriter, Write},
	path::{Path, PathBuf},
	time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use serde::Serialize;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use tuwunel::{Args as ServerArgs, Server, runtime};
use tuwunel_database::Database;

#[derive(Debug, Parser)]
#[command(
	name = "tuwunel-chat-export",
	about = "Read-only Tuwunel chat migration export"
)]
struct Cli {
	/// Existing Tuwunel TOML configuration.
	#[arg(long, global = true)]
	config: PathBuf,
	#[command(subcommand)]
	command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
	/// Export a point-in-time snapshot as versioned JSONL.
	Export {
		#[arg(long)]
		output: Option<PathBuf>,
		/// Count records and bytes without writing any export file.
		#[arg(long)]
		dry_run: bool,
	},
	/// Secondary-instance catch-up loop. Delta extraction is intentionally a later step.
	Tail {
		#[arg(long, default_value_t = 2_000)]
		interval_ms: u64,
	},
}

#[derive(Serialize)]
struct Record<'a> {
	format: &'static str,
	version: u8,
	group: &'a str,
	source_map: &'a str,
	key_b64: String,
	value_b64: String,
}

#[derive(Serialize)]
struct SigningKeyId {
	format: &'static str,
	version: u8,
	group: &'static str,
	source_map: &'static str,
	key_id_b64: String,
}

const MAP_GROUPS: &[(&str, &str)] = &[
	("userid_displayname", "users"),
	("userid_avatarurl", "users"),
	("userid_blurhash", "users"),
	("userid_origin", "sso_links"),
	("userid_oauthid", "sso_links"),
	("eventid_originalpdu", "events"),
	("eventid_outlierpdu", "events"),
	("pduid_pdu", "events"),
	("eventid_pduid", "events"),
	("eventid_shorteventid", "events"),
	("roomid_shortroomid", "events"),
	("timeredacted_eventid", "events"),
	("referencedevents", "events"),
	("roomid_shortstatehash", "current_state"),
	("shortstatehash_statediff", "current_state"),
	("shortstatekey_statekey", "current_state"),
	("shorteventid_shortstatehash", "current_state"),
	("statehash_shortstatehash", "current_state"),
	("statekey_shortstatekey", "current_state"),
	("roomusertype_roomuserdataid", "current_state"),
	("roomuserid_joined", "current_state"),
	("userroomid_leftstate", "current_state"),
	("roomuserdataid_accountdata", "account_data"),
	("senderkey_pusher", "pushers"),
	("pushkey_deviceid", "pushers"),
	("readreceiptid_readreceipt", "receipts"),
	("roomuserid_lastprivatereadupdate", "receipts"),
	("roomuserid_lastnotificationread", "receipts"),
	("roomuserid_privateread", "receipts"),
	("userdeviceid_metadata", "devices"),
	("userdeviceid_token", "devices"),
	("userdeviceid_refresh", "devices"),
	("token_userdeviceid", "devices"),
	("mediaid_file", "media_metadata"),
	("mediaid_user", "media_metadata"),
	("mediaid_sha256", "media_metadata"),
	("media_sha256_refs", "media_metadata"),
	("workspaceid_spaceroomid", "workspaces"),
	("roomid_workspaceid", "workspaces"),
	("workspaceid_roomids", "workspaces"),
	("userid_workspaceids", "workspaces"),
	("workspaceid_userids", "workspaces"),
	("reminderid_reminder", "reminders"),
	("remindertime_reminderid", "reminders"),
	("roomid_reminderids", "reminders"),
	("userroomid_messagepairs", "message_pairs"),
	("usereventsid_deleted", "hidden_dm"),
	("userroomid_deletedbefore", "hidden_dm"),
	("userroomid_clearedbefore", "hidden_dm"),
	("roomid_haslink_pduid", "custom_state"),
	("roomid_hasusermessage", "custom_state"),
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let cli = Cli::parse();
	match cli.command {
		| Command::Export { output, dry_run } => {
			if dry_run && output.is_some() {
				return Err("--dry-run cannot be combined with --output".into());
			}
			if !dry_run && output.is_none() {
				return Err("--output is required unless --dry-run is used".into());
			}
			let (runtime, _server, database) = open_database(&cli.config, false)?;
			if !database.is_read_only() {
				return Err("snapshot export database was not opened read-only".into());
			}
			let mut writer = output
				.map(create_private_output)
				.transpose()?
				.map(BufWriter::new);
			let stats = runtime.block_on(export_records(&database, writer.as_mut()))?;
			if let Some(out) = writer.as_mut() {
				out.flush()?;
			}
			for (group, (records, bytes)) in stats {
				eprintln!("{group}: records={records} raw_bytes={bytes}");
			}
			Ok(())
		},
		| Command::Tail { interval_ms } => {
			let (runtime, _server, database) = open_database(&cli.config, true)?;
			if !database.is_secondary() {
				return Err("tail database was not opened as a RocksDB secondary".into());
			}
			loop {
				database.engine.update()?;
				eprintln!("tail checkpoint sequence={}", database.engine.current_sequence());
				runtime.block_on(tokio::time::sleep(Duration::from_millis(interval_ms)));
			}
		},
	}
}

type OpenedDatabase = (runtime::Runtime, std::sync::Arc<Server>, std::sync::Arc<Database>);

fn open_database(
	config: &Path,
	secondary: bool,
) -> Result<OpenedDatabase, Box<dyn std::error::Error>> {
	let mut args = ServerArgs::try_parse_from([
		"tuwunel-chat-export",
		"--config",
		config
			.to_str()
			.ok_or("config path must be UTF-8")?,
	])?;
	if secondary {
		args.maintenance = true;
		args.option
			.push("rocksdb_secondary=true".to_owned());
	} else {
		args.read_only = true;
	}
	let runtime = runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(runtime.handle()))?;
	let database = runtime.block_on(Database::open(&server.server))?;
	Ok((runtime, server, database))
}

pub(crate) async fn export_records<W: Write>(
	database: &Database,
	mut output: Option<&mut BufWriter<W>>,
) -> Result<BTreeMap<&'static str, (u64, u64)>, Box<dyn std::error::Error>> {
	let mut stats = BTreeMap::new();
	let keys: BTreeSet<&str> = MAP_GROUPS.iter().map(|(map, _)| *map).collect();
	let unhandled: Vec<&str> = database
		.keys()
		.copied()
		.filter(|name| *name != "keyid_key" && !keys.contains(name))
		.collect();
	eprintln!("unhandled_source_maps={unhandled:?}");

	let signing_keys = database.get("keyid_key")?;
	let rows = signing_keys.raw_stream();
	futures::pin_mut!(rows);
	while let Some(row) = rows.next().await {
		let (key_id, _private_key) = row?;
		let stat = stats
			.entry("signing_key_ids")
			.or_insert((0_u64, 0_u64));
		stat.0 = stat
			.0
			.checked_add(1)
			.ok_or("record count overflow")?;
		let key_id_bytes = u64::try_from(key_id.len())?;
		stat.1 = stat
			.1
			.checked_add(key_id_bytes)
			.ok_or("byte count overflow")?;
		if let Some(out) = output.as_deref_mut() {
			let record = SigningKeyId {
				format: "mo-chat-export",
				version: 1,
				group: "signing_key_ids",
				source_map: "keyid_key",
				key_id_b64: STANDARD.encode(key_id),
			};
			serde_json::to_writer(&mut *out, &record)?;
			out.write_all(b"\n")?;
		}
	}

	for &(map_name, group) in MAP_GROUPS {
		let map = database.get(map_name)?;
		let rows = map.raw_stream();
		futures::pin_mut!(rows);
		while let Some(row) = rows.next().await {
			let (key, value) = row?;
			let stat = stats.entry(group).or_insert((0_u64, 0_u64));
			stat.0 = stat
				.0
				.checked_add(1)
				.ok_or("record count overflow")?;
			let raw_bytes = key
				.len()
				.checked_add(value.len())
				.ok_or("byte count overflow")?;
			let raw_bytes = u64::try_from(raw_bytes)?;
			stat.1 = stat
				.1
				.checked_add(raw_bytes)
				.ok_or("byte count overflow")?;
			if let Some(out) = output.as_deref_mut() {
				let record = Record {
					format: "mo-chat-export",
					version: 1,
					group,
					source_map: map_name,
					key_b64: STANDARD.encode(key),
					value_b64: STANDARD.encode(value),
				};
				serde_json::to_writer(&mut *out, &record)?;
				out.write_all(b"\n")?;
			}
		}
	}
	Ok(stats)
}

fn create_private_output(path: PathBuf) -> io::Result<std::fs::File> {
	let mut options = OpenOptions::new();
	options.write(true).create_new(true);
	#[cfg(unix)]
	options.mode(0o600);
	options.open(path)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn allowlist_maps_are_unique_and_private_signing_value_is_not_a_group() {
		let names: BTreeSet<&str> = MAP_GROUPS.iter().map(|(name, _)| *name).collect();
		assert_eq!(names.len(), MAP_GROUPS.len());
		assert!(!names.contains("keyid_key"));
		assert!(names.contains("userdeviceid_token"));
		assert!(names.contains("roomid_workspaceid"));
		assert!(names.contains("reminderid_reminder"));
	}

	#[test]
	fn signing_key_envelope_has_no_private_value_field() {
		let record = SigningKeyId {
			format: "mo-chat-export",
			version: 1,
			group: "signing_key_ids",
			source_map: "keyid_key",
			key_id_b64: STANDARD.encode(b"ed25519:fake"),
		};
		let json = serde_json::to_value(record).unwrap();
		assert_eq!(json["group"], "signing_key_ids");
		assert!(json.get("value_b64").is_none());
	}
}
