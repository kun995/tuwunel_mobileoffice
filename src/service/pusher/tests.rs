#![cfg(test)]

use ruma::{EventId, RoomId, UserId};
use serde_json::{from_value, json};
use tuwunel_database::{
	Ignore, IgnoreAll, Interfix, SEP, deserialize_from_slice, serialize_to_vec,
};

use super::ExtractRelatesTo;

const ROOM: &str = "!room:example.com";
const USER: &str = "@user:example.com";
const THREAD_ROOT_A: &str = "$thread_root_a";
const THREAD_ROOT_B: &str = "$thread_root_b";

fn room() -> &'static RoomId {
	ROOM.try_into().unwrap()
}
fn user() -> &'static UserId {
	USER.try_into().unwrap()
}
fn root_a() -> &'static EventId {
	THREAD_ROOT_A.try_into().unwrap()
}
fn root_b() -> &'static EventId {
	THREAD_ROOT_B.try_into().unwrap()
}

fn main_key() -> Vec<u8> {
	serialize_to_vec((user(), room())).expect("serialize main key")
}
fn thread_key(root: &EventId) -> Vec<u8> {
	serialize_to_vec((user(), room(), root)).expect("serialize thread key")
}
fn interfix_prefix() -> Vec<u8> {
	serialize_to_vec((user(), room(), Interfix)).expect("serialize prefix")
}

/// Main `(user, room)` and thread `(user, room, root)` rows share a CF.
/// The `Interfix` prefix appends a trailing separator so a `starts_with`
/// scan matches only the longer 3-tuple shape.
#[test]
fn interfix_prefix_excludes_main_row() {
	let prefix = interfix_prefix();
	let main = main_key();

	assert!(!main.starts_with(&prefix), "Main 2-tuple row must not match thread prefix");
	assert_eq!(prefix.len(), main.len() + 1);
	assert_eq!(&prefix[..main.len()], &*main);
	assert_eq!(*prefix.last().unwrap(), SEP);
}

#[test]
fn interfix_prefix_includes_thread_row() {
	let prefix = interfix_prefix();
	let thread = thread_key(root_a());

	assert!(thread.starts_with(&prefix), "Thread 3-tuple row must match thread prefix");
}

#[test]
fn distinct_threads_have_distinct_keys() {
	assert_ne!(thread_key(root_a()), thread_key(root_b()));
}

/// Sweeping the 3-tuple prefix removes thread rows but not the main row,
/// per `clear_all_thread_notification_counts`.
#[test]
fn thread_prefix_sweep_preserves_main() {
	let prefix = interfix_prefix();
	let main = main_key();
	let a = thread_key(root_a());
	let b = thread_key(root_b());

	assert!(a.starts_with(&prefix));
	assert!(b.starts_with(&prefix));
	assert!(!main.starts_with(&prefix));
}

#[test]
fn notification_key_room_survives_main_and_thread_tail() {
	for key in [main_key(), thread_key(root_a())] {
		let (_, room_id, _): (Ignore, &RoomId, IgnoreAll) =
			deserialize_from_slice(&key).expect("deserialize notification key");

		assert_eq!(room_id, room());
	}
}

/// A reaction relates to the event it annotates, which is the relation
/// MSC3664 follows to notify an author of reactions to their own message.
#[test]
fn reaction_relates_to_the_annotated_event() {
	let content = json!({
		"m.relates_to": {
			"rel_type": "m.annotation",
			"event_id": THREAD_ROOT_A,
			"key": "👍",
		},
	});

	let ExtractRelatesTo { relates_to } =
		from_value(content).expect("deserialize reaction content");

	assert_eq!(relates_to.rel_type.as_deref(), Some("m.annotation"));
	assert_eq!(relates_to.event_id.as_deref(), Some(root_a()));
	assert!(relates_to.in_reply_to.is_none());
}

/// A rich reply carries no `rel_type`, so it is only reachable through the
/// nested `m.in_reply_to`, which MSC3664 matches as its own relation type.
#[test]
fn reply_relates_only_through_in_reply_to() {
	let content = json!({
		"body": "Sounds good",
		"msgtype": "m.text",
		"m.relates_to": {
			"m.in_reply_to": { "event_id": THREAD_ROOT_A },
		},
	});

	let ExtractRelatesTo { relates_to } = from_value(content).expect("deserialize reply content");

	let in_reply_to = relates_to.in_reply_to.map(|reply| reply.event_id);

	assert!(relates_to.rel_type.is_none());
	assert!(relates_to.event_id.is_none());
	assert_eq!(in_reply_to.as_deref(), Some(root_a()));
}

/// A threaded message carries both relations, so both are resolved: the
/// thread root and the reply fallback target.
#[test]
fn threaded_message_relates_to_root_and_reply() {
	let content = json!({
		"body": "Sounds good",
		"msgtype": "m.text",
		"m.relates_to": {
			"rel_type": "m.thread",
			"event_id": THREAD_ROOT_A,
			"is_falling_back": true,
			"m.in_reply_to": { "event_id": THREAD_ROOT_B },
		},
	});

	let ExtractRelatesTo { relates_to } =
		from_value(content).expect("deserialize threaded content");

	let in_reply_to = relates_to.in_reply_to.map(|reply| reply.event_id);

	assert_eq!(relates_to.event_id.as_deref(), Some(root_a()));
	assert_eq!(in_reply_to.as_deref(), Some(root_b()));
}

/// Content without a relation must not resolve one, which is what keeps
/// the lookup off the path of every ordinary message.
#[test]
fn unrelated_message_resolves_no_relation() {
	let content = json!({ "body": "Dinner at 7?", "msgtype": "m.text" });

	assert!(from_value::<ExtractRelatesTo>(content).is_err(), "content carries no relation");
}

#[test]
fn ioffice_legacy_notification_rows_keep_main_counter() {
	let old = json!({"ts": 1, "sroomid": 2, "actions": ["notify"]});
	let notified: super::Notified = from_value(old).unwrap();
	assert!(notified.thread_root.is_none());
	assert!(
		serde_json::to_value(notified)
			.unwrap()
			.get("thread_root")
			.is_none()
	);
	let threaded =
		json!({"ts": 1, "sroomid": 2, "actions": ["notify"], "thread_root": THREAD_ROOT_A});
	let notified: super::Notified = from_value(threaded).unwrap();
	assert_eq!(notified.thread_root.as_deref(), Some(root_a()));
}
