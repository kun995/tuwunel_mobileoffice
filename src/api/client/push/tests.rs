use std::iter::once;

use ruma::{
	OwnedRoomId, RoomId, UserId,
	push::{
		Action, Actions, EventMatchConditionData, NewConditionalPushRule, NewPatternedPushRule,
		NewPushRule, NewSimplePushRule, PushCondition, RuleKind, Ruleset, SimplePushRule,
	},
};
use tuwunel_service::account_data::{MAX_RULE_BYTES, MAX_RULE_ID_BYTES, MAX_RULES, admits_rule};

use super::{check_rule_admission, check_rule_size};

#[test]
fn admission_refuses_an_overlong_rule_id() {
	let ruleset = Ruleset::new();
	let rule = content_rule(&"r".repeat(MAX_RULE_ID_BYTES + 1), "cat");

	check_rule_admission(&ruleset, &rule).expect_err("an overlong rule id is refused");
}

#[test]
fn admission_takes_a_rule_id_at_the_limit() {
	let ruleset = Ruleset::new();
	let rule = content_rule(&"r".repeat(MAX_RULE_ID_BYTES), "cat");

	check_rule_admission(&ruleset, &rule).expect("a rule id at the limit is taken");
}

#[test]
fn admission_refuses_a_new_rule_on_a_full_ruleset() {
	let ruleset = full_ruleset();
	let rule = content_rule("late", "cat");

	check_rule_admission(&ruleset, &rule).expect_err("a full ruleset takes no further rule");
}

#[test]
fn admission_takes_a_replacement_on_a_full_ruleset() {
	let ruleset = full_ruleset();
	let rule = NewPushRule::Room(NewSimplePushRule::new(room_id(0), notify()));

	check_rule_admission(&ruleset, &rule).expect("replacing a rule adds none");
}

#[test]
fn admits_rule_refuses_an_overlong_room_id() {
	let ruleset = Ruleset::new();
	let long_room = format!("!{}:example.com", "r".repeat(MAX_RULE_ID_BYTES));

	assert!(
		!admits_rule(&ruleset, RuleKind::Room, &long_room),
		"a room id past the ceiling is refused as a rule id"
	);
}

#[test]
fn size_refuses_oversized_conditions() {
	let conditions = (0..MAX_RULE_BYTES)
		.map(|index| {
			PushCondition::EventMatch(EventMatchConditionData {
				key: format!("content.body{index}"),
				pattern: "cat".into(),
			})
		})
		.collect();

	let rule =
		NewPushRule::Override(NewConditionalPushRule::new("heavy".into(), conditions, notify()));

	let ruleset = ruleset_with(rule);

	check_rule_size(&ruleset, RuleKind::Override, "heavy")
		.expect_err("conditions count toward the rule size");
}

#[test]
fn size_takes_a_room_rule() {
	let rule = NewPushRule::Room(NewSimplePushRule::new(room_id(0), notify()));
	let ruleset = ruleset_with(rule);

	check_rule_size(&ruleset, RuleKind::Room, room_id(0).as_str())
		.expect("a room rule carries actions only and fits");
}

#[test]
fn size_refuses_an_oversized_content_pattern() {
	let ruleset = ruleset_with(content_rule("wide", &"*a".repeat(MAX_RULE_BYTES)));

	check_rule_size(&ruleset, RuleKind::Content, "wide")
		.expect_err("the pattern counts toward the rule size");
}

#[test]
fn size_takes_an_ordinary_content_rule() {
	let ruleset = ruleset_with(content_rule("plain", "cat"));

	check_rule_size(&ruleset, RuleKind::Content, "plain").expect("an ordinary rule fits");
}

fn content_rule(rule_id: &str, pattern: &str) -> NewPushRule {
	NewPushRule::Content(NewPatternedPushRule::new(rule_id.into(), pattern.into(), notify()))
}

/// Builds on the server defaults rather than an empty ruleset, matching what
/// an account actually stores; ruma positions a new override rule second, so
/// an empty override set is not a shape this code ever sees.
fn ruleset_with(rule: NewPushRule) -> Ruleset {
	let user = UserId::parse("@test:example.com").expect("the user id parses");
	let mut ruleset = Ruleset::server_default(&user);

	ruleset
		.insert(rule, None, None)
		.expect("the rule inserts");

	ruleset
}

fn full_ruleset() -> Ruleset {
	let room = (0..MAX_RULES)
		.map(|index| SimplePushRule {
			actions: notify(),
			default: false,
			enabled: true,
			rule_id: room_id(index),
		})
		.collect();

	Ruleset { room, ..Ruleset::new() }
}

fn room_id(index: usize) -> OwnedRoomId {
	RoomId::parse(format!("!{index}:example.com")).expect("the room id parses")
}

fn notify() -> Actions { once(Action::Notify).collect() }
