mod notifications;
mod pushers;
mod pushers_set;
mod pushrules;
mod pushrules_global;
mod pushrules_rule;
mod pushrules_rule_actions;
mod pushrules_rule_enabled;
#[cfg(test)]
mod tests;

use ruma::{
	UserId,
	events::{GlobalAccountDataEventType, push_rules::PushRulesEvent},
	push::{
		AnyPushRuleRef, NewPushRule, PredefinedContentRuleId, PredefinedOverrideRuleId, RuleKind,
		Ruleset, SimplePushRule,
	},
};
use tuwunel_core::{Err, Result, err, utils::json::serialized_len};
use tuwunel_service::{
	Services,
	account_data::{MAX_RULE_BYTES, MAX_RULE_ID_BYTES, admits_rule},
};

pub(crate) use self::{
	notifications::get_notifications_route,
	pushers::get_pushers_route,
	pushers_set::set_pushers_route,
	pushrules::get_pushrules_all_route,
	pushrules_global::get_pushrules_global_route,
	pushrules_rule::{delete_pushrule_route, get_pushrule_route, set_pushrule_route},
	pushrules_rule_actions::{get_pushrule_actions_route, set_pushrule_actions_route},
	pushrules_rule_enabled::{get_pushrule_enabled_route, set_pushrule_enabled_route},
};

async fn load_push_rules(services: &Services, sender_user: &UserId) -> Result<PushRulesEvent> {
	services
		.account_data
		.get_global(sender_user, GlobalAccountDataEventType::PushRules)
		.await
		.map_err(|_| err!(Request(NotFound("PushRules event not found."))))
}

async fn save_push_rules(
	services: &Services,
	sender_user: &UserId,
	event: &PushRulesEvent,
) -> Result {
	let ty = GlobalAccountDataEventType::PushRules;

	services
		.account_data
		.update(
			None,
			sender_user,
			ty.to_string().into(),
			&serde_json::to_value(event)?,
		)
		.await
}

fn check_rule_admission(ruleset: &Ruleset, rule: &NewPushRule) -> Result {
	let rule_id = rule.rule_id();

	if rule_id.len() > MAX_RULE_ID_BYTES {
		return Err!(Request(TooLarge("Push rule ID is too long.")));
	}

	if !admits_rule(ruleset, rule.kind(), rule_id) {
		return Err!(Request(InvalidParam("Account has too many push rules.")));
	}

	Ok(())
}

fn check_rule_size(ruleset: &Ruleset, kind: RuleKind, rule_id: &str) -> Result {
	let rule = ruleset
		.get(kind, rule_id)
		.ok_or_else(|| err!(Request(NotFound("Push rule not found."))))?;

	let size = match rule {
		AnyPushRuleRef::Override(rule) | AnyPushRuleRef::Underride(rule) => {
			serialized_len(&(&rule.conditions, &rule.actions))?
		}

		AnyPushRuleRef::Content(rule) => serialized_len(&(&rule.pattern, &rule.actions))?,

		AnyPushRuleRef::Room(SimplePushRule { actions, .. })
		| AnyPushRuleRef::Sender(SimplePushRule { actions, .. }) => serialized_len(actions)?,
	};

	if size > MAX_RULE_BYTES {
		return Err!(Request(TooLarge("Push rule is too large.")));
	}

	Ok(())
}

// The deprecated mention push rules are hidden from clients as per MSC4210.
#[expect(deprecated)]
fn is_deprecated_mention_rule(rule_id: &str) -> bool {
	rule_id == PredefinedContentRuleId::ContainsUserName.as_str()
		|| rule_id == PredefinedOverrideRuleId::ContainsDisplayName.as_str()
		|| rule_id == PredefinedOverrideRuleId::RoomNotif.as_str()
}
