use axum::extract::State;
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue,
	api::client::push::get_pushrules_all,
	events::{
		GlobalAccountDataEventType,
		push_rules::{PushRulesEvent, PushRulesEventContent},
	},
	push::{PredefinedContentRuleId, PredefinedOverrideRuleId, Ruleset},
};
use tuwunel_core::{Result, err};
use tuwunel_service::Services;

use crate::Ruma;

/// # `GET /_matrix/client/r0/pushrules/`
///
/// Retrieves the push rules event for this user.
pub(crate) async fn get_pushrules_all_route(
	State(services): State<crate::State>,
	body: Ruma<get_pushrules_all::v3::Request>,
) -> Result<get_pushrules_all::v3::Response> {
	let sender_user = body.sender_user();

	let Some(content_value) = services
		.account_data
		.get_global::<CanonicalJsonObject>(sender_user, GlobalAccountDataEventType::PushRules)
		.await
		.ok()
		.and_then(|event| event.get("content").cloned())
		.filter(CanonicalJsonValue::is_object)
	else {
		// user somehow has non-existent push rule event. recreate it and return server
		// default silently
		return recreate_push_rules_and_return(&services, sender_user).await;
	};

	let account_data_content =
		serde_json::from_value::<PushRulesEventContent>(content_value.into()).map_err(|e| {
			err!(Database(warn!("Invalid push rules account data event in database: {e}")))
		})?;

	let mut global_ruleset = account_data_content.global;

	// remove old deprecated mentions push rules as per MSC4210
	// and update the stored server default push rules
	#[expect(deprecated)]
	{
		use ruma::push::RuleKind::*;
		if global_ruleset
			.get(Override, PredefinedOverrideRuleId::ContainsDisplayName.as_str())
			.is_some()
			|| global_ruleset
				.get(Override, PredefinedOverrideRuleId::RoomNotif.as_str())
				.is_some()
			|| global_ruleset
				.get(Content, PredefinedContentRuleId::ContainsUserName.as_str())
				.is_some()
			|| global_ruleset
				.get(Override, PredefinedOverrideRuleId::Reply.as_str())
				.is_none()
		{
			global_ruleset
				.remove(Override, PredefinedOverrideRuleId::ContainsDisplayName)
				.ok();
			global_ruleset
				.remove(Override, PredefinedOverrideRuleId::RoomNotif)
				.ok();
			global_ruleset
				.remove(Content, PredefinedContentRuleId::ContainsUserName)
				.ok();

			global_ruleset.update_with_server_default(Ruleset::server_default(sender_user));

			let ty = GlobalAccountDataEventType::PushRules;
			let event = PushRulesEvent {
				content: PushRulesEventContent { global: global_ruleset.clone() },
			};

			services
				.account_data
				.update(None, sender_user, ty.to_string().into(), &serde_json::to_value(event)?)
				.await?;
		}
	};

	Ok(get_pushrules_all::v3::Response { global: global_ruleset })
}

/// user somehow has bad push rules, these must always exist per spec.
/// so recreate it and return server default silently
async fn recreate_push_rules_and_return(
	services: &Services,
	sender_user: &ruma::UserId,
) -> Result<get_pushrules_all::v3::Response> {
	let ty = GlobalAccountDataEventType::PushRules;
	let event = PushRulesEvent {
		content: PushRulesEventContent {
			global: Ruleset::server_default(sender_user),
		},
	};

	services
		.account_data
		.update(None, sender_user, ty.to_string().into(), &serde_json::to_value(event)?)
		.await?;

	Ok(get_pushrules_all::v3::Response {
		global: Ruleset::server_default(sender_user),
	})
}
