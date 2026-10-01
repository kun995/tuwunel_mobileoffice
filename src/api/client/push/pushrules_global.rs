use axum::extract::State;
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue,
	api::client::push::get_pushrules_global_scope,
	events::{
		GlobalAccountDataEventType,
		push_rules::{PushRulesEvent, PushRulesEventContent},
	},
	push::{PredefinedContentRuleId, PredefinedOverrideRuleId, Ruleset},
};
use tuwunel_core::{Result, err};

use crate::Ruma;

/// # `GET /_matrix/client/r0/pushrules/global/`
///
/// Retrieves the push rules event for this user.
///
/// This appears to be the exact same as `GET /_matrix/client/r0/pushrules/`.
pub(crate) async fn get_pushrules_global_route(
	State(services): State<crate::State>,
	body: Ruma<get_pushrules_global_scope::v3::Request>,
) -> Result<get_pushrules_global_scope::v3::Response> {
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

		return Ok(get_pushrules_global_scope::v3::Response {
			global: Ruleset::server_default(sender_user),
		});
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

			services
				.account_data
				.update(
					None,
					sender_user,
					GlobalAccountDataEventType::PushRules
						.to_string()
						.into(),
					&serde_json::to_value(PushRulesEvent {
						content: PushRulesEventContent { global: global_ruleset.clone() },
					})
					.expect("to json always works"),
				)
				.await?;
		}
	};

	Ok(get_pushrules_global_scope::v3::Response { global: global_ruleset })
}
