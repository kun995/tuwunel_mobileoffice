use futures::{
	FutureExt,
	future::{join, join4},
};
use ruma::{Mxc, OwnedMxcUri};
use ruma::{
	UInt, UserId,
	api::{
		client::push::{Pusher, PusherKind},
		push_gateway::send_event_notification::v1::{
			Device, Notification, NotificationCounts, NotificationPriority, Request,
		},
	},
	events::TimelineEventType,
	push::{Action, HighlightTweakValue, HttpPusherData, PushFormat, Ruleset, Tweak},
};
use serde_json::Value;
use serde_json::Value as JsonValue;
use tuwunel_core::{Result, err, error, implement, matrix::Event, trace, utils::BoolExt};
use url::Url;

use super::Evaluate;

const REMINDER_CREATED_EVENT_TYPE: &str = "im.tuwunel.reminder.created";
const REMINDER_DUE_EVENT_TYPE: &str = "im.tuwunel.reminder.due";

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn send_push_notice<E>(
	&self,
	user_id: &UserId,
	pusher: &Pusher,
	ruleset: &Ruleset,
	event: &E,
) -> Result
where
	E: Event,
{
	let power_levels = self
		.services
		.state_accessor
		.get_power_levels(event.room_id())
		.map(Result::ok);

	let (power_levels, related_events) = join(power_levels, self.related_events(event)).await;

	let serialized = event.to_format();
	let actions = self
		.get_actions(Evaluate {
			user: user_id,
			ruleset,
			power_levels: power_levels.as_ref(),
			pdu: &serialized,
			room_id: event.room_id(),
			related_events: related_events.as_ref(),
		})
		.await;

	let notify = actions.iter().any(Action::should_notify);
	let tweak_count = actions
		.iter()
		.filter(|action| matches!(action, Action::SetTweak(_)))
		.count();

	trace!(
		%user_id,
		event_id = %event.event_id(),
		actions = %actions.len(),
		notify,
		tweaks = tweak_count,
		"Push notice decision",
	);

	if notify
		|| is_reminder_push_event(event.kind())
		|| *event.kind() == TimelineEventType::Reaction
		|| self.services.config.push_everything
	{
		let tweaks: Vec<Tweak> = actions
			.iter()
			.filter_map(|action| match action {
				| Action::SetTweak(tweak) => Some(tweak.clone()),
				| _ => None,
			})
			.collect();

		self.send_notice(user_id, pusher, tweaks, event)
			.await?;
	}

	Ok(())
}

/// Send an account-wide counts-only notification to a push gateway.
///
/// Enabled HTTP pushers emit the request, including an explicit zero. The
/// delivery is skipped only when the gateway is known to hold the current
/// total already; an unknown gateway is always sent to.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn send_badge_notice(&self, user_id: &UserId, pusher: &Pusher) -> Result {
	let PusherKind::Http(http) = &pusher.kind else {
		return Ok(());
	};

	if badge_count_disabled(http) {
		return Ok(());
	}

	let unread = UInt::new(self.global_notification_count(user_id).await).unwrap_or(UInt::MAX);

	if self.sent_badge(user_id, &pusher.ids.pushkey) == Some(unread) {
		return Ok(());
	}

	let device = self.prepare_http_pusher(pusher, http)?;
	let mut notify = Notification::new(vec![device]);
	notify.counts = NotificationCounts::new_explicit(Some(unread), None);

	self.send_http_notice(
		user_id,
		pusher,
		http,
		notify,
		Some(unread),
		None,
		None,
		None,
		None,
		None,
		None,
	)
	.await
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip_all)]
async fn send_notice<Pdu: Event>(
	&self,
	user_id: &UserId,
	pusher: &Pusher,
	tweaks: Vec<Tweak>,
	event: &Pdu,
) -> Result {
	// TODO: email
	match &pusher.kind {
		| PusherKind::Http(http) => {
			self.send_http_event_notice(user_id, pusher, http, tweaks, event)
				.await
		},
		// TODO: Handle email
		//PusherKind::Email(_) => Ok(()),
		| _ => Ok(()),
	}
}

#[implement(super::Service)]
async fn send_http_event_notice<Pdu: Event>(
	&self,
	user_id: &UserId,
	pusher: &Pusher,
	http: &HttpPusherData,
	tweaks: Vec<Tweak>,
	event: &Pdu,
) -> Result {
	let mut device = self.prepare_http_pusher(pusher, http)?;

	// TODO (timo): can pusher/devices have conflicting formats
	let event_id_only = http.format == Some(PushFormat::EventIdOnly);

	if !event_id_only {
		device.tweaks.clone_from(&tweaks);
	}

	let mut notify = Notification::new(vec![device]);

	notify.event_id = Some(event.event_id().to_owned());
	notify.room_id = Some(event.room_id().to_owned());

	let workspace_id = self
		.services
		.workspace
		.get_workspace_id(event.room_id())
		.await
		.ok();
	let is_direct_chat = if event_id_only {
		None
	} else {
		Some(
			self.services
				.account_data
				.is_direct(user_id, event.room_id())
				.await || self
				.services
				.state_accessor
				.is_direct(event.room_id(), user_id)
				.await,
		)
	};
	let sender_avatar_url = if event_id_only {
		None
	} else {
		self.services
			.profile
			.avatar_url(event.sender())
			.await
			.ok()
			.map(|avatar_url| avatar_url.to_string())
	};
	let room_avatar_url = if event_id_only {
		None
	} else {
		self.services
			.state_accessor
			.get_avatar(event.room_id())
			.await
			.ok()
			.and_then(|content| content.url)
			.map(|avatar_url| avatar_url.to_string())
	};
	let (image_mxc_uri, image_url) =
		if !event_id_only && *event.kind() == TimelineEventType::RoomMessage {
			if let Some((image_mxc, preview_mxc)) = image_push_uris(event.content()) {
				let image_mxc_uri = Some(image_mxc.to_string());
				let image_url = match Mxc::try_from(&preview_mxc) {
					| Ok(mxc) => self.services.media.push_preview_url(&mxc).await,
					| Err(_) => None,
				};
				(image_mxc_uri, image_url)
			} else {
				(None, None)
			}
		} else {
			(None, None)
		};

	let unread = badge_count_disabled(http)
		.is_false()
		.then_async(async || {
			UInt::new(
				self.room_notification_count_for_push(user_id, event.room_id())
					.await,
			)
			.unwrap_or(UInt::MAX)
		});

	let unread = if !event_id_only {
		if *event.kind() == TimelineEventType::RoomEncrypted
			|| tweaks
				.iter()
				.any(|t| matches!(t, Tweak::Highlight(HighlightTweakValue::Yes) | Tweak::Sound(_)))
		{
			notify.prio = NotificationPriority::High;
		} else {
			notify.prio = NotificationPriority::Low;
		}
		notify.sender = Some(event.sender().to_owned());
		notify.event_type = Some(event.kind().to_owned());
		notify.content = serde_json::value::to_raw_value(event.content()).ok();

		if *event.kind() == TimelineEventType::RoomMember {
			notify.user_is_target = event.state_key() == Some(event.sender().as_str());
		}

		let (display_name, room_name, room_alias, unread) = join4(
			self.services.profile.displayname(event.sender()),
			self.services
				.state_accessor
				.get_name(event.room_id()),
			self.services
				.state_accessor
				.get_canonical_alias(event.room_id()),
			unread,
		)
		.await;

		notify.sender_display_name = display_name.ok();
		notify.room_name = room_name.ok();
		notify.room_alias = room_alias.ok();

		unread
	} else {
		unread.await
	};

	if let Some(unread) = unread {
		notify.counts = NotificationCounts::new(unread, ruma::uint!(0));
	}

	self.send_http_notice(
		user_id,
		pusher,
		http,
		notify,
		unread,
		workspace_id.as_deref(),
		is_direct_chat,
		sender_avatar_url.as_deref(),
		room_avatar_url.as_deref(),
		image_mxc_uri.as_deref(),
		image_url.as_deref(),
	)
	.await
}

#[implement(super::Service)]
fn prepare_http_pusher(&self, pusher: &Pusher, http: &HttpPusherData) -> Result<Device> {
	let address = &http.url;
	let url = Url::parse(address).map_err(|e| {
		err!(Request(InvalidParam(
			warn!(url = %address, error = %e, "HTTP pusher URL is not a valid URL")
		)))
	})?;

	self.check_http_pusher_url(&url)?;

	let mut device = Device::new(pusher.ids.app_id.clone(), pusher.ids.pushkey.clone());
	device.data.data.clone_from(&http.data);
	device.data.data.insert(
		"lang".to_owned(),
		Value::String(super::normalized_language(&pusher.lang).to_owned()),
	);
	device.data.format.clone_from(&http.format);

	Ok(device)
}

/// Deliver one notification to the pusher's gateway and honor its verdict.
///
/// A pushkey the gateway names in `rejected` has its pusher removed. `unread`
/// names the counts value on the wire; it is recorded as delivered only after
/// the gateway accepts, so a failed or rejected send leaves the next refresh
/// unconditional.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip_all)]
async fn send_http_notice(
	&self,
	user_id: &UserId,
	pusher: &Pusher,
	http: &HttpPusherData,
	notify: Notification,
	unread: Option<UInt>,
	workspace_id: Option<&str>,
	is_direct_chat: Option<bool>,
	sender_avatar_url: Option<&str>,
	room_avatar_url: Option<&str>,
	image_mxc_uri: Option<&str>,
	image_url: Option<&str>,
) -> Result {
	let response = self
		.send_request(
			&http.url,
			Request::new(notify),
			workspace_id,
			is_direct_chat,
			sender_avatar_url,
			room_avatar_url,
			image_mxc_uri,
			image_url,
		)
		.await?;

	let pushkey = &pusher.ids.pushkey;

	if response.rejected.contains(pushkey) {
		error!(
			url = %http.url,
			%pushkey,
			"Push gateway rejected the pushkey; removing the pusher. Push notifications \
			 for this device stop until the client registers a new pusher.",
		);

		self.delete_pusher(user_id, pushkey).await;

		return Ok(());
	}

	if let Some(unread) = unread {
		self.record_sent_badge(user_id, pushkey, unread);
	}

	Ok(())
}

fn badge_count_disabled(http: &HttpPusherData) -> bool {
	["org.matrix.msc4076.disable_badge_count", "disable_badge_count"]
		.iter()
		.any(|key| http.data.get(*key).and_then(Value::as_bool) == Some(true))
}

fn is_reminder_push_event(kind: &TimelineEventType) -> bool {
	matches!(
		kind.to_cow_str().as_ref(),
		REMINDER_CREATED_EVENT_TYPE | REMINDER_DUE_EVENT_TYPE
	)
}

fn parse_image_mxc(value: Option<&JsonValue>) -> Option<OwnedMxcUri> {
	let value = value.and_then(JsonValue::as_str)?;
	if !value.starts_with("mxc://") {
		return None;
	}

	let mxc = OwnedMxcUri::from(value.to_owned());
	mxc.is_valid().then_some(mxc)
}

fn image_push_uris(content: &serde_json::value::RawValue) -> Option<(OwnedMxcUri, OwnedMxcUri)> {
	let content = serde_json::from_str::<JsonValue>(content.get()).ok()?;
	if content.get("msgtype").and_then(JsonValue::as_str) != Some("m.image") {
		return None;
	}

	let original = parse_image_mxc(content.get("url"));
	let thumbnail = parse_image_mxc(
		content
			.get("info")
			.and_then(|info| info.get("thumbnail_url")),
	);
	let image = original.clone().or_else(|| thumbnail.clone())?;
	let preview = thumbnail.or_else(|| original.clone())?;

	Some((image, preview))
}

#[cfg(test)]
mod fork_tests {
	use super::image_push_uris;

	fn image_content(value: &str) -> Box<serde_json::value::RawValue> {
		serde_json::value::to_raw_value(&serde_json::from_str::<serde_json::Value>(value).unwrap())
			.unwrap()
	}

	#[test]
	fn image_preview_prefers_thumbnail_uri() {
		let content = image_content(
			r#"{
				"msgtype":"m.image",
				"url":"mxc://chat.example/original",
				"info":{"thumbnail_url":"mxc://chat.example/thumbnail"}
			}"#,
		);

		let (image, preview) = image_push_uris(&content).expect("image metadata");
		assert_eq!(image.as_str(), "mxc://chat.example/original");
		assert_eq!(preview.as_str(), "mxc://chat.example/thumbnail");
	}

	#[test]
	fn image_without_original_uses_thumbnail_as_fallback() {
		let content = image_content(
			r#"{
				"msgtype":"m.image",
				"info":{"thumbnail_url":"mxc://chat.example/thumbnail"}
			}"#,
		);

		let (image, preview) = image_push_uris(&content).expect("image metadata");
		assert_eq!(image.as_str(), "mxc://chat.example/thumbnail");
		assert_eq!(preview.as_str(), "mxc://chat.example/thumbnail");
	}

	#[test]
	fn encrypted_image_without_mxc_url_has_no_preview() {
		let content = image_content(
			r#"{
				"msgtype":"m.image",
				"file":{"v":"v2","key":{},"url":"mxc://chat.example/encrypted"}
			}"#,
		);

		assert!(image_push_uris(&content).is_none());
	}

	#[test]
	fn non_image_events_have_no_image_metadata() {
		let content = image_content(
			r#"{"msgtype":"m.text","body":"not an image","url":"mxc://chat.example/media"}"#,
		);

		assert!(image_push_uris(&content).is_none());
	}
}
