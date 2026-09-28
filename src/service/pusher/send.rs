use ipaddress::IPAddress;
use ruma::{
	Mxc, OwnedMxcUri, UInt, UserId,
	api::{
		client::push::{Pusher, PusherKind},
		push_gateway::send_event_notification::{
			self,
			v1::{Device, Notification, NotificationCounts, NotificationPriority},
		},
	},
	events::TimelineEventType,
	push::{Action, PushFormat, Ruleset, Tweak},
	uint,
};
use serde_json::Value as JsonValue;
use tuwunel_core::{Err, Result, err, implement, matrix::Event};

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
	let mut notify = None;
	let mut tweaks = Vec::new();

	let power_levels = self
		.services
		.state_accessor
		.get_power_levels(event.room_id())
		.await
		.ok();

	let serialized = event.to_format();
	let actions = self
		.get_actions(user_id, ruleset, power_levels.as_ref(), &serialized, event.room_id())
		.await;

	for action in actions {
		let n = match action {
			| Action::Notify => true,
			| Action::SetTweak(tweak) => {
				tweaks.push(tweak.clone());
				continue;
			},
			| _ => false,
		};

		if notify.is_some() {
			return Err!(Request(BadJson(
				r#"Malformed pushrule contains more than one of these actions: ["dont_notify", "notify", "coalesce"]"#
			)));
		}

		notify = Some(n);
	}

	if is_reminder_push_event(event.kind()) {
		notify = Some(true);
	}

	let is_reaction = *event.kind() == TimelineEventType::Reaction;

	if notify == Some(true) || is_reaction || self.services.config.push_everything {
		let unread: UInt = self
			.services
			.pusher
			.notification_count(user_id, event.room_id())
			.await
			.try_into()
			.unwrap_or_else(|_| uint!(1));

		self.send_notice(user_id, unread, pusher, tweaks, event)
			.await?;
	}

	Ok(())
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

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip_all)]
async fn send_notice<Pdu: Event>(
	&self,
	user_id: &UserId,
	unread: UInt,
	pusher: &Pusher,
	tweaks: Vec<Tweak>,
	event: &Pdu,
) -> Result {
	// TODO: email
	match &pusher.kind {
		| PusherKind::Http(http) => {
			let url = &http.url;
			let url = url::Url::parse(&http.url).map_err(|e| {
				err!(Request(InvalidParam(
					warn!(%url, "HTTP pusher URL is not a valid URL: {e}")
				)))
			})?;

			if ["http", "https"]
				.iter()
				.all(|&scheme| scheme != url.scheme().to_lowercase())
			{
				return Err!(Request(InvalidParam(
					warn!(%url, "HTTP pusher URL is not a valid HTTP/HTTPS URL")
				)));
			}

			if let Ok(ip) = IPAddress::parse(url.host_str().expect("URL previously validated"))
				&& !self.services.client.valid_cidr_range(&ip)
			{
				return Err!(Request(InvalidParam(
					warn!(%url, "HTTP pusher URL is a forbidden remote address")
				)));
			}

			// TODO (timo): can pusher/devices have conflicting formats
			let event_id_only = http.format == Some(PushFormat::EventIdOnly);

			let mut device = Device::new(pusher.ids.app_id.clone(), pusher.ids.pushkey.clone());
			device.data.data.clone_from(&http.data);
			device.data.data.insert(
				"lang".to_owned(),
				JsonValue::String(super::normalized_language(&pusher.lang).to_owned()),
			);
			device.data.format.clone_from(&http.format);

			// Tweaks are only added if the format is NOT event_id_only
			if !event_id_only {
				device.tweaks.clone_from(&tweaks);
			}

			let d = vec![device];
			let mut notify = Notification::new(d);
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
						.await
						|| self
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
					.users
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
			let (image_mxc_uri, image_url) = if !event_id_only
				&& *event.kind() == TimelineEventType::RoomMessage
			{
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

			notify.event_id = Some(event.event_id().to_owned());
			notify.room_id = Some(event.room_id().to_owned());
			if http
				.data
				.get("org.matrix.msc4076.disable_badge_count")
				.is_none() && http.data.get("disable_badge_count").is_none()
			{
				notify.counts = NotificationCounts::new(unread, uint!(0));
			} else {
				// counts will not be serialised if it's the default (0, 0)
				// skip_serializing_if = "NotificationCounts::is_default"
				notify.counts = NotificationCounts::default();
			}

			if !event_id_only {
				if *event.kind() == TimelineEventType::RoomEncrypted
					|| tweaks
						.iter()
						.any(|t| matches!(t, Tweak::Highlight(true) | Tweak::Sound(_)))
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

				notify.sender_display_name = self
					.services
					.users
					.displayname(event.sender())
					.await
					.ok();

				notify.room_name = self
					.services
					.state_accessor
					.get_name(event.room_id())
					.await
					.ok();

				notify.room_alias = self
					.services
					.state_accessor
					.get_canonical_alias(event.room_id())
					.await
					.ok();
			}

			self
				.send_request(
					&http.url,
					send_event_notification::v1::Request::new(notify),
					workspace_id.as_deref(),
					is_direct_chat,
					sender_avatar_url.as_deref(),
					room_avatar_url.as_deref(),
					image_mxc_uri.as_deref(),
					image_url.as_deref(),
				)
				.await?;

			Ok(())
		},
		// TODO: Handle email
		//PusherKind::Email(_) => Ok(()),
		| _ => Ok(()),
	}
}

#[cfg(test)]
mod tests {
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
