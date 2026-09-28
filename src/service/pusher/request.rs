use std::{fmt::Debug, mem};

use bytes::BytesMut;
use ipaddress::IPAddress;
use ruma::api::{
	IncomingResponse, MatrixVersion, OutgoingRequest, SendAccessToken, SupportedVersions,
};
use serde_json::Value as JsonValue;
use tuwunel_core::{Err, Result, debug, err, implement, trace, utils::string_from_bytes, warn};

const CALL_KIND_FIELD: &str = "call_kind";
const WORKSPACE_ID_FIELD: &str = "workspaceId";
const IS_DIRECT_CHAT_FIELD: &str = "is_direct_chat";
const SENDER_AVATAR_URL_FIELD: &str = "sender_avatar_url";
const ROOM_AVATAR_URL_FIELD: &str = "room_avatar_url";
const IMAGE_MXC_URI_FIELD: &str = "image_mxc_uri";
const IMAGE_URL_FIELD: &str = "image_url";

fn add_push_gateway_extensions(
	body: &mut BytesMut,
	workspace_id: Option<&str>,
	is_direct_chat: Option<bool>,
	sender_avatar_url: Option<&str>,
	room_avatar_url: Option<&str>,
	image_mxc_uri: Option<&str>,
	image_url: Option<&str>,
) {
	let Ok(mut payload) = serde_json::from_slice::<JsonValue>(body.as_ref()) else {
		return;
	};

	let Some(notification) = payload
		.get_mut("notification")
		.and_then(JsonValue::as_object_mut)
	else {
		return;
	};

	let event_type = notification
		.get("type")
		.and_then(JsonValue::as_str)
		.or_else(|| {
			notification
				.get("event_type")
				.and_then(JsonValue::as_str)
		});

	let mut changed = false;

	if event_type == Some("m.call.invite")
		&& let Some(call_kind) = notification
			.get("content")
			.and_then(|content| content.get(CALL_KIND_FIELD))
			.cloned()
	{
		notification.insert(CALL_KIND_FIELD.to_owned(), call_kind);
		changed = true;
	}

	if let Some(workspace_id) = workspace_id
		.map(str::trim)
		.filter(|workspace_id| !workspace_id.is_empty())
	{
		notification
			.insert(WORKSPACE_ID_FIELD.to_owned(), JsonValue::String(workspace_id.to_owned()));
		changed = true;
	}

	if let Some(is_direct_chat) = is_direct_chat {
		notification.insert(
			IS_DIRECT_CHAT_FIELD.to_owned(),
			JsonValue::Bool(is_direct_chat),
		);
		changed = true;
	}

	if let Some(sender_avatar_url) = sender_avatar_url
		.map(str::trim)
		.filter(|avatar_url| !avatar_url.is_empty())
	{
		notification.insert(
			SENDER_AVATAR_URL_FIELD.to_owned(),
			JsonValue::String(sender_avatar_url.to_owned()),
		);
		changed = true;
	}

	if let Some(room_avatar_url) = room_avatar_url
		.map(str::trim)
		.filter(|avatar_url| !avatar_url.is_empty())
	{
		notification.insert(
			ROOM_AVATAR_URL_FIELD.to_owned(),
			JsonValue::String(room_avatar_url.to_owned()),
		);
		changed = true;
	}

	if let Some(image_mxc_uri) = image_mxc_uri
		.map(str::trim)
		.filter(|uri| !uri.is_empty())
	{
		notification.insert(
			IMAGE_MXC_URI_FIELD.to_owned(),
			JsonValue::String(image_mxc_uri.to_owned()),
		);
		changed = true;
	}

	if let Some(image_url) = image_url
		.map(str::trim)
		.filter(|url| !url.is_empty())
	{
		notification.insert(
			IMAGE_URL_FIELD.to_owned(),
			JsonValue::String(image_url.to_owned()),
		);
		changed = true;
	}

	if changed && let Ok(serialized) = serde_json::to_vec(&payload) {
		*body = BytesMut::from(serialized.as_slice());
	}
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip_all)]
pub(super) async fn send_request<T>(
	&self,
	dest: &str,
	request: T,
	workspace_id: Option<&str>,
	is_direct_chat: Option<bool>,
	sender_avatar_url: Option<&str>,
	room_avatar_url: Option<&str>,
	image_mxc_uri: Option<&str>,
	image_url: Option<&str>,
) -> Result<T::IncomingResponse>
where
	T: OutgoingRequest + Debug + Send,
{
	const VERSIONS: [MatrixVersion; 1] = [MatrixVersion::V1_0];
	let supported = SupportedVersions {
		versions: VERSIONS.into(),
		features: Default::default(),
	};

	let dest = dest.replace(&self.services.config.notification_push_path, "");
	trace!("Push gateway destination: {dest}");

	let mut http_request = request
		.try_into_http_request::<BytesMut>(&dest, SendAccessToken::IfRequired(""), &supported)
		.map_err(|e| {
			err!(BadServerResponse(warn!(
				"Failed to find destination {dest} for push gateway: {e}"
			)))
		})?;

	add_push_gateway_extensions(
		http_request.body_mut(),
		workspace_id,
		is_direct_chat,
		sender_avatar_url,
		room_avatar_url,
		image_mxc_uri,
		image_url,
	);

	let http_request = http_request.map(BytesMut::freeze);

	let reqwest_request = reqwest::Request::try_from(http_request)?;

	// Capture request info for error logging before consuming the request
	let req_method = reqwest_request.method().clone();
	let req_url = reqwest_request.url().clone();
	let req_headers = reqwest_request.headers().clone();
	let req_body_str = reqwest_request
		.body()
		.and_then(|b| b.as_bytes())
		.map(|b| String::from_utf8_lossy(b).into_owned())
		.unwrap_or_default();

	debug!("Push gateway request URL: {} {}", req_method, req_url);
	debug!("Push gateway request headers: {:#?}", req_headers);
	debug!("Push gateway request body: {}", redact_push_gateway_request_body(&req_body_str));

	if let Some(url_host) = req_url.host_str() {
		trace!("Checking request URL for IP");
		if let Ok(ip) = IPAddress::parse(url_host)
			&& !self.services.client.valid_cidr_range(&ip)
		{
			return Err!(BadServerResponse("Not allowed to send requests to this IP"));
		}
	}

	let response = self
		.services
		.client
		.pusher
		.execute(reqwest_request)
		.await;

	match response {
		| Ok(mut response) => {
			// reqwest::Response -> http::Response conversion

			trace!("Checking response destination's IP");
			if let Some(remote_addr) = response.remote_addr()
				&& let Ok(ip) = IPAddress::parse(remote_addr.ip().to_string())
				&& !self.services.client.valid_cidr_range(&ip)
			{
				return Err!(BadServerResponse("Not allowed to send requests to this IP"));
			}

			let status = response.status();
			let mut http_response_builder = http::Response::builder()
				.status(status)
				.version(response.version());

			mem::swap(
				response.headers_mut(),
				http_response_builder
					.headers_mut()
					.expect("http::response::Builder is usable"),
			);

			let body = response.bytes().await?; // TODO: handle timeout

			if !status.is_success() {
				warn!(
					"Push gateway {dest} returned unsuccessful HTTP response: {status} | body: {}",
					string_from_bytes(&body).unwrap_or_else(|_| String::from("<non-utf8 body>"))
				);
				warn!(
					"Push gateway request details: method={req_method} url={req_url} | request body: {}",
					redact_push_gateway_request_body(&req_body_str)
				);
				return Err!(BadServerResponse(
					"Push gateway {dest} returned unsuccessful HTTP response: {status}"
				));
			}

			debug!(
				"Push gateway {dest} returned successful HTTP response: {status} | request body: {}",
				redact_push_gateway_request_body(&req_body_str)
			);

			let response = T::IncomingResponse::try_from_http_response(
				http_response_builder
					.body(body)
					.expect("reqwest body is valid http body"),
			);

			response.map_err(|e| {
				err!(BadServerResponse(warn!(
					"Push gateway {dest} returned invalid response: {e}"
				)))
			})
		},
		| Err(e) => {
			warn!("Could not send request to pusher {dest}: {e}");
			Err(e.into())
		},
	}
}

fn redact_push_gateway_request_body(body: &str) -> String {
	let Ok(mut payload) = serde_json::from_str::<JsonValue>(body) else {
		return String::from("<invalid JSON body>");
	};

	if let Some(notification) = payload
		.get_mut("notification")
		.and_then(JsonValue::as_object_mut)
	{
		if let Some(image_url) = notification
			.get(IMAGE_URL_FIELD)
			.and_then(JsonValue::as_str)
			.map(redact_signed_image_url)
		{
			notification.insert(IMAGE_URL_FIELD.to_owned(), JsonValue::String(image_url));
		}

		if let Some(devices) = notification
			.get_mut("devices")
			.and_then(JsonValue::as_array_mut)
		{
			for device in devices {
				if let Some(pushkey) = device
					.get("pushkey")
					.and_then(JsonValue::as_str)
					.map(str::to_owned)
				{
					if let Some(device_object) = device.as_object_mut() {
						device_object.insert(
							"pushkey".to_owned(),
							JsonValue::String(mask_secret(&pushkey)),
						);
					}
				}
			}
		}
	}

	serde_json::to_string(&payload).unwrap_or_else(|_| String::from("<unable to serialize body>"))
}

fn redact_signed_image_url(image_url: &str) -> String {
	let Ok(mut url) = url::Url::parse(image_url) else {
		return String::from("<redacted image URL>");
	};

	let query = url
		.query_pairs()
		.map(|(key, value)| {
			if key == "sig" {
				(key.into_owned(), "***".to_owned())
			} else {
				(key.into_owned(), value.into_owned())
			}
		})
		.collect::<Vec<_>>();
	url.query_pairs_mut().clear().extend_pairs(query);
	url.to_string()
}

fn mask_secret(secret: &str) -> String {
	let chars = secret.chars().collect::<Vec<_>>();
	if chars.len() <= 8 {
		return "***".to_owned();
	}

	let prefix = chars.iter().take(4).collect::<String>();
	let suffix = chars.iter().rev().take(4).rev().collect::<String>();
	format!("{prefix}...{suffix}")
}

#[cfg(test)]
mod tests {
	use super::{
		CALL_KIND_FIELD, IMAGE_MXC_URI_FIELD, IMAGE_URL_FIELD, IS_DIRECT_CHAT_FIELD,
		ROOM_AVATAR_URL_FIELD, SENDER_AVATAR_URL_FIELD, WORKSPACE_ID_FIELD,
		add_push_gateway_extensions,
	};
	use bytes::BytesMut;

	#[test]
	fn promotes_call_kind_for_call_invite() {
		let mut body = BytesMut::from(
			r#"{"notification":{"type":"m.call.invite","content":{"call_kind":"video"}}}"#,
		);

		add_push_gateway_extensions(&mut body, None, None, None, None, None, None);

		let value: serde_json::Value = serde_json::from_slice(body.as_ref()).expect("valid json");
		assert_eq!(
			value["notification"][CALL_KIND_FIELD],
			serde_json::Value::String("video".to_owned())
		);
	}

	#[test]
	fn ignores_non_call_invite_events() {
		let mut body = BytesMut::from(
			r#"{"notification":{"type":"m.room.message","content":{"call_kind":"voice"}}}"#,
		);

		add_push_gateway_extensions(&mut body, None, None, None, None, None, None);

		let value: serde_json::Value = serde_json::from_slice(body.as_ref()).expect("valid json");
		assert!(
			value["notification"]
				.get(CALL_KIND_FIELD)
				.is_none()
		);
	}

	#[test]
	fn adds_workspace_id_when_present() {
		let mut body = BytesMut::from(r#"{"notification":{"type":"m.room.message"}}"#);

		add_push_gateway_extensions(&mut body, Some(" workspace-1 "), None, None, None, None, None);

		let value: serde_json::Value = serde_json::from_slice(body.as_ref()).expect("valid json");
		assert_eq!(
			value["notification"][WORKSPACE_ID_FIELD],
			serde_json::Value::String("workspace-1".to_owned())
		);
	}

	#[test]
	fn ignores_empty_workspace_id() {
		let mut body = BytesMut::from(r#"{"notification":{"type":"m.room.message"}}"#);

		add_push_gateway_extensions(&mut body, Some("   "), None, None, None, None, None);

		let value: serde_json::Value = serde_json::from_slice(body.as_ref()).expect("valid json");
		assert!(
			value["notification"]
				.get(WORKSPACE_ID_FIELD)
				.is_none()
		);
	}

	#[test]
	fn adds_room_metadata_when_present() {
		let mut body = BytesMut::from(r#"{"notification":{"type":"m.room.message"}}"#);

		add_push_gateway_extensions(
			&mut body,
			None,
			Some(false),
			Some("mxc://server/sender"),
			Some("mxc://server/room"),
			None,
			None,
		);

		let value: serde_json::Value = serde_json::from_slice(body.as_ref()).expect("valid json");
		assert_eq!(
			value["notification"][IS_DIRECT_CHAT_FIELD],
			serde_json::Value::Bool(false)
		);
		assert_eq!(
			value["notification"][SENDER_AVATAR_URL_FIELD],
			serde_json::Value::String("mxc://server/sender".to_owned())
		);
		assert_eq!(
			value["notification"][ROOM_AVATAR_URL_FIELD],
			serde_json::Value::String("mxc://server/room".to_owned())
		);
	}

	#[test]
	fn adds_image_metadata_when_present() {
		let mut body = BytesMut::from(r#"{"notification":{"type":"m.room.message"}}"#);

		add_push_gateway_extensions(
			&mut body,
			None,
			None,
			None,
			None,
			Some("mxc://server/media"),
			Some("https://matrix.example/preview.png?expires=10&sig=secret"),
		);

		let value: serde_json::Value = serde_json::from_slice(body.as_ref()).expect("valid json");
		assert_eq!(
			value["notification"][IMAGE_MXC_URI_FIELD],
			serde_json::Value::String("mxc://server/media".to_owned())
		);
		assert_eq!(
			value["notification"][IMAGE_URL_FIELD],
			serde_json::Value::String("https://matrix.example/preview.png?expires=10&sig=secret".to_owned())
		);
	}

	#[test]
	fn redacts_pushkeys_in_request_logs() {
		let body = r#"{"notification":{"devices":[{"pushkey":"1234567890abcdef"}]}}"#;
		let redacted = super::redact_push_gateway_request_body(body);

		assert!(!redacted.contains("1234567890abcdef"));
		assert!(redacted.contains("1234...cdef"));
	}

	#[test]
	fn redacts_signed_image_url_in_request_logs() {
		let body = r#"{"notification":{"image_url":"https://matrix.example/preview.png?expires=10&sig=secret"}}"#;
		let redacted = super::redact_push_gateway_request_body(body);

		assert!(!redacted.contains("sig=secret"));
		assert!(redacted.contains("sig=***"));
	}
}
