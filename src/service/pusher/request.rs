use std::{fmt::Debug, mem};

use bytes::BytesMut;
use ipaddress::IPAddress;
use ruma::api::{
	IncomingResponse, MatrixVersion, OutgoingRequest, SendAccessToken, SupportedVersions,
};
use serde_json::Value as JsonValue;
use tuwunel_core::{
	Err, Result, debug, err, implement, trace, utils::string_from_bytes, warn,
};

const CALL_KIND_FIELD: &str = "org.tuwunel.call.kind";

fn add_top_level_call_kind(body: &mut BytesMut) {
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
		.or_else(|| notification.get("event_type").and_then(JsonValue::as_str));

	if event_type != Some("m.call.invite") {
		return;
	}

	let call_kind = notification
		.get("content")
		.and_then(|content| content.get(CALL_KIND_FIELD))
		.cloned();

	if let Some(call_kind) = call_kind {
		notification.insert(CALL_KIND_FIELD.to_owned(), call_kind);

		if let Ok(serialized) = serde_json::to_vec(&payload) {
			*body = BytesMut::from(serialized.as_slice());
		}
	}
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip_all)]
pub(super) async fn send_request<T>(&self, dest: &str, request: T) -> Result<T::IncomingResponse>
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
		})?
	;

	add_top_level_call_kind(http_request.body_mut());

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
	debug!("Push gateway request body: {}", req_body_str);

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
					"Push gateway request details: method={req_method} url={req_url} | request body: {req_body_str}"
				);
				return Err!(BadServerResponse(
					"Push gateway {dest} returned unsuccessful HTTP response: {status}"
				));
			}

				warn!(
				"Push gateway {dest} returned successful HTTP response: {status} | request body: {req_body_str}"
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

#[cfg(test)]
mod tests {
	use super::{CALL_KIND_FIELD, add_top_level_call_kind};
	use bytes::BytesMut;

	#[test]
	fn promotes_call_kind_for_call_invite() {
		let mut body = BytesMut::from(
			r#"{"notification":{"type":"m.call.invite","content":{"org.tuwunel.call.kind":"video"}}}"#,
		);

		add_top_level_call_kind(&mut body);

		let value: serde_json::Value = serde_json::from_slice(body.as_ref()).expect("valid json");
		assert_eq!(
			value["notification"][CALL_KIND_FIELD],
			serde_json::Value::String("video".to_owned())
		);
	}

	#[test]
	fn ignores_non_call_invite_events() {
		let mut body = BytesMut::from(
			r#"{"notification":{"type":"m.room.message","content":{"org.tuwunel.call.kind":"voice"}}}"#,
		);

		add_top_level_call_kind(&mut body);

		let value: serde_json::Value = serde_json::from_slice(body.as_ref()).expect("valid json");
		assert!(value["notification"].get(CALL_KIND_FIELD).is_none());
	}
}
