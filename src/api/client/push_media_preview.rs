use axum::{
	body::Body,
	extract::{Path, State},
	response::Response,
};
use http::Uri;
use ruma::{Mxc, ServerName, media::Method};
use serde::Deserialize;
use tuwunel_core::{Err, Result, err};
use tuwunel_service::media::Dim;

const MAX_DIMENSION: u32 = 512;

#[derive(Debug, Deserialize)]
pub(crate) struct PushMediaPreviewQuery {
	pub width: u32,
	pub height: u32,
	pub method: String,
	pub expires: u64,
	pub sig: String,
}

/// # `GET /_matrix/client/unstable/org.tuwunel/media/thumbnail/{server_name}/{media_id}/preview.png`
///
/// Serves a short-lived, signed thumbnail without requiring a Matrix access
/// token. The URL is intended for push providers and notification extensions.
pub(crate) async fn get_push_media_thumbnail_route(
	State(services): State<crate::State>,
	Path((server_name, media_id)): Path<(String, String)>,
	uri: Uri,
) -> Result<Response> {
	let query = serde_html_form::from_str::<PushMediaPreviewQuery>(uri.query().unwrap_or_default())
		.map_err(|error| {
			err!(Request(InvalidParam(
				"Invalid signed media preview query: {error}"
			)))
		})?;

	if query.width == 0
		|| query.width > MAX_DIMENSION
		|| query.height == 0
		|| query.height > MAX_DIMENSION
	{
		return Err!(Request(InvalidParam(
			"Thumbnail width and height must be between 1 and 512 pixels."
		)));
	}

	let method = match query.method.as_str() {
		"scale" => Method::Scale,
		"crop" => Method::Crop,
		_ => return Err!(Request(InvalidParam("Unsupported thumbnail method."))),
	};

	let server_name: &ServerName = server_name
		.as_str()
		.try_into()
		.map_err(|_| err!(Request(InvalidParam("Invalid media server name."))))?;

	if !services
		.media
		.verify_push_preview_signature(
			server_name,
			&media_id,
			query.width,
			query.height,
			&query.method,
			query.expires,
			&query.sig,
		)
		.await
	{
		return Err!(Request(Forbidden(
			"Invalid or expired media preview signature."
		)));
	}

	let mxc = Mxc {
		server_name,
		media_id: &media_id,
	};
	let dim = Dim::new(query.width, query.height, Some(method));
	let Some(file) = services.media.get_push_thumbnail(&mxc, &dim).await? else {
		return Err!(Request(NotFound("Media thumbnail not found.")));
	};
	let content = file.content;

	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map(|duration| duration.as_secs())
		.unwrap_or_default();
	let max_age = query.expires.saturating_sub(now);

	Ok(Response::builder()
		.header("Content-Type", "image/png")
		.header("Cache-Control", format!("private, max-age={max_age}"))
		.header("Cross-Origin-Resource-Policy", "cross-origin")
		.body(Body::from(content))
		.expect("push media preview response headers are valid"))
}
