use std::path::PathBuf;

use serde::Deserialize;
use tuwunel_macros::config_example_generator;
use url::Url;

/// Configuration for short-lived, signed media preview URLs used by push notifications.
#[derive(Clone, Debug, Deserialize, Default)]
#[config_example_generator(
	filename = "tuwunel-example.toml",
	section = "global.push_media_preview"
)]
pub struct PushMediaPreviewConfig {
	/// Enable signed, unauthenticated thumbnail URLs for push notifications.
	///
	/// default: false
	#[serde(default)]
	pub enabled: bool,

	/// HTTPS origin that is reachable by the push provider and notification clients.
	///
	/// example: "https://matrix.example.com"
	pub public_base_url: Option<Url>,

	/// File containing a dedicated HMAC signing secret.
	///
	/// example: "/etc/tuwunel/push_media_preview_secret"
	pub secret_file: Option<PathBuf>,

	/// Lifetime of generated preview URLs in seconds.
	///
	/// default: 86400
	#[serde(default = "default_ttl_seconds")]
	pub ttl_seconds: u64,

	/// Default preview width in pixels.
	///
	/// default: 320
	#[serde(default = "default_dimension")]
	pub width: u32,

	/// Default preview height in pixels.
	///
	/// default: 320
	#[serde(default = "default_dimension")]
	pub height: u32,
}

fn default_ttl_seconds() -> u64 {
	24 * 60 * 60
}

fn default_dimension() -> u32 {
	320
}
