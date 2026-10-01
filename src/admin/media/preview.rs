use tuwunel_core::Result;
use url::Url;

use crate::admin_command;

#[admin_command]
pub(super) async fn preview(&self, url: Url, no_cache: bool) -> Result {
	let url_preview = if no_cache {
		self.services
			.media
			.request_url_preview(&url)
			.await?
	} else {
		self.services.media.get_url_preview(&url).await?
	};

	let preview_str = serde_json::to_string_pretty(&url_preview)?;

	self.write_str(&format!("Result:\n```\n{preview_str}\n```"))
		.await
}
