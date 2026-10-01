//! Handles media thumbnail requests.
//!
//! Request routing, source admission, dimensions, still generation, animation
//! policy, and animated encoding live in focused units below.

mod animate;
mod dimension;
#[cfg(feature = "media_thumbnail")]
mod encode;
#[cfg(feature = "media_thumbnail")]
mod generate;
mod request;
mod sniff;
#[cfg(all(test, feature = "media_thumbnail"))]
mod tests;

#[cfg(test)]
pub(super) use animate::ANIMATED_TYPES;
pub use animate::Animate;
pub use dimension::Dim;
#[cfg(all(test, feature = "media_thumbnail"))]
pub(super) use encode::encode_frames;
#[cfg(all(test, feature = "media_thumbnail"))]
pub(super) use generate::thumbnail_generate;
pub(super) use sniff::sequence;
#[cfg(test)]
pub(super) use sniff::{animated_type, animates};

use ruma::Mxc;
use tuwunel_core::{Result, err};

impl super::Service {
	pub async fn get_push_thumbnail(
		&self,
		mxc: &Mxc<'_>,
		dim: &Dim,
	) -> Result<Option<super::Media>> {
		let Some(mut file) = self
			.get_stored_thumbnail(mxc, dim, Animate::Allowed)
			.await
			.ok()
		else {
			return Ok(None);
		};
		let content = std::mem::take(&mut file.content);

		if content.starts_with(b"\x89PNG\r\n\x1a\n") {
			file.content = content;
			file.content_type = Some("image/png".to_owned());
			return Ok(Some(file));
		}

		#[cfg(feature = "media_thumbnail")]
		{
			let Ok(image) = image::load_from_memory(&content) else {
				return Ok(None);
			};

			let mut png = Vec::new();
			let mut cursor = std::io::Cursor::new(&mut png);
			image
				.write_to(&mut cursor, image::ImageFormat::Png)
				.map_err(|error| err!(error!(?error, "Error writing push preview PNG.")))?;

			file.content = png;
			file.content_type = Some("image/png".to_owned());
			return Ok(Some(file));
		}

		#[cfg(not(feature = "media_thumbnail"))]
		{
			Ok(None)
		}
	}
}
