//! HTTP content-disposition selection and filename sanitization.
//!
//! Media types are checked against the safe-inline list before selecting a
//! disposition. Optional filenames are sanitized before header construction.

use ruma::http_headers::{ContentDisposition, ContentDispositionType};

use crate::debug_info;

/// as defined by MSC2702
const ALLOWED_INLINE_CONTENT_TYPES: [&str; 26] = [
	// keep sorted
	"application/json",
	"application/ld+json",
	"audio/aac",
	"audio/flac",
	"audio/mp4",
	"audio/mpeg",
	"audio/ogg",
	"audio/wav",
	"audio/wave",
	"audio/webm",
	"audio/x-flac",
	"audio/x-pn-wav",
	"audio/x-wav",
	"image/apng",
	"image/avif",
	"image/gif",
	"image/jpeg",
	"image/png",
	"image/webp",
	"text/css",
	"text/csv",
	"text/plain",
	"video/mp4",
	"video/ogg",
	"video/quicktime",
	"video/webm",
];

/// Returns a Content-Disposition of `attachment` or `inline`, depending on the
/// Content-Type against MSC2702 list of safe inline Content-Types
/// (`ALLOWED_INLINE_CONTENT_TYPES`)
#[must_use]
pub fn content_disposition_type(content_type: Option<&str>) -> ContentDispositionType {
	let Some(content_type) = content_type else {
		debug_info!("No Content-Type was given, assuming attachment for Content-Disposition");
		return ContentDispositionType::Attachment;
	};

	debug_assert!(
		ALLOWED_INLINE_CONTENT_TYPES.is_sorted(),
		"ALLOWED_INLINE_CONTENT_TYPES is not sorted"
	);

	let essence = content_type_essence(content_type);

	// The list is lowercase and ordered bytewise, so folding the needle's case as
	// it is compared searches the same order without copying it.
	let allowed = ALLOWED_INLINE_CONTENT_TYPES
		.binary_search_by(|allowed| {
			allowed.bytes().cmp(
				essence
					.bytes()
					.map(|byte| byte.to_ascii_lowercase()),
			)
		})
		.is_ok();

	if allowed {
		ContentDispositionType::Inline
	} else {
		ContentDispositionType::Attachment
	}
}

/// Whether a Content-Type names the given media type.
///
/// Media types are case-insensitive per RFC 9110 section 8.3.1, so the
/// comparison folds case rather than requiring an exact spelling.
#[inline]
#[must_use]
pub fn content_type_is(content_type: Option<&str>, essence: &str) -> bool {
	content_type.is_some_and(|content_type| {
		content_type_essence(content_type).eq_ignore_ascii_case(essence)
	})
}

/// The media type of a Content-Type, without its parameters.
///
/// A header value is `type/subtype` followed by optional `;` parameters, and
/// callers deciding what a body is must weigh only the former: a parameter that
/// merely contains a media type does not make the body that type.
#[inline]
#[must_use]
pub fn content_type_essence(content_type: &str) -> &str {
	content_type
		.split(';')
		.next()
		.unwrap_or(content_type)
		.trim()
}

/// sanitises the file name for the Content-Disposition using
/// `sanitize_filename` crate
#[tracing::instrument(level = "debug")]
pub fn sanitise_filename(filename: &str) -> String {
	sanitize_filename::sanitize_with_options(filename, sanitize_filename::Options {
		truncate: false,
		..Default::default()
	})
}

/// creates the final Content-Disposition based on whether the filename exists
/// or not, or if a requested filename was specified (media download with
/// filename)
///
/// if filename exists:
/// `Content-Disposition: attachment/inline; filename=filename.ext`
///
/// else: `Content-Disposition: attachment/inline`
pub fn make_content_disposition(
	content_disposition: Option<&ContentDisposition>,
	content_type: Option<&str>,
	filename: Option<&str>,
) -> ContentDisposition {
	ContentDisposition::new(content_disposition_type(content_type)).with_filename(
		filename
			.or_else(|| {
				content_disposition
					.and_then(|content_disposition| content_disposition.filename.as_deref())
			})
			.map(sanitise_filename),
	)
}

#[cfg(test)]
mod tests {
	#[test]
	fn string_sanitisation() {
		const SAMPLE: &str = "🏳️‍⚧️this\\r\\n įs \r\\n ä \\r\nstrïng 🥴that\n\r \
		                      ../../../../../../../may be\r\n malicious🏳️‍⚧️";
		const SANITISED: &str = "🏳️‍⚧️thisrn įs n ä rstrïng 🥴that ..............may be malicious🏳️‍⚧️";

		let options = sanitize_filename::Options {
			windows: true,
			truncate: true,
			replacement: "",
		};

		// cargo test -- --nocapture
		println!("{SAMPLE}");
		println!("{}", sanitize_filename::sanitize_with_options(SAMPLE, options.clone()));
		println!("{SAMPLE:?}");
		println!("{:?}", sanitize_filename::sanitize_with_options(SAMPLE, options.clone()));

		assert_eq!(SANITISED, sanitize_filename::sanitize_with_options(SAMPLE, options.clone()));
	}

	#[test]
	fn empty_sanitisation() {
		use crate::utils::string::EMPTY;

		let result =
			sanitize_filename::sanitize_with_options(EMPTY, sanitize_filename::Options {
				windows: true,
				truncate: true,
				replacement: "",
			});

		assert_eq!(EMPTY, result);
	}

	#[test]
	fn content_type_essence_drops_parameters() {
		use super::content_type_essence;

		assert_eq!(content_type_essence("text/html; charset=utf-8"), "text/html");
		assert_eq!(content_type_essence(" text/html "), "text/html");
		assert_eq!(content_type_essence("text/html"), "text/html");
	}

	#[test]
	fn content_type_is_matches_any_case() {
		use super::content_type_is;

		for content_type in ["text/html", "Text/HTML", "TEXT/HTML", "text/HTML; charset=utf-8"] {
			assert!(content_type_is(Some(content_type), "text/html"), "{content_type} is html");
		}
	}

	#[test]
	fn content_type_is_rejects_other_types() {
		use super::content_type_is;

		for content_type in
			["application/json; x=text/html", "text/plain", "application/xhtml+xml"]
		{
			assert!(
				!content_type_is(Some(content_type), "text/html"),
				"{content_type} is not html"
			);
		}

		assert!(!content_type_is(None, "text/html"), "an absent content type is not html");
	}

	#[test]
	fn inline_disposition_ignores_case_and_parameters() {
		use ruma::http_headers::ContentDispositionType;

		use super::content_disposition_type;

		for content_type in
			["image/png", "IMAGE/PNG", "Image/Png; charset=binary", " text/plain "]
		{
			assert!(
				matches!(
					content_disposition_type(Some(content_type)),
					ContentDispositionType::Inline
				),
				"{content_type} is safe to inline"
			);
		}
	}

	#[test]
	fn everything_else_is_an_attachment() {
		use ruma::http_headers::ContentDispositionType;

		use super::content_disposition_type;

		for content_type in [Some("text/html"), Some("application/octet-stream"), Some(""), None]
		{
			assert!(
				matches!(
					content_disposition_type(content_type),
					ContentDispositionType::Attachment
				),
				"{content_type:?} is not safe to inline"
			);
		}
	}
}
