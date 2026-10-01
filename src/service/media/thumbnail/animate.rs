//! Defines animated thumbnail admission and selection policy.

use tuwunel_core::implement;

use super::{
	super::Fetched,
	sniff::{animated_type, animates},
};

/// Content types naming a container that can carry a frame sequence.
///
/// A picture read as animating is stored under the one its header names, so a
/// later lookup holding the key rather than the picture still knows what the
/// row carries.
pub(super) const APNG: &str = "image/apng";
pub(super) const GIF: &str = "image/gif";
pub(super) const WEBP: &str = "image/webp";

/// Content types withheld from a request that asked for a still picture.
///
/// A still `image/webp` cannot be told from an animated one without decoding
/// it, so the family is withheld whole. MSC2705 also names `image/png` for
/// APNG, which cannot join the list because every generated thumbnail is one.
pub(in super::super) const ANIMATED_TYPES: [&str; 3] = [APNG, GIF, WEBP];

/// Whether a thumbnail request will accept an animated result.
///
/// MSC2705 gives the `animated` parameter three states and two behaviors: only
/// `animated=true` may be answered with animation, while `animated=false` and
/// an absent parameter alike forbid it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Animate {
	/// The response must be a still picture.
	#[default]
	Never,

	/// The response may animate.
	Allowed,
}
/// Returns true when the request will accept animation.
///
/// Only `animated=true` reaches this state; `animated=false` and an absent
/// parameter alike forbid animation.
#[implement(Animate)]
#[inline]
#[must_use]
pub fn allowed(self) -> bool { matches!(self, Self::Allowed) }

/// Returns true when content of this type may answer the request at all.
///
/// This reads the declared type, which whoever uploaded the picture chose,
/// so it is only for deciding between stored rows, where the pictures
/// themselves are not in hand. Prefer [`Self::accepts_picture`] anywhere
/// the bytes are.
#[implement(Animate)]
#[inline]
#[must_use]
pub fn accepts_type(self, content_type: Option<&str>) -> bool {
	self.allowed() || !content_type.is_some_and(declares_animation)
}

/// Returns true when this type is the variant the request asked for.
///
/// A source that animates leaves both variants stored at a size, so which
/// one a lookup answers with must be stated rather than left to the order
/// their keys fall in, which a remote row's own disposition decides. The
/// other variant still answers when it is the only one there, so this
/// orders the rows rather than refusing any of them.
#[implement(Animate)]
#[inline]
#[must_use]
pub fn prefers_type(self, content_type: Option<&str>) -> bool {
	self.allowed() == content_type.is_some_and(declares_animation)
}

/// Returns true when this picture may answer the request.
///
/// The bytes decide, so a file cannot pass a request that forbade
/// animation by declaring a content type that does not animate, and an
/// APNG is caught despite being an `image/png` like every thumbnail.
#[implement(Animate)]
#[inline]
#[must_use]
pub fn accepts_picture(self, bytes: &[u8]) -> bool { self.allowed() || !animates(bytes) }

/// Returns true when a fetched picture may answer, walking it if nobody
/// has.
///
/// Filing a row settles this on the way, and a redirect files none, so the
/// walk that was skipped there happens here for the one caller that asks.
#[implement(Animate)]
#[must_use]
pub fn accepts_fetched(self, fetched: &Fetched) -> bool {
	self.accepts_walk(fetched.animates, &fetched.media.content)
}

/// Returns true when this picture may answer in a thumbnail's place.
///
/// Nothing can be derived from a picture that will not decode, so the
/// choice is between the original and refusing media the server holds. Only
/// a walk that settled on animation withholds it here, where
/// [`Self::accepts_picture`] withholds anything it could not settle, since
/// refusing every unreadable still would cost more than it buys.
#[implement(Animate)]
#[inline]
#[must_use]
pub fn accepts_fallback(self, bytes: &[u8]) -> bool {
	self.allowed() || animated_type(bytes).is_none()
}

/// Returns true when this picture may answer, walking it if nobody has.
///
/// A caller already holding a walk of these bytes states what it settled
/// rather than paying for a second one over them, and where none was taken
/// this is [`Self::accepts_picture`] exactly.
#[implement(Animate)]
#[inline]
#[must_use]
pub(in super::super) fn accepts_walk(self, animates: Option<bool>, bytes: &[u8]) -> bool {
	animates
		.map_or_else(|| self.accepts_picture(bytes), |animates| self.accepts_animation(animates))
}

/// Returns true when this picture may answer in a thumbnail's place,
/// walking it if nobody has.
///
/// The settled-only rule of [`Self::accepts_fallback`] holds here too, so
/// what the caller states is whether its walk *named* an animation rather
/// than whether it withheld one.
#[implement(Animate)]
#[cfg(feature = "media_thumbnail")]
#[inline]
#[must_use]
pub(in super::super) fn accepts_fallback_walk(self, names: Option<bool>, bytes: &[u8]) -> bool {
	names.map_or_else(|| self.accepts_fallback(bytes), |names| self.allowed() || !names)
}

/// Returns true when a picture may answer, given what a walk settled about
/// it.
///
/// The walk that settles this is the same one picking the type a fetched
/// row is filed under, so a caller already holding its answer states it
/// here rather than reading the same bytes again through
/// [`Self::accepts_picture`].
#[implement(Animate)]
#[inline]
#[must_use]
fn accepts_animation(self, animates: bool) -> bool { self.allowed() || !animates }

impl From<Option<bool>> for Animate {
	fn from(animated: Option<bool>) -> Self {
		match animated {
			| Some(true) => Self::Allowed,
			| Some(false) | None => Self::Never,
		}
	}
}

impl From<Animate> for Option<bool> {
	fn from(animate: Animate) -> Self { Some(animate.allowed()) }
}

/// Whether a declared content type names an animating container.
///
/// This reads what an upload claimed rather than the picture itself, so it
/// decides only between stored rows, where no picture is in hand. The reader
/// that decides from the bytes is `sniff::animates`.
fn declares_animation(content_type: &str) -> bool {
	let essence = content_type
		.split_once(';')
		.map_or(content_type, |(essence, _)| essence)
		.trim();

	ANIMATED_TYPES
		.iter()
		.any(|kind| essence.eq_ignore_ascii_case(kind))
}
