//! Sniffs containers for animation.
//!
//! A picture names its own format in its first bytes and carries its frame
//! sequence, if it has one, at a place each format fixes. Reading those is what
//! lets a request for a still be honored against a file whose declared content
//! type says otherwise, or says nothing useful, as `image/png` does for an
//! APNG.
//!
//! A walk answers one of three states, because the two questions asked of it
//! take opposite defaults. Whether a picture may be served fails closed, so an
//! unreadable one is withheld and a wrong answer costs a needless re-encode
//! rather than a violation; what a picture is names nothing the walk did not
//! settle, since a guess there would be written down as fact.

use tuwunel_core::{implement, utils::math::checked_ops};

use super::animate::{APNG, GIF, WEBP};

// Leading signatures of the three animating containers.
const PNG_MAGIC: &[u8] = b"\x89PNG\r\n\x1a\n";
const RIFF_MAGIC: &[u8] = b"RIFF";
const WEBP_MAGIC: &[u8] = b"WEBP";
const GIF_MAGIC: [&[u8]; 2] = [b"GIF87a", b"GIF89a"];

// Chunk names that settle a PNG.
//
// The animation control is required to precede the pixel data, so meeting
// the data first settles the question without reading any of it.
const PNG_ANIMATION: &[u8] = b"acTL";
const PNG_DATA: &[u8] = b"IDAT";

// Chunk names a WebP can open with, and the animation flag's own bit.
//
// Only the extended form can hold a frame sequence, so a file opening with a
// plain lossy or lossless chunk is a still without reading further.
const WEBP_EXTENDED: &[u8] = b"VP8X";
const WEBP_LOSSY: &[u8] = b"VP8 ";
const WEBP_LOSSLESS: &[u8] = b"VP8L";
const WEBP_ANIMATION: u8 = 0x02;

// Leading byte of each block in a GIF body.
//
// A body is a flat sequence of these, ending at the trailer, and animation
// is the presence of a second image rather than anything the format states.
const GIF_EXTENSION: u8 = 0x21;
const GIF_IMAGE: u8 = 0x2C;
const GIF_TRAILER: u8 = 0x3B;

// GIF colour-table flag and size bits.
//
// The flag says whether one follows and the size field holds the exponent of
// its entry count, each entry being a three byte colour.
const GIF_COLOR_TABLE: u8 = 0x80;
const GIF_TABLE_SIZE: u8 = 0x07;

/// Bytes the signature and the logical screen descriptor occupy together.
///
/// A global colour table, when one is announced, follows them, and the block
/// sequence follows that.
const GIF_HEADER_LEN: usize = 13;

/// What a container's header settles about a frame sequence.
///
/// The two questions a caller asks of it want opposite defaults, so an
/// unsettled walk is its own answer rather than being folded into either.
#[derive(Clone, Copy)]
pub(in super::super) enum Sequence {
	/// This picture holds no sequence, either because its container cannot or
	/// because the walk reached the end of one.
	Absent,

	/// The walk found a sequence, in a container of this type.
	Present(&'static str),

	/// The walk ran out of picture, or met a structure it does not know.
	Unsettled,
}

/// Whether these bytes may carry more than one frame.
///
/// Walks the container first, for a caller holding no walk of its own.
#[inline]
pub(in super::super) fn animates(bytes: &[u8]) -> bool { sequence(bytes).animates() }

/// Whether the walked picture may carry more than one frame.
///
/// Only a settled walk answers false, so a truncated or unrecognized picture is
/// withheld from a request that forbade animation rather than served to it.
#[implement(Sequence)]
#[inline]
pub(in super::super) fn animates(self) -> bool { !matches!(self, Self::Absent) }

/// Whether the walk settled on an animation rather than merely allowing one.
///
/// This is the narrower half of [`Self::animates`], which withholds an
/// unsettled walk too: naming a picture takes proof where refusing to serve one
/// does not.
#[cfg(feature = "media_thumbnail")]
#[implement(Sequence)]
#[inline]
pub(in super::super) fn names_animation(self) -> bool { self.animated_type().is_some() }

/// The container type a picture's own bytes name.
///
/// Walks the container first, for a caller holding no walk of its own.
#[inline]
pub(in super::super) fn animated_type(bytes: &[u8]) -> Option<&'static str> {
	sequence(bytes).animated_type()
}

/// The content type the walked picture ought to be stored under.
///
/// A settled walk names the container itself, and the declared type stands
/// where the walk settled nothing. The label is what a lookup goes on wherever
/// the picture is not in hand: choosing between the rows at a size walks keys
/// alone, and a redirect hands the object over without reading it.
#[implement(Sequence)]
#[inline]
pub(in super::super) fn stored_type(self, declared: Option<&str>) -> Option<&str> {
	self.animated_type().or(declared)
}

/// The container type the walked picture's own bytes name.
///
/// Only a settled walk answers `Some`, since this names what a picture is
/// rather than deciding what may be served, and a guess would be recorded as
/// fact.
#[implement(Sequence)]
#[inline]
fn animated_type(self) -> Option<&'static str> {
	match self {
		| Self::Present(content_type) => Some(content_type),
		| Self::Absent | Self::Unsettled => None,
	}
}

/// Walks the container its first bytes name.
///
/// A container that cannot hold a sequence answers `Absent`, and so does one
/// whose walk reaches the end of its blocks without finding a second frame. A
/// walk that runs out of picture, or meets a structure it does not know,
/// answers `Unsettled` instead of guessing either way.
pub(in super::super) fn sequence(bytes: &[u8]) -> Sequence {
	let is_gif = GIF_MAGIC
		.iter()
		.any(|magic| bytes.starts_with(magic));

	let (content_type, settled) = match bytes {
		| _ if bytes.starts_with(PNG_MAGIC) => (APNG, png_sequence(bytes)),
		| _ if bytes.starts_with(RIFF_MAGIC) && bytes.get(8..12) == Some(WEBP_MAGIC) =>
			(WEBP, webp_sequence(bytes)),
		| _ if is_gif => (GIF, gif_sequence(bytes)),
		| _ => return Sequence::Absent,
	};

	match settled {
		| Some(true) => Sequence::Present(content_type),
		| Some(false) => Sequence::Absent,
		| None => Sequence::Unsettled,
	}
}

/// Whether a PNG holds the control chunk that makes it an APNG.
///
/// Each hop clears a whole chunk, so the walk advances by at least its twelve
/// byte frame every time and ends when a hop lands past the picture.
fn png_sequence(bytes: &[u8]) -> Option<bool> {
	let mut rest = bytes.get(PNG_MAGIC.len()..)?;

	loop {
		let kind = rest.get(4..8)?;

		if kind == PNG_ANIMATION {
			return Some(true);
		}

		if kind == PNG_DATA {
			return Some(false);
		}

		// the length counts the data alone, which follows a four byte length
		// and a four byte type and precedes a four byte checksum
		rest = rest
			.get(..4)
			.and_then(|field| field.try_into().ok())
			.map(u32::from_be_bytes)
			.and_then(|length| usize::try_from(length).ok())
			.and_then(|length| length.checked_add(12))
			.and_then(|skip| rest.get(skip..))?;
	}
}

/// Whether a WebP announces animation in its opening chunk.
///
/// The chunk name and the flags sit at fixed offsets, so this reads two fields
/// and never walks. Only the two plain still forms answer as a still, since a
/// chunk name neither they nor the extended header claim is a structure this
/// does not recognize.
fn webp_sequence(bytes: &[u8]) -> Option<bool> {
	let chunk = bytes.get(12..16)?;

	match chunk {
		| _ if chunk == WEBP_LOSSY || chunk == WEBP_LOSSLESS => Some(false),
		| _ if chunk == WEBP_EXTENDED => bytes
			.get(20)
			.map(|flags| flags & WEBP_ANIMATION != 0),
		| _ => None,
	}
}

/// Whether a GIF holds more than one image descriptor.
///
/// Frame count is written nowhere in the format, so the blocks are walked
/// until a second image is found or the trailer ends them. Every block clears
/// at least its own introducer and a terminating sub-block, so the offset
/// rises on every pass and a walk that runs off the picture ends.
fn gif_sequence(bytes: &[u8]) -> Option<bool> {
	let &screen = bytes.get(10)?;
	let global_table = (screen & GIF_COLOR_TABLE != 0)
		.then(|| color_table_len(screen))
		.unwrap_or_default();

	let mut at = global_table.checked_add(GIF_HEADER_LEN)?;
	let mut seen = false;

	loop {
		let &block = bytes.get(at)?;
		let image = block == GIF_IMAGE;

		if image && seen {
			return Some(true);
		}

		seen |= image;

		let next = match block {
			| GIF_TRAILER => return Some(false),
			| GIF_EXTENSION => at.checked_add(2),
			| GIF_IMAGE => image_data_start(bytes, at),
			| _ => return None,
		};

		at = next.and_then(|next| skip_sub_blocks(bytes, next))?;
	}
}

/// Size in bytes of the colour table a descriptor announces.
///
/// The size field is an exponent rather than a count, and each entry is a
/// three byte colour.
fn color_table_len(packed: u8) -> usize {
	let exponent = u32::from(packed & GIF_TABLE_SIZE).saturating_add(1);

	2_usize.saturating_pow(exponent).saturating_mul(3)
}

/// Offset of an image descriptor's sub-block chain.
///
/// The descriptor is a fixed nine bytes carrying the flag for a colour table
/// of its own, and one further byte holds the code size the data opens with.
fn image_data_start(bytes: &[u8], at: usize) -> Option<usize> {
	let descriptor = at.checked_add(1)?;
	let packed = *bytes.get(descriptor.checked_add(8)?)?;
	let local_table = (packed & GIF_COLOR_TABLE != 0)
		.then(|| color_table_len(packed))
		.unwrap_or_default();

	checked_ops!(descriptor + 9 + local_table + 1)
}

/// Offset past the sub-block chain starting here, when it ends.
///
/// Each block announces its own length and a zero length closes the chain, so
/// every pass clears at least the length byte and a chain that neither closes
/// nor runs out of picture cannot occur.
fn skip_sub_blocks(bytes: &[u8], mut at: usize) -> Option<usize> {
	loop {
		let size = usize::from(*bytes.get(at)?);

		at = checked_ops!(at + 1 + size)?;

		if size == 0 {
			return Some(at);
		}
	}
}
