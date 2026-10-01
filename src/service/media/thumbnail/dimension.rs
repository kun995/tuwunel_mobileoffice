//! Defines thumbnail dimensions and scaling rules.
//!
//! A dimension carries the requested extent and resize method, then normalizes
//! them to the bounded variants stored by the media service.

use std::{cmp::min, num::Saturating as Sat};

use ruma::{UInt, media::Method};
use tuwunel_core::{Result, checked, err, implement};

/// Dimension specification for a thumbnail.
///
/// Width and height describe the requested output extent. The method selects
/// proportional scaling or cropping to fill that extent.
#[derive(Debug)]
pub struct Dim {
	/// Requested output width in pixels.
	pub width: u32,

	/// Requested output height in pixels.
	pub height: u32,

	/// Resize operation applied to the source.
	pub method: Method,
}

/// Creates dimensions from Ruma integers.
///
/// Both values are checked before conversion, and an absent method uses the
/// endpoint's scaling default.
#[implement(Dim)]
pub fn from_ruma(width: UInt, height: UInt, method: Option<Method>) -> Result<Self> {
	let width = width
		.try_into()
		.map_err(|e| err!(Request(InvalidParam("Width is invalid: {e:?}"))))?;

	let height = height
		.try_into()
		.map_err(|e| err!(Request(InvalidParam("Height is invalid: {e:?}"))))?;

	Ok(Self::new(width, height, method))
}

/// Creates dimensions with an optional method.
///
/// An absent method selects proportional scaling.
#[implement(Dim)]
#[inline]
#[must_use]
pub fn new(width: u32, height: u32, method: Option<Method>) -> Self {
	Self {
		width,
		height,
		method: method.unwrap_or(Method::Scale),
	}
}

/// Scales dimensions to fit within a source.
///
/// The result preserves the source aspect ratio and never exceeds either the
/// requested extent or the source extent.
#[implement(Dim)]
pub fn scaled(&self, image: &Self) -> Result<Self> {
	let image_width = image.width;
	let image_height = image.height;

	let width = min(self.width, image_width);
	let height = min(self.height, image_height);

	let use_width = Sat(width) * Sat(image_height) < Sat(height) * Sat(image_width);

	let x = if use_width {
		let dividend = (Sat(height) * Sat(image_width)).0;
		checked!(dividend / image_height)?
	} else {
		width
	};

	let y = if !use_width {
		let dividend = (Sat(width) * Sat(image_height)).0;
		checked!(dividend / image_width)?
	} else {
		height
	};

	Ok(Self {
		width: x,
		height: y,
		method: Method::Scale,
	})
}

/// Returns whether generation cannot improve on the source.
///
/// A request passes through when it would upscale the source or when scaling
/// produces the source's own dimensions.
#[implement(Dim)]
pub fn is_passthrough(&self, source: &Self) -> Result<bool> {
	if self.width > source.width || self.height > source.height {
		return Ok(true);
	}

	let (width, height) = if self.crop() {
		(self.width, self.height)
	} else {
		let scaled = self.scaled(source)?;

		(scaled.width, scaled.height)
	};

	Ok(width == source.width && height == source.height)
}

/// The size a requested one is answered at.
///
/// Bucketing keeps the number of stored variants bounded, so the requested
/// method is discarded along with the requested size. A request above every
/// bucket answers the sentinel, which stands for the original file.
#[implement(Dim)]
#[must_use]
pub fn normalized(&self) -> Self {
	match (self.width, self.height) {
		| (0..=32, 0..=32) => Self::new(32, 32, Some(Method::Crop)),
		| (0..=96, 0..=96) => Self::new(96, 96, Some(Method::Crop)),
		| (0..=320, 0..=240) => Self::new(320, 240, Some(Method::Scale)),
		| (0..=640, 0..=480) => Self::new(640, 480, Some(Method::Scale)),
		| (0..=800, 0..=600) => Self::largest(),
		| _ => Self::default(),
	}
}

/// The largest size a thumbnail is generated at.
///
/// Every request above this normalizes to the sentinel instead, so it is
/// also the fallback for a picture whose own size cannot be read.
#[implement(Dim)]
#[inline]
#[must_use]
pub fn largest() -> Self { Self::new(800, 600, Some(Method::Scale)) }

/// Returns whether the method crops.
///
/// Scaling preserves the whole source, while cropping fills the requested
/// extent.
#[implement(Dim)]
#[inline]
#[must_use]
pub fn crop(&self) -> bool { self.method == Method::Crop }

/// Returns true for the sentinel that stands for the original file.
///
/// Every request too large to thumbnail normalizes to zero by zero, which
/// is also the key the original itself is stored under, so nothing may be
/// withheld there.
#[implement(Dim)]
#[inline]
#[must_use]
pub fn is_original(&self) -> bool { self.width == 0 && self.height == 0 }

impl Default for Dim {
	#[inline]
	fn default() -> Self {
		Self {
			width: 0,
			height: 0,
			method: Method::Scale,
		}
	}
}
