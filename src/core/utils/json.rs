//! Serialization helpers for raw and canonical JSON values.
//!
//! The conversion functions bridge Serde values to Ruma's raw and canonical
//! representations. A generic deserializer adapts string-backed fields to types
//! implementing `FromStr`.

use std::{fmt, io, marker::PhantomData, str::FromStr};

use ruma::{
	CanonicalJsonError, CanonicalJsonObject, canonical_json::try_from_json_map, serde::Raw,
};

use crate::Result;

/// An `io::Write` sink that counts bytes without buffering them.
#[derive(Default)]
struct Counter(usize);

/// Serializes a value into Ruma's raw JSON representation.
///
/// The input is first converted to a `serde_json::Value`, then stored as
/// `Raw<U>` without deserializing `U`. Serialization or JSON conversion
/// failures are returned.
pub fn to_raw<T: serde::Serialize, U>(input: T) -> Result<Raw<U>> {
	Ok(serde_json::from_value(serde_json::to_value(input)?)?)
}

/// Converts a serializable value into a canonical JSON object.
///
/// The value must serialize to a JSON object. Serialization errors, non-object
/// values, and data outside canonical JSON's representation are returned as
/// `CanonicalJsonError`.
pub fn to_canonical_object<T: serde::Serialize>(
	value: T,
) -> Result<CanonicalJsonObject, CanonicalJsonError> {
	use CanonicalJsonError::SerDe;
	use serde::ser::Error;

	match serde_json::to_value(value).map_err(SerDe)? {
		| serde_json::Value::Object(map) => try_from_json_map(map),
		| _ => Err(SerDe(serde_json::Error::custom("Value must be an object"))),
	}
}

/// Measures the length of a value's JSON serialization.
///
/// The value is written to a sink that counts bytes rather than retaining
/// them, so the length costs no buffer. Serialization failures are returned.
pub fn serialized_len<T: serde::Serialize>(value: &T) -> Result<usize> {
	let mut counter = Counter::default();

	serde_json::to_writer(&mut counter, value)?;

	Ok(counter.0)
}

impl io::Write for Counter {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		self.0 = self.0.saturating_add(buf.len());

		Ok(buf.len())
	}

	fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

/// Deserializes a string and parses it through `FromStr`.
///
/// Only string input is accepted. Parse failures become custom deserialization
/// errors using their `Display` messages.
pub fn deserialize_from_str<'de, D, T, E>(deserializer: D) -> Result<T, D::Error>
where
	D: serde::de::Deserializer<'de>,
	T: FromStr<Err = E>,
	E: fmt::Display,
{
	struct Visitor<T: FromStr<Err = E>, E>(PhantomData<T>);

	impl<T, Err> serde::de::Visitor<'_> for Visitor<T, Err>
	where
		T: FromStr<Err = Err>,
		Err: fmt::Display,
	{
		type Value = T;

		fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
			write!(formatter, "a parsable string")
		}

		fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
		where
			E: serde::de::Error,
		{
			v.parse().map_err(serde::de::Error::custom)
		}
	}

	deserializer.deserialize_str(Visitor(PhantomData))
}
