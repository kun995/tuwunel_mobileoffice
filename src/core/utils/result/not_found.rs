use super::Result;
use crate::Error;

/// Classifies and adapts results carrying the crate's not-found error family.
///
/// Successful results and unrelated errors are not classified as missing, and
/// only a not-found error converts to an absent value. Classification
/// delegates to `Error::is_not_found`.
pub trait NotFound<T> {
	/// Reports whether the result contains a not-found error.
	///
	/// An `Ok` value always returns false. Other error variants also return
	/// false without changing the result.
	#[must_use]
	fn is_not_found(&self) -> bool;

	/// Converts a not-found error into an absent value.
	///
	/// An `Ok` value is wrapped in `Some` and a not-found error becomes
	/// `Ok(None)`, so a caller distinguishes an absent value from a failed
	/// operation. Every other error is returned unchanged.
	fn optional(self) -> Result<Option<T>>;
}

impl<T> NotFound<T> for Result<T, Error> {
	#[inline]
	fn is_not_found(&self) -> bool { self.as_ref().is_err_and(Error::is_not_found) }

	#[inline]
	fn optional(self) -> Result<Option<T>> {
		self.map(Some)
			.or_else(|error| error.is_not_found().then_some(None).ok_or(error))
	}
}
