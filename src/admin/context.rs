pub(crate) use core::{write as core_write, writeln as core_writeln};
use std::ops::Deref;

/// Drop-in replacement for [`core::write!`] that block-wraps the call so the
/// `format_args!` temporary is dropped before any `.await`, keeping the
/// resulting future `Send`. Sync callers see no behavior difference.
///
/// Workaround for a recurring footgun: `core::write!($self, "...", x).await`
/// holds a `fmt::Arguments<'_>` temporary (containing `!Send` vtable pointers)
/// across the await point, poisoning the dispatcher's `Send` bound. The
/// block scope ends the temporary's lifetime before the future is awaited.
///
/// Escape hatch: [`core_write!`] is the unmodified macro.
macro_rules! write {
	($self:expr, $($arg:tt)*) => { { $crate::context::core_write!($self, $($arg)*) } };
}

/// Drop-in replacement for [`core::writeln!`]; see [`write!`] for rationale.
macro_rules! writeln {
	($self:expr $(, $($arg:tt)*)?) => {
		{ $crate::context::core_writeln!($self $(, $($arg)*)?) }
	};
}

/// Orphan-rule wrapper: `#[implement(crate::Context)]` on each handler emits
/// an inherent impl, illegal on a foreign type.
pub(crate) struct Context<'a>(&'a tuwunel_service::admin::Context<'a>);

impl<'a> Context<'a> {
	#[inline]
	pub(crate) const fn new(inner: &'a tuwunel_service::admin::Context<'a>) -> Self {
		Self(inner)
	}
}

impl<'a> Deref for Context<'a> {
	type Target = tuwunel_service::admin::Context<'a>;

	#[inline]
	fn deref(&self) -> &Self::Target { self.0 }
}
