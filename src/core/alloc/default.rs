//! Default allocator with no special features

use crate::Result;

/// Reclaims unused pages from every initialized arena.
///
/// The default allocator exposes no reclamation facility, so nothing is purged
/// and the call always succeeds.
pub fn trim() -> Result { Ok(()) }

/// Always returns None
#[must_use]
pub fn memory_stats(_opts: &str) -> Option<String> { None }
