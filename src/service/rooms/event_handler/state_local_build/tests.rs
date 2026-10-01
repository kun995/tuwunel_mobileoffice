use std::sync::Arc;

use super::{Fallback, StateLocalCounters, WalkAttempt, WalkOutcome};

#[test]
fn settled_walks_partition_into_one_outcome() {
	let counters = Arc::new(StateLocalCounters::default());

	WalkAttempt::start(counters.clone()).settle(WalkOutcome::Resolved, 2);

	for fallback in [
		Fallback::Absent,
		Fallback::Ceiling,
		Fallback::AuthMissing,
		Fallback::AllCommitted,
		Fallback::Entries,
		Fallback::Canary,
		Fallback::CreateMismatch,
		Fallback::Unevaluable,
		Fallback::Error,
	] {
		WalkAttempt::start(counters.clone()).settle(WalkOutcome::Fallback(fallback), 1);
	}

	WalkAttempt::start(counters.clone()).settle(WalkOutcome::Failure, 3);
	drop(WalkAttempt::start(counters.clone()));

	let metrics = counters.snapshot();
	let fallbacks = [
		metrics.fallback_absent,
		metrics.fallback_ceiling,
		metrics.fallback_auth_missing,
		metrics.fallback_all_committed,
		metrics.fallback_entries,
		metrics.fallback_canary,
		metrics.fallback_create_mismatch,
		metrics.fallback_unevaluable,
		metrics.fallback_error,
	];

	let fallback_total: u64 = fallbacks.into_iter().sum();

	assert_eq!(metrics.walk_resolved, 1);
	assert_eq!(fallbacks, [1; 9]);
	assert_eq!(metrics.walk_failures, 2);
	assert_eq!(metrics.gate_denials, 14);
	assert_eq!(
		metrics.walk_attempts,
		metrics.walk_resolved + fallback_total + metrics.walk_failures
	);
}
