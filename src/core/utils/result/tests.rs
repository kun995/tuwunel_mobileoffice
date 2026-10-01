use super::{NotFound, Result};
use crate::err;

#[test]
fn only_a_not_found_error_is_an_absent_value() {
	let present: Result<u8> = Ok(1);
	let missing: Result<u8> = Err(err!(Request(NotFound("test value"))));
	let failed: Result<u8> = Err(err!(Database("test failure")));

	assert!(matches!(present.optional(), Ok(Some(1))));
	assert!(matches!(missing.optional(), Ok(None)));
	assert!(matches!(failed.optional(), Err(error) if !error.is_not_found()));
}
