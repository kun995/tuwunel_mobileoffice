use std::time::Instant;

use tuwunel_core::Result;

use crate::admin_command;

#[inline(never)]
#[rustfmt::skip]
#[admin_command]
pub(super) async fn timer(&self) -> Result {
	let started = Instant::now();
	timed(self.body);

	let elapsed = started.elapsed();
	write!(self, "completed in {elapsed:#?}").await
}

#[inline(never)]
#[rustfmt::skip]
#[expect(unused_variables)]
fn timed(body: &[&str]) {

}
