use tuwunel_core::Result;

use crate::admin_command;

#[inline(never)]
#[rustfmt::skip]
#[admin_command]
pub(super) async fn tester(&self) -> Result {

	self.write_str("Ok").await
}
