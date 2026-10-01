use tuwunel_core::Result;

use crate::admin_command;

#[rustfmt::skip]
#[admin_command]
pub(super) async fn panic(&self) -> Result {

	panic!("panicked")
}
