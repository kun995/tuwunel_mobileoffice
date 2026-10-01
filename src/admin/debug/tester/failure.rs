use tuwunel_core::{Err, Result};

use crate::admin_command;

#[rustfmt::skip]
#[admin_command]
pub(super) async fn failure(&self) -> Result {

	Err!("failed")
}
