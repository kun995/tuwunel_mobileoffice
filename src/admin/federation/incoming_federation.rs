use tuwunel_core::{Err, Result};

use crate::admin_command;

#[admin_command]
pub(super) async fn incoming_federation(&self) -> Result {
	Err!("This command is temporarily disabled")
}
