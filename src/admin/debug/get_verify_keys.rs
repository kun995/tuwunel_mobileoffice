use ruma::OwnedServerName;
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn get_verify_keys(&self, server_name: Option<OwnedServerName>) -> Result {
	let server_name = server_name.unwrap_or_else(|| self.services.server.name.clone());

	let keys = self
		.services
		.server_keys
		.verify_keys_for(&server_name)
		.await;

	writeln!(self, "| Key ID | Public Key |").await?;
	writeln!(self, "| --- | --- |").await?;
	for (key_id, key) in keys {
		writeln!(self, "| {key_id} | {key:?} |").await?;
	}

	Ok(())
}
