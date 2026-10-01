use tuwunel_core::{Result, err};

use crate::admin_command;

#[admin_command]
pub(super) async fn appservice_unregister(&self, appservice_identifier: String) -> Result {
	self.services
		.appservice
		.unregister_appservice(&appservice_identifier)
		.await
		.map_err(|e| err!("Failed to unregister appservice: {e}"))?;

	self.write_str("Appservice unregistered.").await
}
