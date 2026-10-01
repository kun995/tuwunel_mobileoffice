use tuwunel_core::{Result, err};

use crate::admin_command;

#[admin_command]
pub(super) async fn appservice_show_config(&self, appservice_identifier: String) -> Result {
	let config = self
		.services
		.appservice
		.get_registration(&appservice_identifier)
		.await
		.ok_or(err!("Appservice does not exist."))?;

	let config_str = serde_yaml::to_string(&config)?;

	write!(self, "Config for {appservice_identifier}:\n\n```yaml\n{config_str}\n```").await
}
