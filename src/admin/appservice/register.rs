use ruma::api::appservice::Registration;
use tuwunel_core::{Err, Result, checked, err};

use crate::admin_command;

#[admin_command]
pub(super) async fn appservice_register(&self) -> Result {
	let body = &self.body;
	let body_len = self.body.len();
	if body_len < 2
		|| !body[0].trim().starts_with("```")
		|| body.last().unwrap_or(&"").trim() != "```"
	{
		return Err!("Expected code block in command body. Add --help for details.");
	}

	let range = 1..checked!(body_len - 1)?;
	let appservice_config_body = body[range].join("\n");

	let registration: Registration = serde_yaml::from_str(&appservice_config_body)
		.map_err(|e| err!("Could not parse appservice config as YAML: {e}"))?;

	let id = registration.id.clone();

	self.services
		.appservice
		.register_appservice(registration)
		.await
		.map_err(|e| err!("Failed to register appservice: {e}"))?;

	write!(self, "Appservice registered with ID: {id}").await
}
