use ruma::CanonicalJsonObject;
use tuwunel_core::{Err, Result, err, utils::math::Expected};

use crate::admin_command;

#[admin_command]
pub(super) async fn verify_json(&self) -> Result {
	if self.body.len() < 2
		|| !self.body[0].trim().starts_with("```")
		|| self.body.last().unwrap_or(&"").trim() != "```"
	{
		return Err!("Expected code block in command body. Add --help for details.");
	}

	let string = self.body[1..self.body.len().expected_sub(1)].join("\n");

	let value = serde_json::from_str::<CanonicalJsonObject>(&string)
		.map_err(|e| err!("Invalid json: {e}"))?;

	self.services
		.server_keys
		.verify_json(&value, None)
		.await
		.map_err(|e| err!("Signature verification failed: {e}"))?;

	self.write_str("Signature correct").await
}
