use tuwunel_core::{Err, Result, err, utils::math::Expected};

use crate::admin_command;

#[admin_command]
pub(super) async fn sign_json(&self) -> Result {
	if self.body.len() < 2
		|| !self.body[0].trim().starts_with("```")
		|| self.body.last().unwrap_or(&"").trim() != "```"
	{
		return Err!("Expected code block in command body. Add --help for details.");
	}

	let string = self.body[1..self.body.len().expected_sub(1)].join("\n");
	let mut value = serde_json::from_str(&string).map_err(|e| err!("Invalid json: {e}"))?;

	self.services.server_keys.sign_json(&mut value)?;

	let json_text = serde_json::to_string_pretty(&value)?;
	self.write_str(&json_text).await
}
