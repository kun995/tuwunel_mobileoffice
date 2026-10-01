use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn oauth_adopt(&self, provider: String) -> Result {
	let provider = self
		.services
		.oauth
		.providers
		.get(&provider)
		.await?;

	let counts = self
		.services
		.oauth
		.sessions
		.adopt_foreign_subjects(&provider)
		.await?;

	if !counts.foreign_column {
		return writeln!(self, "No foreign identity column was found.").await;
	}

	writeln!(
		self,
		"adopted={} already={} collision={} absent={} invalid={}",
		counts.adopted, counts.already_bound, counts.collision, counts.absent, counts.invalid,
	)
	.await
}
