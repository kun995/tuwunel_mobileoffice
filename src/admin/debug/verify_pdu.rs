use ruma::{OwnedEventId, signatures::Verified};
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn verify_pdu(&self, event_id: OwnedEventId) -> Result {
	let mut event = self
		.services
		.timeline
		.get_pdu_json(&event_id)
		.await?;

	event.remove("event_id");
	let msg = match self
		.services
		.server_keys
		.verify_event(&event, None)
		.await?
	{
		| Verified::Signatures => "signatures OK, but content hash failed (redaction).",
		| Verified::All => "signatures and hashes OK.",
	};

	self.write_str(msg).await
}
