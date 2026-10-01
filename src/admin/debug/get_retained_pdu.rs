use ruma::OwnedEventId;
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn get_retained_pdu(&self, event_id: OwnedEventId) -> Result {
	let pdu = self
		.services
		.retention
		.get_original_pdu_json(&event_id)
		.await?;

	let text = serde_json::to_string_pretty(&pdu)?;

	write!(self, "Original PDU:\n```json\n{text}\n```").await?;

	Ok(())
}
