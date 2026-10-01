use ruma::OwnedEventId;
use tuwunel_core::{Result, err};

use crate::admin_command;

#[admin_command]
pub(super) async fn get_pdu(&self, event_id: OwnedEventId) -> Result {
	let mut outlier = false;
	let mut pdu_json = self
		.services
		.timeline
		.get_non_outlier_pdu_json(&event_id)
		.await;

	if pdu_json.is_err() {
		outlier = true;
		pdu_json = self
			.services
			.timeline
			.get_pdu_json(&event_id)
			.await;
	}

	let json = pdu_json.map_err(|_| err!("PDU not found locally."))?;

	let text = serde_json::to_string_pretty(&json)?;
	let msg = if outlier {
		"Outlier (Rejected / Soft Failed) PDU found in our database"
	} else {
		"PDU found in our database"
	};

	write!(self, "{msg}\n```json\n{text}\n```").await
}
