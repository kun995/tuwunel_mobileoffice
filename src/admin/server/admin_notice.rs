use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn admin_notice(&self, message: Vec<String>) -> Result {
	let message = message.join(" ");
	self.services.admin.send_text(&message).await;

	self.write_str("Notice was sent to #admins").await
}
