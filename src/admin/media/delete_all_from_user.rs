use tuwunel_core::Result;

use crate::{admin_command, utils::parse_local_user_id};

#[admin_command]
pub(super) async fn delete_all_from_user(&self, username: String) -> Result {
	let user_id = parse_local_user_id(self.services, &username)?;

	let deleted_count = self
		.services
		.media
		.delete_from_user(&user_id)
		.await?;

	write!(self, "Deleted {deleted_count} total files.").await
}
