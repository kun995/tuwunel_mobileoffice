use ruma::{OwnedDeviceId, OwnedUserId};
use tuwunel_core::Result;
use tuwunel_service::sync::into_connection_key;

use crate::admin_command;

#[admin_command]
pub(super) async fn show_connection(
	&self,
	user_id: OwnedUserId,
	device_id: Option<OwnedDeviceId>,
	conn_id: Option<String>,
) -> Result {
	let key = into_connection_key(user_id, device_id, conn_id);
	let cache = self
		.services
		.sync
		.get_loaded_connection(&key)
		.await?;

	let out;
	{
		let cached = cache.lock().await;
		out = format!("{cached:#?}");
	};

	self.write_str(out.as_str()).await
}
