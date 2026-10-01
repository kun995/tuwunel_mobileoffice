use tuwunel_core::{Result, err};

use super::parse_alias_from_localpart;
use crate::admin_command;

#[admin_command]
pub(super) async fn alias_which(&self, room_alias_localpart: String) -> Result {
	let room_alias = parse_alias_from_localpart(self.services, &room_alias_localpart)?;

	let id = self
		.services
		.alias
		.resolve_local_alias(&room_alias)
		.await
		.map_err(|_| err!("Alias isn't in use."))?;

	write!(self, "Alias resolves to {id}").await
}
