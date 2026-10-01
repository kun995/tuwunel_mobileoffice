use futures::StreamExt;
use ruma::{OwnedRoomAliasId, OwnedRoomId};
use tuwunel_core::Result;

use crate::{Context, admin_command};

#[admin_command]
pub(super) async fn alias_list(&self, room_id: Option<OwnedRoomId>) -> Result {
	match room_id {
		| Some(room_id) => list_aliases_for_room(self, room_id).await,
		| None => list_all_aliases(self).await,
	}
}

async fn list_aliases_for_room(context: &Context<'_>, room_id: OwnedRoomId) -> Result {
	let aliases: Vec<OwnedRoomAliasId> = context
		.services
		.alias
		.local_aliases_for_room(&room_id)
		.map(Into::into)
		.collect()
		.await;

	writeln!(context, "Aliases for {room_id}:").await?;
	for alias in aliases {
		writeln!(context, "- {alias}").await?;
	}

	Ok(())
}

async fn list_all_aliases(context: &Context<'_>) -> Result {
	let aliases = context
		.services
		.alias
		.all_local_aliases()
		.map(|(room_id, localpart)| (room_id.to_owned(), localpart.to_owned()))
		.collect::<Vec<_>>()
		.await;

	let server_name = context.services.globals.server_name();

	writeln!(context, "Aliases:").await?;
	for (room_id, alias_id) in aliases {
		writeln!(context, "- `{room_id}` -> #{alias_id}:{server_name}").await?;
	}

	Ok(())
}
