use futures::StreamExt;
use ruma::OwnedServerName;
use tuwunel_core::{Result, utils::time};
use tuwunel_service::resolver::cache::CachedDest;

use crate::admin_command;

#[admin_command]
pub(super) async fn destinations_cache(&self, server_name: Option<OwnedServerName>) -> Result {
	writeln!(self, "| Server Name | Destination | Hostname | Expires |").await?;
	writeln!(self, "| ----------- | ----------- | -------- | ------- |").await?;

	let mut destinations = self
		.services
		.resolver
		.cache
		.destinations()
		.boxed();

	while let Some((name, CachedDest { dest, host, expire })) = destinations.next().await {
		if let Some(server_name) = server_name.as_ref()
			&& name != server_name
		{
			continue;
		}

		let expire = time::format(expire, "%+");
		write!(self, "| {name} | {dest} | {host} | {expire} |\n").await?;
	}

	Ok(())
}
