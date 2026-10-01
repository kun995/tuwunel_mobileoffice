use axum::extract::State;
use futures::StreamExt;
use ruma::api::client::device::{self, get_devices};
use tuwunel_core::Result;

use crate::Ruma;

/// # `GET /_matrix/client/r0/devices`
///
/// Get metadata on all devices of the sender user.
pub(crate) async fn get_devices_route(
	State(services): State<crate::State>,
	body: Ruma<get_devices::v3::Request>,
) -> Result<get_devices::v3::Response> {
	let devices: Vec<device::Device> = services
		.users
		.all_devices_metadata(body.sender_user())
		.collect()
		.await;

	Ok(get_devices::v3::Response { devices })
}
