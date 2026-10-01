use axum::extract::State;
use ruma::api::client::push::set_pusher;
use tuwunel_core::Result;

use crate::Ruma;

/// # `POST /_matrix/client/r0/pushers/set`
///
/// Adds a pusher for the sender user.
///
/// - TODO: Handle `append`
pub(crate) async fn set_pushers_route(
	State(services): State<crate::State>,
	body: Ruma<set_pusher::v3::Request>,
) -> Result<set_pusher::v3::Response> {
	let sender_user = body.sender_user();

	services
		.pusher
		.set_pusher(sender_user, body.sender_device()?, &body.action)
		.await?;

	Ok(set_pusher::v3::Response::new())
}
