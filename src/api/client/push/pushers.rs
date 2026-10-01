use axum::extract::State;
use ruma::api::client::push::get_pushers;
use tuwunel_core::Result;

use crate::Ruma;

/// # `GET /_matrix/client/r0/pushers`
///
/// Gets all currently active pushers for the sender user.
pub(crate) async fn get_pushers_route(
	State(services): State<crate::State>,
	body: Ruma<get_pushers::v3::Request>,
) -> Result<get_pushers::v3::Response> {
	let sender_user = body.sender_user();

	Ok(get_pushers::v3::Response {
		pushers: services.pusher.get_pushers(sender_user).await,
	})
}
