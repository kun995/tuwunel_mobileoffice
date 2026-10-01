use axum::extract::State;
use ruma::api::client::account::whoami;
use tuwunel_core::{Result, err};

use crate::Ruma;

/// # `GET _matrix/client/r0/account/whoami`
///
/// Get `user_id` of the sender user.
///
/// Note: Also works for Application Services
pub(crate) async fn whoami_route(
	State(services): State<crate::State>,
	body: Ruma<whoami::v3::Request>,
) -> Result<whoami::v3::Response> {
	Ok(whoami::v3::Response {
		user_id: body.sender_user().to_owned(),
		device_id: body.sender_device.clone(),
		is_guest: body.appservice_info.is_none()
			&& services
				.users
				.is_deactivated(body.sender_user())
				.await
				.map_err(|_| err!(Request(Forbidden("User does not exist."))))?,
	})
}
