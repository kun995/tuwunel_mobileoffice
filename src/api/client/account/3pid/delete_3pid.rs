use axum::extract::State;
use ruma::api::client::account::{
	ThirdPartyIdRemovalStatus,
	delete_3pid::{self, v3::Response},
};
use tuwunel_core::Result;
use tuwunel_service::threepid::canonicalize_email;

use crate::Ruma;

/// # `POST /_matrix/client/v3/account/3pid/delete`
///
/// Remove a third party identifier from this account.
///
/// We never bound the address to an identity server, so the unbind result is
/// always `no-support`; any supplied `id_server` is ignored rather than
/// rejected.
#[tracing::instrument(skip_all, name = "delete_3pid")]
pub(crate) async fn delete_3pid_route(
	State(services): State<crate::State>,
	body: Ruma<delete_3pid::v3::Request>,
) -> Result<Response> {
	let sender_user = body.sender_user();

	if let Ok(email_canon) = canonicalize_email(&body.address) {
		services
			.threepid
			.del_binding(sender_user, &email_canon)
			.await;
	}

	Ok(Response::new(ThirdPartyIdRemovalStatus::NoSupport))
}
