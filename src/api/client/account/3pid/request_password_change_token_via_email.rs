use axum::extract::State;
use ruma::api::client::account::request_password_change_token_via_email::{self, v3::Response};
use tuwunel_core::{Err, Result};
use tuwunel_service::threepid::canonicalize_email;

use super::email_token::send_email_token;
use crate::{ClientIp, Ruma};

/// # `POST /_matrix/client/v3/account/password/email/requestToken`
///
/// Request a validation token to verify an email address before a password
/// reset.
pub(crate) async fn request_password_change_token_via_email_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<request_password_change_token_via_email::v3::Request>,
) -> Result<Response> {
	if !services.sendmail.is_enabled() {
		return Err!(Request(ThreepidDenied("Email verification is not configured")));
	}

	let email_canon = canonicalize_email(&body.email)?;

	// The directional error: a reset for an unbound address reports not-found, not
	// in-use. The message goes to the stored canonical address, never the raw
	// client spelling.
	if services
		.threepid
		.user_id_for_email(&email_canon)
		.await?
		.is_none()
	{
		return Err!(Request(ThreepidNotFound("That email address is not bound to an account")));
	}

	let sid = send_email_token(
		&services,
		client,
		body.client_secret.as_str(),
		&email_canon,
		body.send_attempt.into(),
	)
	.await?;

	Ok(Response::new(sid))
}
