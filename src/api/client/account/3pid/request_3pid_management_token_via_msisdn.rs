use ruma::api::client::account::request_3pid_management_token_via_msisdn;
use tuwunel_core::{Err, Result};

use crate::Ruma;

/// # `POST /_matrix/client/v3/account/3pid/msisdn/requestToken`
///
/// Request a validation token to add a phone number to the account. The phone
/// medium is not supported.
pub(crate) async fn request_3pid_management_token_via_msisdn_route(
	_body: Ruma<request_3pid_management_token_via_msisdn::v3::Request>,
) -> Result<request_3pid_management_token_via_msisdn::v3::Response> {
	Err!(Request(ThreepidMediumNotSupported(
		"Phone number verification is not supported"
	)))
}
