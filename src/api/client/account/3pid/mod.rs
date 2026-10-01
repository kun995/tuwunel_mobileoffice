mod add_3pid;
mod delete_3pid;
mod email_token;
mod email_validate;
mod request_3pid_management_token_via_email;
mod request_3pid_management_token_via_msisdn;
mod request_password_change_token_via_email;
mod request_registration_token_via_email;
mod third_party;

pub(crate) use self::{
	add_3pid::add_3pid_route,
	delete_3pid::delete_3pid_route,
	email_validate::{get_email_validate_route, post_email_validate_route},
	request_3pid_management_token_via_email::request_3pid_management_token_via_email_route,
	request_3pid_management_token_via_msisdn::request_3pid_management_token_via_msisdn_route,
	request_password_change_token_via_email::request_password_change_token_via_email_route,
	request_registration_token_via_email::request_registration_token_via_email_route,
	third_party::third_party_route,
};
