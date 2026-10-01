mod available;
mod register;
mod token_validity;

use tuwunel_service::appservice::RegistrationInfo;

pub(crate) use self::{
	available::get_register_available_route, register::register_route,
	token_validity::check_registration_token_validity,
};
use super::SESSION_ID_LENGTH;

// workaround for https://github.com/matrix-org/matrix-appservice-irc/issues/1780
pub(super) fn is_matrix_appservice_irc(appservice_info: Option<&RegistrationInfo>) -> bool {
	appservice_info.is_some_and(|appservice| {
		let id = &appservice.registration.id;
		id == "irc"
			|| id.contains("matrix-appservice-irc")
			|| id.contains("matrix_appservice_irc")
	})
}
