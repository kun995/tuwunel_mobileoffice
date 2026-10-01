mod all;
mod get_registration;

use clap::Subcommand;
use tuwunel_core::Result;

use crate::admin_command_dispatch;

#[admin_command_dispatch(handler_prefix = "appservice")]
#[derive(Debug, Subcommand)]
/// All the getters and iterators from src/service/appservice/
pub(crate) enum AppserviceCommand {
	/// - Gets the appservice registration info/details from the ID as a string
	GetRegistration {
		/// Appservice registration ID
		appservice_id: String,
	},

	/// - Gets all appservice registrations with their ID and registration info
	All,
}
