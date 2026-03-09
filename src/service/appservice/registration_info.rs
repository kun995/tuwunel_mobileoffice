use ruma::{OwnedUserId, ServerName, UserId, api::appservice::Registration};
use tuwunel_core::Result;

use super::NamespaceRegex;

/// Appservice registration combined with its compiled regular expressions.
#[derive(Clone, Debug)]
pub struct RegistrationInfo {
	pub registration: Registration,
	pub users: NamespaceRegex,
	pub aliases: NamespaceRegex,
	pub rooms: NamespaceRegex,
	pub sender: OwnedUserId,
}

impl RegistrationInfo {
	pub fn new(registration: Registration, server_name: &ServerName) -> Result<Self> {
		let sender =
			OwnedUserId::parse(format!("@{}:{}", registration.sender_localpart, server_name))?;

		Ok(Self {
			users: registration.namespaces.users.clone().try_into()?,
			aliases: registration
				.namespaces
				.aliases
				.clone()
				.try_into()?,
			rooms: registration.namespaces.rooms.clone().try_into()?,
			registration,
			sender,
		})
	}

	#[must_use]
	pub fn is_user_match(&self, user_id: &UserId) -> bool {
		user_id == self.sender || self.users.is_match(user_id.as_str())
	}

	#[inline]
	#[must_use]
	pub fn is_exclusive_user_match(&self, user_id: &UserId) -> bool {
		user_id == self.sender || self.users.is_exclusive_match(user_id.as_str())
	}
}
