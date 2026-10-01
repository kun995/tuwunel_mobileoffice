use tuwunel_core::Result;
use tuwunel_service::threepid::canonicalize_email;

use crate::{admin_command, utils::parse_local_user_id};

#[admin_command]
pub(super) async fn del_email(&self, username: String, address: String) -> Result {
	let user_id = parse_local_user_id(self.services, &username)?;

	let email_canon = canonicalize_email(&address)?;

	self.services
		.threepid
		.del_binding(&user_id, &email_canon)
		.await;

	write!(self, "Removed email {email_canon} from user {user_id}.").await
}
