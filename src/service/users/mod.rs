mod dehydrated_device;
pub mod device;
mod keys;
mod ldap;
mod profile;
mod register;

use std::sync::Arc;

use futures::{Stream, StreamExt, TryFutureExt};
use ruma::{
	OwnedRoomId, OwnedUserId, UserId,
	api::client::filter::FilterDefinition,
	events::{GlobalAccountDataEventType, ignored_user_list::IgnoredUserListEvent},
};
use tuwunel_core::{
	Err, Result, debug_warn, err, is_equal_to,
	pdu::PduBuilder,
	trace,
	utils::{self, ReadyExt, stream::TryIgnore},
	warn,
};
use tuwunel_database::{Database, Deserialized, Json, Map, SEP};

pub use self::{keys::parse_master_key, register::Register};

pub struct Service {
	services: Arc<crate::services::OnceServices>,
	db: Data,
}

struct Data {
	database: Arc<Database>,
	keychangeid_userid: Arc<Map>,
	keyid_key: Arc<Map>,
	onetimekeyid_onetimekeys: Arc<Map>,
	openidtoken_expiresatuserid: Arc<Map>,
	logintoken_expiresatuserid: Arc<Map>,
	todeviceid_events: Arc<Map>,
	token_userdeviceid: Arc<Map>,
	userdeviceid_metadata: Arc<Map>,
	userdeviceid_token: Arc<Map>,
	userdeviceid_refresh: Arc<Map>,
	userfilterid_filter: Arc<Map>,
	userid_avatarurl: Arc<Map>,
	userid_blurhash: Arc<Map>,
	userid_dehydrateddevice: Arc<Map>,
	userid_devicelistversion: Arc<Map>,
	userid_displayname: Arc<Map>,
	userid_lastonetimekeyupdate: Arc<Map>,
	userid_masterkeyid: Arc<Map>,
	userid_password: Arc<Map>,
	userid_origin: Arc<Map>,
	userid_selfsigningkeyid: Arc<Map>,
	userid_usersigningkeyid: Arc<Map>,
	useridprofilekey_value: Arc<Map>,
}

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			services: args.services.clone(),
			db: Data {
				database: args.db.clone(),
				keychangeid_userid: args.db["keychangeid_userid"].clone(),
				keyid_key: args.db["keyid_key"].clone(),
				onetimekeyid_onetimekeys: args.db["onetimekeyid_onetimekeys"].clone(),
				openidtoken_expiresatuserid: args.db["openidtoken_expiresatuserid"].clone(),
				logintoken_expiresatuserid: args.db["logintoken_expiresatuserid"].clone(),
				todeviceid_events: args.db["todeviceid_events"].clone(),
				token_userdeviceid: args.db["token_userdeviceid"].clone(),
				userdeviceid_metadata: args.db["userdeviceid_metadata"].clone(),
				userdeviceid_token: args.db["userdeviceid_token"].clone(),
				userdeviceid_refresh: args.db["userdeviceid_refresh"].clone(),
				userfilterid_filter: args.db["userfilterid_filter"].clone(),
				userid_avatarurl: args.db["userid_avatarurl"].clone(),
				userid_blurhash: args.db["userid_blurhash"].clone(),
				userid_dehydrateddevice: args.db["userid_dehydrateddevice"].clone(),
				userid_devicelistversion: args.db["userid_devicelistversion"].clone(),
				userid_displayname: args.db["userid_displayname"].clone(),
				userid_lastonetimekeyupdate: args.db["userid_lastonetimekeyupdate"].clone(),
				userid_masterkeyid: args.db["userid_masterkeyid"].clone(),
				userid_password: args.db["userid_password"].clone(),
				userid_origin: args.db["userid_origin"].clone(),
				userid_selfsigningkeyid: args.db["userid_selfsigningkeyid"].clone(),
				userid_usersigningkeyid: args.db["userid_usersigningkeyid"].clone(),
				useridprofilekey_value: args.db["useridprofilekey_value"].clone(),
			},
		}))
	}

	fn name(&self) -> &str {
		crate::service::make_name(std::module_path!())
	}
}

impl Service {
	/// Returns true/false based on whether the recipient/receiving user has
	/// blocked the sender
	pub async fn user_is_ignored(&self, sender_user: &UserId, recipient_user: &UserId) -> bool {
		self.services
			.account_data
			.get_global(recipient_user, GlobalAccountDataEventType::IgnoredUserList)
			.await
			.is_ok_and(|ignored: IgnoredUserListEvent| {
				ignored
					.content
					.ignored_users
					.keys()
					.any(|blocked_user| blocked_user == sender_user)
			})
	}

	/// Check if a user is an admin
	#[inline]
	pub async fn is_admin(&self, user_id: &UserId) -> bool {
		self.services.admin.user_is_admin(user_id).await
	}

	/// Create a new user account on this homeserver.
	///
	/// User origin is by default "password" (meaning that it will login using
	/// its user_id/password). Users with other origins (currently only "ldap"
	/// is available) have special login processes.
	#[inline]
	pub async fn create(
		&self,
		user_id: &UserId,
		password: Option<&str>,
		origin: Option<&str>,
	) -> Result {
		let origin = origin.unwrap_or("password");
		self.db.userid_origin.insert(user_id, origin);
		self.set_password(user_id, password).await
	}

	/// Deactivate account
	pub async fn deactivate_account(&self, user_id: &UserId) -> Result {
		// Revoke any SSO authorizations
		self.services
			.oauth
			.revoke_user_tokens(user_id)
			.await;

		// Remove all associated devices
		self.all_device_ids(user_id)
			.for_each(|device_id| self.remove_device(user_id, device_id))
			.await;

		// Set the password to "" to indicate a deactivated account. Hashes will never
		// result in an empty string, so the user will not be able to log in again.
		// Systems like changing the password without logging in should check if the
		// account is deactivated.
		self.set_password(user_id, None).await?;

		// TODO: Unhook 3PID
		Ok(())
	}

	/// Check if a user has an account on this homeserver.
	#[inline]
	pub async fn exists(&self, user_id: &UserId) -> bool {
		self.db.userid_password.get(user_id).await.is_ok()
	}

	/// Check if account is deactivated
	pub async fn is_deactivated(&self, user_id: &UserId) -> Result<bool> {
		self.db
			.userid_password
			.get(user_id)
			.map_ok(|val| val.is_empty())
			.map_err(|_| err!(Request(NotFound("User does not exist."))))
			.await
	}

	/// Check if account is active, infallible
	pub async fn is_active(&self, user_id: &UserId) -> bool {
		!self.is_deactivated(user_id).await.unwrap_or(true)
	}

	/// Check if account is active, infallible
	pub async fn is_active_local(&self, user_id: &UserId) -> bool {
		self.services.globals.user_is_local(user_id) && self.is_active(user_id).await
	}

	/// Removes recreatable account state after the user has been fully
	/// deactivated. Room timeline events are intentionally preserved because
	/// they are part of the Matrix room DAG.
	pub async fn purge_account_records(&self, user_id: &UserId) -> usize {
		const USER_PREFIX_MAPS: &[&str] = &[
			"backupid_algorithm",
			"backupid_etag",
			"backupkeyid_backup",
			"keyid_key",
			"lazyloadedids",
			"onetimekeyid_onetimekeys",
			"senderkey_pusher",
			"todeviceid_events",
			"userdeviceconnid_conn",
			"userdeviceid_metadata",
			"userdeviceid_refresh",
			"userdeviceid_token",
			"userdevicesessionid_uiaainfo",
			"userdevicetxnid_response",
			"usereventsid_deleted",
			"userfilterid_filter",
			"userid_avatarurl",
			"userid_blurhash",
			"userid_dehydrateddevice",
			"userid_devicelistversion",
			"userid_displayname",
			"userid_lastonetimekeyupdate",
			"userid_masterkeyid",
			"userid_oauthid",
			"userid_origin",
			"userid_presenceid",
			"userid_selfsigningkeyid",
			"userid_usersigningkeyid",
			"userid_workspaceids",
			"useridcount_notification",
			"useridprofilekey_value",
			"userroomid_clearedbefore",
			"userroomid_deletedbefore",
			"userroomid_highlightcount",
			"userroomid_invitestate",
			"userroomid_joined",
			"userroomid_knockedstate",
			"userroomid_leftstate",
			"userroomid_notificationcount",
		];
		const USER_REVERSE_KEY_MAPS: &[&str] = &[
			"presenceid_presence",
			"roomuserdataid_accountdata",
			"roomusertype_roomuserdataid",
			"roomuserid_invitecount",
			"roomuserid_joined",
			"roomuserid_knockedcount",
			"roomuserid_lastnotificationread",
			"roomuserid_lastprivatereadupdate",
			"roomuserid_leftcount",
			"roomuserid_privateread",
			"workspaceid_userids",
		];
		const USER_VALUE_MAPS: &[&str] = &[
			"keychangeid_userid",
			"logintoken_expiresatuserid",
			"openidtoken_expiresatuserid",
			"token_userdeviceid",
		];

		// Deactivation normally removes these through service APIs. Repeat the
		// cleanup here to cover stale rows left by interrupted older operations.
		let pushkeys = self
			.services
			.pusher
			.get_pushkeys(user_id)
			.map(ToOwned::to_owned)
			.collect::<Vec<_>>()
			.await;
		for pushkey in &pushkeys {
			self.services
				.pusher
				.delete_pusher(user_id, pushkey)
				.await;
		}
		self.services
			.oauth
			.delete_user_sessions(user_id)
			.await;
		self.services
			.presence
			.remove_presence(user_id)
			.await;
		let reminder_count = self
			.services
			.reminders
			.purge_user_reminders(user_id)
			.await;
		let uiaa_request_count = self.services.uiaa.clear_user_requests(user_id);

		let _cork = self.db.database.cork_and_flush();
		let mut removed = pushkeys.len() + reminder_count + uiaa_request_count;
		for map_name in USER_PREFIX_MAPS {
			removed += remove_user_prefix_rows(&self.db.database[*map_name], user_id).await;
		}
		for map_name in USER_REVERSE_KEY_MAPS {
			removed += remove_user_keyed_rows(&self.db.database[*map_name], user_id).await;
		}
		for map_name in USER_VALUE_MAPS {
			removed += remove_user_value_rows(&self.db.database[*map_name], user_id).await;
		}

		// The existence check and all registration paths use userid_password as
		// the source of truth. Remove it last so a failed cleanup remains safely
		// deactivated and can be retried instead of being recreated too early.
		if self.db.userid_password.get(user_id).await.is_ok() {
			self.db.userid_password.remove(user_id);
			removed += 1;
		}

		removed
	}

	/// Returns the number of users registered on this server.
	#[inline]
	pub async fn count(&self) -> usize {
		self.db.userid_password.count().await
	}

	/// Returns an iterator over all users on this homeserver.
	pub fn stream(&self) -> impl Stream<Item = &UserId> + Send {
		self.db.userid_password.keys().ignore_err()
	}

	/// Returns a list of local users as list of usernames.
	///
	/// A user account is considered `local` if the length of it's password is
	/// greater then zero.
	pub fn list_local_users(&self) -> impl Stream<Item = &UserId> + Send + '_ {
		self.db
			.userid_password
			.stream()
			.ignore_err()
			.ready_filter_map(|(u, p): (&UserId, &[u8])| (!p.is_empty()).then_some(u))
	}

	/// Returns the origin of the user (password/LDAP/...).
	pub async fn origin(&self, user_id: &UserId) -> Result<String> {
		self.db
			.userid_origin
			.get(user_id)
			.await
			.deserialized()
	}

	/// Returns the password hash for the given user.
	pub async fn password_hash(&self, user_id: &UserId) -> Result<String> {
		self.db
			.userid_password
			.get(user_id)
			.await
			.deserialized()
	}

	/// Hash and set the user's password to the Argon2 hash
	pub async fn set_password(&self, user_id: &UserId, password: Option<&str>) -> Result {
		// Cannot change the password of a LDAP user. There are two special cases :
		// - a `None` password can be used to deactivate a LDAP user
		// - a "*" password is used as the default password of an active LDAP user
		//
		// The above now applies to all non-password origin users by default unless an
		// exception is made for that origin in the condition below. Note that users
		// with no origin are also password-origin users.
		let allowed_origins = ["password", "sso"];

		if let Some(password) = password
			&& password != "*"
		{
			let origin = self.origin(user_id).await;
			let origin = origin.as_deref().unwrap_or("password");

			if !allowed_origins.iter().any(is_equal_to!(&origin)) {
				return Err!(Request(InvalidParam(
					"Cannot change password of an {origin:?} user."
				)));
			}
		}

		match password.map(utils::hash::password) {
			| None => {
				self.db.userid_password.insert(user_id, b"");
			},
			| Some(Ok(hash)) => {
				self.db.userid_password.insert(user_id, hash);
				self.db.userid_origin.insert(user_id, "password");
			},
			| Some(Err(e)) => {
				return Err!(Request(InvalidParam(
					"Password does not meet the requirements: {e}"
				)));
			},
		}

		Ok(())
	}

	/// Creates a new sync filter. Returns the filter id.
	#[must_use]
	pub fn create_filter(&self, user_id: &UserId, filter: &FilterDefinition) -> String {
		let filter_id = utils::random_string(4);

		let key = (user_id, &filter_id);
		self.db.userfilterid_filter.put(key, Json(filter));

		filter_id
	}

	pub async fn get_filter(
		&self,
		user_id: &UserId,
		filter_id: &str,
	) -> Result<FilterDefinition> {
		let key = (user_id, filter_id);
		self.db
			.userfilterid_filter
			.qry(&key)
			.await
			.deserialized()
	}

	/// Creates an OpenID token, which can be used to prove that a user has
	/// access to an account (primarily for integrations)
	pub fn create_openid_token(&self, user_id: &UserId, token: &str) -> Result<u64> {
		use std::num::Saturating as Sat;

		let expires_in = self.services.server.config.openid_token_ttl;
		let expires_at = Sat(utils::millis_since_unix_epoch()) + Sat(expires_in) * Sat(1000);

		let mut value = expires_at.0.to_be_bytes().to_vec();
		value.extend_from_slice(user_id.as_bytes());

		self.db
			.openidtoken_expiresatuserid
			.insert(token.as_bytes(), value.as_slice());

		Ok(expires_in)
	}

	/// Find out which user an OpenID access token belongs to.
	pub async fn find_from_openid_token(&self, token: &str) -> Result<OwnedUserId> {
		let Ok(value) = self
			.db
			.openidtoken_expiresatuserid
			.get(token)
			.await
		else {
			return Err!(Request(Unauthorized("OpenID token is unrecognised")));
		};

		let (expires_at_bytes, user_bytes) = value.split_at(0_u64.to_be_bytes().len());
		let expires_at =
			u64::from_be_bytes(expires_at_bytes.try_into().map_err(|e| {
				err!(Database("expires_at in openid_userid is invalid u64. {e}"))
			})?);

		if expires_at < utils::millis_since_unix_epoch() {
			debug_warn!("OpenID token is expired, removing");
			self.db
				.openidtoken_expiresatuserid
				.remove(token.as_bytes());

			return Err!(Request(Unauthorized("OpenID token is expired")));
		}

		let user_string = utils::string_from_bytes(user_bytes)
			.map_err(|e| err!(Database("User ID in openid_userid is invalid unicode. {e}")))?;

		OwnedUserId::try_from(user_string)
			.map_err(|e| err!(Database("User ID in openid_userid is invalid. {e}")))
	}

	/// Creates a short-lived login token, which can be used to log in using the
	/// `m.login.token` mechanism.
	#[must_use]
	pub fn create_login_token(&self, user_id: &UserId, token: &str) -> u64 {
		use std::num::Saturating as Sat;

		let expires_in = self.services.server.config.login_token_ttl;
		let expires_at = Sat(utils::millis_since_unix_epoch()) + Sat(expires_in);

		let value = (expires_at.0, user_id);
		self.db
			.logintoken_expiresatuserid
			.raw_put(token, value);

		expires_in
	}

	/// Find out which user a login token belongs to.
	/// Removes the token to prevent double-use attacks.
	pub async fn find_from_login_token(&self, token: &str) -> Result<OwnedUserId> {
		let Ok(value) = self
			.db
			.logintoken_expiresatuserid
			.get(token)
			.await
		else {
			return Err!(Request(Forbidden("Login token is unrecognised")));
		};
		let (expires_at, user_id): (u64, OwnedUserId) = value.deserialized()?;

		if expires_at < utils::millis_since_unix_epoch() {
			trace!(?user_id, ?token, "Removing expired login token");

			self.db.logintoken_expiresatuserid.remove(token);

			return Err!(Request(Forbidden("Login token is expired")));
		}

		self.db.logintoken_expiresatuserid.remove(token);

		Ok(user_id)
	}

	#[cfg(not(feature = "ldap"))]
	#[expect(clippy::unused_async)]
	pub async fn search_ldap(&self, _user_id: &UserId) -> Result<Vec<(String, bool)>> {
		Err!(FeatureDisabled("ldap"))
	}

	#[cfg(not(feature = "ldap"))]
	#[expect(clippy::unused_async)]
	pub async fn auth_ldap(&self, _user_dn: &str, _password: &str) -> Result {
		Err!(FeatureDisabled("ldap"))
	}

	async fn update_all_rooms(&self, user_id: &UserId, rooms: Vec<(PduBuilder, &OwnedRoomId)>) {
		for (pdu_builder, room_id) in rooms {
			let state_lock = self.services.state.mutex.lock(room_id).await;
			if let Err(e) = self
				.services
				.timeline
				.build_and_append_pdu(pdu_builder, user_id, room_id, &state_lock)
				.await
			{
				warn!(%user_id, %room_id, "Failed to update/send new profile join membership update in room: {e}");
			}
		}
	}
}

fn serialized_component_matches(bytes: &[u8], user_id: &UserId) -> bool {
	let user = user_id.as_bytes();
	bytes == user
		|| bytes
			.split(|byte| *byte == SEP)
			.any(|component| component == user)
}

async fn remove_user_keyed_rows(map: &Arc<Map>, user_id: &UserId) -> usize {
	let keys = map
		.raw_keys()
		.ignore_err()
		.ready_filter_map(|key: &[u8]| {
			serialized_component_matches(key, user_id).then(|| key.to_vec())
		})
		.collect::<Vec<_>>()
		.await;

	for key in &keys {
		map.remove(key);
	}

	keys.len()
}

async fn remove_user_prefix_rows(map: &Arc<Map>, user_id: &UserId) -> usize {
	let mut keys = Vec::new();
	if map.get(user_id).await.is_ok() {
		keys.push(user_id.as_bytes().to_vec());
	}

	let mut prefix = user_id.as_bytes().to_vec();
	prefix.push(SEP);
	keys.extend(
		map.raw_keys_prefix(&prefix)
			.ignore_err()
			.map(<[u8]>::to_vec)
			.collect::<Vec<_>>()
			.await,
	);

	for key in &keys {
		map.remove(key);
	}

	keys.len()
}

async fn remove_user_value_rows(map: &Arc<Map>, user_id: &UserId) -> usize {
	let keys = map
		.raw_stream()
		.ignore_err()
		.ready_filter_map(|(key, value): (&[u8], &[u8])| {
			(serialized_component_matches(value, user_id) || value.ends_with(user_id.as_bytes()))
				.then(|| key.to_vec())
		})
		.collect::<Vec<_>>()
		.await;

	for key in &keys {
		map.remove(key);
	}

	keys.len()
}
