use std::collections::BTreeMap;

use axum::{
	Json,
	extract::{Path, State},
};
use axum_extra::{
	TypedHeader,
	headers::{Authorization, authorization::Bearer},
};
use ruma::{OwnedUserId, UserId, api::error::ErrorKind};
use serde::{Deserialize, Serialize};
use tuwunel_core::{Err, Error, Result, err, info, utils};
use tuwunel_service::{Services, users::Register};

const AUTO_GEN_PASSWORD_LENGTH: usize = 25;

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateUserWorkspacesBody {
	pub workspace_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateWorkspaceUsersBody {
	#[serde(default)]
	pub user_ids: Vec<OwnedUserId>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BulkCreateUsersBody {
	#[serde(default)]
	pub workspace_id: Option<String>,
	#[serde(default)]
	pub users: Vec<BulkCreateUserItem>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BulkCreateUserItem {
	pub localpart: String,
	#[serde(default)]
	pub displayname: Option<String>,
	#[serde(default)]
	pub password: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct UserWorkspacesResponse {
	pub user_id: OwnedUserId,
	pub workspace_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct AdminUserInfoResponse {
	pub user_id: OwnedUserId,
	pub displayname: Option<String>,
	pub avatar_url: Option<String>,
	pub workspace_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct WorkspaceUsersResponse {
	pub workspace_id: String,
	pub user_ids: Vec<OwnedUserId>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BulkCreateUsersResponse {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub workspace_id: Option<String>,
	pub users: Vec<BulkCreatedUser>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BulkCreatedUser {
	pub user_id: OwnedUserId,
	pub localpart: String,
	pub displayname: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub generated_password: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct UserDisplaynameUpdate {
	pub user_id: OwnedUserId,
	pub displayname: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateUserDisplaynamesBody {
	#[serde(default)]
	pub users: Vec<UserDisplaynameUpdate>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PurgeUserBody {
	pub confirm_user_id: OwnedUserId,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BulkPurgeUsersBody {
	#[serde(default)]
	pub user_ids: Vec<OwnedUserId>,
	#[serde(default)]
	pub confirm: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct UpdateUserDisplaynamesResponse {
	pub users: Vec<UserDisplaynameUpdate>,
}

#[derive(Debug, Serialize)]
pub(crate) struct PurgeUserResponse {
	pub user_id: OwnedUserId,
	pub purged: bool,
	pub recreatable: bool,
	pub historical_events_preserved: bool,
	pub removed_records: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct BulkPurgeUsersResponse {
	pub total: usize,
	pub purged: usize,
	pub failed: usize,
	pub historical_events_preserved: bool,
	pub users: Vec<BulkPurgeUserResult>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BulkPurgeUserResult {
	pub user_id: OwnedUserId,
	pub purged: bool,
	pub recreatable: bool,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub removed_records: Option<usize>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub error: Option<String>,
}

/// # `GET /_matrix/client/unstable/org.tuwunel/user/workspaces`
///
/// Returns workspace mappings for the authenticated user.
pub(crate) async fn get_user_workspaces_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
) -> Result<Json<UserWorkspacesResponse>> {
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken(ruma::api::error::UnknownTokenErrorData { soft_logout: false }),
				"Unknown access token.",
			)
		})?;

	if !services.users.exists(&sender_user).await {
		return Err!(Request(NotFound("User does not exist.")));
	}

	let workspace_ids = services.workspace.user_workspaces(&sender_user).await;

	Ok(Json(UserWorkspacesResponse {
		user_id: sender_user,
		workspace_ids,
	}))
}

/// # `GET /_matrix/client/unstable/org.tuwunel/users/{userId}/workspaces`
///
/// Returns workspace mappings for a specific user. Admin-only because this can
/// inspect mappings for other users.
pub(crate) async fn get_user_workspaces_admin_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path(user_id): Path<OwnedUserId>,
) -> Result<Json<UserWorkspacesResponse>> {
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken(ruma::api::error::UnknownTokenErrorData { soft_logout: false }),
				"Unknown access token.",
			)
		})?;

	let sender_user_ref: &UserId = sender_user.as_ref();
	if !services.admin.user_is_admin(sender_user_ref).await {
		return Err!(Request(Forbidden(
			"You do not have permission to view workspace users."
		)));
	}

	let user_id_ref: &UserId = user_id.as_ref();
	if !services.users.exists(user_id_ref).await {
		return Err!(Request(NotFound("User does not exist.")));
	}

	let workspace_ids = services.workspace.user_workspaces(user_id_ref).await;

	Ok(Json(UserWorkspacesResponse {
		user_id,
		workspace_ids,
	}))
}

/// # `GET /_matrix/client/unstable/org.tuwunel/users/{userId}`
///
/// Returns profile and workspace mappings for a specific user. Admin-only.
pub(crate) async fn get_user_info_admin_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path(user_id): Path<OwnedUserId>,
) -> Result<Json<AdminUserInfoResponse>> {
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken(ruma::api::error::UnknownTokenErrorData { soft_logout: false }),
				"Unknown access token.",
			)
		})?;

	let sender_user_ref: &UserId = sender_user.as_ref();
	if !services.admin.user_is_admin(sender_user_ref).await {
		return Err!(Request(Forbidden(
			"You do not have permission to view user information."
		)));
	}

	let user_id_ref: &UserId = user_id.as_ref();
	if !services.users.exists(user_id_ref).await {
		return Err!(Request(NotFound("User does not exist.")));
	}

	let displayname = services.profile.displayname(user_id_ref).await.ok();
	let avatar_url = services
		.profile
		.avatar_url(user_id_ref)
		.await
		.ok()
		.map(|avatar_url| avatar_url.to_string());
	let workspace_ids = services.workspace.user_workspaces(user_id_ref).await;

	Ok(Json(AdminUserInfoResponse {
		user_id,
		displayname,
		avatar_url,
		workspace_ids,
	}))
}

/// # `PUT /_matrix/client/unstable/org.tuwunel/user/workspaces`
///
/// Adds one workspace mapping for the authenticated user.
pub(crate) async fn update_user_workspaces_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Json(body): Json<UpdateUserWorkspacesBody>,
) -> Result<Json<UserWorkspacesResponse>> {
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken(ruma::api::error::UnknownTokenErrorData { soft_logout: false }),
				"Unknown access token.",
			)
		})?;

	if !services.users.exists(&sender_user).await {
		return Err!(Request(NotFound("User does not exist.")));
	}

	let workspace_ids = services
		.workspace
		.add_user_workspace(&sender_user, &body.workspace_id)
		.await;

	Ok(Json(UserWorkspacesResponse {
		user_id: sender_user,
		workspace_ids,
	}))
}

/// # `PUT /_matrix/client/unstable/org.tuwunel/workspaces/{workspaceId}/users`
///
/// Adds one workspace mapping for a list of users. Admin-only because this
/// updates mappings for other users.
pub(crate) async fn update_workspace_users_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path(workspace_id): Path<String>,
	Json(body): Json<UpdateWorkspaceUsersBody>,
) -> Result<Json<WorkspaceUsersResponse>> {
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken(ruma::api::error::UnknownTokenErrorData { soft_logout: false }),
				"Unknown access token.",
			)
		})?;

	let sender_user_ref: &UserId = sender_user.as_ref();
	if !services.admin.user_is_admin(sender_user_ref).await {
		return Err!(Request(Forbidden(
			"You do not have permission to update workspace users."
		)));
	}

	let workspace_id = workspace_id.trim().to_owned();
	if workspace_id.is_empty() {
		return Err!(Request(InvalidParam("workspace_id must not be empty.")));
	}

	let mut user_ids = body.user_ids;
	user_ids.sort();
	user_ids.dedup();

	for user_id in &user_ids {
		let user_id_ref: &UserId = user_id.as_ref();
		if !services.users.exists(user_id_ref).await {
			return Err!(Request(NotFound("User does not exist.")));
		}
	}

	for user_id in &user_ids {
		let user_id_ref: &UserId = user_id.as_ref();
		services
			.workspace
			.add_user_workspace(user_id_ref, &workspace_id)
			.await;
	}

	Ok(Json(WorkspaceUsersResponse {
		workspace_id,
		user_ids,
	}))
}

/// # `POST /_matrix/client/unstable/org.tuwunel/users/bulk_create`
///
/// Creates a list of local users from localparts. Admin-only.
///
/// Validation is all-or-nothing: if any item is invalid, no user is created.
/// When `workspace_id` is provided, the newly-created users are mapped into
/// that workspace after registration succeeds.
pub(crate) async fn bulk_create_users_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Json(body): Json<BulkCreateUsersBody>,
) -> Result<Json<BulkCreateUsersResponse>> {
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken(ruma::api::error::UnknownTokenErrorData { soft_logout: false }),
				"Unknown access token.",
			)
		})?;

	let sender_user_ref: &UserId = sender_user.as_ref();
	if !services.admin.user_is_admin(sender_user_ref).await {
		return Err!(Request(Forbidden(
			"You do not have permission to create users."
		)));
	}

	let workspace_id = body
		.workspace_id
		.map(|workspace_id| workspace_id.trim().to_owned());
	if workspace_id.as_ref().is_some_and(String::is_empty) {
		return Err!(Request(InvalidParam("workspace_id must not be empty.")));
	}

	if body.users.is_empty() {
		return Err!(Request(InvalidParam("users must not be empty.")));
	}

	let mut validation_errors = Vec::new();
	let mut seen_localparts = BTreeMap::<String, usize>::new();
	let mut validated_users = Vec::with_capacity(body.users.len());

	for (index, entry) in body.users.into_iter().enumerate() {
		let localpart = entry.localpart.trim().to_lowercase();
		if localpart.is_empty() {
			validation_errors.push(format!("users[{index}].localpart must not be empty."));
			continue;
		}

		if let Some(first_index) = seen_localparts.insert(localpart.clone(), index) {
			validation_errors.push(format!(
				"users[{index}].localpart duplicates users[{first_index}].localpart."
			));
			continue;
		}

		let user_id =
			match UserId::parse_with_server_name(&localpart, services.globals.server_name()) {
				Ok(user_id) => user_id,
				Err(e) => {
					validation_errors.push(format!("users[{index}].localpart is invalid: {e}"));
					continue;
				}
			};

		if let Err(e) = user_id.validate_strict()
			&& services.config.emergency_password.is_none()
		{
			validation_errors.push(format!(
				"users[{index}].localpart contains disallowed characters or spaces: {e}"
			));
			continue;
		}

		if services.users.exists(&user_id).await {
			validation_errors.push(format!(
				"users[{index}].localpart already exists as {user_id}."
			));
			continue;
		}

		let displayname = match entry.displayname {
			Some(displayname) => {
				let displayname = displayname.trim().to_owned();
				if displayname.is_empty() {
					validation_errors
						.push(format!("users[{index}].displayname must not be empty."));
					continue;
				}

				Some(displayname)
			}
			None => None,
		};

		let (password, generated_password) = match entry.password {
			Some(password) if password.trim().is_empty() => {
				validation_errors.push(format!("users[{index}].password must not be empty."));
				continue;
			}
			Some(password) => (password, None),
			None => {
				let password = utils::random_string(AUTO_GEN_PASSWORD_LENGTH);
				(password.clone(), Some(password))
			}
		};

		validated_users.push(ValidatedBulkCreateUser {
			index,
			user_id,
			localpart,
			displayname,
			password,
			generated_password,
		});
	}

	if !validation_errors.is_empty() {
		return Err!(Request(InvalidParam(
			"Bulk create validation failed: {}",
			validation_errors.join(" ")
		)));
	}

	let mut created_users = Vec::with_capacity(validated_users.len());
	for user in &validated_users {
		let user_id_ref: &UserId = user.user_id.as_ref();
		services
			.users
			.full_register(Register {
				user_id: Some(user_id_ref),
				password: Some(user.password.as_str()),
				grant_first_user_admin: true,
				displayname: user.displayname.as_deref(),
				..Default::default()
			})
			.await
			.map_err(|e| {
				err!(Request(InvalidParam(
					"Failed while creating users[{}] ({}) as {}: {e}",
					user.index,
					user.localpart,
					user.user_id
				)))
			})?;
	}

	if let Some(workspace_id) = workspace_id.as_deref() {
		for user in &validated_users {
			let user_id_ref: &UserId = user.user_id.as_ref();
			services
				.workspace
				.add_user_workspace(user_id_ref, workspace_id)
				.await;
		}
	}

	for user in validated_users {
		let user_id_ref: &UserId = user.user_id.as_ref();
		let displayname = services.profile.displayname(user_id_ref).await.ok();

		created_users.push(BulkCreatedUser {
			user_id: user.user_id,
			localpart: user.localpart,
			displayname,
			generated_password: user.generated_password,
		});
	}

	Ok(Json(BulkCreateUsersResponse {
		workspace_id,
		users: created_users,
	}))
}

/// # `PUT /_matrix/client/unstable/org.tuwunel/users/displaynames`
///
/// Bulk updates global display names for a list of local users. Admin-only.
pub(crate) async fn update_user_displaynames_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Json(body): Json<UpdateUserDisplaynamesBody>,
) -> Result<Json<UpdateUserDisplaynamesResponse>> {
	let (sender_user, ..) = services
		.users
		.find_from_token(token.token())
		.await
		.map_err(|_| {
			Error::BadRequest(
				ErrorKind::UnknownToken(ruma::api::error::UnknownTokenErrorData { soft_logout: false }),
				"Unknown access token.",
			)
		})?;

	let sender_user_ref: &UserId = sender_user.as_ref();
	if !services.admin.user_is_admin(sender_user_ref).await {
		return Err!(Request(Forbidden(
			"You do not have permission to update user display names."
		)));
	}

	let mut deduped = BTreeMap::<OwnedUserId, String>::new();
	for entry in body.users {
		let user_id_ref: &UserId = entry.user_id.as_ref();
		if !services.globals.user_is_local(user_id_ref) {
			return Err!(Request(InvalidParam("Only local users can be updated.")));
		}

		if !services.users.exists(user_id_ref).await {
			return Err!(Request(NotFound("User does not exist.")));
		}

		let displayname = entry.displayname.trim().to_owned();
		if displayname.is_empty() {
			return Err!(Request(InvalidParam("displayname must not be empty.")));
		}

		deduped.insert(entry.user_id, displayname);
	}

	let mut updated = Vec::with_capacity(deduped.len());
	for (user_id, displayname) in deduped {
		let user_id_ref: &UserId = user_id.as_ref();
		services
			.profile
			.set_displayname(
				user_id_ref,
				Some(displayname.as_str()),
				Some(tuwunel_service::profile::Propagation::All),
			)
			.await?;

		updated.push(UserDisplaynameUpdate {
			user_id,
			displayname,
		});
	}

	Ok(Json(UpdateUserDisplaynamesResponse { users: updated }))
}

/// # `DELETE /_matrix/client/unstable/org.tuwunel/users/{userId}`
///
/// Permanently removes recreatable account state for a local non-admin user.
/// Historical room events remain in the Matrix DAG under the same user ID.
pub(crate) async fn purge_user_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Path(target_user): Path<OwnedUserId>,
	Json(body): Json<PurgeUserBody>,
) -> Result<Json<PurgeUserResponse>> {
	let sender_user = authenticated_admin(&services, token.token()).await?;

	if body.confirm_user_id != target_user {
		return Err!(Request(InvalidParam(
			"confirm_user_id must exactly match the user_id in the path."
		)));
	}

	validate_purge_target(&services, &sender_user, &target_user).await?;
	Ok(Json(
		purge_validated_user(&services, &sender_user, target_user).await?,
	))
}

/// # `DELETE /_matrix/client/unstable/org.tuwunel/users`
///
/// Purges up to 100 local users. Validation is all-or-nothing; runtime purge
/// results are returned per user so failed items can be retried safely.
pub(crate) async fn bulk_purge_users_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(token)): TypedHeader<Authorization<Bearer>>,
	Json(body): Json<BulkPurgeUsersBody>,
) -> Result<Json<BulkPurgeUsersResponse>> {
	let sender_user = authenticated_admin(&services, token.token()).await?;
	if !body.confirm {
		return Err!(Request(InvalidParam("confirm must be true.")));
	}
	if body.user_ids.is_empty() {
		return Err!(Request(InvalidParam("user_ids must not be empty.")));
	}
	if body.user_ids.len() > 100 {
		return Err!(Request(InvalidParam(
			"user_ids must contain at most 100 users."
		)));
	}

	let mut targets = body.user_ids;
	targets.sort();
	targets.dedup();

	let mut validation_errors = Vec::new();
	for (index, target_user) in targets.iter().enumerate() {
		if let Err(error) = validate_purge_target(&services, &sender_user, target_user).await {
			validation_errors.push(format!("user_ids[{index}] ({target_user}): {error}"));
		}
	}
	if !validation_errors.is_empty() {
		return Err!(Request(InvalidParam(
			"Bulk purge validation failed: {}",
			validation_errors.join(" ")
		)));
	}

	let total = targets.len();
	let mut users = Vec::with_capacity(total);
	for target_user in targets {
		let user_id = target_user.clone();
		match purge_validated_user(&services, &sender_user, target_user).await {
			Ok(result) => users.push(BulkPurgeUserResult {
				user_id: result.user_id,
				purged: true,
				recreatable: result.recreatable,
				removed_records: Some(result.removed_records),
				error: None,
			}),
			Err(error) => {
				let recreatable = !services.users.exists(&user_id).await;
				users.push(BulkPurgeUserResult {
					user_id,
					purged: false,
					recreatable,
					removed_records: None,
					error: Some(error.to_string()),
				});
			}
		}
	}

	let purged = users.iter().filter(|result| result.purged).count();
	Ok(Json(BulkPurgeUsersResponse {
		total,
		purged,
		failed: total.saturating_sub(purged),
		historical_events_preserved: true,
		users,
	}))
}

async fn authenticated_admin(services: &Services, token: &str) -> Result<OwnedUserId> {
	let (sender_user, ..) = services.users.find_from_token(token).await.map_err(|_| {
		Error::BadRequest(
			ErrorKind::UnknownToken(ruma::api::error::UnknownTokenErrorData { soft_logout: false }),
			"Unknown access token.",
		)
	})?;
	if !services.admin.user_is_admin(&sender_user).await {
		return Err!(Request(Forbidden(
			"You do not have permission to purge users."
		)));
	}
	Ok(sender_user)
}

async fn validate_purge_target(
	services: &Services,
	sender_user: &UserId,
	target_user: &UserId,
) -> Result {
	if !services.globals.user_is_local(target_user) {
		return Err!(Request(InvalidParam("Only local users can be purged.")));
	}
	if target_user == sender_user {
		return Err!(Request(Forbidden(
			"An administrator cannot purge their own account."
		)));
	}
	if target_user.as_str() == services.globals.server_user.as_str() {
		return Err!(Request(Forbidden(
			"The server service account cannot be purged."
		)));
	}
	if !services.users.exists(target_user).await {
		return Err!(Request(NotFound("User does not exist.")));
	}
	if services.admin.user_is_admin(target_user).await {
		return Err!(Request(Forbidden(
			"Administrator accounts must have admin privileges revoked before purge."
		)));
	}
	Ok(())
}

async fn purge_validated_user(
	services: &Services,
	sender_user: &UserId,
	target_user: OwnedUserId,
) -> Result<PurgeUserResponse> {
	services.deactivate.full_deactivate(&target_user, false).await?;
	let removed_records = services.users.purge_account_records(&target_user).await;
	let recreatable = !services.users.exists(&target_user).await;
	if !recreatable {
		return Err!(Request(Unknown(
			"Account purge did not remove the user identity record."
		)));
	}

	info!(
		admin = %sender_user,
		target = %target_user,
		removed_records,
		"Administrator permanently purged a local user account"
	);

	Ok(PurgeUserResponse {
		user_id: target_user,
		purged: true,
		recreatable,
		historical_events_preserved: true,
		removed_records,
	})
}

struct ValidatedBulkCreateUser {
	index: usize,
	user_id: OwnedUserId,
	localpart: String,
	displayname: Option<String>,
	password: String,
	generated_password: Option<String>,
}
