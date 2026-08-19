use std::str::FromStr;

use jwt::{Algorithm, DecodingKey, Validation, decode};
use ruma::{
	OwnedUserId, UserId,
	api::client::session::login::v3::{Request, Token},
};
use serde::Deserialize;
use tuwunel_core::{Err, Result, at, config::JwtConfig, debug, err, jwt, warn};
use tuwunel_service::Services;

use crate::Ruma;

#[derive(Debug, Deserialize)]
struct Claim {
	/// Subject is the localpart of the User MXID
	sub: String,
	preferred_username: Option<String>,
}

pub(super) async fn handle_login(
	services: &Services,
	_body: &Ruma<Request>,
	info: &Token,
) -> Result<OwnedUserId> {
	let user_id = resolve_login_user(services, &info.token).await?;
	if !services.users.exists(&user_id).await {
		let config = &services.config.jwt;
		if !config.register_user {
			return Err!(Request(NotFound("User {user_id} is not registered on this server.")));
		}

		services
			.users
			.create(&user_id, Some("*"), Some("jwt"))
			.await?;
	}

	Ok(user_id)
}

pub(crate) async fn validate_user(services: &Services, token: &str) -> Result<OwnedUserId> {
	let (_, candidates) = validate_user_candidates(services, token)?;

	if let Some(user_id) = find_existing_candidate(services, &candidates).await {
		return Ok(user_id);
	}

	Err!(Request(NotFound("JWT identity is not mapped to an existing Matrix account.")))
}

async fn resolve_login_user(services: &Services, token: &str) -> Result<OwnedUserId> {
	let (raw_is_email, candidates) = validate_user_candidates(services, token)?;

	if let Some(user_id) = find_existing_candidate(services, &candidates).await {
		return Ok(user_id);
	}

	let user_id = if raw_is_email {
		candidates.last()
	} else {
		candidates.first()
	}
	.cloned()
	.ok_or_else(|| {
		err!(Request(InvalidUsername("JWT subject is empty after localpart normalization.")))
	})?;

	Ok(user_id)
}

fn validate_user_candidates(
	services: &Services,
	token: &str,
) -> Result<(bool, Vec<OwnedUserId>)> {
	let config = &services.config.jwt;
	if !config.enable {
		return Err!(Request(Unauthorized("JWT login is not enabled.")));
	}

	let claim = validate(config, token)?;
	jwt_user_id_candidates(services, &claim)
}

async fn find_existing_candidate(
	services: &Services,
	candidates: &[OwnedUserId],
) -> Option<OwnedUserId> {
	for user_id in candidates {
		if services.users.exists(user_id).await {
			return Some(user_id.clone());
		}
	}

	None
}

fn jwt_user_id_candidates(
	services: &Services,
	claim: &Claim,
) -> Result<(bool, Vec<OwnedUserId>)> {
	let raw_local = claim
		.preferred_username
		.as_deref()
		.filter(|preferred_username| !preferred_username.trim().is_empty())
		.unwrap_or(&claim.sub);
	let raw_is_email = raw_local.contains('@');

	let mut localparts = Vec::new();
	push_candidate_localpart(&mut localparts, raw_local.trim().to_lowercase());

	if let Some(normalized) = normalize_jwt_localpart(raw_local) {
		push_candidate_localpart(&mut localparts, normalized);
	}

	let server = &services.server.name;
	let mut user_ids = Vec::with_capacity(localparts.len());
	let mut last_error = None;

	for localpart in localparts {
		match UserId::parse_with_server_name(localpart, server) {
			| Ok(user_id) => user_ids.push(user_id),
			| Err(e) => last_error = Some(e),
		}
	}

	if user_ids.is_empty() {
		if let Some(e) = last_error {
			return Err!(Request(InvalidUsername("JWT subject is not a valid user MXID: {e}")));
		}
	}

	Ok((raw_is_email, user_ids))
}

fn push_candidate_localpart(localparts: &mut Vec<String>, localpart: String) {
	if !localpart.is_empty()
		&& !localparts
			.iter()
			.any(|candidate| candidate == &localpart)
	{
		localparts.push(localpart);
	}
}

fn normalize_jwt_localpart(value: &str) -> Option<String> {
	let value = value.trim();
	let localpart = value
		.split_once('@')
		.map_or(value, |(localpart, _)| localpart)
		.trim()
		.to_lowercase();

	(!localpart.is_empty()).then_some(localpart)
}

fn validate(config: &JwtConfig, token: &str) -> Result<Claim> {
	let verifier = init_verifier(config)?;
	let validator = init_validator(config)?;
	decode::<Claim>(token, &verifier, &validator)
		.map(|decoded| (decoded.header, decoded.claims))
		.inspect(|(head, claim)| debug!(?head, ?claim, "JWT token decoded"))
		.map_err(|e| err!(Request(Forbidden("Invalid JWT token: {e}"))))
		.map(at!(1))
}

fn init_verifier(config: &JwtConfig) -> Result<DecodingKey> {
	let key = &config.key;
	let format = config.format.to_uppercase();

	Ok(match format.as_str() {
		| "HMAC" => DecodingKey::from_secret(key.as_bytes()),

		| "HMACB64" => DecodingKey::from_base64_secret(key.as_str())
			.map_err(|e| err!(Config("jwt.key", "JWT key is not valid base64: {e}")))?,

		| "ECDSA" => DecodingKey::from_ec_pem(key.as_bytes())
			.map_err(|e| err!(Config("jwt.key", "JWT key is not valid ECDSA PEM: {e}")))?,

		| "EDDSA" => DecodingKey::from_ed_pem(key.as_bytes())
			.map_err(|e| err!(Config("jwt.key", "JWT key is not valid EDDSA PEM: {e}")))?,
		| "RSA" => DecodingKey::from_rsa_pem(key.as_bytes())
			.map_err(|e| err!(Config("jwt.key", "JWT key is not valid RSA PEM: {e}")))?,

		| _ => return Err!(Config("jwt.format", "Key format {format:?} is not supported.")),
	})
}

fn init_validator(config: &JwtConfig) -> Result<Validation> {
	let alg = config.algorithm.as_str();
	let alg = Algorithm::from_str(alg).map_err(|e| {
		err!(Config("jwt.algorithm", "JWT algorithm is not recognized or configured {e}"))
	})?;

	let mut validator = Validation::new(alg);
	let mut required_spec_claims: Vec<_> = ["sub"].into();

	validator.validate_exp = config.validate_exp;
	if config.require_exp {
		required_spec_claims.push("exp");
	}

	validator.validate_nbf = config.validate_nbf;
	if config.require_nbf {
		required_spec_claims.push("nbf");
	}

	if !config.audience.is_empty() {
		required_spec_claims.push("aud");
		validator.set_audience(&config.audience);
	}

	if !config.issuer.is_empty() {
		required_spec_claims.push("iss");
		validator.set_issuer(&config.issuer);
	}

	if cfg!(debug_assertions) && !config.validate_signature {
		warn!("JWT signature validation is disabled!");
		validator.insecure_disable_signature_validation();
	}

	validator.set_required_spec_claims(&required_spec_claims);
	debug!(?validator, "JWT configured");

	Ok(validator)
}

#[cfg(test)]
mod tests {
	use super::normalize_jwt_localpart;

	#[test]
	fn strips_email_domain_from_jwt_localpart() {
		assert_eq!(
			normalize_jwt_localpart("Trieuhonganhsmail@gmail.com").as_deref(),
			Some("trieuhonganhsmail")
		);
	}

	#[test]
	fn keeps_non_email_jwt_localpart() {
		assert_eq!(normalize_jwt_localpart(" 0940035066 ").as_deref(), Some("0940035066"));
	}

	#[test]
	fn rejects_empty_jwt_localpart_after_normalization() {
		assert_eq!(normalize_jwt_localpart("@gmail.com"), None);
		assert_eq!(normalize_jwt_localpart("   "), None);
	}
}
