use std::str::FromStr;

use serde::Serialize;
use tuwunel_core::{
	Err, Result, err,
	jwt::{Algorithm, EncodingKey, Header, encode},
	utils::time::now_secs,
};

use crate::admin_command;

#[admin_command]
pub(super) async fn create_jwt(
	&self,
	user: String,
	exp_from_now: Option<u64>,
	nbf_from_now: Option<u64>,
	issuer: Option<String>,
	audience: Option<String>,
) -> Result {
	#[derive(Serialize)]
	struct Claim {
		sub: String,
		iss: Option<String>,
		aud: Option<String>,
		exp: Option<usize>,
		nbf: Option<usize>,
	}

	let config = &self.services.config.jwt;
	if config.format.as_str() != "HMAC" {
		return Err!("This command only supports HMAC key format, not {}.", config.format);
	}

	let key = EncodingKey::from_secret(config.key.as_ref());
	let alg = Algorithm::from_str(config.algorithm.as_str()).map_err(|e| {
		err!(Config("jwt.algorithm", "JWT algorithm is not recognized or configured {e}"))
	})?;

	let header = Header { alg, ..Default::default() };
	let claim = Claim {
		sub: user,

		iss: issuer,

		aud: audience,

		exp: exp_from_now
			.and_then(|val| now_secs().checked_add(val))
			.map(TryInto::try_into)
			.and_then(Result::ok),

		nbf: nbf_from_now
			.and_then(|val| now_secs().checked_add(val))
			.map(TryInto::try_into)
			.and_then(Result::ok),
	};

	encode(&header, &claim, &key)
		.map_err(|e| err!("Failed to encode JWT: {e}"))
		.map(async |token| self.write_str(&token).await)?
		.await
}
