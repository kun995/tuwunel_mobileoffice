use std::{collections::BTreeMap, fmt};

use axum::extract::State;
use ruma::{
	OwnedUserId, UserId,
	api::client::keys::upload_signatures::v3::{Failure, FailureErrorCode, Request, Response},
};
use serde::{
	Deserialize, Deserializer,
	de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor},
};
use serde_json::{error::Category, json, value::RawValue};
use tuwunel_core::{
	Error, Result, debug, err,
	smallvec::SmallVec,
	utils::{IterStream, ReadyExt, stream::BroadbandExt},
	warn,
};
use tuwunel_service::Services;

use crate::Ruma;

type Failures = BTreeMap<OwnedUserId, BTreeMap<String, Failure>>;
type Rejected = (FailureErrorCode, String);
type Signatures = SmallVec<[(String, String); 1]>;

struct ObjectField<'a>(&'a str);

struct ObjectFieldVisitor<'a>(&'a str);

struct SignaturePairs(Signatures);

struct SignaturePairsVisitor;

/// Uploads end-to-end key signatures from the sender user.
///
/// `POST /_matrix/client/r0/keys/signatures/upload`
pub(crate) async fn upload_signatures_route(
	State(services): State<crate::State>,
	body: Ruma<Request>,
) -> Result<Response> {
	let sender_user = body.sender_user();

	if body.signed_keys.is_empty() {
		debug!("Empty signed_keys sent in key signature upload");
		return Ok(Response::new());
	}

	let failures = body
		.signed_keys
		.iter()
		.flat_map(|(user_id, keys)| {
			keys.iter()
				.map(move |(key_id, key)| (user_id.as_ref(), key_id, key))
		})
		.stream()
		.broad_filter_map(async |(user_id, key_id, key)| {
			sign_key(&services, sender_user, user_id, key_id, key)
				.await
				.err()
				.map(|rejected| (user_id, key_id, rejected))
		})
		.ready_fold(
			Ok(Failures::new()),
			|failures: Result<Failures>, (user_id, key_id, rejected)| {
				let mut failures = failures?;
				let failure = failure(rejected)?;

				failures
					.entry(user_id.to_owned())
					.or_default()
					.insert(key_id.to_owned(), failure);

				Ok(failures)
			},
		)
		.await?;

	Ok(Response { failures })
}

async fn sign_key(
	services: &Services,
	sender_user: &UserId,
	user_id: &UserId,
	key_id: &str,
	key: &RawValue,
) -> Result<(), Rejected> {
	let signatures = signatures_from_key(sender_user, key)
		.map_err(|error| (FailureErrorCode::InvalidSignature, error))?;

	services
		.users
		.sign_key(user_id, key_id, signatures, sender_user)
		.await
		.map_err(|error| match error {
			| Error::Signatures(_) => (
				FailureErrorCode::InvalidSignature,
				"Signature does not verify against the stored key.".to_owned(),
			),
			| error => {
				if !matches!(&error, Error::Request(..)) {
					warn!(?error, "Failed to upload key signature");
				}

				(FailureErrorCode::from(error.kind().to_string()), error.sanitized_message())
			},
		})
}

fn signatures_from_key(sender_user: &UserId, key: &RawValue) -> Result<Signatures, String> {
	let signatures = object_field(key.get(), "signatures")
		.map_err(|error| {
			if matches!(error.classify(), Category::Data) {
				String::from("The signed key must be an object.")
			} else {
				format!("Invalid signed key JSON: {error}")
			}
		})?
		.ok_or_else(|| String::from("No signature from the uploading user."))?;

	let signatures = object_field(signatures.get(), sender_user.as_str())
		.map_err(|_| String::from("The signatures field must be an object."))?
		.ok_or_else(|| String::from("No signature from the uploading user."))?;

	if !signatures.get().trim_start().starts_with('{') {
		return Err(String::from("Signatures from the uploading user must be an object."));
	}

	let SignaturePairs(signatures) = serde_json::from_str(signatures.get())
		.map_err(|_| String::from("Signature values must be strings."))?;

	(!signatures.is_empty())
		.then_some(signatures)
		.ok_or_else(|| String::from("No signature from the uploading user."))
}

fn failure((errcode, error): Rejected) -> Result<Failure> {
	serde_json::from_value(json!({ "errcode": errcode, "error": error }))
		.map_err(|error| err!(SerdeDe("Failed to encode signature upload failure: {error}")))
}

fn object_field<'de>(json: &'de str, field: &str) -> serde_json::Result<Option<&'de RawValue>> {
	let mut deserializer = serde_json::Deserializer::from_str(json);
	let value = ObjectField(field).deserialize(&mut deserializer)?;

	deserializer.end()?;
	Ok(value)
}

impl<'de> DeserializeSeed<'de> for ObjectField<'_> {
	type Value = Option<&'de RawValue>;

	fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
	where
		D: Deserializer<'de>,
	{
		deserializer.deserialize_map(ObjectFieldVisitor(self.0))
	}
}

impl<'de> Visitor<'de> for ObjectFieldVisitor<'_> {
	type Value = Option<&'de RawValue>;

	fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str("a JSON object")
	}

	fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
	where
		A: MapAccess<'de>,
	{
		let mut value = None;

		while let Some(name) = map.next_key::<String>()? {
			if name == self.0 {
				value = Some(map.next_value()?);
			} else {
				map.next_value::<IgnoredAny>()?;
			}
		}

		Ok(value)
	}
}

impl<'de> Deserialize<'de> for SignaturePairs {
	fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		deserializer.deserialize_map(SignaturePairsVisitor)
	}
}

impl<'de> Visitor<'de> for SignaturePairsVisitor {
	type Value = SignaturePairs;

	fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str("an object containing string signatures")
	}

	fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
	where
		A: MapAccess<'de>,
	{
		let mut signatures = Signatures::new();

		while let Some((key_id, signature)) = map.next_entry::<String, String>()? {
			if let Some(index) = signatures
				.iter()
				.position(|(existing, _)| existing == &key_id)
			{
				signatures[index].1 = signature;
			} else {
				signatures.push((key_id, signature));
			}
		}

		Ok(SignaturePairs(signatures))
	}
}

#[cfg(test)]
mod tests {
	use ruma::user_id;
	use serde_json::value::to_raw_value;

	use super::*;

	#[test]
	fn extracts_only_uploading_users_signatures() {
		let sender_user = user_id!("@alice:example.com");
		let key = to_raw_value(&json!({
			"signatures": {
				"@alice:example.com": { "ed25519:ALICE": "alice-signature" },
				"@bob:example.com": { "ed25519:BOB": "bob-signature" },
			},
		}))
		.expect("signed key should serialize");

		let signatures = signatures_from_key(sender_user, &key)
			.expect("the uploading user's signature should be extracted");

		assert_eq!(signatures.as_slice(), &[(
			"ed25519:ALICE".to_owned(),
			"alice-signature".to_owned()
		)]);
	}

	#[test]
	fn rejects_empty_uploading_user_signatures() {
		let sender_user = user_id!("@alice:example.com");
		let key = to_raw_value(&json!({ "signatures": { "@alice:example.com": {} } }))
			.expect("signed key should serialize");

		let error = signatures_from_key(sender_user, &key)
			.expect_err("an empty signature map should be rejected");

		assert_eq!(error, "No signature from the uploading user.");
	}

	#[test]
	fn rejects_missing_uploading_user_signatures() {
		let sender_user = user_id!("@alice:example.com");
		let key = to_raw_value(&json!({ "user_id": "@alice:example.com" }))
			.expect("signed key should serialize");

		let error = signatures_from_key(sender_user, &key)
			.expect_err("a missing signature map should be rejected");

		assert_eq!(error, "No signature from the uploading user.");
	}

	#[test]
	fn rejects_non_object_signed_key() {
		let sender_user = user_id!("@alice:example.com");
		let key = to_raw_value(&json!([])).expect("signed key should serialize");

		let error = signatures_from_key(sender_user, &key)
			.expect_err("a non-object signed key should be rejected");

		assert_eq!(error, "The signed key must be an object.");
	}

	#[test]
	fn rejects_non_string_signature() {
		let sender_user = user_id!("@alice:example.com");
		let key = to_raw_value(&json!({
			"signatures": { "@alice:example.com": { "ed25519:ALICE": 7 } },
		}))
		.expect("signed key should serialize");

		let error = signatures_from_key(sender_user, &key)
			.expect_err("a non-string signature should be rejected");

		assert_eq!(error, "Signature values must be strings.");
	}

	#[test]
	fn serializes_typed_failure() {
		let failure = failure((
			FailureErrorCode::InvalidSignature,
			"Signature does not verify.".to_owned(),
		))
		.expect("failure should deserialize");

		let failure = serde_json::to_value(failure).expect("failure should serialize");

		assert_eq!(failure["errcode"], "M_INVALID_SIGNATURE");
		assert_eq!(failure["error"], "Signature does not verify.");
	}
}
