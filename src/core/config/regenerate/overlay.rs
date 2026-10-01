use std::sync::LazyLock;

use figment::{
	Figment, Profile, Provider, Source,
	value::{Dict, Value},
};
use itertools::Itertools as _;
use regex::RegexSet;

use super::super::KNOWN_KEYS;
use crate::{Err, Result, err};

static NONCONFIG_KEYS: LazyLock<Result<RegexSet, regex::Error>> =
	LazyLock::new(|| RegexSet::new(KNOWN_KEYS));

pub(super) fn validate_file_overlay(figment: &Figment) -> Result {
	let profiles = Provider::data(figment)?;
	let unsupported = profiles
		.iter()
		.filter(|(profile, values)| {
			!values.is_empty() && **profile != Profile::Default && **profile != Profile::Global
		})
		.map(|(profile, _)| profile.as_str())
		.join(", ");

	if !unsupported.is_empty() {
		return Err!("Unsupported configuration profiles: {unsupported}.");
	}

	let valid = profiles
		.values()
		.flat_map(Dict::values)
		.all(|value| file_tree(figment, value));

	if !valid {
		return Err!("The regeneration overlay contains a value not sourced from a file.");
	}

	Ok(())
}

fn file_tree(figment: &Figment, value: &Value) -> bool {
	match value {
		| Value::Dict(_, values) => values
			.values()
			.all(|value| file_tree(figment, value)),
		| Value::Array(_, values) => values
			.iter()
			.all(|value| file_tree(figment, value)),
		| _ => is_file_value(figment, value),
	}
}

pub(super) fn is_file_value(figment: &Figment, value: &Value) -> bool {
	figment
		.get_metadata(value.tag())
		.and_then(|metadata| metadata.source.as_ref())
		.and_then(Source::file_path)
		.is_some()
}

pub(super) fn filter_nonconfig_environment(figment: &Figment, values: &mut Dict) -> Result {
	let known = NONCONFIG_KEYS
		.as_ref()
		.map_err(|error| err!("Invalid known configuration key expression: {error}"))?;

	values.retain(|key, value| !known.is_match(key) || is_file_value(figment, value));

	Ok(())
}
