use std::collections::HashMap;

use syn::{Expr, ExprLit, Generics, Lit, Meta, MetaNameValue, parse_str};

use crate::Result;

pub(crate) fn get_crate_name() -> Option<String> {
	let cargo_crate_name = std::env::var("CARGO_CRATE_NAME");
	match cargo_crate_name.as_ref() {
		| Err(_) => None,
		| Ok(crate_name) => Some(
			crate_name
				.trim_start_matches("tuwunel_")
				.to_owned(),
		),
	}
}

pub(crate) fn get_simple_settings(args: &[Meta]) -> HashMap<String, String> {
	args.iter().fold(HashMap::new(), |mut map, arg| {
		let Meta::NameValue(MetaNameValue { path, value, .. }) = arg else {
			return map;
		};

		let Expr::Lit(ExprLit { lit: Lit::Str(str), .. }, ..) = value else {
			return map;
		};

		if let Some(key) = path
			.segments
			.iter()
			.next()
			.map(|s| s.ident.clone())
		{
			map.insert(key.to_string(), str.value());
		}

		map
	})
}

pub(crate) fn is_cargo_compile() -> bool {
	// True for `cargo build` (`--emit=link[,...]`) and for `cargo check` /
	// `cargo clippy` (`--emit=metadata[,...]`). Accepts both `--emit=foo` and
	// `--emit foo` forms.
	let args: Vec<String> = std::env::args().collect();
	let inline = args
		.iter()
		.filter_map(|a| a.strip_prefix("--emit="));

	let separated = args
		.windows(2)
		.filter_map(|pair| (pair[0] == "--emit").then_some(pair[1].as_str()));

	inline
		.chain(separated)
		.flat_map(|v| v.split(','))
		.any(|elem| elem == "link" || elem == "metadata")
}

pub(crate) fn is_cargo_test() -> bool { std::env::args().any(|flag| flag == "--test") }

pub(crate) fn get_named_generics(args: &[Meta], name: &str) -> Result<Generics> {
	const DEFAULT: &str = "<>";

	parse_str::<Generics>(&get_named_string(args, name).unwrap_or_else(|| DEFAULT.to_owned()))
}

pub(crate) fn get_named_string(args: &[Meta], name: &str) -> Option<String> {
	args.iter().find_map(|arg| {
		let value = arg.require_name_value().ok()?;
		let Expr::Lit(ref lit) = value.value else {
			return None;
		};
		let Lit::Str(ref str) = lit.lit else {
			return None;
		};
		value.path.is_ident(name).then_some(str.value())
	})
}

#[must_use]
pub(crate) fn camel_to_snake_string(s: &str) -> String {
	let mut output = String::with_capacity(
		s.chars()
			.fold(s.len(), |a, ch| a.saturating_add(usize::from(ch.is_ascii_uppercase()))),
	);

	let mut state = false;
	s.chars().for_each(|ch| {
		let m = ch.is_ascii_uppercase();
		let s = exchange(&mut state, !m);
		if m && s {
			output.push('_');
		}
		output.push(ch.to_ascii_lowercase());
	});

	output
}

#[inline]
pub(crate) fn exchange<T>(state: &mut T, source: T) -> T { std::mem::replace(state, source) }
