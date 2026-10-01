use std::{
	env::temp_dir,
	fs::{remove_file, write},
	iter::{empty, once},
	path::PathBuf,
	process::id,
};

use super::{Figment, Sources};
use crate::Err;

// Not a real option, so an exported TUWUNEL_ variable cannot address it.
const PROBE: &str = "reload_probe";

#[test]
fn default_sources_apply_nothing() {
	let raw = Figment::new().merge((PROBE, "example.com"));
	let applied = Sources::default().apply(raw).expect("applies");

	assert_eq!(probe(&applied).as_deref(), Some("example.com"));
}

#[test]
fn overrides_are_applied() {
	let sources = Sources {
		overrides: Some(Box::new(|raw: Figment| Ok(raw.merge((PROBE, "override.example"))))),
		..Default::default()
	};

	let raw = Figment::new().merge((PROBE, "example.com"));
	let applied = sources.apply(raw).expect("applies");

	assert_eq!(probe(&applied).as_deref(), Some("override.example"));
}

#[test]
fn a_rejected_override_fails_the_load() {
	let sources = Sources {
		overrides: Some(Box::new(|_| Err!("rejected"))),
		..Default::default()
	};

	sources
		.apply(Figment::new())
		.expect_err("the override rejects it");
}

#[test]
fn retained_paths_are_reread() {
	let path = config_file("tuwunel_sources_retained", "retained.example");
	let sources = Sources {
		paths: vec![path.clone()],
		..Default::default()
	};

	let raw = sources.load(empty()).expect("loads");

	assert_eq!(probe(&raw).as_deref(), Some("retained.example"));

	remove_file(&path).expect("temp config removed");
}

#[test]
fn an_extra_path_layers_over_the_retained_ones() {
	let retained = config_file("tuwunel_sources_base", "retained.example");
	let extra = config_file("tuwunel_sources_extra", "extra.example");
	let sources = Sources {
		paths: vec![retained.clone()],
		..Default::default()
	};

	let raw = sources
		.load(once(extra.as_path()))
		.expect("loads");

	assert_eq!(probe(&raw).as_deref(), Some("extra.example"));

	remove_file(&retained).expect("temp config removed");
	remove_file(&extra).expect("temp config removed");
}

#[test]
fn mixed_profile_styles_preserve_layer_order() {
	let global = config_file("tuwunel_sources_profiled", "global.example");
	let headerless = headerless_config_file("tuwunel_sources_headerless", "headerless.example");

	let global_then_headerless = Sources {
		paths: vec![global.clone(), headerless.clone()],
		..Default::default()
	};

	let headerless_then_global = Sources {
		paths: vec![headerless.clone(), global.clone()],
		..Default::default()
	};

	let raw = global_then_headerless
		.load(empty())
		.expect("loads");

	assert_eq!(probe(&raw).as_deref(), Some("headerless.example"));

	let raw = headerless_then_global
		.load(empty())
		.expect("loads");

	assert_eq!(probe(&raw).as_deref(), Some("global.example"));

	remove_file(&global).expect("temp config removed");
	remove_file(&headerless).expect("temp config removed");
}

fn probe(raw: &Figment) -> Option<String> { raw.find_value(PROBE).ok()?.into_string() }

fn config_file(name: &str, value: &str) -> PathBuf {
	write_config_file(name, value, "[global]\n")
}

fn headerless_config_file(name: &str, value: &str) -> PathBuf {
	write_config_file(name, value, "")
}

fn write_config_file(name: &str, value: &str, prefix: &str) -> PathBuf {
	// Uniquified so a concurrent run cannot unlink the file mid-load.
	let filename = format!("{name}.{}.toml", id());
	let path = temp_dir().join(filename);
	let document = format!("{prefix}{PROBE} = \"{value}\"\n");

	write(&path, document).expect("temp config written");

	path
}
