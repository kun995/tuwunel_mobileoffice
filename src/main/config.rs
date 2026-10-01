//! Configuration file generation commands.
//!
//! This module handles standalone config operations before server startup.

use std::{
	io::{Write, stdout},
	path::Path,
};

use tuwunel_core::{
	Err, Result,
	config::{
		Overwrite, RegenerateOptions, example_config, regenerate_config, write_example_config,
	},
};

use crate::{args::Args, server::config_sources};

/// Runs a requested standalone configuration command.
///
/// Returns true after handling a command, leaving normal startup to the caller
/// otherwise.
pub fn run(args: &Args) -> Result<bool> {
	match (&args.generate_config, &args.regenerate_config) {
		| (None, None) => Ok(false),
		| (Some(output), None) => generate(output.as_deref(), args.force).map(|()| true),
		| (None, Some(output)) => regenerate(args, output.as_deref()).map(|()| true),
		| (Some(_), Some(_)) => Err!("Configuration generation commands cannot be combined."),
	}
}

fn generate(output: Option<&Path>, force: bool) -> Result {
	if output.is_none() && force {
		return Err!("Force requires a generated configuration file path.");
	}

	match output {
		| None => {
			let example = example_config()?;
			let stdout = stdout();
			let mut out = stdout.lock();

			out.write_all(example.as_bytes())?;
		},
		| Some(output) => {
			let overwrite = force
				.then_some(Overwrite::Allow)
				.unwrap_or(Overwrite::Deny);

			write_example_config(output, overwrite)?;

			let stdout = stdout();
			let mut out = stdout.lock();

			writeln!(out, "Wrote example configuration to {}.", output.display())?;
		},
	}

	Ok(())
}

fn regenerate(args: &Args, output: Option<&Path>) -> Result {
	let sources = config_sources(args);
	let options = RegenerateOptions {
		output,
		force: args.force,
		include_env: args.include_env,
		strip_unknown: args.strip_unknown,
	};

	let summary = regenerate_config(&sources, options)?;
	let stdout = stdout();
	let mut out = stdout.lock();

	if summary.input_count() > 1 {
		writeln!(
			out,
			"Warning: collapsed {} configuration files in source order.",
			summary.input_count(),
		)?;
	}

	writeln!(
		out,
		"Regenerated {} with {} configured values and {} residue values.",
		summary.output().display(),
		summary.configured(),
		summary.residue(),
	)?;

	for key in summary.dropped_keys() {
		writeln!(out, "Dropped key {key}: startup-only migration controls are never emitted.")?;
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use std::{
		env::temp_dir,
		ffi::OsString,
		fs::{create_dir_all, read_to_string, remove_dir_all, write},
		path::Path,
		process::id,
		sync::atomic::{AtomicU64, Ordering},
	};

	use clap::Parser;

	use super::{Args, run};

	static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

	#[test]
	fn no_config_command_leaves_startup_unhandled() {
		let args = Args::parse_from(["tuwunel"]);

		assert!(!run(&args).expect("no command is accepted"));
	}

	#[test]
	fn conflicting_programmatic_commands_return_an_error() {
		let mut args = Args::parse_from(["tuwunel".into(), long("generate-config")]);

		args.regenerate_config = Some(None);

		run(&args).expect_err("conflicting commands rejected");
	}

	fn long(name: &str) -> OsString {
		let mut argument = OsString::with_capacity(name.len().saturating_add(2));

		argument.push("-");
		argument.push("-");
		argument.push(name);

		argument
	}

	#[test]
	fn stdout_generation_rejects_force() {
		let args = Args::parse_from(["tuwunel".into(), long("generate-config"), long("force")]);

		run(&args).expect_err("force without a generation path rejected");
	}

	#[test]
	fn option_overrides_do_not_contaminate_regeneration() {
		let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
		let temp = temp_dir().join(format!("tuwunel-config-command-{}-{sequence}", id()));
		let input = temp.join("input.toml");
		let output = temp.join("output.toml");

		create_dir_all(&temp).expect("temporary directory created");
		write(&input, "[global]\nserver_name = \"override.example\"\nlistening = true\n")
			.expect("configuration written");

		let regenerate = long_path("regenerate-config", &output);
		let argv: [OsString; 6] = [
			"tuwunel".into(),
			"-c".into(),
			input.into_os_string(),
			"-O".into(),
			"listening=false".into(),
			regenerate,
		];

		let args = Args::parse_from(argv);

		run(&args).expect("configuration regenerated");

		let rendered = read_to_string(output).expect("regenerated configuration read");

		assert!(
			rendered
				.lines()
				.any(|line| line == "listening = true")
		);

		assert!(
			!rendered
				.lines()
				.any(|line| line == "listening = false")
		);

		remove_dir_all(temp).expect("temporary directory removed");
	}

	#[test]
	fn ioffice_generate_config_includes_fork_sections() {
		let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
		let temp = temp_dir().join(format!("tuwunel-config-generate-{}-{sequence}", id()));
		let output = temp.join("example.toml");
		create_dir_all(&temp).expect("temporary directory created");
		let args = Args::parse_from(["tuwunel".into(), long_path("generate-config", &output)]);
		assert!(run(&args).expect("configuration generated"));
		let rendered = read_to_string(output).expect("generated configuration read");
		for section in [
			"global.media_storage",
			"global.media_storage.s3",
			"global.media_storage.hybrid",
			"global.push_media_preview",
		] {
			assert!(rendered.lines().any(|line| line == format!("#[{section}]")), "missing {section}");
		}
		remove_dir_all(temp).expect("temporary directory removed");
	}

	fn long_path(name: &str, path: &Path) -> OsString {
		let mut argument = long(name);

		argument.push("=");
		argument.push(path);

		argument
	}
}
