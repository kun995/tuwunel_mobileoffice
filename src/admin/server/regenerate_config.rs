use std::path::PathBuf;

use tuwunel_core::{
	Err, Result,
	config::{RegenerateOptions, RegenerationSummary, Sources, regenerate_config as regenerate},
	err, error,
};

use crate::admin_command;

#[admin_command]
pub(super) async fn regenerate_config(
	&self,
	path: PathBuf,
	force: bool,
	include_env: bool,
	strip_unknown: bool,
) -> Result {
	if !path.is_absolute() {
		return Err!("Configuration regeneration destination must be an absolute path.");
	}

	let sources = Sources {
		paths: self.services.server.config_sources.paths.clone(),
		overrides: None,
	};

	let regeneration = move || {
		let options = RegenerateOptions {
			output: Some(path.as_path()),
			force,
			include_env,
			strip_unknown,
		};

		regenerate(&sources, options)
	};

	let task = self
		.services
		.server
		.runtime()
		.spawn_blocking(regeneration);

	let summary = task
		.await
		.inspect_err(|error| error!(?error, "Configuration regeneration task failed"))
		.map_err(|_| err!("Configuration regeneration failed. Consult the server log."))?
		.inspect_err(|error| error!(?error, "Configuration regeneration failed"))
		.map_err(|_| err!("Configuration regeneration failed. Consult the server log."))?;

	let dropped = format_dropped_keys(&summary);
	let dropped = (!dropped.is_empty())
		.then_some(dropped.as_str())
		.unwrap_or("none");

	let warning = (summary.input_count() > 1)
		.then(|| {
			format!(
				"Warning: {} configuration files were collapsed in source order.\n",
				summary.input_count(),
			)
		})
		.unwrap_or_default();

	write!(
		self,
		"Configuration regenerated at `{}`.\n\n{warning}Configured values: {}\nResidue values: \
		 {}\nDropped startup-only migration controls (never emitted): {dropped}",
		summary.output().display(),
		summary.configured(),
		summary.residue(),
	)
	.await
}

fn format_dropped_keys(summary: &RegenerationSummary) -> String {
	let separator_bytes = summary
		.dropped_keys()
		.count()
		.saturating_sub(1)
		.saturating_mul(2);

	let capacity = summary
		.dropped_keys()
		.map(str::len)
		.sum::<usize>()
		.saturating_add(separator_bytes);

	summary
		.dropped_keys()
		.fold(String::with_capacity(capacity), |mut dropped, key| {
			if !dropped.is_empty() {
				dropped.push_str(", ");
			}

			dropped.push_str(key);
			dropped
		})
}
