mod configs;
mod copy;
mod debug;
mod delete;
mod differences;
mod duplicates;
mod list;
mod r#move;
mod providers;
mod show;
mod sync;

use tuwunel_core::Result;

use crate::admin_command_dispatch;

#[admin_command_dispatch(handler_prefix = "query_storage")]
#[derive(Debug, clap::Subcommand)]
pub(crate) enum StorageCommand {
	/// List provider configurations.
	Configs,

	/// List provider instances.
	Providers,

	Debug {
		/// Use configured provider by name.
		provider: String,
	},

	/// Show metadata for an object.
	Show {
		/// Use configured provider by name.
		#[arg(short, long)]
		provider: Option<String>,

		/// Path to the object.
		src: String,
	},

	/// List metadata for all objects.
	List {
		/// Use configured provider by name.
		#[arg(short, long)]
		provider: Option<String>,

		/// Optionally filter by matching prefix.
		prefix: Option<String>,
	},

	/// List objects duplicated between two providers
	Duplicates {
		/// The first provider name.
		src: String,

		/// The second provider name.
		dst: String,
	},

	/// List objects duplicated between two providers
	Differences {
		/// The first provider name.
		src: String,

		/// The second provider name.
		dst: String,
	},

	/// Copy an object from a source to a destination.
	Copy {
		/// Use configured provider by name.
		#[arg(short, long)]
		provider: Option<String>,

		/// Overwrite existing destination.
		#[arg(short, long)]
		force: bool,

		/// Path to the source.
		src: String,

		/// Path to the destination.
		dst: String,
	},

	/// Move an object from a source to a destination.
	Move {
		/// Use configured provider by name.
		#[arg(short, long)]
		provider: Option<String>,

		/// Overwrite existing destination.
		#[arg(short, long)]
		force: bool,

		/// Path to the source.
		src: String,

		/// Path to the destination.
		dst: String,
	},

	/// Delete an object at the specified location.
	Delete {
		/// Use configured provider by name.
		#[arg(short, long)]
		provider: Option<String>,

		/// Path to the location to delete. Multiple arguments allowed.
		src: Vec<String>,

		/// Report successful results in addition to failures.
		#[arg(short, long)]
		verbose: bool,
	},

	/// Copy objects from a source provider which do not exist on a
	/// destination provider, then report the number copied.
	Sync {
		/// Use source configured provider by name.
		src: String,

		/// Use destination configured provider by name.
		dst: String,
	},
}
