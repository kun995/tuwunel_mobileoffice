mod failure;
mod panic;
mod tester;
mod timer;

use tuwunel_core::Result;

use crate::admin_command_dispatch;

#[admin_command_dispatch]
#[derive(Debug, clap::Subcommand)]
pub(crate) enum TesterCommand {
	Panic,
	Failure,
	Tester,
	Timer,
}
