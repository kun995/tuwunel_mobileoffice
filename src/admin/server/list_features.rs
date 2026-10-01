use tuwunel_core::{Result, info};

use crate::admin_command;

#[admin_command]
pub(super) async fn list_features(&self, available: bool, enabled: bool, comma: bool) -> Result {
	let delim = if comma { "," } else { " " };
	if enabled && !available {
		let features = info::rustc::features().join(delim);
		return write!(self, "`\n{features}\n`").await;
	}

	if available && !enabled {
		let features = info::cargo::features().join(delim);
		return write!(self, "`\n{features}\n`").await;
	}

	let enabled = info::rustc::features();
	let available = info::cargo::features();
	for feature in available {
		let active = enabled.contains(&feature.as_str());
		let emoji = if active { "✅" } else { "❌" };
		let remark = if active { "[enabled]" } else { "" };
		writeln!(self, "{emoji} {feature} {remark}").await?;
	}

	Ok(())
}
