use std::path::Path;

use futures::{Stream, TryFutureExt, TryStreamExt, future::try_join3};
use tokio::{
	fs::{self, OpenOptions},
	io::AsyncWriteExt,
};
use tuwunel_core::Result;

use crate::admin_command;

#[admin_command]
pub(super) async fn dump_pdus(&self, dir: String) -> Result {
	let dir_path = Path::new(&dir);
	fs::create_dir_all(dir_path).await?;

	let normal = dumper(dir_path, "normal", self.services.timeline.pdus_raw());
	let outlier = dumper(dir_path, "outliers", self.services.timeline.outlier_pdus_raw());
	let retained = dumper(dir_path, "retaineds", self.services.retention.retained_pdus_raw());
	try_join3(normal, outlier, retained).await?;

	Ok(())
}

async fn dumper<'a, S>(dir: &Path, name: &str, stream: S) -> Result
where
	S: Stream<Item = Result<&'a [u8]>> + Send,
{
	let mut fopts = OpenOptions::new();
	fopts.write(true);
	fopts.create(true);
	fopts.truncate(true);

	let path = dir.join(name);
	let file = fopts.open(path).await?;
	stream
		.try_fold(file, async |mut file, data| {
			file.write_all(data).await?;

			Ok(file)
		})
		.and_then(async |mut file| Ok(file.shutdown().await?))
		.await
}
