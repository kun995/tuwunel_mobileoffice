pub mod blurhash;
mod data;
pub(super) mod migrations;
mod preview;
mod remote;
pub mod storage;
mod tests;
mod thumbnail;
use std::{path::PathBuf, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use ruma::{Mxc, OwnedMxcUri, UserId, http_headers::ContentDisposition};
use sha2::Digest;
use tokio::fs;
use tokio::sync::OnceCell;
use tuwunel_core::{
	Err, Result, debug, debug_error, debug_info, debug_warn, err, error, trace,
	utils::{self, MutexMap},
	warn,
};

use self::data::{Data, Metadata};
pub use self::thumbnail::Dim;

#[derive(Debug)]
pub struct FileMeta {
	pub content: Option<Vec<u8>>,
	pub content_type: Option<String>,
	pub content_disposition: Option<ContentDisposition>,
}

pub struct Service {
	url_preview_mutex: MutexMap<String, ()>,
	pub(super) db: Data,
	storage: Arc<OnceCell<Arc<dyn storage::MediaStorage>>>,
	services: Arc<crate::services::OnceServices>,
}

/// generated MXC ID (`media-id`) length
pub const MXC_LENGTH: usize = 32;

/// Cache control for immutable objects.
pub const CACHE_CONTROL_IMMUTABLE: &str = "private,max-age=31536000,immutable";

/// Default cross-origin resource policy.
pub const CORP_CROSS_ORIGIN: &str = "cross-origin";

#[async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			url_preview_mutex: MutexMap::new(),
			db: Data::new(args.db),
			storage: Arc::new(OnceCell::new()),
			services: args.services.clone(),
		}))
	}

	async fn worker(self: Arc<Self>) -> Result {
		// Initialize storage (lazy init)
		let storage = Self::build_storage(&self.services.server.config).await?;
		self.storage
			.set(storage)
			.map_err(|_| err!(Config("media_storage", "Storage already initialized")))?;

		self.create_media_dir().await?;

		Ok(())
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	/// Build storage backend based on configuration
	async fn build_storage(config: &tuwunel_core::config::Config) -> Result<Arc<dyn storage::MediaStorage>> {
		use tuwunel_core::config::StorageStrategy;

		let media_path = config.database_path.join("media");

		match config.media_storage.strategy {
			StorageStrategy::Filesystem => {
				debug!("Initializing Filesystem storage");
				Ok(Arc::new(storage::filesystem::FilesystemStorage::new(
					media_path,
				)?))
			},

			#[cfg(feature = "s3_storage")]
			StorageStrategy::S3 => {
				debug!("Initializing S3 storage");
				let s3_config = config
					.media_storage
					.s3
					.as_ref()
					.ok_or_else(|| err!(Config("media_storage.s3", "S3 configuration required for S3 storage strategy")))?;
				let s3 = storage::s3::S3Storage::new(s3_config).await?;
				let s3: Arc<dyn storage::MediaStorage> = Arc::new(s3);
				Ok(s3)
			},

			#[cfg(feature = "s3_storage")]
			StorageStrategy::HybridS3Primary => {
				debug!("Initializing Hybrid S3 Primary storage");
				let fs = Arc::new(storage::filesystem::FilesystemStorage::new(media_path)?);
				let s3_config = config
					.media_storage
					.s3
					.as_ref()
					.ok_or_else(|| err!(Config("media_storage.s3", "S3 configuration required for Hybrid S3 Primary strategy")))?;
				let s3: Arc<dyn storage::MediaStorage> = Arc::new(storage::s3::S3Storage::new(s3_config).await?);

				Ok(Arc::new(storage::hybrid::HybridStorage::new(
					s3, // primary
					fs, // secondary (cache)
					config.media_storage.hybrid.clone(),
				)))
			},

			#[cfg(not(feature = "s3_storage"))]
			_ => Err(err!(Config(
				"S3 storage strategy requires compilation with --features s3_storage"
			))),
		}
	}

	/// Get storage backend (panics if not initialized)
	#[inline]
	fn get_storage(&self) -> &Arc<dyn storage::MediaStorage> {
		self.storage
			.get()
			.expect("Storage not initialized - this is a bug")
	}

	/// Uploads a file, with content-based deduplication.
	///
	/// Files with identical content are stored only once in the storage backend.
	/// Deduplication is based on SHA-256 content hash. The storage backend is
	/// checked with `exists()` before uploading — this avoids a race condition
	/// where concurrent uploads of the same file would all see refcount=0.
	/// If two concurrent uploads both pass the exists() check, the S3 PUT is
	/// idempotent (same key, same bytes → same object), so no corruption occurs.
	pub async fn create(
		&self,
		mxc: &Mxc<'_>,
		user: Option<&UserId>,
		content_disposition: Option<&ContentDisposition>,
		content_type: Option<&str>,
		file: &[u8],
	) -> Result {
		// 1. Compute SHA-256 hash of the file content (= storage key)
		let content_hash: [u8; 32] = sha2::Sha256::digest(file).into();
		let hash_hex: String = content_hash.iter().map(|b| format!("{b:02x}")).collect();

		trace!(?mxc, "Dedup: file size={} hash={}", file.len(), &hash_hex[..16]);

		// 2. Register MXC metadata in the database (unchanged behaviour)
		let key = self.db.create_file_metadata(
			mxc,
			user,
			&Dim::default(),
			content_disposition,
			content_type,
		)?;

		// 3. Store the hash → db-key mapping so delete() can find the hash
		self.db.set_media_hash(&key, &content_hash);

		// 4. Increment the reference count for this content hash
		self.db.increment_sha256_ref(&content_hash)?;

		// 5. Check storage backend to decide whether to upload.
		//    Using exists() avoids a race condition: refcount is not atomic
		//    across concurrent async tasks. If the file already exists in
		//    storage, skip the upload (dedup hit). If two concurrent uploads
		//    both see exists()==false, the PUT is idempotent (same key, same
		//    bytes), so at worst we upload twice but end up with one object.
		let already_stored = self
			.get_storage()
			.exists(&content_hash)
			.await
			.unwrap_or(false);

		trace!(?mxc, "Dedup: exists={} hash={}", already_stored, &hash_hex[..16]);

		if already_stored {
			debug!(?mxc, "Dedup: SKIP upload — file already in storage (hash={})", &hash_hex[..16]);
		} else {
			debug!(?mxc, "Dedup: UPLOAD — new unique file (hash={})", &hash_hex[..16]);
			self.get_storage().create(&content_hash, file).await?;
		}

		Ok(())
	}


	/// Deletes a file in the database and from the storage backend via an MXC.
	///
	/// Due to content-based deduplication, the physical file is only removed
	/// from storage when the reference count drops to zero (i.e. no other MXC
	/// URIs point to the same content).
	pub async fn delete(&self, mxc: &Mxc<'_>) -> Result {
		match self.db.search_mxc_metadata_prefix(mxc).await {
			| Ok(keys) => {
				for key in &keys {
					trace!(?mxc, "MXC Key: {key:?}");

					// Look up the content hash stored at upload time
					if let Some(hash) = self.db.get_media_hash(key) {
						let remaining = self.db.decrement_sha256_ref(&hash)?;
						if remaining == 0 {
							// Last reference gone — physically delete from storage
							debug_info!(?mxc, "Last reference removed — deleting from storage");
							if let Err(e) = self.get_storage().delete(&hash).await {
								debug_error!(?mxc, "Failed to delete from storage: {e}");
							}
						} else {
							debug_info!(?mxc, "Still {remaining} reference(s) remaining — keeping file in storage");
						}
						self.db.remove_media_hash(key);
					} else {
						// No hash found — legacy file (uploaded before dedup was added).
						// Fall back to deleting by the DB key directly.
						debug_info!(?mxc, "No content hash found for key — using legacy delete path");
						if let Err(e) = self.get_storage().delete(key).await {
							debug_error!(?mxc, "Failed to delete from storage (legacy): {e}");
						}
					}
				}

				debug_info!(?mxc, "Deleting from database");
				self.db.delete_file_mxc(mxc).await;

				Ok(())
			},
			| _ => {
				Err!(Database(error!(
					"Failed to find any media keys for MXC {mxc} in our database."
				)))
			},
		}
	}

	/// Deletes all media by the specified user
	///
	/// currently, this is only practical for local users
	pub async fn delete_from_user(&self, user: &UserId) -> Result<usize> {
		let mxcs = self.db.get_all_user_mxcs(user).await;
		let mut deletion_count: usize = 0;

		for mxc in mxcs {
			let Ok(mxc) = mxc.as_str().try_into().inspect_err(|e| {
				debug_error!(?mxc, "Failed to parse MXC URI from database: {e}");
			}) else {
				continue;
			};

			debug_info!(%deletion_count, "Deleting MXC {mxc} by user {user} from database and filesystem");
			match self.delete(&mxc).await {
				| Ok(()) => {
					deletion_count = deletion_count.saturating_add(1);
				},
				| Err(e) => {
					debug_error!(%deletion_count, "Failed to delete {mxc} from user {user}, ignoring error: {e}");
				},
			}
		}

		Ok(deletion_count)
	}

	/// Downloads a file.
	pub async fn get(&self, mxc: &Mxc<'_>) -> Result<Option<FileMeta>> {
		match self
			.db
			.search_file_metadata(mxc, &Dim::default())
			.await
		{
			| Ok(Metadata { content_disposition, content_type, key }) => {
				// Get the content hash for this file, or fallback to the legacy db key
				let storage_key = self
					.db
					.get_media_hash(&key)
					.map(|hash| hash.to_vec())
					.unwrap_or(key);

				// Use storage trait to read file
				match self.get_storage().read(&storage_key).await? {
					Some(bytes) => Ok(Some(FileMeta {
						content: Some(bytes.to_vec()),
						content_type,
						content_disposition,
					})),
					None => Ok(None),
				}
			},
			| _ => Ok(None),
		}
	}

	/// Gets all the MXC URIs in our media database
	pub async fn get_all_mxcs(&self) -> Result<Vec<OwnedMxcUri>> {
		let all_keys = self.db.get_all_media_keys().await;

		let mut mxcs = Vec::with_capacity(all_keys.len());

		for key in all_keys {
			trace!("Full MXC key from database: {key:?}");

			let mut parts = key.split(|&b| b == 0xFF);
			let mxc = parts
				.next()
				.map(|bytes| {
					utils::string_from_bytes(bytes).map_err(|e| {
						err!(Database(error!(
							"Failed to parse MXC unicode bytes from our database: {e}"
						)))
					})
				})
				.transpose()?;

			let Some(mxc_s) = mxc else {
				debug_warn!(
					?mxc,
					"Parsed MXC URL unicode bytes from database but is still invalid"
				);
				continue;
			};

			trace!("Parsed MXC key to URL: {mxc_s}");
			let mxc = OwnedMxcUri::from(mxc_s);

			if mxc.is_valid() {
				mxcs.push(mxc);
			} else {
				debug_warn!("{mxc:?} from database was found to not be valid");
			}
		}

		Ok(mxcs)
	}

	/// Deletes all remote only media files in the given at or after
	/// time/duration. Returns a usize with the amount of media files deleted.
	pub async fn delete_all_remote_media_at_after_time(
		&self,
		time: SystemTime,
		before: bool,
		after: bool,
		yes_i_want_to_delete_local_media: bool,
	) -> Result<usize> {
		let all_keys = self.db.get_all_media_keys().await;
		let mut remote_mxcs = Vec::with_capacity(all_keys.len());

		for key in all_keys {
			trace!("Full MXC key from database: {key:?}");
			let mut parts = key.split(|&b| b == 0xFF);
			let mxc = parts
				.next()
				.map(|bytes| {
					utils::string_from_bytes(bytes).map_err(|e| {
						err!(Database(error!(
							"Failed to parse MXC unicode bytes from our database: {e}"
						)))
					})
				})
				.transpose()?;

			let Some(mxc_s) = mxc else {
				debug_warn!(
					?mxc,
					"Parsed MXC URL unicode bytes from database but is still invalid"
				);
				continue;
			};

			trace!("Parsed MXC key to URL: {mxc_s}");
			let mxc = OwnedMxcUri::from(mxc_s);
			if (mxc.server_name() == Ok(self.services.globals.server_name())
				&& !yes_i_want_to_delete_local_media)
				|| !mxc.is_valid()
			{
				debug!("Ignoring local or broken media MXC: {mxc}");
				continue;
			}

			let path = self.get_media_file(&key);

			let file_metadata = match fs::metadata(path.clone()).await {
				| Ok(file_metadata) => file_metadata,
				| Err(e) => {
					error!(
						"Failed to obtain file metadata for MXC {mxc} at file path \
						 \"{path:?}\", skipping: {e}"
					);
					continue;
				},
			};

			trace!(%mxc, ?path, "File metadata: {file_metadata:?}");

			let file_created_at = match file_metadata.modified() {
				| Ok(value) => value,
				| Err(err) => {
					error!("Could not delete MXC {mxc} at path {path:?}: {err:?}. Skipping...");
					continue;
				},
			};

			debug!("File created at: {file_created_at:?}");

			if file_created_at >= time && before {
				debug!(
					"File is within (before) user duration, pushing to list of file paths and \
					 keys to delete."
				);
				remote_mxcs.push(mxc.to_string());
			} else if file_created_at <= time && after {
				debug!(
					"File is not within (after) user duration, pushing to list of file paths \
					 and keys to delete."
				);
				remote_mxcs.push(mxc.to_string());
			}
		}

		if remote_mxcs.is_empty() {
			return Err!(Database("Did not found any eligible MXCs to delete."));
		}

		debug_info!("Deleting media now in the past {time:?}");

		let mut deletion_count: usize = 0;

		for mxc in remote_mxcs {
			let Ok(mxc) = mxc.as_str().try_into() else {
				debug_warn!("Invalid MXC in database, skipping");
				continue;
			};

			debug_info!("Deleting MXC {mxc} from database and filesystem");

			match self.delete(&mxc).await {
				| Ok(()) => {
					deletion_count = deletion_count.saturating_add(1);
				},
				| Err(e) => {
					warn!("Failed to delete {mxc}, ignoring error and skipping: {e}");
					continue;
				},
			}
		}

		Ok(deletion_count)
	}

	pub async fn create_media_dir(&self) -> Result {
		let dir = self.get_media_dir();
		Ok(fs::create_dir_all(dir).await?)
	}

	async fn remove_media_file(&self, key: &[u8]) -> Result {
		let path = self.get_media_file(key);
		let legacy = self.get_media_file_b64(key);
		debug!(?key, ?path, ?legacy, "Removing media file");

		let file_rm = fs::remove_file(&path);
		let legacy_rm = fs::remove_file(&legacy);
		let (file_rm, legacy_rm) = tokio::join!(file_rm, legacy_rm);
		if let Err(e) = legacy_rm
			&& self.services.server.config.media_compat_file_link
		{
			debug_error!(?key, ?legacy, "Failed to remove legacy media symlink: {e}");
		}

		Ok(file_rm?)
	}

	async fn create_media_file(&self, key: &[u8]) -> Result<fs::File> {
		let path = self.get_media_file(key);
		debug!(?key, ?path, "Creating media file");

		let file = fs::File::create(&path).await?;
		if self.services.server.config.media_compat_file_link {
			let legacy = self.get_media_file_b64(key);
			if let Err(e) = fs::symlink(&path, &legacy).await {
				debug_error!(
					key = ?encode_key(key), ?path, ?legacy,
					"Failed to create legacy media symlink: {e}"
				);
			}
		}

		Ok(file)
	}

	#[inline]
	pub async fn get_metadata(&self, mxc: &Mxc<'_>) -> Option<FileMeta> {
		self.db
			.search_file_metadata(mxc, &Dim::default())
			.await
			.map(|metadata| FileMeta {
				content_disposition: metadata.content_disposition,
				content_type: metadata.content_type,
				content: None,
			})
			.ok()
	}

	#[inline]
	#[must_use]
	pub fn get_media_file(&self, key: &[u8]) -> PathBuf { self.get_media_file_sha256(key) }

	/// new SHA256 file name media function. requires database migrated. uses
	/// SHA256 hash of the base64 key as the file name
	#[must_use]
	pub fn get_media_file_sha256(&self, key: &[u8]) -> PathBuf {
		let mut r = self.get_media_dir();
		// Using the hash of the base64 key as the filename
		// This is to prevent the total length of the path from exceeding the maximum
		// length in most filesystems
		let digest = <sha2::Sha256 as Digest>::digest(key);
		let encoded = encode_key(&digest);
		r.push(encoded);
		r
	}

	/// old base64 file name media function
	/// This is the old version of `get_media_file` that uses the full base64
	/// key as the filename.
	#[must_use]
	pub fn get_media_file_b64(&self, key: &[u8]) -> PathBuf {
		let mut r = self.get_media_dir();
		let encoded = encode_key(key);
		r.push(encoded);
		r
	}

	#[must_use]
	pub fn get_media_dir(&self) -> PathBuf {
		let mut r = PathBuf::new();
		r.push(self.services.server.config.database_path.clone());
		r.push("media");
		r
	}
}

#[inline]
#[must_use]
pub fn encode_key(key: &[u8]) -> String { general_purpose::URL_SAFE_NO_PAD.encode(key) }
