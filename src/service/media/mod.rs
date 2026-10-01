mod data;
pub(super) mod migrations;
mod preview;
mod remote;
pub mod storage;
mod tests;
mod thumbnail;
#[cfg(feature = "media_thumbnail")]
mod video;
use std::{
	collections::{HashMap, HashSet},
	path::PathBuf,
	sync::{Arc, Mutex},
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use futures::{Stream, StreamExt, TryFutureExt};
use http::StatusCode;
use object_store::ObjectMeta;
use ruma::{
	Mxc, OwnedMxcUri, OwnedUserId, UserId,
	api::error::{ErrorKind, RetryAfter},
	http_headers::ContentDisposition,
};
#[cfg(feature = "media_thumbnail")]
use tokio::sync::Semaphore;
use tokio::{fs, sync::Notify};
use tuwunel_core::{
	Err, Error, Result, debug, debug_error, debug_info, debug_warn, err, trace,
	utils::{
		self, BoolExt, MutexMap,
		stream::{BroadbandExt, IterStream, ReadyExt},
		time::now_millis,
	},
	warn,
};
use url::Url;

#[cfg(feature = "media_thumbnail")]
use self::video::{FAILURES, Failures, sweep_staging_dir};
use self::{data::Data, preview::Agent, remote::Fetch, thumbnail::sequence};
pub use self::{
	data::Metadata,
	preview::UrlPreviewData,
	thumbnail::{Animate, Dim},
};
use hmac::{Hmac, Mac};
use ruma::ServerName;
use sha2::Digest;
use tokio::sync::{OnceCell, RwLock};
use tuwunel_core::config::PushMediaPreviewConfig;

#[derive(Debug)]
pub struct Media {
	pub content: Vec<u8>,
	pub content_type: Option<String>,
	pub content_disposition: Option<ContentDisposition>,
}

/// A picture fetched from a peer, and what a walk of its container settled.
///
/// The walk picking the type a fetched row is filed under settles the same
/// question a caller asks before serving the picture, so the answer travels
/// with the bytes rather than being read out of them a second time. A redirect
/// files no row and so walks nothing, which is what an absent answer means.
#[derive(Debug)]
pub struct Fetched {
	pub media: Media,
	/// Whether the bytes carry a frame sequence, where storing them settled it.
	///
	/// Private to the crate so nobody outside it can pair an answer with bytes
	/// it was not walked from, which would defeat the gate reading it.
	pub(crate) animates: Option<bool>,
}

/// One row of a user's uploaded media, holding only the fields tuwunel can
/// derive from the uploader index and storage-provider object metadata. The
/// uploader is carried when the index holds a row for it.
#[derive(Clone, Debug)]
pub struct UserMediaEntry {
	pub mxc: OwnedMxcUri,
	pub media_type: Option<String>,
	pub upload_name: Option<String>,
	pub media_length: Option<u64>,
	pub created_ts: u64,
	pub user_id: Option<OwnedUserId>,
}

/// One locally-uploaded media item's uploader and storage-object byte length
/// and modification time, the row shape of the media-statistics scan.
#[derive(Clone, Debug)]
pub struct UploadStat {
	pub user_id: OwnedUserId,
	pub media_length: u64,
	pub created_ts: u64,
}

/// For MSC2246
struct MXCState {
	/// Save the notifier for each pending media upload
	notifiers: Mutex<HashMap<OwnedMxcUri, Arc<Notify>>>,
	/// Save the ratelimiter for each user
	ratelimiter: Mutex<HashMap<OwnedUserId, (Instant, f64)>>,
}

pub struct Service {
	storage: Arc<OnceCell<Arc<dyn storage::MediaStorage>>>,
	push_preview_secret: RwLock<Option<Vec<u8>>>,
	pub(super) db: Data,
	services: Arc<crate::services::OnceServices>,
	url_preview_mutex: MutexMap<String, ()>,
	federation_mutex: MutexMap<String, ()>,
	mxc_state: MXCState,
	#[cfg(feature = "media_thumbnail")]
	animated_thumbnail_slots: Arc<Semaphore>,
	#[cfg(feature = "media_thumbnail")]
	video_thumbnail_slots: Semaphore,
	#[cfg(feature = "media_thumbnail")]
	video_thumbnail_failures: Mutex<Failures>,
}

/// generated MXC ID (`media-id`) length
pub const MXC_LENGTH: usize = 32;

/// Cache control for immutable objects.
pub const CACHE_CONTROL_IMMUTABLE: &str = "private,max-age=31536000,immutable";

/// Default cross-origin resource policy.
pub const CORP_CROSS_ORIGIN: &str = "cross-origin";

/// Validity window for a presigned media download redirect (MSC3860).
const REDIRECT_TTL: Duration = Duration::from_mins(5);

#[async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		#[cfg(feature = "media_thumbnail")]
		let animated_thumbnail_slots = Arc::new(Semaphore::new(
			args.server
				.config
				.media_thumbnail_animated_concurrency
				.max(1),
		));

		let service = Arc::new(Self {
			db: Data::new(args.db),
			storage: Arc::new(OnceCell::new()),
			push_preview_secret: RwLock::new(None),
			services: args.services.clone(),
			url_preview_mutex: MutexMap::new(),
			federation_mutex: MutexMap::new(),
			mxc_state: MXCState {
				notifiers: Mutex::new(HashMap::new()),
				ratelimiter: Mutex::new(HashMap::new()),
			},
			#[cfg(feature = "media_thumbnail")]
			video_thumbnail_failures: Failures::new(FAILURES).into(),
			#[cfg(feature = "media_thumbnail")]
			animated_thumbnail_slots,
			#[cfg(feature = "media_thumbnail")]
			video_thumbnail_slots: Semaphore::new(
				args.server.config.media_video_thumbnail_concurrency.max(1),
			),
		});

		#[cfg(feature = "media_thumbnail")]
		sweep_staging_dir(&args.server.config);

		Ok(service)
	}

	async fn worker(self: Arc<Self>) -> Result {
		// Initialize storage (lazy init)
		let storage = Self::build_storage(&self.services.server.config).await?;
		self.storage
			.set(storage)
			.map_err(|_| err!(Config("media_storage", "Storage already initialized")))?;

		let push_preview_config = self.services.server.config.push_media_preview.clone();
		if push_preview_config.enabled {
			let secret = Self::read_push_preview_secret(&push_preview_config)
				.await
				.ok_or_else(|| {
					err!(Config(
						"push_media_preview.secret_file",
						"Failed to load the configured push media preview secret."
					))
				})?;
			*self.push_preview_secret.write().await = Some(secret);
		}

		self.create_media_dir().await?;

		Ok(())
	}

	fn name(&self) -> &str {
		crate::service::make_name(std::module_path!())
	}
}

impl Service {
	/// Create a pending media upload ID.
	#[tracing::instrument(level = "debug", skip(self))]
	pub async fn create_pending(
		&self,
		mxc: &Mxc<'_>,
		user: &UserId,
		unused_expires_at: u64,
	) -> Result {
		let config = &self.services.server.config;

		// Rate limiting (rc_media_create)
		let rate = f64::from(config.media_rc_create_per_second);
		let burst = f64::from(config.media_rc_create_burst_count);

		// Check rate limiting
		if rate > 0.0 && burst > 0.0 {
			let now = Instant::now();
			let mut ratelimiter = self.mxc_state.ratelimiter.lock()?;

			let (last_time, tokens) = ratelimiter
				.entry(user.to_owned())
				.or_insert_with(|| (now, burst));

			let elapsed = now.duration_since(*last_time).as_secs_f64();
			let new_tokens = elapsed.mul_add(rate, *tokens).min(burst);

			if new_tokens >= 1.0 {
				*last_time = now;
				*tokens = new_tokens - 1.0;
			} else {
				return Err(Error::Request(
					ErrorKind::LimitExceeded(ruma::api::error::LimitExceededErrorData {
						retry_after: None,
					}),
					"Too many pending media creation requests.".into(),
					StatusCode::TOO_MANY_REQUESTS,
				));
			}
		}

		let max_uploads = config.max_pending_media_uploads;
		let (current_uploads, earliest_expiration) = self.db.count_pending_mxc_for_user(user).await;

		// Check if the user has reached the maximum number of pending media uploads
		if current_uploads >= max_uploads {
			let retry_after = earliest_expiration.saturating_sub(now_millis());
			return Err(Error::Request(
				ErrorKind::LimitExceeded(ruma::api::error::LimitExceededErrorData {
					retry_after: Some(RetryAfter::Delay(Duration::from_millis(retry_after))),
				}),
				"Maximum number of pending media uploads reached.".into(),
				StatusCode::TOO_MANY_REQUESTS,
			));
		}

		self.db.insert_pending_mxc(mxc, user, unused_expires_at);

		Ok(())
	}

	/// Uploads content to a pending media ID.
	#[tracing::instrument(level = "debug", skip(self))]
	pub async fn upload_pending(
		&self,
		mxc: &Mxc<'_>,
		user: &UserId,
		content_disposition: Option<&ContentDisposition>,
		content_type: Option<&str>,
		file: &[u8],
	) -> Result {
		let Ok((owner_id, expires_at)) = self.db.search_pending_mxc(mxc).await else {
			if self.get_metadata(mxc).await.is_some() {
				return Err!(Request(CannotOverwriteMedia(
					"Media ID already has content"
				)));
			}

			return Err!(Request(NotFound("Media not found")));
		};

		if owner_id != user {
			return Err!(Request(Forbidden("You did not create this media ID")));
		}

		let current_time = now_millis();
		if expires_at < current_time {
			return Err!(Request(NotFound("Pending media ID expired")));
		}

		self.create(mxc, Some(user), content_disposition, content_type, file)
			.await?;

		self.db.remove_pending_mxc(mxc);

		let mxc_uri: OwnedMxcUri = mxc.to_string().into();
		let notifier = self.mxc_state.notifiers.lock()?.remove(&mxc_uri);

		if let Some(notifier) = notifier {
			notifier.notify_waiters();
		}

		Ok(())
	}

	/// Uploads a file and reports whether its own bytes carry a sequence.
	///
	/// The declared type is whoever uploaded it saying so, and a picture that
	/// animates is stored under the type its own container names instead. That
	/// is the only record of whether the media animates that a later lookup can
	/// read without fetching the whole of it. The disposition is left as the
	/// caller computed it, so a file already bound for download stays there.
	pub async fn create(
		&self,
		mxc: &Mxc<'_>,
		user: Option<&UserId>,
		content_disposition: Option<&ContentDisposition>,
		content_type: Option<&str>,
		file: &[u8],
	) -> Result<bool> {
		let walk = sequence(file);

		// Width, Height = 0 if it's not a thumbnail
		let key = self.db.create_file_metadata(
			mxc,
			user,
			&Dim::default(),
			content_disposition,
			walk.stored_type(content_type),
		)?;

		//TODO: Dangling metadata in database if creation fails
		self.create_media_file(&key, file)
			.map_ok(|()| walk.animates())
			.await
	}

	/// Deletes a file in the database and from the media directory via an MXC
	#[tracing::instrument(level = "trace", skip(self))]
	pub async fn delete(&self, mxc: &Mxc<'_>) -> Result {
		// lazy URL-preview media has no file keys of its own; drop its reference
		// and staged bytes whenever present so a delete can't re-mint the media
		let had_lazy = self.db.search_lazy_media(mxc).await.is_ok();
		if had_lazy {
			let key = mxc.to_string();
			let mut txn = self.services.db.txn();

			self.db.remove_lazy_media(&mut txn, &key);
			self.db.remove_lazy_content(&mut txn, &key);
			txn.execute();
		}

		match self.db.search_mxc_metadata_prefix(mxc).await {
			Ok(keys) => {
				for key in keys {
					trace!(?mxc, "MXC Key: {key:?}");
					debug_info!(?mxc, "Deleting from storage provider");

					if let Err(e) = self.remove_media_file(&key).await {
						debug_error!(?mxc, "Failed to remove media file: {e}");
					}

					debug_info!(?mxc, "Deleting from database");
					self.db.delete_file_mxc(mxc).await;
				}

				Ok(())
			}
			_ if had_lazy => Ok(()),
			_ => Err!(Database(error!(
				"Failed to find any media keys for MXC {mxc} in our database."
			))),
		}
	}

	/// Deletes all media by the specified user
	///
	/// currently, this is only practical for local users
	#[tracing::instrument(level = "trace", skip(self))]
	pub async fn delete_from_user(&self, user: &UserId) -> Result<usize> {
		let mxcs = self.db.get_all_user_mxcs(user).await;
		let mut deletion_count: usize = 0;

		for mxc in mxcs {
			let Ok(mxc) = mxc.as_str().try_into().inspect_err(|e| {
				debug_error!(?mxc, "Failed to parse MXC URI from database: {e}");
			}) else {
				continue;
			};

			debug_info!(
				%deletion_count,
				"Deleting MXC {mxc} by user {user} from database and filesystem",
			);
			match self.delete(&mxc).await {
				Ok(()) => {
					deletion_count = deletion_count.saturating_add(1);
				}
				Err(e) => {
					debug_error!(
						%deletion_count,
						"Failed to delete {mxc} from user {user}, ignoring error: {e}"
					);
				}
			}
		}

		Ok(deletion_count)
	}

	/// Get file from local storage or make a federation request if it
	/// originates remotely.
	#[tracing::instrument(
		level = "debug",
		err(level = "debug")
		skip(self),
	)]
	pub async fn get_or_fetch(&self, mxc: &Mxc<'_>, timeout_ms: Duration) -> Result<Media> {
		if let Ok(media) = self.get(mxc, Some(timeout_ms)).await {
			return Ok(media);
		}

		if self.services.globals.server_is_ours(mxc.server_name) {
			return Err!(Request(NotFound("Local media not found.")));
		}

		let lock = self.federation_mutex.lock(&mxc.to_string()).await;

		if self.db.file_metadata_exists(mxc, &Dim::default()).await {
			drop(lock);
			return self.get(mxc, None).await;
		}

		self.fetch_remote_content(mxc, None, timeout_ms)
			.map_ok(|fetched| fetched.media)
			.await
	}

	/// Get file from local storage while waiting up to a timeout_ms if it is
	/// pending.
	#[tracing::instrument(
		level = "debug",
		err(level = "trace")
		skip(self),
	)]
	pub async fn get(&self, mxc: &Mxc<'_>, timeout: Option<Duration>) -> Result<Media> {
		if let Ok(meta) = self.get_stored(mxc).await {
			return Ok(meta);
		}

		let Some(timeout) = timeout else {
			return Err!(Request(NotFound("Media not found.")));
		};

		let Ok(_pending) = self.db.search_pending_mxc(mxc).await else {
			return Err!(Request(NotFound("Media not found.")));
		};

		let notifier = self
			.mxc_state
			.notifiers
			.lock()?
			.entry(mxc.to_string().into())
			.or_insert_with(|| Arc::new(Notify::new()))
			.clone();

		if tokio::time::timeout(timeout, notifier.notified())
			.await
			.is_err()
		{
			return Err!(Request(NotYetUploaded("Media has not been uploaded yet")));
		}

		self.get_stored(mxc).await
	}

	/// Get file from local storage.
	///
	/// A metadata miss falls through to the lazy-media path, which serializes
	/// on a per-mxc mutex and fetches the origin once.
	#[tracing::instrument(level = "debug", skip(self))]
	pub async fn get_stored(&self, mxc: &Mxc<'_>) -> Result<Media> {
		let meta = self
			.db
			.search_file_metadata(mxc, &Dim::default(), Animate::Allowed)
			.await;

		let Ok(Metadata {
			content_type,
			content_disposition,
			key,
		}) = meta
		else {
			// query-depth firewall
			return Box::pin(self.fetch_lazy_media(mxc)).await;
		};

		let content = self.fetch_bytes(&key).await?.to_vec();
		Ok(Media {
			content,
			content_type,
			content_disposition,
		})
	}

	/// Resolve lazy URL-preview media on first download, promoting the staged
	/// or freshly-fetched bytes into the media store so the origin is fetched
	/// at most once per item.
	#[tracing::instrument(level = "debug", skip(self))]
	async fn fetch_lazy_media(&self, mxc: &Mxc<'_>) -> Result<Media> {
		let key = mxc.to_string();

		// bound outbound amplification to one in-flight fetch per mxc; requests
		// for the same mxc queue rather than fan out
		let _lock = self.federation_mutex.lock(&key).await;

		// a queued waiter for the same mxc may have promoted it already
		if self
			.db
			.search_file_metadata(mxc, &Dim::default(), Animate::Allowed)
			.await
			.is_ok()
		{
			// box the cold re-entry to break the get_stored/fetch_lazy_media cycle
			return Box::pin(self.get_stored(mxc)).await;
		}

		let media = match self.db.get_lazy_content(&key).await {
			Ok(media) => media,
			Err(_) => {
				let Ok(url) = self.db.search_lazy_media(mxc).await else {
					return Err!(Request(NotFound("Media not found.")));
				};

				let limit = self.services.config.url_preview_max_media_size;

				self.location_request(Fetch::Preview(Agent::Media), &url, limit)
					.await?
			}
		};

		// promote through the ordinary upload path so real file metadata makes
		// every later download and thumbnail a normal-media hit
		if let Err(e) = self
			.create(
				mxc,
				None,
				media.content_disposition.as_ref(),
				media.content_type.as_deref(),
				&media.content,
			)
			.await
		{
			// a failed promotion leaves metadata without bytes, masking the lazy
			// fallback on every later read; drop it so the next read retries
			self.db.delete_file_mxc(mxc).await;

			return Err(e);
		}

		let mut txn = self.services.db.txn();

		self.db.remove_lazy_media(&mut txn, &key);
		self.db.remove_lazy_content(&mut txn, &key);
		txn.execute();

		Ok(media)
	}

	/// Presigned redirect URL for locally-stored media (MSC3860).
	///
	/// Returns the first configured provider's signed URL for the object, or
	/// `None` when redirects are disabled, the media is unknown, or no
	/// provider can presign it (filesystem-only media). A stored file that
	/// animates also answers `None` to a request forbidding animation, since
	/// handing over the object as it stands cannot re-encode it.
	#[tracing::instrument(level = "debug", skip(self))]
	pub async fn redirect_url(
		&self,
		_mxc: &Mxc<'_>,
		_dim: &Dim,
		_animate: Animate,
	) -> Result<Option<Url>> {
		Ok(None)
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

	/// Every media item uploaded by a local user, carrying the fields tuwunel
	/// can derive: content type, upload name, byte length and modification time
	/// from storage-provider object metadata. Untracked columns (last-access,
	/// quarantine, url-cache) are not represented.
	#[tracing::instrument(level = "debug", skip(self))]
	pub async fn user_media(&self, user: &UserId) -> Result<Vec<UserMediaEntry>> {
		let entries = self
			.db
			.get_all_user_mxcs(user)
			.await
			.into_iter()
			.stream()
			.broad_filter_map(async |mxc| self.user_media_entry(Some(user), mxc).await)
			.collect()
			.await;

		Ok(entries)
	}

	/// Derivable metadata for the single media item at the given MXC, with the
	/// uploading local user resolved from the uploader index when present.
	#[tracing::instrument(level = "debug", skip(self))]
	pub async fn media_entry(&self, mxc: &Mxc<'_>) -> Option<UserMediaEntry> {
		let user = self.db.mxc_user(mxc).await;

		self.user_media_entry(user.as_deref(), mxc.to_string().into())
			.await
	}

	async fn user_media_entry(
		&self,
		user: Option<&UserId>,
		mxc: OwnedMxcUri,
	) -> Option<UserMediaEntry> {
		let parts = mxc.parts().ok()?;
		let Metadata {
			content_type,
			content_disposition,
			key,
		} = self.get_metadata(&parts).await?;

		let object = self.head_meta(&key).await;
		let upload_name = content_disposition.and_then(|disposition| disposition.filename);

		Some(UserMediaEntry {
			media_type: content_type,
			upload_name,
			media_length: object.as_ref().map(|object| object.size),
			created_ts: object.as_ref().map(mtime_millis).unwrap_or(0),
			user_id: user.map(ToOwned::to_owned),
			mxc,
		})
	}

	/// Uploader, byte length and storage modification time of every media item
	/// uploaded by a local user, one row per upload; media missing from every
	/// storage provider are skipped.
	pub fn upload_stats(&self) -> impl Stream<Item = UploadStat> + Send + '_ {
		self.db
			.all_uploads()
			.broad_filter_map(async |(mxc, user_id)| {
				let parts = mxc.parts().ok()?;
				let Metadata { key, .. } = self.get_metadata(&parts).await?;
				let object = self.head_meta(&key).await?;

				Some(UploadStat {
					user_id,
					media_length: object.size,
					created_ts: mtime_millis(&object),
				})
			})
	}

	/// Deletes local media older than `before_ts` (by storage-provider mtime)
	/// and strictly larger than `size_gt` bytes, sparing profile and
	/// room-avatar media when `keep_profiles`. Returns the deleted MXCs, empty
	/// when none match. Quarantine and protection flags are not tracked, so no
	/// media is spared on those grounds.
	#[tracing::instrument(level = "debug", skip(self))]
	pub async fn delete_by_date_size(
		&self,
		before_ts: u64,
		size_gt: u64,
		keep_profiles: bool,
	) -> Result<Vec<OwnedMxcUri>> {
		let spared = keep_profiles
			.then_async(|| self.avatar_mxcs())
			.await
			.unwrap_or_default();

		let candidates = self.get_all_mxcs().await?;

		let deleted: Vec<OwnedMxcUri> = candidates
			.into_iter()
			.stream()
			.ready_filter(|mxc| self.is_local(mxc) && !spared.contains(mxc))
			.broad_filter_map(async |mxc| {
				let parts = mxc.parts().ok()?;
				let Metadata { key, .. } = self.get_metadata(&parts).await?;
				let object = self.head_meta(&key).await?;

				let eligible = mtime_millis(&object) < before_ts && object.size > size_gt;

				eligible
					.then_async(|| self.delete(&parts))
					.await
					.and_then(Result::ok)
					.map(|()| mxc)
			})
			.collect()
			.await;

		Ok(deleted)
	}

	/// The MXCs of every local user's profile avatar and every room's avatar,
	/// the spare-set honoured by `keep_profiles`.
	async fn avatar_mxcs(&self) -> HashSet<OwnedMxcUri> {
		let user_avatars = self
			.services
			.users
			.list_local_users()
			.map(ToOwned::to_owned)
			.broad_filter_map(async |user| self.services.profile.avatar_url(&user).await.ok());

		let room_avatars = self
			.services
			.metadata
			.iter_ids()
			.map(ToOwned::to_owned)
			.broad_filter_map(async |room_id| {
				self.services
					.state_accessor
					.get_avatar(&room_id)
					.await
					.ok()
					.and_then(|avatar| avatar.url)
			});

		user_avatars.chain(room_avatars).collect().await
	}

	fn is_local(&self, mxc: &OwnedMxcUri) -> bool {
		mxc.server_name()
			.is_ok_and(|server| self.services.globals.server_is_ours(server))
	}

	/// First storage provider's object metadata for the media stored under
	/// `key` (byte length and modification time), or `None` when no provider
	/// holds it.
	async fn head_meta(&self, key: &[u8]) -> Option<ObjectMeta> {
		let storage_key = self
			.db
			.get_media_hash(key)
			.map(|hash| hash.to_vec())
			.unwrap_or_else(|| key.to_vec());
		let metadata = self.get_storage().metadata(&storage_key).await.ok()??;
		Some(ObjectMeta {
			location: self.get_media_name_sha256(key).into(),
			last_modified: metadata.modified.into(),
			size: metadata.size,
			e_tag: None,
			version: None,
		})
	}

	/// Deletes all media files before or after the given time. Returns a usize
	/// with the number of media files deleted.
	pub async fn delete_range(
		&self,
		time: SystemTime,
		older_than: bool,
		newer_than: bool,
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

			let Some(file_metadata) = self.head_meta(&key).await else {
				continue;
			};
			let file_created_at = SystemTime::from(file_metadata.last_modified);

			debug!("File created at: {file_created_at:?}");

			if file_created_at <= time && older_than {
				debug!(
					"File is older than user duration, pushing to list of file paths and keys \
					 to delete."
				);
				remote_mxcs.push(mxc.to_string());
			} else if file_created_at >= time && newer_than {
				debug!(
					"File is newer than user duration, pushing to list of file paths and keys \
					 to delete."
				);
				remote_mxcs.push(mxc.to_string());
			}
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
				Ok(()) => {
					deletion_count = deletion_count.saturating_add(1);
				}
				Err(e) => {
					warn!("Failed to delete {mxc}, ignoring error and skipping: {e}");
				}
			}
		}

		Ok(deletion_count)
	}

	pub async fn create_media_dir(&self) -> Result {
		let dir = self.get_media_dir();
		Ok(fs::create_dir_all(dir).await?)
	}

	async fn remove_media_file(&self, key: &[u8]) -> Result {
		if let Some(hash) = self.db.get_media_hash(key) {
			if self.db.decrement_sha256_ref(&hash)? == 0 {
				self.get_storage().delete(&hash).await?;
			}
			self.db.remove_media_hash(key);
		} else {
			self.get_storage().delete(key).await?;
		}
		Ok(())
	}

	async fn create_media_file(&self, key: &[u8], file: &[u8]) -> Result {
		let hash: [u8; 32] = sha2::Sha256::digest(file).into();
		self.db.set_media_hash(key, &hash);
		self.db.increment_sha256_ref(&hash)?;
		if !self.get_storage().exists(&hash).await.unwrap_or(false) {
			self.get_storage().create(&hash, file).await?;
		}
		Ok(())
	}

	#[inline]
	pub async fn get_metadata(&self, mxc: &Mxc<'_>) -> Option<Metadata> {
		self.db
			.search_file_metadata(mxc, &Dim::default(), Animate::Allowed)
			.await
			.ok()
	}

	#[inline]
	#[must_use]
	pub fn get_media_path_sha256(&self, key: &[u8]) -> PathBuf {
		self.get_media_dir().join(self.get_media_name_sha256(key))
	}

	/// new SHA256 file name media function. requires database migrated. uses
	/// SHA256 hash of the base64 key as the file name
	#[inline]
	#[must_use]
	pub fn get_media_name_sha256(&self, key: &[u8]) -> String {
		// Using the hash of the base64 key as the filename prevents the total
		// length of the path from exceeding the maximum length in most
		// filesystems
		let digest = <sha2::Sha256 as sha2::Digest>::digest(key);
		encode_key(&digest)
	}

	/// old base64 file name media function
	/// This is the old version of `get_media_path_sha256` that uses the full
	/// base64 key as the filename.
	#[must_use]
	pub fn get_media_path_b64(&self, key: &[u8]) -> PathBuf {
		self.get_media_dir().join(encode_key(key))
	}

	#[must_use]
	pub fn get_media_dir(&self) -> PathBuf {
		self.services.server.config.database_path.join("media")
	}
}

#[inline]
#[must_use]
pub fn encode_key(key: &[u8]) -> String {
	general_purpose::URL_SAFE_NO_PAD.encode(key)
}

fn mtime_millis(object: &ObjectMeta) -> u64 {
	u64::try_from(object.last_modified.timestamp_millis()).unwrap_or(0)
}

impl Service {
	async fn build_storage(
		config: &tuwunel_core::config::Config,
	) -> Result<Arc<dyn storage::MediaStorage>> {
		use tuwunel_core::config::StorageStrategy;

		let media_path = config.database_path.join("media");

		match config.media_storage.strategy {
			StorageStrategy::Filesystem => {
				debug!("Initializing Filesystem storage");
				Ok(Arc::new(storage::filesystem::FilesystemStorage::new(
					media_path,
				)?))
			}

			#[cfg(feature = "s3_storage")]
			StorageStrategy::S3 => {
				debug!("Initializing S3 storage");
				let s3_config = config.media_storage.s3.as_ref().ok_or_else(|| {
					err!(Config(
						"media_storage.s3",
						"S3 configuration required for S3 storage strategy"
					))
				})?;
				let s3 = storage::s3::S3Storage::new(s3_config).await?;
				let s3: Arc<dyn storage::MediaStorage> = Arc::new(s3);
				Ok(s3)
			}

			#[cfg(feature = "s3_storage")]
			StorageStrategy::HybridS3Primary => {
				debug!("Initializing Hybrid S3 Primary storage");
				let fs = Arc::new(storage::filesystem::FilesystemStorage::new(media_path)?);
				let s3_config = config.media_storage.s3.as_ref().ok_or_else(|| {
					err!(Config(
						"media_storage.s3",
						"S3 configuration required for Hybrid S3 Primary strategy"
					))
				})?;
				let s3: Arc<dyn storage::MediaStorage> =
					Arc::new(storage::s3::S3Storage::new(s3_config).await?);

				Ok(Arc::new(storage::hybrid::HybridStorage::new(
					s3, // primary
					fs, // secondary (cache)
					config.media_storage.hybrid.clone(),
				)))
			}

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

	/// Generate a short-lived HTTPS URL for a local MXC thumbnail.
	pub async fn push_preview_url(&self, mxc: &Mxc<'_>) -> Option<String> {
		let config = self.services.server.config.push_media_preview.clone();
		if !config.enabled
			|| !self.services.globals.server_is_ours(mxc.server_name)
			|| !valid_push_preview_media_id(mxc.media_id)
		{
			return None;
		}

		let secret = self.get_push_preview_secret(&config).await?;
		let now = unix_seconds()?;
		let expires = now.checked_add(config.ttl_seconds)?;
		let signature = sign_push_preview(
			&secret,
			mxc.server_name,
			mxc.media_id,
			config.width,
			config.height,
			PUSH_PREVIEW_METHOD,
			expires,
		)?;

		let mut url = config.public_base_url?;
		url.set_path(&push_preview_path(mxc.server_name, mxc.media_id));
		{
			let mut query = url.query_pairs_mut();
			query
				.append_pair("width", &config.width.to_string())
				.append_pair("height", &config.height.to_string())
				.append_pair("method", PUSH_PREVIEW_METHOD)
				.append_pair("expires", &expires.to_string())
				.append_pair("sig", &signature);
		}

		Some(url.to_string())
	}

	/// Verify a signed push preview request without exposing the signing secret.
	pub async fn verify_push_preview_signature(
		&self,
		server_name: &ServerName,
		media_id: &str,
		width: u32,
		height: u32,
		method: &str,
		expires: u64,
		signature: &str,
	) -> bool {
		let config = self.services.server.config.push_media_preview.clone();
		let Some(now) = unix_seconds() else {
			return false;
		};

		if !config.enabled
			|| !self.services.globals.server_is_ours(server_name)
			|| !valid_push_preview_media_id(media_id)
			|| width == 0
			|| width > PUSH_PREVIEW_MAX_DIMENSION
			|| height == 0
			|| height > PUSH_PREVIEW_MAX_DIMENSION
			|| !matches!(method, "scale" | "crop")
			|| expires <= now
		{
			return false;
		}

		let Some(secret) = self.get_push_preview_secret(&config).await else {
			return false;
		};
		let Ok(signature) = general_purpose::URL_SAFE_NO_PAD.decode(signature) else {
			return false;
		};
		if signature.len() != 32 {
			return false;
		}

		let Ok(mut mac) = HmacSha256::new_from_slice(&secret) else {
			return false;
		};
		mac.update(
			push_preview_signature_message(server_name, media_id, width, height, method, expires)
				.as_bytes(),
		);
		mac.verify_slice(&signature).is_ok()
	}

	async fn read_push_preview_secret(config: &PushMediaPreviewConfig) -> Option<Vec<u8>> {
		let path = config.secret_file.as_ref()?;
		let secret = fs::read_to_string(path).await.ok()?;
		let secret = secret.trim().as_bytes().to_vec();
		(!secret.is_empty()).then_some(secret)
	}

	async fn get_push_preview_secret(&self, config: &PushMediaPreviewConfig) -> Option<Vec<u8>> {
		if let Some(secret) = self.push_preview_secret.read().await.clone() {
			return Some(secret);
		}

		let secret = Self::read_push_preview_secret(config).await?;
		let mut cached = self.push_preview_secret.write().await;
		Some(cached.get_or_insert(secret).clone())
	}
}

const PUSH_PREVIEW_PATH_PREFIX: &str = "/_matrix/client/unstable/org.tuwunel/media/thumbnail";
const PUSH_PREVIEW_METHOD: &str = "scale";
const PUSH_PREVIEW_MAX_DIMENSION: u32 = 512;
type HmacSha256 = Hmac<sha2::Sha256>;

fn unix_seconds() -> Option<u64> {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.ok()
		.map(|duration| duration.as_secs())
}

fn valid_push_preview_media_id(media_id: &str) -> bool {
	!media_id.is_empty()
		&& media_id
			.bytes()
			.all(|byte| !matches!(byte, b'/' | b'\\' | b'?' | b'#'))
}

fn push_preview_path(server_name: &ServerName, media_id: &str) -> String {
	format!("{PUSH_PREVIEW_PATH_PREFIX}/{server_name}/{media_id}/preview.png")
}

fn push_preview_signature_message(
	server_name: &ServerName,
	media_id: &str,
	width: u32,
	height: u32,
	method: &str,
	expires: u64,
) -> String {
	format!(
		"v1\n{}\n{width}\n{height}\n{method}\n{expires}",
		push_preview_path(server_name, media_id)
	)
}

fn sign_push_preview(
	secret: &[u8],
	server_name: &ServerName,
	media_id: &str,
	width: u32,
	height: u32,
	method: &str,
	expires: u64,
) -> Option<String> {
	let mut mac = HmacSha256::new_from_slice(secret).ok()?;
	mac.update(
		push_preview_signature_message(server_name, media_id, width, height, method, expires)
			.as_bytes(),
	);

	Some(general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}
