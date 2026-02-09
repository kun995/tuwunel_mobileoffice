/// S3/MinIO storage backend
///
/// Stores media files on S3-compatible object storage.

#[cfg(feature = "s3_storage")]
use std::time::SystemTime;

#[cfg(feature = "s3_storage")]
use async_trait::async_trait;
#[cfg(feature = "s3_storage")]
use aws_config::BehaviorVersion;
#[cfg(feature = "s3_storage")]
use aws_sdk_s3::{
	Client,
	config::{Credentials as S3Credentials, Region},
	primitives::ByteStream,
};
#[cfg(feature = "s3_storage")]
use bytes::Bytes;
#[cfg(feature = "s3_storage")]
use tuwunel_core::{err, Result};

#[cfg(feature = "s3_storage")]
use super::{MediaStorage, StorageMetadata};

/// S3-based media storage
#[cfg(feature = "s3_storage")]
pub struct S3Storage {
	client: Client,
	bucket: String,
	prefix: Option<String>,
}

#[cfg(feature = "s3_storage")]
impl S3Storage {
	/// Create a new S3 storage backend
	///
	/// # Arguments
	/// * `config` - S3 configuration
	pub async fn new(config: &tuwunel_core::config::S3StorageConfig) -> Result<Self> {
		// Build AWS SDK config with Tokio sleep implementation
		let sdk_config = aws_config::defaults(BehaviorVersion::latest())
			.endpoint_url(&config.endpoint)
			.region(Region::new(config.region.clone()))
			.credentials_provider(S3Credentials::new(
				&config.access_key,
				&config.secret_key,
				None,
				None,
				"tuwunel-s3",
			))
			.sleep_impl(aws_smithy_async::rt::sleep::TokioSleep::new())
			.load()
			.await;

		// Build S3 client config
		let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);
		if config.force_path_style {
			s3_config_builder = s3_config_builder.force_path_style(true);
		}

		let client = Client::from_conf(s3_config_builder.build());

		Ok(Self {
			client,
			bucket: config.bucket.clone(),
			prefix: config.prefix.clone(),
		})
	}



	/// Get the S3 key for a given storage key
	/// Extracts media_id from the key (format: mxc://server/MEDIA_ID)
	/// to use as S3 key instead of base64-encoded metadata
	fn get_s3_key(&self, key: &[u8]) -> String {
		// Try to extract media_id from key
		// Key format is typically: mxc://server/MEDIA_ID + metadata
		let key_str = String::from_utf8_lossy(key);
		
		// Extract media_id from MXC URI
		let media_id = if let Some(mxc_part) = key_str.split('\0').next() {
			// MXC format: mxc://server/MEDIA_ID
			if let Some(id) = mxc_part.split('/').last() {
				// Clean the media_id: only keep alphanumeric and safe chars
				id.chars()
					.filter(|c| c.is_alphanumeric() || *c == '_' || *c == '-')
					.collect::<String>()
			} else {
				String::new()
			}
		} else {
			String::new()
		};
		
		// Use media_id if valid, otherwise fallback to base64
		let s3_key = if !media_id.is_empty() && media_id.len() >= 10 {
			media_id
		} else {
			// Fallback to base64 encoding
			encode_key(key)
		};
		
		let final_key = match &self.prefix {
			Some(prefix) => format!("{}/{}", prefix, s3_key),
			None => s3_key,
		};

		// Remove leading slash if present (S3 best practice)
		final_key.trim_start_matches('/').to_string()
	}


}

#[cfg(feature = "s3_storage")]
#[async_trait]
impl MediaStorage for S3Storage {
	async fn create(&self, key: &[u8], data: &[u8]) -> Result<()> {
		let s3_key = self.get_s3_key(key);

		self.client
			.put_object()
			.bucket(&self.bucket)
			.key(&s3_key)
			.body(ByteStream::from(Bytes::copy_from_slice(data)))
			.send()
			.await
			.map_err(|e| {
				use aws_sdk_s3::error::SdkError;
				let error_details = match &e {
					SdkError::ServiceError(se) => {
						format!("HTTP {}: {:?}", se.raw().status(), se.err())
					},
					SdkError::ConstructionFailure(cf) => format!("Construction: {:?}", cf),
					SdkError::TimeoutError(_) => "Timeout".to_string(),
					SdkError::DispatchFailure(df) => format!("Dispatch: {:?}", df),
					_ => format!("{:?}", e),
				};
				err!(Database(error!(
					"S3 put_object failed: bucket={}, key={}, details={}",
					self.bucket,
					s3_key,
					error_details
				)))
			})?;

		Ok(())
	}

	async fn read(&self, key: &[u8]) -> Result<Option<Bytes>> {
		let s3_key = self.get_s3_key(key);

		match self
			.client
			.get_object()
			.bucket(&self.bucket)
			.key(&s3_key)
			.send()
			.await
		{
			Ok(output) => {
				let bytes = output
					.body
					.collect()
					.await
					.map_err(|e| err!(Database(error!("S3 body read failed: {}", e))))?
					.into_bytes();
				Ok(Some(bytes))
			},
			Err(e) => {
				// Check if it's a 404 Not Found error
				if is_not_found_error(&e) {
					Ok(None)
				} else {
					Err(err!(Database(error!("S3 get_object failed: {}", e))))
				}
			},
		}
	}

	async fn delete(&self, key: &[u8]) -> Result<()> {
		let s3_key = self.get_s3_key(key);

		self.client
			.delete_object()
			.bucket(&self.bucket)
			.key(&s3_key)
			.send()
			.await
			.map_err(|e| err!(Database(error!("S3 delete_object failed: {}", e))))?;

		Ok(())
	}

	async fn exists(&self, key: &[u8]) -> Result<bool> {
		let s3_key = self.get_s3_key(key);

		match self
			.client
			.head_object()
			.bucket(&self.bucket)
			.key(&s3_key)
			.send()
			.await
		{
			Ok(_) => Ok(true),
			Err(e) if is_not_found_error(&e) => Ok(false),
			Err(e) => Err(err!(Database(error!("S3 head_object failed: {}", e)))),
		}
	}

	async fn metadata(&self, key: &[u8]) -> Result<Option<StorageMetadata>> {
		let s3_key = self.get_s3_key(key);

		match self
			.client
			.head_object()
			.bucket(&self.bucket)
			.key(&s3_key)
			.send()
			.await
		{
			Ok(output) => {
				let size = output.content_length().unwrap_or(0) as u64;
				let modified = output
					.last_modified()
					.and_then(|dt| SystemTime::try_from(*dt).ok())
					.unwrap_or_else(SystemTime::now);

				Ok(Some(StorageMetadata { size, modified }))
			},
			Err(e) if is_not_found_error(&e) => Ok(None),
			Err(e) => Err(err!(Database(error!("S3 head_object failed: {}", e)))),
		}
	}

	async fn list_keys(&self) -> Result<Vec<Vec<u8>>> {
		// This is a placeholder - will be implemented when needed for migration
		Ok(Vec::new())
	}
}

/// Check if an S3 error is a "Not Found" error
#[cfg(feature = "s3_storage")]
fn is_not_found_error<E>(err: &aws_sdk_s3::error::SdkError<E>) -> bool
where
	E: std::error::Error + 'static,
{
	use aws_sdk_s3::error::SdkError;

	matches!(err, SdkError::ServiceError(e) if e.raw().status().as_u16() == 404)
}

/// Encode a key (hash digest) to a string for use as S3 key
#[cfg(feature = "s3_storage")]
fn encode_key(key: &[u8]) -> String {
	use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
	URL_SAFE_NO_PAD.encode(key)
}
