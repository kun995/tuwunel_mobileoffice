/// Hybrid storage backend
///
/// Combines two storage backends (primary and secondary) with configurable behavior.

#[cfg(feature = "s3_storage")]
use std::{sync::Arc, time::{Duration, SystemTime}};

#[cfg(feature = "s3_storage")]
use async_trait::async_trait;
#[cfg(feature = "s3_storage")]
use bytes::Bytes;
#[cfg(feature = "s3_storage")]
use tracing::{info, warn};

#[cfg(feature = "s3_storage")]
use super::{MediaStorage, StorageMetadata};
#[cfg(feature = "s3_storage")]
use tuwunel_core::{config::HybridStrategyConfig, err, Result};

/// Hybrid storage combining two backends
#[cfg(feature = "s3_storage")]
pub struct HybridStorage {
	primary: Arc<dyn MediaStorage>,
	secondary: Arc<dyn MediaStorage>,
	config: HybridStrategyConfig,
}

#[cfg(feature = "s3_storage")]
impl HybridStorage {
	/// Create a new hybrid storage backend
	///
	/// # Arguments
	/// * `primary` - Primary storage backend (e.g., S3)
	/// * `secondary` - Secondary storage backend (e.g., filesystem cache)
	/// * `config` - Hybrid strategy configuration
	pub fn new(primary: Arc<dyn MediaStorage>, secondary: Arc<dyn MediaStorage>, config: HybridStrategyConfig) -> Self {
		Self {
			primary,
			secondary,
			config,
		}
	}

	/// Check if cached file has expired based on TTL
	async fn is_cache_expired(&self, key: &[u8]) -> Result<bool> {
		// If TTL is 0, cache never expires
		if self.config.cache_ttl_seconds == 0 {
			return Ok(false);
		}

		// Get metadata from secondary storage
		if let Some(meta) = self.secondary.metadata(key).await? {
			let now = SystemTime::now();
			let age = now.duration_since(meta.modified).unwrap_or(Duration::ZERO);
			let ttl = Duration::from_secs(self.config.cache_ttl_seconds);

			if age > ttl {
				info!("Cache expired: age={:?}, ttl={:?}", age, ttl);
				return Ok(true);
			}
		}

		Ok(false)
	}
}

#[cfg(feature = "s3_storage")]
#[async_trait]
impl MediaStorage for HybridStorage {
	async fn create(&self, key: &[u8], data: &[u8]) -> Result<()> {
		// Always write to primary storage
		self.primary.create(key, data).await?;

		// Optionally write to secondary storage
		if self.config.write_to_both {
			if self.config.async_secondary_write {
				// Async write to secondary (don't block)
				let secondary = self.secondary.clone();
				let key = key.to_vec();
				let data = data.to_vec();
				tokio::spawn(async move {
					if let Err(e) = secondary.create(&key, &data).await {
						warn!("Failed to write to secondary storage: {}", e);
					}
				});
			} else {
				// Sync write to secondary
				self.secondary.create(key, data).await?;
			}
		}

		Ok(())
	}

	async fn read(&self, key: &[u8]) -> Result<Option<Bytes>> {
		// Try reading from secondary (cache) first
		match self.secondary.read(key).await? {
			Some(data) => {
				// Check if cache has expired
				if self.is_cache_expired(key).await? {
					info!("Cache expired, deleting and fetching from primary");
					// Delete expired cache (don't fail if deletion fails)
					let _ = self.secondary.delete(key).await;
					// Continue to fetch from primary
				} else {
					info!("Cache hit for media key");
					return Ok(Some(data));
				}
			},
			None => {
				info!("Cache miss for media key");
			},
		}

		// Cache miss - read from primary if fallback is enabled
		if self.config.read_fallback {
			if let Some(data) = self.primary.read(key).await? {
				info!("Read from primary storage");

				// Cache the data to secondary if enabled
				if self.config.cache_on_read {
					let secondary = self.secondary.clone();
					let key = key.to_vec();
					let data_clone = data.clone();
					tokio::spawn(async move {
						if let Err(e) = secondary.create(&key, &data_clone).await {
							warn!("Failed to cache data to secondary storage: {}", e);
						} else {
							info!("Cached data to secondary storage");
						}
					});
				}

				return Ok(Some(data));
			}
		}

		Ok(None)
	}

	async fn delete(&self, key: &[u8]) -> Result<()> {
		// Delete from both storages
		// We don't fail if one fails, just log a warning
		let primary_result = self.primary.delete(key).await;
		let secondary_result = self.secondary.delete(key).await;

		match (primary_result, secondary_result) {
			(Err(e1), Err(e2)) => {
				// Both failed - this is an error
				Err(err!(Database(error!(
					"Failed to delete from both storages: primary={}, secondary={}",
					e1,
					e2
				))))
			},
			(Err(e), _) | (_, Err(e)) => {
				// One failed - log warning but don't fail
				warn!("Failed to delete from one storage: {}", e);
				Ok(())
			},
			_ => Ok(()),
		}
	}

	async fn exists(&self, key: &[u8]) -> Result<bool> {
		// Check secondary first (faster)
		if self.secondary.exists(key).await? {
			return Ok(true);
		}

		// Check primary if fallback enabled
		if self.config.read_fallback {
			return self.primary.exists(key).await;
		}

		Ok(false)
	}

	async fn metadata(&self, key: &[u8]) -> Result<Option<StorageMetadata>> {
		// Try secondary first
		if let Some(meta) = self.secondary.metadata(key).await? {
			return Ok(Some(meta));
		}

		// Try primary if fallback enabled
		if self.config.read_fallback {
			return self.primary.metadata(key).await;
		}

		Ok(None)
	}

	async fn list_keys(&self) -> Result<Vec<Vec<u8>>> {
		// For hybrid, we list keys from primary storage
		self.primary.list_keys().await
	}
}
