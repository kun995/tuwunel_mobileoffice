/// Storage abstraction for media files
///
/// This module provides a trait-based abstraction for media storage,
/// allowing different backends (filesystem, S3, hybrid) to be used
/// interchangeably.

pub mod filesystem;

#[cfg(feature = "s3_storage")]
pub mod s3;

#[cfg(feature = "s3_storage")]
pub mod hybrid;

use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;

use tuwunel_core::Result;

/// Trait for media storage backends
///
/// This trait defines the interface that all storage backends must implement.
/// It provides basic CRUD operations for media files.
#[async_trait]
pub trait MediaStorage: Send + Sync {
	/// Create/upload a new file
	///
	/// # Arguments
	/// * `key` - Unique identifier for the file (typically a hash)
	/// * `data` - File content as bytes
	///
	/// # Returns
	/// * `Ok(())` if successful
	/// * `Err` if upload fails
	async fn create(&self, key: &[u8], data: &[u8]) -> Result<()>;

	/// Read a file
	///
	/// # Arguments
	/// * `key` - Unique identifier for the file
	///
	/// # Returns
	/// * `Ok(Some(bytes))` if file exists
	/// * `Ok(None)` if file not found
	/// * `Err` if read fails
	async fn read(&self, key: &[u8]) -> Result<Option<Bytes>>;

	/// Delete a file
	///
	/// # Arguments
	/// * `key` - Unique identifier for the file
	///
	/// # Returns
	/// * `Ok(())` if successful (even if file didn't exist)
	/// * `Err` if delete fails
	async fn delete(&self, key: &[u8]) -> Result<()>;

	/// Check if a file exists
	///
	/// # Arguments
	/// * `key` - Unique identifier for the file
	///
	/// # Returns
	/// * `Ok(true)` if file exists
	/// * `Ok(false)` if file doesn't exist
	/// * `Err` if check fails
	async fn exists(&self, key: &[u8]) -> Result<bool>;

	/// Get file metadata
	///
	/// # Arguments
	/// * `key` - Unique identifier for the file
	///
	/// # Returns
	/// * `Ok(Some(metadata))` if file exists
	/// * `Ok(None)` if file not found
	/// * `Err` if metadata retrieval fails
	async fn metadata(&self, key: &[u8]) -> Result<Option<StorageMetadata>>;

	/// List all keys in storage
	///
	/// This is primarily used for migration between storage backends.
	/// May be slow for large datasets.
	///
	/// # Returns
	/// * `Ok(Vec<Vec<u8>>)` - List of all keys
	/// * `Err` if listing fails
	async fn list_keys(&self) -> Result<Vec<Vec<u8>>>;
}

/// Metadata about a stored file
#[derive(Debug, Clone)]
pub struct StorageMetadata {
	/// File size in bytes
	pub size: u64,
	/// Last modified time
	pub modified: SystemTime,
}
