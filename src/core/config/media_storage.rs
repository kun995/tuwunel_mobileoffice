use std::path::PathBuf;

use serde::Deserialize;

/// Media storage configuration
#[derive(Clone, Debug, Deserialize)]
pub struct MediaStorageConfig {
	/// Storage strategy to use
	///
	/// Options:
	/// - "filesystem": Store media on local filesystem (default)
	/// - "s3": Store media on S3/MinIO
	/// - "hybrid_s3_primary": S3 primary with filesystem cache
	///
	/// default: "filesystem"
	#[serde(default = "default_storage_strategy")]
	pub strategy: StorageStrategy,

	/// Filesystem storage configuration
	#[serde(default)]
	pub filesystem: FilesystemStorageConfig,

	/// S3/MinIO storage configuration
	pub s3: Option<S3StorageConfig>,

	/// Hybrid storage strategy configuration
	#[serde(default)]
	pub hybrid: HybridStrategyConfig,
}

impl Default for MediaStorageConfig {
	fn default() -> Self {
		Self {
			strategy: default_storage_strategy(),
			filesystem: FilesystemStorageConfig::default(),
			s3: None,
			hybrid: HybridStrategyConfig::default(),
		}
	}
}

/// Storage strategy enum
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StorageStrategy {
	/// Store media on local filesystem only
	Filesystem,
	/// Store media on S3/MinIO only
	S3,
	/// Hybrid: S3 primary with filesystem cache
	HybridS3Primary,
}

/// Filesystem storage configuration
#[derive(Clone, Debug, Deserialize)]
pub struct FilesystemStorageConfig {
	/// Path to store media files. If not set, uses {database_path}/media
	pub path: Option<PathBuf>,
}

impl Default for FilesystemStorageConfig {
	fn default() -> Self {
		Self { path: None }
	}
}

/// S3/MinIO storage configuration
#[derive(Clone, Debug, Deserialize)]
pub struct S3StorageConfig {
	/// S3 endpoint URL
	///
	/// Examples:
	/// - MinIO: "http://minio:9000"
	/// - AWS S3: "https://s3.amazonaws.com"
	/// - AWS S3 regional: "https://s3.ap-southeast-1.amazonaws.com"
	pub endpoint: String,

	/// S3 bucket name
	pub bucket: String,

	/// S3 region
	///
	/// example: "us-east-1"
	pub region: String,

	/// S3 access key ID
	///
	/// Can use environment variable: "${AWS_ACCESS_KEY_ID}"
	pub access_key: String,

	/// S3 secret access key
	///
	/// Can use environment variable: "${AWS_SECRET_ACCESS_KEY}"
	///
	/// display: sensitive
	pub secret_key: String,

	/// Optional prefix for all S3 keys
	///
	/// example: "media"
	pub prefix: Option<String>,

	/// Force path-style S3 URLs (required for MinIO)
	///
	/// default: false
	#[serde(default)]
	pub force_path_style: bool,
}

/// Hybrid storage strategy configuration
#[derive(Clone, Debug, Deserialize)]
pub struct HybridStrategyConfig {
	/// Write to both primary and secondary storage
	///
	/// default: false
	#[serde(default)]
	pub write_to_both: bool,

	/// Read from secondary storage if not found in primary
	///
	/// default: true
	#[serde(default = "default_true")]
	pub read_fallback: bool,

	/// Cache to secondary storage when reading from primary
	///
	/// default: true
	#[serde(default = "default_true")]
	pub cache_on_read: bool,

	/// Write to secondary storage asynchronously
	///
	/// default: true
	#[serde(default = "default_true")]
	pub async_secondary_write: bool,
}

impl Default for HybridStrategyConfig {
	fn default() -> Self {
		Self {
			write_to_both: false,
			read_fallback: true,
			cache_on_read: true,
			async_secondary_write: true,
		}
	}
}

const fn default_true() -> bool {
	true
}

fn default_storage_strategy() -> StorageStrategy {
	StorageStrategy::Filesystem
}
