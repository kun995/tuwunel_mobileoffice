/// Filesystem storage backend
///
/// Stores media files on the local filesystem using a hash-based directory structure.

use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use sha2::Digest;
use tokio::{fs, io::AsyncWriteExt};

use super::{MediaStorage, StorageMetadata};
use tuwunel_core::Result;

/// Filesystem-based media storage
pub struct FilesystemStorage {
	base_path: PathBuf,
}

impl FilesystemStorage {
	/// Create a new filesystem storage backend
	///
	/// # Arguments
	/// * `base_path` - Root directory for media storage
	pub fn new(base_path: PathBuf) -> Result<Self> {
		Ok(Self { base_path })
	}

	/// Get the full path for a given key
	fn get_path(&self, key: &[u8]) -> PathBuf {
		let mut path = self.base_path.clone();
		// Hash the key to get a consistent filename
		let digest = sha2::Sha256::digest(key);
		let encoded = encode_key(&digest);
		path.push(encoded);
		path
	}
}

#[async_trait]
impl MediaStorage for FilesystemStorage {
	async fn create(&self, key: &[u8], data: &[u8]) -> Result<()> {
		let path = self.get_path(key);

		// Ensure parent directory exists
		if let Some(parent) = path.parent() {
			fs::create_dir_all(parent).await?;
		}

		// Write file
		let mut file = fs::File::create(&path).await?;
		file.write_all(data).await?;
		file.sync_all().await?;

		Ok(())
	}

	async fn read(&self, key: &[u8]) -> Result<Option<Bytes>> {
		let path = self.get_path(key);

		match fs::read(&path).await {
			Ok(data) => Ok(Some(Bytes::from(data))),
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
			Err(e) => Err(e.into()),
		}
	}

	async fn delete(&self, key: &[u8]) -> Result<()> {
		let path = self.get_path(key);

		match fs::remove_file(&path).await {
			Ok(()) => Ok(()),
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()), // Already deleted
			Err(e) => Err(e.into()),
		}
	}

	async fn exists(&self, key: &[u8]) -> Result<bool> {
		let path = self.get_path(key);
		Ok(path.exists())
	}

	async fn metadata(&self, key: &[u8]) -> Result<Option<StorageMetadata>> {
		let path = self.get_path(key);

		match fs::metadata(&path).await {
			Ok(meta) => Ok(Some(StorageMetadata {
				size: meta.len(),
				modified: meta.modified()?,
			})),
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
			Err(e) => Err(e.into()),
		}
	}

	async fn list_keys(&self) -> Result<Vec<Vec<u8>>> {
		// This is a placeholder - will be implemented when needed for migration
		// For now, return empty list
		Ok(Vec::new())
	}
}

/// Encode a key (hash digest) to a string for use as filename
fn encode_key(key: &[u8]) -> String {
	use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
	URL_SAFE_NO_PAD.encode(key)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn test_filesystem_create_read_delete() {
		let temp_dir = tempfile::tempdir().unwrap();
		let storage = FilesystemStorage::new(temp_dir.path().to_path_buf()).unwrap();

		let key = b"test-key";
		let data = b"test-data";

		// Create
		storage.create(key, data).await.unwrap();

		// Read
		let read_data = storage.read(key).await.unwrap().unwrap();
		assert_eq!(read_data.as_ref(), data);

		// Exists
		assert!(storage.exists(key).await.unwrap());

		// Metadata
		let meta = storage.metadata(key).await.unwrap().unwrap();
		assert_eq!(meta.size, data.len() as u64);

		// Delete
		storage.delete(key).await.unwrap();
		assert!(!storage.exists(key).await.unwrap());

		// Read after delete
		assert!(storage.read(key).await.unwrap().is_none());
	}

	#[test]
	fn test_encode_key() {
		let key = b"hello world";
		let encoded = encode_key(key);
		assert!(!encoded.is_empty());
		assert!(!encoded.contains('='));
		assert!(!encoded.contains('/'));
	}
}
