//! File-based persistence for AWS IoT Device Shadow documents
//!
//! This module provides simple file-based storage for shadow documents using JSON format.
//! It ensures atomic writes and handles concurrent access safely.

use crate::utils::shadow::error::ShadowResult;
use crate::utils::shadow::ShadowState;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use tokio::fs as async_fs;
use tokio::io::AsyncWriteExt;

/// File-based persistence for shadow documents
#[derive(Debug, Clone)]
pub struct FileBasedPersistence<T> {
    file_path: PathBuf,
    _phantom: PhantomData<T>,
}

#[allow(unused)]
impl<T> FileBasedPersistence<T>
where
    T: ShadowState + Serialize + DeserializeOwned,
{
    /// Create a new file-based persistence instance
    pub fn new(file_path: PathBuf) -> Self {
        Self {
            file_path,
            _phantom: PhantomData,
        }
    }

    /// Load the shadow document from the file
    pub async fn load(&self) -> ShadowResult<Option<T>> {
        // Check if file exists
        if !self.file_path.exists() {
            return Ok(None);
        }

        // Read the file content
        let content = async_fs::read_to_string(&self.file_path).await?;

        // Handle empty files
        if content.trim().is_empty() {
            return Ok(None);
        }

        // Parse JSON
        Ok(Some(serde_json::from_str(&content)?))
    }

    /// Save the shadow document to the file atomically
    pub async fn save(&self, document: &T) -> ShadowResult<()> {
        // Ensure parent directory exists
        if let Some(parent) = self.file_path.parent() {
            async_fs::create_dir_all(parent).await?;
        }

        // Serialize to JSON with pretty printing for human readability
        let json_content = serde_json::to_string_pretty(document)?;

        // Write atomically using a temporary file
        let temp_path = self.temp_file_path();

        // Write to temporary file first
        let mut temp_file = async_fs::File::create(&temp_path).await?;
        temp_file.write_all(json_content.as_bytes()).await?;
        temp_file.flush().await?;
        temp_file.sync_all().await?;
        drop(temp_file);

        // Atomically move temporary file to final location
        async_fs::rename(&temp_path, &self.file_path).await?;

        Ok(())
    }

    /// Delete the shadow document file
    pub async fn delete(&self) -> ShadowResult<()> {
        if self.file_path.exists() {
            async_fs::remove_file(&self.file_path).await?;
        }
        Ok(())
    }

    /// Check if the shadow document file exists
    pub fn exists(&self) -> bool {
        self.file_path.exists()
    }

    /// Get the file path for this persistence instance
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    /// Generate a temporary file path for atomic writes
    fn temp_file_path(&self) -> PathBuf {
        let mut temp_path = self.file_path.clone();
        if let Some(file_name) = temp_path.file_name() {
            let temp_name = format!("{}.tmp", file_name.to_string_lossy());
            temp_path.set_file_name(temp_name);
        }
        temp_path
    }

    /// Get the size of the shadow document file in bytes
    pub async fn file_size(&self) -> ShadowResult<u64> {
        let metadata = async_fs::metadata(&self.file_path).await?;
        Ok(metadata.len())
    }

    /// Get the last modified time of the shadow document file
    pub async fn last_modified(&self) -> ShadowResult<std::time::SystemTime> {
        let metadata = async_fs::metadata(&self.file_path).await?;
        Ok(metadata.modified()?)
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::shadow::shadow;

    use super::*;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[shadow]
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestState {
        temperature: f64,
        humidity: f64,
    }

    #[tokio::test]
    async fn test_save_and_load() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_shadow.json");
        let persistence = FileBasedPersistence::new(file_path);

        let original_doc = TestState::default();

        // Save the document
        persistence.save(&original_doc).await.unwrap();

        // Load it back
        let loaded_doc = persistence.load().await.unwrap().unwrap();

        assert_eq!(original_doc, loaded_doc);
    }

    #[tokio::test]
    async fn test_load_nonexistent_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("nonexistent.json");
        let persistence: FileBasedPersistence<TestState> = FileBasedPersistence::new(file_path);

        let result = persistence.load().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_delete() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_shadow.json");
        let persistence = FileBasedPersistence::new(file_path.clone());

        let doc = TestState::default();

        // Save and verify exists
        persistence.save(&doc).await.unwrap();
        assert!(file_path.exists());

        // Delete and verify removed
        persistence.delete().await.unwrap();
        assert!(!file_path.exists());
    }

    #[tokio::test]
    async fn test_atomic_write() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("atomic_test.json");
        let persistence = FileBasedPersistence::new(file_path.clone());

        let doc = TestState::default();

        // Save the document
        persistence.save(&doc).await.unwrap();

        // Verify no temporary file remains
        let temp_path = persistence.temp_file_path();
        assert!(!temp_path.exists());

        // Verify the actual file exists and is valid
        assert!(file_path.exists());
        let loaded = persistence.load().await.unwrap().unwrap();
        assert_eq!(doc, loaded);
    }

    #[tokio::test]
    async fn test_file_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("metadata_test.json");
        let persistence = FileBasedPersistence::new(file_path);

        let doc = TestState::default();
        persistence.save(&doc).await.unwrap();

        // Test file size
        let size = persistence.file_size().await.unwrap();
        assert!(size > 0);

        // Test last modified
        let modified = persistence.last_modified().await.unwrap();
        assert!(modified.elapsed().unwrap().as_secs() < 10); // Should be recent
    }
}
