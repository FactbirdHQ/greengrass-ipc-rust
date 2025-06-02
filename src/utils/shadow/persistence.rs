//! File-based persistence for AWS IoT Device Shadow documents
//!
//! This module provides simple file-based storage for shadow documents using JSON format.
//! It ensures atomic writes and handles concurrent access safely.

use crate::utils::shadow::error::ShadowResult;
use crate::utils::shadow::ShadowDocument;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
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
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync,
{
    /// Create a new file-based persistence instance
    pub fn new(file_path: PathBuf) -> Self {
        Self {
            file_path,
            _phantom: PhantomData,
        }
    }

    /// Load the shadow document from the file
    pub async fn load(&self) -> ShadowResult<Option<ShadowDocument<T>>> {
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
        let document: ShadowDocument<T> = serde_json::from_str(&content)?;
        Ok(Some(document))
    }

    /// Save the shadow document to the file atomically
    pub async fn save(&self, document: &ShadowDocument<T>) -> ShadowResult<()> {
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

    /// Load synchronously (for cases where async is not needed)
    pub fn load_sync(&self) -> ShadowResult<Option<ShadowDocument<T>>> {
        // Check if file exists
        if !self.file_path.exists() {
            return Ok(None);
        }

        // Read the file content
        let content = fs::read_to_string(&self.file_path)?;

        // Handle empty files
        if content.trim().is_empty() {
            return Ok(None);
        }

        // Parse JSON
        let document: ShadowDocument<T> = serde_json::from_str(&content)?;
        Ok(Some(document))
    }

    /// Save synchronously (for cases where async is not needed)
    pub fn save_sync(&self, document: &ShadowDocument<T>) -> ShadowResult<()> {
        // Ensure parent directory exists
        if let Some(parent) = self.file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Serialize to JSON with pretty printing
        let json_content = serde_json::to_string_pretty(document)?;

        // Write atomically using a temporary file
        let temp_path = self.temp_file_path();

        // Write to temporary file first
        let mut temp_file = fs::File::create(&temp_path)?;
        temp_file.write_all(json_content.as_bytes())?;
        temp_file.flush()?;
        temp_file.sync_all()?;
        drop(temp_file);

        // Atomically move temporary file to final location
        fs::rename(&temp_path, &self.file_path)?;

        Ok(())
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
    use super::*;
    use crate::utils::shadow::{ShadowDocument, ShadowError, ShadowState};
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    /// Builder for creating file-based persistence with default paths
    pub struct PersistenceBuilder {
        base_dir: Option<PathBuf>,
        thing_name: Option<String>,
        shadow_name: Option<String>,
    }

    impl PersistenceBuilder {
        /// Create a new persistence builder
        pub fn new() -> Self {
            Self {
                base_dir: None,
                thing_name: None,
                shadow_name: None,
            }
        }

        /// Set the base directory for shadow files
        pub fn base_dir<P: Into<PathBuf>>(mut self, dir: P) -> Self {
            self.base_dir = Some(dir.into());
            self
        }

        /// Set the thing name
        pub fn thing_name<S: Into<String>>(mut self, name: S) -> Self {
            self.thing_name = Some(name.into());
            self
        }

        /// Set the shadow name (for named shadows)
        pub fn shadow_name<S: Into<String>>(mut self, name: S) -> Self {
            self.shadow_name = Some(name.into());
            self
        }

        /// Build the persistence instance with default paths
        pub fn build<T>(self) -> ShadowResult<FileBasedPersistence<T>>
        where
            T: Serialize + for<'de> Deserialize<'de> + Send + Sync,
        {
            let base_dir = self.base_dir.unwrap_or_else(|| {
                // Use a default directory in the user's home or temp
                if let Some(home) = dirs::home_dir() {
                    home.join(".aws").join("shadows")
                } else {
                    std::env::temp_dir().join("aws_shadows")
                }
            });

            let thing_name = self.thing_name.ok_or_else(|| {
                ShadowError::InvalidDocument("Thing name is required".to_string())
            })?;

            let file_name = if let Some(shadow_name) = self.shadow_name {
                format!("{}_{}.json", thing_name, shadow_name)
            } else {
                format!("{}_classic.json", thing_name)
            };

            let file_path = base_dir.join(file_name);
            Ok(FileBasedPersistence::new(file_path))
        }
    }

    impl Default for PersistenceBuilder {
        fn default() -> Self {
            Self::new()
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestState {
        temperature: f64,
        humidity: f64,
    }

    fn create_test_document() -> ShadowDocument<TestState> {
        ShadowDocument {
            state: ShadowState {
                desired: Some(TestState {
                    temperature: 25.0,
                    humidity: 50.0,
                }),
                reported: Some(TestState {
                    temperature: 23.5,
                    humidity: 45.2,
                }),
                delta: None,
            },
            metadata: None,
            version: Some(1),
            timestamp: Some(1234567890),
            client_token: Some("test-token".to_string()),
        }
    }

    #[tokio::test]
    async fn test_save_and_load() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_shadow.json");
        let persistence = FileBasedPersistence::new(file_path);

        let original_doc = create_test_document();

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

        let doc = create_test_document();

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

        let doc = create_test_document();

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

    #[test]
    fn test_sync_operations() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("sync_test.json");
        let persistence = FileBasedPersistence::new(file_path);

        let doc = create_test_document();

        // Save synchronously
        persistence.save_sync(&doc).unwrap();

        // Load synchronously
        let loaded = persistence.load_sync().unwrap().unwrap();
        assert_eq!(doc, loaded);
    }

    #[tokio::test]
    async fn test_persistence_builder() {
        let temp_dir = TempDir::new().unwrap();

        let persistence: FileBasedPersistence<TestState> = PersistenceBuilder::new()
            .base_dir(temp_dir.path())
            .thing_name("test-device")
            .shadow_name("config")
            .build()
            .unwrap();

        let expected_path = temp_dir.path().join("test-device_config.json");
        assert_eq!(persistence.file_path(), expected_path);

        // Test classic shadow (no shadow name)
        let classic_persistence: FileBasedPersistence<TestState> = PersistenceBuilder::new()
            .base_dir(temp_dir.path())
            .thing_name("test-device")
            .build()
            .unwrap();

        let expected_classic_path = temp_dir.path().join("test-device_classic.json");
        assert_eq!(classic_persistence.file_path(), expected_classic_path);
    }

    #[tokio::test]
    async fn test_file_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("metadata_test.json");
        let persistence = FileBasedPersistence::new(file_path);

        let doc = create_test_document();
        persistence.save(&doc).await.unwrap();

        // Test file size
        let size = persistence.file_size().await.unwrap();
        assert!(size > 0);

        // Test last modified
        let modified = persistence.last_modified().await.unwrap();
        assert!(modified.elapsed().unwrap().as_secs() < 10); // Should be recent
    }
}
