use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProjectError {
    #[error("Project root not found")]
    RootNotFound,
    #[error("Failed to read postgel.toml: {0}")]
    ReadError(String),
    #[error("Failed to write postgel.toml: {0}")]
    WriteError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub postgres_version: Option<String>,
}

impl Default for ProjectConfig {
    fn default() -> Self {
        Self {
            postgres_version: None,
        }
    }
}

pub struct ProjectRoot(PathBuf);

impl ProjectRoot {
    pub fn find(start: &Path) -> Result<Self> {
        let mut current = start.to_path_buf();
        loop {
            let config_path = current.join("postgel.toml");
            if config_path.exists() {
                return Ok(Self(current));
            }
            if !current.pop() {
                break;
            }
        }
        Err(ProjectError::RootNotFound.into())
    }

    pub fn find_or_create(start: &Path) -> Result<Self> {
        match Self::find(start) {
            Ok(root) => Ok(root),
            Err(_) => {
                // Create in current directory
                let root = Self(start.to_path_buf());
                root.ensure_config()?;
                Ok(root)
            }
        }
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn config_path(&self) -> PathBuf {
        self.0.join("postgel.toml")
    }

    pub fn ensure_config(&self) -> Result<()> {
        let config_path = self.config_path();
        if !config_path.exists() {
            let config = ProjectConfig::default();
            let contents = toml::to_string_pretty(&config)
                .context("Failed to serialize default config")?;
            fs::write(&config_path, contents)
                .context("Failed to write postgel.toml")?;
        }
        Ok(())
    }

    pub fn load_config(&self) -> Result<ProjectConfig> {
        let config_path = self.config_path();
        let contents = fs::read_to_string(&config_path)
            .context("Failed to read postgel.toml")?;
        let config: ProjectConfig = toml::from_str(&contents)
            .context("Failed to parse postgel.toml")?;
        Ok(config)
    }

    pub fn save_config(&self, config: &ProjectConfig) -> Result<()> {
        let config_path = self.config_path();
        let contents = toml::to_string_pretty(config)
            .context("Failed to serialize config")?;
        fs::write(&config_path, contents)
            .context("Failed to write postgel.toml")?;
        Ok(())
    }

    pub fn generate_instance_name(&self) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        use hex;

        let path_str = self.0.to_string_lossy();
        let mut hasher = DefaultHasher::new();
        path_str.hash(&mut hasher);
        let hash = hasher.finish();
        let hash_str = hex::encode(hash.to_be_bytes());
        let short_hash = &hash_str[..8];

        // Try to derive a slug from the directory name
        let slug = self.0
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("project")
            .to_lowercase()
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '-')
            .collect::<String>();

        format!("{}-{}", slug, short_hash)
    }
}
