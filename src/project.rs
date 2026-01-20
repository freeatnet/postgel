use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProjectError {
    #[error("Project root not found")]
    RootNotFound,
    #[error("Project config is missing postgres_version")]
    MissingPostgresVersion,
    #[error("Failed to read postgel.toml: {0}")]
    ReadError(String),
    #[error("Failed to write postgel.toml: {0}")]
    WriteError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProjectConfig {
    pub postgres_version: Option<String>,
}

impl ProjectConfig {
    pub fn new(postgres_version: String) -> Self {
        Self {
            postgres_version: Some(postgres_version),
        }
    }

    pub fn require_postgres_version(&self) -> Result<String> {
        self.postgres_version
            .clone()
            .ok_or_else(|| ProjectError::MissingPostgresVersion.into())
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

    pub fn from_path(path: PathBuf) -> Self {
        Self(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn config_path(&self) -> PathBuf {
        self.0.join("postgel.toml")
    }

    pub fn load_config(&self) -> Result<ProjectConfig> {
        let config_path = self.config_path();
        let contents = fs::read_to_string(&config_path).context("Failed to read postgel.toml")?;
        let config: ProjectConfig =
            toml::from_str(&contents).context("Failed to parse postgel.toml")?;
        Ok(config)
    }

    pub fn save_config(&self, config: &ProjectConfig) -> Result<()> {
        let config_path = self.config_path();
        let contents = toml::to_string_pretty(config).context("Failed to serialize config")?;
        fs::write(&config_path, contents).context("Failed to write postgel.toml")?;
        Ok(())
    }
}
