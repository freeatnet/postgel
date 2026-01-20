use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

#[derive(Debug, Serialize, Deserialize, Default)]
struct PostgresVersionsData {
    versions: HashMap<String, PathBuf>,
}

pub struct PostgresVersionStore {
    data: Arc<RwLock<PostgresVersionsData>>,
    path: PathBuf,
}

impl PostgresVersionStore {
    pub fn load(path: PathBuf) -> Result<Self> {
        let data = if path.exists() {
            let contents = fs::read_to_string(&path)
                .context("Failed to read postgres versions registry file")?;
            serde_json::from_str(&contents)
                .context("Failed to parse postgres versions registry file")?
        } else {
            PostgresVersionsData::default()
        };

        Ok(Self {
            data: Arc::new(RwLock::new(data)),
            path,
        })
    }

    pub fn add(&self, version: String, path: PathBuf) -> Result<()> {
        let mut data = self.data.write().unwrap();
        data.versions.insert(version, path);
        drop(data);
        self.save()
    }

    pub fn get(&self, version: &str) -> Option<PathBuf> {
        let data = self.data.read().unwrap();
        data.versions.get(version).cloned()
    }

    fn save(&self) -> Result<()> {
        let data = self.data.read().unwrap();
        let contents = serde_json::to_string_pretty(&*data)
            .context("Failed to serialize postgres versions")?;
        fs::write(&self.path, contents)
            .context("Failed to write postgres versions registry file")?;
        Ok(())
    }
}
