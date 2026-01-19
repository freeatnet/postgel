use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    pub slug: String,
    pub postgres_version: String,
    pub data_dir: PathBuf,
    pub run_dir: PathBuf,
    pub port: u16,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct InstancesData {
    instances: HashMap<String, Instance>,
}

pub struct InstanceStore {
    data: Arc<RwLock<InstancesData>>,
    path: PathBuf,
}

impl InstanceStore {
    pub fn load(path: PathBuf) -> Result<Self> {
        let data = if path.exists() {
            let contents =
                fs::read_to_string(&path).context("Failed to read instances registry file")?;
            serde_json::from_str(&contents).context("Failed to parse instances registry file")?
        } else {
            InstancesData::default()
        };

        Ok(Self {
            data: Arc::new(RwLock::new(data)),
            path,
        })
    }

    pub fn add(&self, instance: Instance) -> Result<()> {
        let mut data = self.data.write().unwrap();
        data.instances.insert(instance.slug.clone(), instance);
        drop(data);
        self.save()
    }

    pub fn get(&self, slug: &str) -> Option<Instance> {
        let data = self.data.read().unwrap();
        data.instances.get(slug).cloned()
    }

    pub fn list(&self) -> Vec<Instance> {
        let data = self.data.read().unwrap();
        data.instances.values().cloned().collect()
    }

    pub fn remove(&self, slug: &str) -> Result<()> {
        let mut data = self.data.write().unwrap();
        data.instances.remove(slug);
        drop(data);
        self.save()
    }

    pub fn contains(&self, slug: &str) -> bool {
        let data = self.data.read().unwrap();
        data.instances.contains_key(slug)
    }

    fn save(&self) -> Result<()> {
        let data = self.data.read().unwrap();
        let contents =
            serde_json::to_string_pretty(&*data).context("Failed to serialize instances")?;
        fs::write(&self.path, contents).context("Failed to write instances registry file")?;
        Ok(())
    }
}
