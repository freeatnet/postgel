use anyhow::{Context, Result};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("Failed to read registry: {0}")]
    ReadError(String),
    #[error("Failed to write registry: {0}")]
    WriteError(String),
    #[error("Instance not found: {0}")]
    InstanceNotFound(String),
    #[error("Link not found")]
    LinkNotFound,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    pub id: InstanceId,
    pub display_name: String,
    pub postgres_version: String,
    pub data_dir: PathBuf,
    pub run_dir: PathBuf,
    pub port: u16,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct InstanceId(pub String);

impl InstanceId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Link {
    pub id: LinkId,
    pub project_path: PathBuf,
    pub instance_id: InstanceId,
    pub db_name: String,
    pub db_user: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct LinkId(pub String);

impl LinkId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RegistryData {
    instances: HashMap<InstanceId, Instance>,
    links: HashMap<LinkId, Link>,
    #[serde(default)]
    instance_by_path: HashMap<PathBuf, InstanceId>,
}

impl Default for RegistryData {
    fn default() -> Self {
        Self {
            instances: HashMap::new(),
            links: HashMap::new(),
            instance_by_path: HashMap::new(),
        }
    }
}

pub struct Registry {
    data: Arc<RwLock<RegistryData>>,
    registry_path: PathBuf,
}

impl Registry {
    pub fn load() -> Result<Self> {
        let dirs = ProjectDirs::from("dev", "postgel", "postgel")
            .context("Failed to determine config directory")?;
        let config_dir = dirs.config_dir();
        fs::create_dir_all(config_dir).context("Failed to create config directory")?;

        let registry_path = config_dir.join("registry.json");

        let data = if registry_path.exists() {
            let contents = fs::read_to_string(&registry_path)
                .context("Failed to read registry file")?;
            serde_json::from_str(&contents)
                .context("Failed to parse registry file")?
        } else {
            RegistryData::default()
        };

        Ok(Self {
            data: Arc::new(RwLock::new(data)),
            registry_path: registry_path.to_path_buf(),
        })
    }

    fn save(&self) -> Result<()> {
        let data = self.data.read().unwrap();
        let contents = serde_json::to_string_pretty(&*data)
            .context("Failed to serialize registry")?;
        fs::write(&self.registry_path, contents)
            .context("Failed to write registry file")?;
        Ok(())
    }

    pub fn add_instance(&self, instance: Instance) -> Result<()> {
        let mut data = self.data.write().unwrap();
        data.instances.insert(instance.id.clone(), instance);
        drop(data);
        self.save()?;
        Ok(())
    }

    pub fn get_instance(&self, id: &InstanceId) -> Option<Instance> {
        let data = self.data.read().unwrap();
        data.instances.get(id).cloned()
    }

    pub fn find_instance_by_name(&self, name: &str) -> Option<Instance> {
        let data = self.data.read().unwrap();
        data.instances
            .values()
            .find(|inst| inst.display_name == name)
            .cloned()
    }

    pub fn list_instances(&self) -> Vec<Instance> {
        let data = self.data.read().unwrap();
        data.instances.values().cloned().collect()
    }

    pub fn remove_instance(&self, id: &InstanceId) -> Result<()> {
        let mut data = self.data.write().unwrap();
        data.instances.remove(id);
        // Remove any links pointing to this instance
        data.links.retain(|_, link| link.instance_id != *id);
        data.instance_by_path.retain(|_, inst_id| inst_id != id);
        drop(data);
        self.save()?;
        Ok(())
    }

    pub fn add_link(&self, link: Link) -> Result<()> {
        let mut data = self.data.write().unwrap();
        data.links.insert(link.id.clone(), link.clone());
        data.instance_by_path.insert(link.project_path.clone(), link.instance_id.clone());
        drop(data);
        self.save()?;
        Ok(())
    }

    pub fn get_link_by_path(&self, path: &Path) -> Option<Link> {
        let data = self.data.read().unwrap();
        data.instance_by_path
            .get(path)
            .and_then(|inst_id| {
                data.links
                    .values()
                    .find(|link| link.instance_id == *inst_id && link.project_path == path)
                    .cloned()
            })
    }

    pub fn list_links(&self) -> Vec<Link> {
        let data = self.data.read().unwrap();
        data.links.values().cloned().collect()
    }

    pub fn remove_link(&self, id: &LinkId) -> Result<()> {
        let mut data = self.data.write().unwrap();
        if let Some(link) = data.links.remove(id) {
            data.instance_by_path.remove(&link.project_path);
        }
        drop(data);
        self.save()?;
        Ok(())
    }

    pub fn remove_link_by_path(&self, path: &Path) -> Result<Option<LinkId>> {
        let mut data = self.data.write().unwrap();
        let link_id = data
            .links
            .iter()
            .find(|(_, link)| link.project_path == path)
            .map(|(id, _)| id.clone());
        if let Some(ref id) = link_id {
            if let Some(link) = data.links.remove(id) {
                data.instance_by_path.remove(&link.project_path);
            }
        }
        drop(data);
        self.save()?;
        Ok(link_id)
    }

    pub fn prune_dead_links(&self) -> Result<Vec<LinkId>> {
        let mut data = self.data.write().unwrap();
        let mut removed = Vec::new();
        let mut paths_to_remove = Vec::new();

        for (id, link) in data.links.iter() {
            if !link.project_path.exists() {
                removed.push(id.clone());
                paths_to_remove.push(link.project_path.clone());
            }
        }

        for id in &removed {
            data.links.remove(id);
        }
        for path in &paths_to_remove {
            data.instance_by_path.remove(path);
        }

        drop(data);
        self.save()?;
        Ok(removed)
    }
}
