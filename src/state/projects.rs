use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Link {
    pub id: LinkId,
    pub project_path: PathBuf,
    pub instance_slug: String,
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

impl Default for LinkId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct ProjectsData {
    links: HashMap<LinkId, Link>,
    #[serde(default)]
    links_by_path: HashMap<PathBuf, LinkId>,
}

pub struct ProjectStore {
    data: Arc<RwLock<ProjectsData>>,
    path: PathBuf,
}

impl ProjectStore {
    pub fn load(path: PathBuf) -> Result<Self> {
        let data = if path.exists() {
            let contents =
                fs::read_to_string(&path).context("Failed to read links registry file")?;
            serde_json::from_str(&contents).context("Failed to parse links registry file")?
        } else {
            ProjectsData::default()
        };

        Ok(Self {
            data: Arc::new(RwLock::new(data)),
            path,
        })
    }

    pub fn add(&self, link: Link) -> Result<()> {
        let mut data = self.data.write().unwrap();
        data.links_by_path
            .insert(link.project_path.clone(), link.id.clone());
        data.links.insert(link.id.clone(), link);
        drop(data);
        self.save()
    }

    pub fn get_by_path(&self, path: &Path) -> Option<Link> {
        let data = self.data.read().unwrap();
        data.links_by_path
            .get(path)
            .and_then(|id| data.links.get(id).cloned())
    }

    pub fn list(&self) -> Vec<Link> {
        let data = self.data.read().unwrap();
        data.links.values().cloned().collect()
    }

    pub fn remove(&self, id: &LinkId) -> Result<()> {
        let mut data = self.data.write().unwrap();
        if let Some(link) = data.links.remove(id) {
            data.links_by_path.remove(&link.project_path);
        }
        drop(data);
        self.save()
    }

    pub fn remove_by_path(&self, path: &Path) -> Result<Option<LinkId>> {
        let mut data = self.data.write().unwrap();
        let link_id = data.links_by_path.get(path).cloned();
        if let Some(ref id) = link_id {
            if let Some(link) = data.links.remove(id) {
                data.links_by_path.remove(&link.project_path);
            }
        }
        drop(data);
        self.save()?;
        Ok(link_id)
    }

    pub fn remove_links_for_instance(&self, instance_slug: &str) -> Result<Vec<LinkId>> {
        let mut data = self.data.write().unwrap();
        let mut removed = Vec::new();
        let mut paths_to_remove = Vec::new();

        for (id, link) in data.links.iter() {
            if link.instance_slug == instance_slug {
                removed.push(id.clone());
                paths_to_remove.push(link.project_path.clone());
            }
        }

        for id in &removed {
            data.links.remove(id);
        }
        for path in &paths_to_remove {
            data.links_by_path.remove(path);
        }

        drop(data);
        self.save()?;
        Ok(removed)
    }

    pub fn prune_dead_links<F>(&self, instance_exists: F) -> Result<Vec<LinkId>>
    where
        F: Fn(&str) -> bool,
    {
        let mut data = self.data.write().unwrap();
        let mut removed = Vec::new();
        let mut paths_to_remove = Vec::new();

        for (id, link) in data.links.iter() {
            let path_missing = !link.project_path.exists();
            let instance_missing = !instance_exists(&link.instance_slug);
            if path_missing || instance_missing {
                removed.push(id.clone());
                paths_to_remove.push(link.project_path.clone());
            }
        }

        for id in &removed {
            data.links.remove(id);
        }
        for path in &paths_to_remove {
            data.links_by_path.remove(path);
        }

        drop(data);
        self.save()?;
        Ok(removed)
    }

    fn save(&self) -> Result<()> {
        let data = self.data.read().unwrap();
        let contents = serde_json::to_string_pretty(&*data).context("Failed to serialize links")?;
        fs::write(&self.path, contents).context("Failed to write links registry file")?;
        Ok(())
    }
}
