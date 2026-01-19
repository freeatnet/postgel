use anyhow::{Context, Result};
use directories::ProjectDirs;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub mod instances;
pub mod projects;

pub use instances::Instance;
pub use projects::{Link, LinkId};

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

pub struct Registry {
    instances: instances::InstanceStore,
    projects: projects::ProjectStore,
}

impl Registry {
    pub fn load() -> Result<Self> {
        let config_dir = config_dir()?;
        std::fs::create_dir_all(&config_dir).context("Failed to create config directory")?;

        let instances_path = config_dir.join("instances.json");
        let links_path = config_dir.join("links.json");

        let instances = instances::InstanceStore::load(instances_path)?;
        let projects = projects::ProjectStore::load(links_path)?;

        Ok(Self {
            instances,
            projects,
        })
    }

    pub fn add_instance(&self, instance: Instance) -> Result<()> {
        self.instances.add(instance)
    }

    pub fn get_instance(&self, slug: &str) -> Option<Instance> {
        self.instances.get(slug)
    }

    pub fn list_instances(&self) -> Vec<Instance> {
        self.instances.list()
    }

    pub fn remove_instance(&self, slug: &str) -> Result<()> {
        self.instances.remove(slug)?;
        self.projects.remove_links_for_instance(slug)?;
        Ok(())
    }

    pub fn add_link(&self, link: Link) -> Result<()> {
        self.projects.add(link)
    }

    pub fn get_link_by_path(&self, path: &Path) -> Option<Link> {
        self.projects.get_by_path(path)
    }

    pub fn list_links(&self) -> Vec<Link> {
        self.projects.list()
    }

    pub fn remove_link(&self, id: &LinkId) -> Result<()> {
        self.projects.remove(id)
    }

    pub fn remove_link_by_path(&self, path: &Path) -> Result<Option<LinkId>> {
        self.projects.remove_by_path(path)
    }

    pub fn prune_dead_links(&self) -> Result<Vec<LinkId>> {
        self.projects
            .prune_dead_links(|slug| self.instances.contains(slug))
    }
}

fn config_dir() -> Result<PathBuf> {
    let dirs = ProjectDirs::from("dev", "postgel", "postgel")
        .context("Failed to determine config directory")?;
    Ok(dirs.config_dir().to_path_buf())
}
