use anyhow::Result;
use std::path::Path;

use crate::state::Instance;

pub trait ServiceManager {
    fn install(&self, binary_path: &Path, instance: &Instance) -> Result<()>;
    fn remove(&self, instance: &Instance) -> Result<()>;
}

#[cfg(target_os = "macos")]
pub mod launchd;

#[cfg(target_os = "macos")]
pub fn default_manager() -> Option<Box<dyn ServiceManager>> {
    Some(Box::new(launchd::LaunchdManager::new()))
}

#[cfg(not(target_os = "macos"))]
pub fn default_manager() -> Option<Box<dyn ServiceManager>> {
    None
}
