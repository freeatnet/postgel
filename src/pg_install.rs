use anyhow::{Context, Result};
use directories::ProjectDirs;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

pub struct PgInstall {
    pub version: String,
    pub bin_dir: PathBuf,
}

impl PgInstall {
    pub fn install(version: &str) -> Result<Self> {
        let dirs = ProjectDirs::from("dev", "postgel", "postgel")
            .context("Failed to determine data directory")?;
        let data_dir = dirs.data_dir();
        let install_dir = data_dir.join("pg").join(version);

        if install_dir.exists() {
            // Already installed
            let bin_dir = install_dir.join("bin");
            return Ok(Self {
                version: version.to_string(),
                bin_dir,
            });
        }

        // Use Homebrew to fetch the bottle
        eprintln!("Fetching PostgreSQL {} bottle from Homebrew...", version);
        let output = Command::new("brew")
            .arg("fetch")
            .arg("--force-bottle")
            .arg(version)
            .output()
            .context("Failed to run brew fetch. Is Homebrew installed?")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("brew fetch failed: {}", stderr);
        }

        // Get the bottle path
        let cache_output = Command::new("brew")
            .arg("--cache")
            .arg(version)
            .output()
            .context("Failed to get brew cache path")?;

        if !cache_output.status.success() {
            anyhow::bail!("Failed to get brew cache path");
        }

        let cache_path = String::from_utf8_lossy(&cache_output.stdout)
            .trim()
            .to_string();

        // Find the actual bottle file (it might be a .tar.gz)
        let bottle_path = if cache_path.ends_with(".tar.gz") {
            PathBuf::from(cache_path)
        } else {
            // Try common bottle extensions
            for ext in &[".tar.gz", ".tar.xz"] {
                let candidate = format!("{}{}", cache_path, ext);
                if Path::new(&candidate).exists() {
                    return Self::extract_bottle(Path::new(&candidate), &install_dir, version);
                }
            }
            anyhow::bail!("Could not find bottle file for {}", version);
        };

        Self::extract_bottle(&bottle_path, &install_dir, version)
    }

    fn extract_bottle(bottle_path: &Path, install_dir: &PathBuf, version: &str) -> Result<Self> {
        eprintln!("Extracting bottle to {}...", install_dir.display());
        fs::create_dir_all(install_dir).context("Failed to create install directory")?;

        // Extract the tarball
        let output = Command::new("tar")
            .arg("-xzf")
            .arg(bottle_path)
            .arg("-C")
            .arg(install_dir)
            .output()
            .context("Failed to extract bottle")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Failed to extract bottle: {}", stderr);
        }

        // Homebrew bottles typically extract to a versioned directory
        // Find the actual bin directory
        // Homebrew bottles typically extract to a versioned directory
        let mut bin_dir = None;

        // Try the versioned path first
        let candidate = install_dir.join(format!("{}/bin", version));
        if candidate.exists() {
            bin_dir = Some(candidate);
        } else {
            // Try to find bin directory in extracted contents
            for entry in fs::read_dir(install_dir).context("Failed to read install directory")? {
                let entry = entry.context("Failed to read directory entry")?;
                let path = entry.path();
                if path.is_dir() {
                    let candidate = path.join("bin");
                    if candidate.exists() {
                        bin_dir = Some(candidate);
                        break;
                    }
                }
            }
        }

        let bin_dir = bin_dir
            .ok_or_else(|| anyhow::anyhow!("Could not find bin directory in extracted bottle"))?;

        // Fetch and extract dependencies
        eprintln!("Fetching dependencies...");
        let deps_output = Command::new("brew")
            .arg("deps")
            .arg("--include-optional")
            .arg("--skip-recommended")
            .arg(version)
            .output()
            .context("Failed to get dependencies")?;

        if deps_output.status.success() {
            let deps = String::from_utf8_lossy(&deps_output.stdout);
            for dep in deps.lines() {
                let dep = dep.trim();
                if !dep.is_empty() {
                    // Recursively fetch dependencies (simplified - in production you'd want proper dependency resolution)
                    let _ = Command::new("brew")
                        .arg("fetch")
                        .arg("--force-bottle")
                        .arg(dep)
                        .output();
                }
            }
        }

        Ok(Self {
            version: version.to_string(),
            bin_dir,
        })
    }

    pub fn get_or_install(version: &str) -> Result<Self> {
        let dirs = ProjectDirs::from("dev", "postgel", "postgel")
            .context("Failed to determine data directory")?;
        let data_dir = dirs.data_dir();
        let install_dir = data_dir.join("pg").join(version);
        let bin_dir = install_dir.join("bin");

        if bin_dir.exists() && bin_dir.join("postgres").exists() {
            Ok(Self {
                version: version.to_string(),
                bin_dir,
            })
        } else {
            Self::install(version)
        }
    }
}
