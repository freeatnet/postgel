use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::state::Registry;

pub const DEFAULT_PG_VERSION: &str = "18";

pub struct PostgresInstall {
    pub version: String,
    pub bin_dir: PathBuf,
}

impl PostgresInstall {
    pub fn get_or_install(registry: &Registry, version: &str) -> Result<Self> {
        let normalized = normalize_pg_version(version)?;
        let formula = formula_name(&normalized);

        if let Some(existing) = Self::get(registry, &normalized)? {
            return Ok(existing);
        }

        Self::install(registry, &formula, &normalized)
    }

    pub fn pg_ctl_path(&self) -> PathBuf {
        self.bin_dir.join("pg_ctl")
    }

    pub fn postgres_path(&self) -> PathBuf {
        self.bin_dir.join("postgres")
    }

    pub fn pg_isready_path(&self) -> PathBuf {
        self.bin_dir.join("pg_isready")
    }

    pub fn initdb_path(&self) -> PathBuf {
        self.bin_dir.join("initdb")
    }

    fn get(registry: &Registry, normalized: &str) -> Result<Option<Self>> {
        if let Some(prefix) = registry.get_postgres_path(normalized) {
            // Registry entry exists - verify the installation
            let bin_dir = verify_postgres_installation(&prefix).map_err(|e| {
                anyhow::anyhow!(
                    "PostgreSQL version {} is registered at {}, but {}",
                    normalized,
                    prefix.display(),
                    e
                )
            })?;

            return Ok(Some(Self {
                version: normalized.to_string(),
                bin_dir,
            }));
        }

        Ok(None)
    }

    fn install(registry: &Registry, formula: &str, normalized: &str) -> Result<Self> {
        // Check brew availability before attempting install
        brew_check()?;

        // Resolve the prefix path (brew_prefix returns the path whether installed or not)
        let prefix = brew_prefix(formula)?;

        // Check if already installed via brew (but not in registry)
        if let Ok(bin_dir) = verify_postgres_installation(&prefix) {
            // Found installed version - add to registry
            registry.add_postgres_version(normalized.to_string(), prefix.clone())?;
            return Ok(Self {
                version: normalized.to_string(),
                bin_dir,
            });
        }

        // Not found - need to install
        brew_install_formula(formula)?;

        // Verify installation after brew install
        let bin_dir = verify_postgres_installation(&prefix)?;

        // Record in registry
        registry.add_postgres_version(normalized.to_string(), prefix.clone())?;

        Ok(Self {
            version: normalized.to_string(),
            bin_dir,
        })
    }
}

fn verify_postgres_installation(prefix: &Path) -> Result<PathBuf> {
    let bin_dir = prefix.join("bin");
    let postgres_binary = bin_dir.join("postgres");
    if !bin_dir.is_dir() || !postgres_binary.exists() {
        anyhow::bail!(
            "PostgreSQL binary not found at {}",
            postgres_binary.display()
        );
    }

    Ok(bin_dir)
}

fn formula_name(normalized: &str) -> String {
    format!("postgresql@{}", normalized)
}

fn brew_check() -> Result<()> {
    let output = Command::new("brew")
        .arg("--version")
        .output()
        .context("Failed to run brew --version. Is Homebrew installed?")?;

    if !output.status.success() {
        anyhow::bail!(
            "Homebrew is not installed or not available in PATH. Please install Homebrew: https://brew.sh/"
        );
    }

    Ok(())
}

fn brew_prefix(formula: &str) -> Result<PathBuf> {
    let output = Command::new("brew")
        .arg("--prefix")
        .arg(formula)
        .output()
        .context("Failed to run brew --prefix. Is Homebrew installed?")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "brew --prefix failed: {}. Is {} installed?",
            stderr,
            formula
        );
    }

    let prefix_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(PathBuf::from(prefix_str))
}

fn brew_install_formula(formula: &str) -> Result<()> {
    let output = Command::new("brew")
        .arg("install")
        .arg("--skip-link")
        .arg("--skip-post-install")
        .arg(formula)
        .output()
        .context("Failed to run brew install. Is Homebrew installed?")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("brew install failed: {}", stderr);
    }

    Ok(())
}

pub fn normalize_pg_version(input: &str) -> Result<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        anyhow::bail!("Postgres version cannot be empty");
    }

    let normalized = trimmed.strip_prefix("postgresql@").unwrap_or(trimmed);

    if normalized.is_empty() {
        anyhow::bail!("Postgres version cannot be empty");
    }

    // Only accept major versions (no minor/patch versions)
    if normalized.contains('.') {
        anyhow::bail!(
            "Postgres version must be a major version only (e.g., '18', not '18.1'). Homebrew does not support minor/patch version specifications."
        );
    }

    if !normalized.chars().all(|c| c.is_ascii_digit()) {
        anyhow::bail!("Invalid Postgres version: {}", trimmed);
    }

    Ok(normalized.to_string())
}

pub fn resolve_pg_version(input: Option<&str>) -> Result<String> {
    match input {
        Some(value) => normalize_pg_version(value),
        None => Ok(DEFAULT_PG_VERSION.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_PG_VERSION, normalize_pg_version, resolve_pg_version};

    #[test]
    fn normalize_pg_version_accepts_major_versions() {
        assert_eq!(normalize_pg_version("18").unwrap(), "18");
        assert_eq!(normalize_pg_version("16").unwrap(), "16");
    }

    #[test]
    fn normalize_pg_version_rejects_minor_patch_versions() {
        assert!(normalize_pg_version("16.1").is_err());
        assert!(normalize_pg_version("18.2.3").is_err());
    }

    #[test]
    fn normalize_pg_version_strips_postgresql_prefix() {
        assert_eq!(normalize_pg_version("postgresql@15").unwrap(), "15");
    }

    #[test]
    fn normalize_pg_version_trims_whitespace() {
        assert_eq!(normalize_pg_version(" 14 ").unwrap(), "14");
    }

    #[test]
    fn normalize_pg_version_rejects_invalid_input() {
        assert!(normalize_pg_version("").is_err());
        assert!(normalize_pg_version("  ").is_err());
        assert!(normalize_pg_version("postgresql@").is_err());
        assert!(normalize_pg_version("v15").is_err());
        assert!(normalize_pg_version("15-beta").is_err());
        assert!(normalize_pg_version("15.1").is_err());
    }

    #[test]
    fn resolve_pg_version_uses_default() {
        assert_eq!(
            resolve_pg_version(None).unwrap(),
            DEFAULT_PG_VERSION.to_string()
        );
    }

    #[test]
    fn resolve_pg_version_normalizes_value() {
        assert_eq!(resolve_pg_version(Some("postgresql@17")).unwrap(), "17");
    }
}
