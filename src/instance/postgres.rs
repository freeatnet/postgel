use anyhow::{Context, Result};
use directories::ProjectDirs;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

pub const DEFAULT_PG_VERSION: &str = "18";

pub struct PostgresInstall {
    pub version: String,
    pub bin_dir: PathBuf,
}

impl PostgresInstall {
    pub fn get_or_install(version: &str) -> Result<Self> {
        let normalized = normalize_pg_version(version)?;
        let dirs = ProjectDirs::from("dev", "postgel", "postgel")
            .context("Failed to determine data directory")?;
        let data_dir = dirs.data_dir();
        let install_dir = data_dir.join("pg").join(&normalized);
        let bin_dir = install_dir.join("bin");

        if bin_dir.exists() && bin_dir.join("postgres").exists() {
            Ok(Self {
                version: normalized.to_string(),
                bin_dir,
            })
        } else {
            Self::install(&normalized, &install_dir)
        }
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

    fn install(normalized: &str, install_dir: &PathBuf) -> Result<Self> {
        let formula = Self::formula_name(normalized);
        if install_dir.exists() && install_dir.join("bin").join("postgres").exists() {
            let bin_dir = install_dir.join("bin");
            return Ok(Self {
                version: normalized.to_string(),
                bin_dir,
            });
        }

        eprintln!("Fetching PostgreSQL {} bottle from Homebrew...", formula);
        let output = Command::new("brew")
            .arg("fetch")
            .arg("--force-bottle")
            .arg(&formula)
            .output()
            .context("Failed to run brew fetch. Is Homebrew installed?")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("brew fetch failed: {}", stderr);
        }

        let cache_output = Command::new("brew")
            .arg("--cache")
            .arg(&formula)
            .output()
            .context("Failed to get brew cache path")?;

        if !cache_output.status.success() {
            anyhow::bail!("Failed to get brew cache path");
        }

        let cache_path = String::from_utf8_lossy(&cache_output.stdout)
            .trim()
            .to_string();

        let bottle_path = if cache_path.ends_with(".tar.gz") {
            PathBuf::from(cache_path)
        } else {
            for ext in &[".tar.gz", ".tar.xz"] {
                let candidate = format!("{}{}", cache_path, ext);
                if Path::new(&candidate).exists() {
                    return Self::extract_bottle(
                        Path::new(&candidate),
                        install_dir,
                        &formula,
                        normalized,
                    );
                }
            }
            anyhow::bail!("Could not find bottle file for {}", formula);
        };

        Self::extract_bottle(&bottle_path, install_dir, &formula, normalized)
    }

    fn extract_bottle(
        bottle_path: &Path,
        install_dir: &PathBuf,
        formula: &str,
        normalized: &str,
    ) -> Result<Self> {
        eprintln!("Extracting bottle to {}...", install_dir.display());
        fs::create_dir_all(install_dir).context("Failed to create install directory")?;

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

        let mut bin_dir = None;
        let candidate = install_dir.join(format!("{}/bin", formula));
        if candidate.exists() {
            bin_dir = Some(candidate);
        } else {
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

        eprintln!("Fetching dependencies...");
        let deps_output = Command::new("brew")
            .arg("deps")
            .arg("--include-optional")
            .arg("--skip-recommended")
            .arg(formula)
            .output()
            .context("Failed to get dependencies")?;

        if deps_output.status.success() {
            let deps = String::from_utf8_lossy(&deps_output.stdout);
            for dep in deps.lines() {
                let dep = dep.trim();
                if !dep.is_empty() {
                    let _ = Command::new("brew")
                        .arg("fetch")
                        .arg("--force-bottle")
                        .arg(dep)
                        .output();
                }
            }
        }

        Ok(Self {
            version: normalized.to_string(),
            bin_dir,
        })
    }

    fn formula_name(normalized: &str) -> String {
        format!("postgresql@{}", normalized)
    }
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

    if !normalized.chars().all(|c| c.is_ascii_digit() || c == '.') {
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
    fn normalize_pg_version_accepts_numeric_versions() {
        assert_eq!(normalize_pg_version("18").unwrap(), "18");
        assert_eq!(normalize_pg_version("16.1").unwrap(), "16.1");
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
