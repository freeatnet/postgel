use anyhow::{Context, Result};
use std::path::PathBuf;
use std::process::Command;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PgInstanceError {
    #[error("Postgres binaries not found")]
    BinariesNotFound,
    #[error("Failed to initialize database: {0}")]
    InitFailed(String),
    #[error("Failed to start Postgres: {0}")]
    StartFailed(String),
    #[error("Failed to stop Postgres: {0}")]
    StopFailed(String),
}

pub struct PgInstance {
    pub bin_dir: PathBuf,
    pub data_dir: PathBuf,
    pub run_dir: PathBuf,
    pub port: u16,
}

impl PgInstance {
    pub fn new(bin_dir: PathBuf, data_dir: PathBuf, run_dir: PathBuf, port: u16) -> Self {
        Self {
            bin_dir,
            data_dir,
            run_dir,
            port,
        }
    }

    pub fn initdb(&self) -> Result<()> {
        let initdb_bin = self.bin_dir.join("initdb");
        if !initdb_bin.exists() {
            return Err(PgInstanceError::BinariesNotFound.into());
        }

        if self.data_dir.exists() {
            // Already initialized
            return Ok(());
        }

        std::fs::create_dir_all(&self.data_dir).context("Failed to create data directory")?;
        std::fs::create_dir_all(&self.run_dir).context("Failed to create run directory")?;

        let locale = std::env::var("LC_ALL")
            .or_else(|_| std::env::var("LANG"))
            .unwrap_or_else(|_| "C".to_string());

        let output = Command::new(&initdb_bin)
            .arg("-D")
            .arg(&self.data_dir)
            .arg("--locale")
            .arg(&locale)
            .env("LC_ALL", &locale)
            .env("LANG", &locale)
            .output()
            .context("Failed to run initdb")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(PgInstanceError::InitFailed(stderr.to_string()).into());
        }

        Ok(())
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

    pub fn socket_path(&self) -> PathBuf {
        self.run_dir.join(format!(".s.PGSQL.{}", self.port))
    }
}
