use anyhow::{Context, Result};
use directories::ProjectDirs;
use std::path::Path;
use std::process::Command;

use crate::state::{Instance, Registry};

use super::CreateInstanceRequest;
use super::ports::resolve_port;
use super::postgres::{PostgresInstall, resolve_pg_version};
use super::proxy::ProxyConfig;
use super::service_manager;

pub const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 600;

pub fn find_instance_by_slug(registry: &Registry, slug: &str) -> Result<Instance> {
    registry
        .get_instance(slug)
        .ok_or_else(|| anyhow::anyhow!("Instance not found: {}", slug))
}

pub fn create_instance(registry: &Registry, request: CreateInstanceRequest) -> Result<Instance> {
    if request.slug.is_empty() {
        anyhow::bail!("Instance slug cannot be empty");
    }
    if !request
        .slug
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        anyhow::bail!("Invalid instance slug: {}", request.slug);
    }

    if registry.get_instance(&request.slug).is_some() {
        anyhow::bail!("Instance already exists: {}", request.slug);
    }

    let postgres_version = resolve_pg_version(request.pg_version.as_deref())?;
    let port = resolve_port(registry, request.port)?;

    let dirs = ProjectDirs::from("dev", "postgel", "postgel")
        .context("Failed to determine data directory")?;
    let instance_dir = dirs.data_dir().join("instances").join(&request.slug);
    let data_dir = instance_dir.join("data");
    let run_dir = instance_dir.join("run");

    let pg_install = PostgresInstall::get_or_install(registry, &postgres_version)?;
    initdb(&pg_install, &data_dir, &run_dir)?;

    let instance = Instance {
        slug: request.slug,
        postgres_version,
        data_dir,
        run_dir,
        port,
        created_at: chrono::Utc::now(),
    };

    registry.add_instance(instance.clone())?;

    if !request.no_launchd {
        let manager = service_manager::default_manager().ok_or_else(|| {
            anyhow::anyhow!("launchd is only available on macOS (use --no-launchd)")
        })?;
        let binary_path = std::env::current_exe().context("Failed to locate current binary")?;
        if let Err(err) = manager.install(&binary_path, &instance) {
            let _ = registry.remove_instance(&instance.slug);
            return Err(err);
        }
    }

    Ok(instance)
}

pub async fn run_instance(registry: &Registry, slug: &str, from_launchd: bool) -> Result<()> {
    let instance = find_instance_by_slug(registry, slug)?;
    let pg_install = PostgresInstall::get_or_install(registry, &instance.postgres_version)?;
    initdb(&pg_install, &instance.data_dir, &instance.run_dir)?;

    let config = ProxyConfig {
        postgres_bin_dir: pg_install.bin_dir.clone(),
        postgres_data_dir: instance.data_dir.clone(),
        postgres_run_dir: instance.run_dir.clone(),
        idle_timeout_secs: DEFAULT_IDLE_TIMEOUT_SECS,
        use_launchd: from_launchd,
        port: if from_launchd {
            None
        } else {
            Some(instance.port)
        },
    };

    if from_launchd {
        eprintln!(
            "Running instance {} via launchd socket activation",
            instance.slug
        );
    } else {
        eprintln!(
            "Running instance {} in foreground on port {}",
            instance.slug, instance.port
        );
    }

    super::proxy::run_proxy(config).await
}

pub fn delete_instance_by_slug(registry: &Registry, slug: &str) -> Result<()> {
    let instance = registry
        .get_instance(slug)
        .ok_or_else(|| anyhow::anyhow!("Instance not found: {}", slug))?;

    if let Some(manager) = service_manager::default_manager() {
        let _ = manager.remove(&instance);
    }

    if instance.data_dir.exists() {
        std::fs::remove_dir_all(&instance.data_dir)
            .context("Failed to remove instance data directory")?;
    }

    registry.remove_instance(slug)?;
    Ok(())
}

fn initdb(install: &PostgresInstall, data_dir: &Path, run_dir: &Path) -> Result<()> {
    let initdb_bin = install.initdb_path();
    if !initdb_bin.exists() {
        anyhow::bail!("Postgres binaries not found");
    }

    if data_dir.exists() {
        std::fs::create_dir_all(run_dir).context("Failed to create run directory")?;
        return Ok(());
    }

    std::fs::create_dir_all(data_dir).context("Failed to create data directory")?;
    std::fs::create_dir_all(run_dir).context("Failed to create run directory")?;

    let locale = std::env::var("LC_ALL")
        .or_else(|_| std::env::var("LANG"))
        .unwrap_or_else(|_| "C".to_string());

    let output = Command::new(&initdb_bin)
        .arg("-D")
        .arg(data_dir)
        .arg("--locale")
        .arg(&locale)
        .env("LC_ALL", &locale)
        .env("LANG", &locale)
        .output()
        .context("Failed to run initdb")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("Failed to initialize database: {}", stderr);
    }

    Ok(())
}
