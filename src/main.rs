use clap::{Parser, Subcommand};
use postgel::{
    Instance, InstanceId, Link, LinkId, launchd::LaunchdService, pg_install::PgInstall,
    pg_instance::PgInstance, project::ProjectRoot, proxy::ProxyConfig, state::Registry,
};
use std::path::PathBuf;
use std::process;

#[derive(Parser)]
#[command(name = "postgel")]
#[command(about = "PostgreSQL project manager for local development")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Manage projects
    Project {
        #[command(subcommand)]
        command: ProjectCommands,
    },
    /// Manage instances
    Instance {
        #[command(subcommand)]
        command: InstanceCommands,
    },
    /// Run the proxy (for launchd or foreground mode)
    Proxy {
        /// Postgres binaries directory
        #[arg(long)]
        postgres_bin_dir: PathBuf,
        /// Postgres data directory
        #[arg(long)]
        postgres_data_dir: PathBuf,
        /// Postgres run directory for Unix sockets
        #[arg(long)]
        postgres_run_dir: PathBuf,
        /// Idle timeout in seconds before shutting down (default: 600)
        #[arg(long, default_value = "600")]
        idle_timeout_secs: u64,
    },
}

#[derive(Subcommand)]
enum ProjectCommands {
    /// Initialize a new project
    Init {
        /// Don't enable launchd (default: enabled on macOS)
        #[arg(long)]
        no_launchd: bool,
    },
    /// Show project information
    Info,
    /// Output connection environment variables
    Env {
        /// Output format
        #[arg(long, default_value = "sh")]
        format: String,
    },
    /// Unlink project from instance
    Unlink {
        /// Also destroy the linked instance
        #[arg(long)]
        destroy_instance: bool,
    },
    /// Prune dead project links
    Prune,
    /// Enable launchd service for this project
    EnableLaunchd,
    /// Disable launchd service for this project
    DisableLaunchd,
}

#[derive(Subcommand)]
enum InstanceCommands {
    /// List all instances
    List,
    /// Show instance information
    Info {
        /// Instance ID or name
        id_or_name: String,
    },
    /// Run instance in foreground (proxy mode)
    Run {
        /// Instance ID or name
        id_or_name: String,
    },
    /// Delete an instance
    Delete {
        /// Instance ID or name
        id_or_name: String,
        /// Force deletion without confirmation
        #[arg(short, long)]
        force: bool,
    },
    /// Prune orphaned instances
    Prune {
        /// Remove instances with no links
        #[arg(long)]
        orphaned: bool,
    },
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Project { command } => handle_project(command).await,
        Commands::Instance { command } => handle_instance(command).await,
        Commands::Proxy {
            postgres_bin_dir,
            postgres_data_dir,
            postgres_run_dir,
            idle_timeout_secs,
        } => {
            let config = ProxyConfig {
                postgres_bin_dir,
                postgres_data_dir,
                postgres_run_dir,
                idle_timeout_secs,
                use_launchd: true, // Proxy subcommand is for launchd mode
                port: None,        // Port is managed by launchd
            };
            postgel::proxy::run_proxy(config).await?;
            Ok(())
        }
    }
}

async fn handle_project(cmd: ProjectCommands) -> anyhow::Result<()> {
    match cmd {
        ProjectCommands::Init { no_launchd } => {
            let cwd = std::env::current_dir()?;
            let root = ProjectRoot::find_or_create(&cwd)?;
            let _config = root.load_config()?;

            let registry = Registry::load()?;

            // Check if already linked
            if registry.get_link_by_path(root.path()).is_some() {
                eprintln!("Project already linked. Use 'postgel project unlink' to unlink first.");
                return Ok(());
            }

            // Determine Postgres version
            let pg_version = _config
                .postgres_version
                .as_deref()
                .unwrap_or("postgresql@16");
            eprintln!("Installing PostgreSQL {}...", pg_version);

            let pg_install = PgInstall::get_or_install(pg_version)?;

            // Generate instance name and paths
            let instance_name = root.generate_instance_name();
            let instance_id = InstanceId::new();

            let dirs = directories::ProjectDirs::from("dev", "postgel", "postgel")
                .ok_or_else(|| anyhow::anyhow!("Failed to determine data directory"))?;
            let data_dir = dirs.data_dir().join("instances").join(&instance_id.0);
            let run_dir = data_dir.join("run");

            // Find an available port
            let port = find_available_port()?;

            // Create instance
            let pg_instance = PgInstance::new(
                pg_install.bin_dir.clone(),
                data_dir.clone(),
                run_dir.clone(),
                port,
            );

            pg_instance.initdb()?;

            let instance = Instance {
                id: instance_id.clone(),
                display_name: instance_name.clone(),
                postgres_version: pg_version.to_string(),
                data_dir: data_dir.clone(),
                run_dir: run_dir.clone(),
                port,
                created_at: chrono::Utc::now(),
            };

            registry.add_instance(instance.clone())?;

            // Create link
            let link = Link {
                id: LinkId::new(),
                project_path: root.path().to_path_buf(),
                instance_id: instance_id.clone(),
                db_name: "postgres".to_string(),
                db_user: "postgres".to_string(),
                created_at: chrono::Utc::now(),
            };

            registry.add_link(link)?;

            eprintln!("Project initialized!");
            eprintln!("Instance: {} (port {})", instance_name, port);

            // Enable launchd by default on macOS
            #[cfg(target_os = "macos")]
            if !no_launchd {
                enable_launchd_for_instance(&registry, &instance_id, &instance).await?;
            }

            Ok(())
        }
        ProjectCommands::Info => {
            let cwd = std::env::current_dir()?;
            let root = ProjectRoot::find(&cwd)?;
            let _config = root.load_config()?;

            let registry = Registry::load()?;
            let link = registry
                .get_link_by_path(root.path())
                .ok_or_else(|| anyhow::anyhow!("Project not linked"))?;

            let instance = registry
                .get_instance(&link.instance_id)
                .ok_or_else(|| anyhow::anyhow!("Instance not found"))?;

            println!("Project root: {}", root.path().display());
            println!(
                "Linked instance: {} ({})",
                instance.display_name, instance.id.0
            );
            println!("PostgreSQL version: {}", instance.postgres_version);
            println!("Port: {}", instance.port);
            println!("Database: {}", link.db_name);
            println!("User: {}", link.db_user);
            println!("Data directory: {}", instance.data_dir.display());
            println!("Run directory: {}", instance.run_dir.display());

            Ok(())
        }
        ProjectCommands::Env { format } => {
            let cwd = std::env::current_dir()?;
            let root = ProjectRoot::find(&cwd)?;

            let registry = Registry::load()?;
            let link = registry
                .get_link_by_path(root.path())
                .ok_or_else(|| anyhow::anyhow!("Project not linked"))?;

            let instance = registry
                .get_instance(&link.instance_id)
                .ok_or_else(|| anyhow::anyhow!("Instance not found"))?;

            let database_url = format!(
                "postgres://{}@127.0.0.1:{}/{}",
                link.db_user, instance.port, link.db_name
            );

            match format.as_str() {
                "sh" => {
                    println!("export PGHOST=127.0.0.1");
                    println!("export PGPORT={}", instance.port);
                    println!("export PGUSER={}", link.db_user);
                    println!("export PGDATABASE={}", link.db_name);
                    println!("export PGSSLMODE=disable");
                    println!("export DATABASE_URL=\"{}\"", database_url);
                }
                "dotenv" => {
                    println!("PGHOST=127.0.0.1");
                    println!("PGPORT={}", instance.port);
                    println!("PGUSER={}", link.db_user);
                    println!("PGDATABASE={}", link.db_name);
                    println!("PGSSLMODE=disable");
                    println!("DATABASE_URL={}", database_url);
                }
                "json" => {
                    let json = serde_json::json!({
                        "PGHOST": "127.0.0.1",
                        "PGPORT": instance.port,
                        "PGUSER": link.db_user,
                        "PGDATABASE": link.db_name,
                        "PGSSLMODE": "disable",
                        "DATABASE_URL": database_url,
                    });
                    println!("{}", serde_json::to_string_pretty(&json)?);
                }
                _ => {
                    anyhow::bail!("Unknown format: {}. Supported: sh, dotenv, json", format);
                }
            }

            Ok(())
        }
        ProjectCommands::Unlink { destroy_instance } => {
            let cwd = std::env::current_dir()?;
            let root = ProjectRoot::find(&cwd)?;

            let registry = Registry::load()?;
            let link = registry
                .get_link_by_path(root.path())
                .ok_or_else(|| anyhow::anyhow!("Project not linked"))?;

            if destroy_instance {
                let instance = registry
                    .get_instance(&link.instance_id)
                    .ok_or_else(|| anyhow::anyhow!("Instance not found"))?;

                // Remove launchd service
                #[cfg(target_os = "macos")]
                {
                    let label = format!("dev.postgel.{}", instance.id.0);
                    let service = LaunchdService::new(label);
                    let _ = service.remove();
                }

                // Remove instance data
                if instance.data_dir.exists() {
                    std::fs::remove_dir_all(&instance.data_dir)?;
                }

                registry.remove_instance(&link.instance_id)?;
            }

            registry.remove_link(&link.id)?;
            eprintln!("Project unlinked");

            Ok(())
        }
        ProjectCommands::Prune => {
            let registry = Registry::load()?;
            let removed = registry.prune_dead_links()?;
            eprintln!("Removed {} dead link(s)", removed.len());
            Ok(())
        }
        ProjectCommands::EnableLaunchd => {
            let cwd = std::env::current_dir()?;
            let root = ProjectRoot::find(&cwd)?;

            let registry = Registry::load()?;
            let link = registry
                .get_link_by_path(root.path())
                .ok_or_else(|| anyhow::anyhow!("Project not linked"))?;

            let instance = registry
                .get_instance(&link.instance_id)
                .ok_or_else(|| anyhow::anyhow!("Instance not found"))?;

            #[cfg(target_os = "macos")]
            {
                enable_launchd_for_instance(&registry, &link.instance_id, &instance).await?;
            }

            #[cfg(not(target_os = "macos"))]
            {
                anyhow::bail!("launchd is only available on macOS");
            }

            Ok(())
        }
        ProjectCommands::DisableLaunchd => {
            let cwd = std::env::current_dir()?;
            let root = ProjectRoot::find(&cwd)?;

            let registry = Registry::load()?;
            let link = registry
                .get_link_by_path(root.path())
                .ok_or_else(|| anyhow::anyhow!("Project not linked"))?;

            let instance = registry
                .get_instance(&link.instance_id)
                .ok_or_else(|| anyhow::anyhow!("Instance not found"))?;

            #[cfg(target_os = "macos")]
            {
                let label = format!("dev.postgel.{}", instance.id.0);
                let service = LaunchdService::new(label);
                service.remove()?;
                eprintln!("Launchd service disabled");
            }

            #[cfg(not(target_os = "macos"))]
            {
                anyhow::bail!("launchd is only available on macOS");
            }

            Ok(())
        }
    }
}

async fn handle_instance(cmd: InstanceCommands) -> anyhow::Result<()> {
    match cmd {
        InstanceCommands::List => {
            let registry = Registry::load()?;
            let instances = registry.list_instances();
            let links = registry.list_links();

            println!("Instances:");
            for instance in instances {
                let link_count = links
                    .iter()
                    .filter(|l| l.instance_id == instance.id)
                    .count();
                println!(
                    "  {} ({}) - port {}, {} link(s)",
                    instance.display_name, instance.id.0, instance.port, link_count
                );
            }
            Ok(())
        }
        InstanceCommands::Info { id_or_name } => {
            let registry = Registry::load()?;
            let instance = find_instance(&registry, &id_or_name)?;

            println!("Instance: {}", instance.display_name);
            println!("ID: {}", instance.id.0);
            println!("PostgreSQL version: {}", instance.postgres_version);
            println!("Port: {}", instance.port);
            println!("Data directory: {}", instance.data_dir.display());
            println!("Run directory: {}", instance.run_dir.display());

            let links = registry.list_links();
            let instance_links: Vec<_> = links
                .into_iter()
                .filter(|l| l.instance_id == instance.id)
                .collect();

            if !instance_links.is_empty() {
                println!("\nLinked projects:");
                for link in instance_links {
                    println!("  {}", link.project_path.display());
                }
            }

            Ok(())
        }
        InstanceCommands::Run { id_or_name } => {
            let registry = Registry::load()?;
            let instance = find_instance(&registry, &id_or_name)?;

            // Get Postgres installation
            let pg_install = PgInstall::get_or_install(&instance.postgres_version)?;

            let pg_instance = PgInstance::new(
                pg_install.bin_dir.clone(),
                instance.data_dir.clone(),
                instance.run_dir.clone(),
                instance.port,
            );

            // Ensure Postgres is initialized
            pg_instance.initdb()?;

            let config = ProxyConfig {
                postgres_bin_dir: pg_instance.bin_dir.clone(),
                postgres_data_dir: pg_instance.data_dir.clone(),
                postgres_run_dir: pg_instance.run_dir.clone(),
                idle_timeout_secs: 600,
                use_launchd: false, // Foreground mode
                port: Some(instance.port),
            };

            eprintln!(
                "Running proxy in foreground mode on port {}...",
                instance.port
            );
            postgel::proxy::run_proxy(config).await?;
            Ok(())
        }
        InstanceCommands::Delete { id_or_name, force } => {
            let registry = Registry::load()?;
            let instance = find_instance(&registry, &id_or_name)?;

            let links = registry.list_links();
            let instance_links: Vec<_> = links
                .into_iter()
                .filter(|l| l.instance_id == instance.id)
                .collect();

            if !instance_links.is_empty() {
                eprintln!(
                    "Warning: This instance is linked to {} project(s):",
                    instance_links.len()
                );
                for link in &instance_links {
                    eprintln!("  {}", link.project_path.display());
                }
            }

            if !force {
                eprint!("Delete instance {}? [y/N]: ", instance.display_name);
                use std::io::{self, Write};
                io::stdout().flush()?;
                let mut input = String::new();
                io::stdin().read_line(&mut input)?;
                if !input.trim().eq_ignore_ascii_case("y") {
                    eprintln!("Cancelled");
                    return Ok(());
                }
            }

            // Remove launchd service
            #[cfg(target_os = "macos")]
            {
                let label = format!("dev.postgel.{}", instance.id.0);
                let service = LaunchdService::new(label);
                let _ = service.remove();
            }

            // Remove instance data
            if instance.data_dir.exists() {
                std::fs::remove_dir_all(&instance.data_dir)?;
            }

            // Remove links
            for link in instance_links {
                registry.remove_link(&link.id)?;
            }

            registry.remove_instance(&instance.id)?;
            eprintln!("Instance deleted");

            Ok(())
        }
        InstanceCommands::Prune { orphaned } => {
            let registry = Registry::load()?;
            let instances = registry.list_instances();
            let links = registry.list_links();

            let mut removed = 0;
            for instance in instances {
                let link_count = links
                    .iter()
                    .filter(|l| l.instance_id == instance.id)
                    .count();

                if orphaned && link_count == 0 {
                    if instance.data_dir.exists() {
                        std::fs::remove_dir_all(&instance.data_dir)?;
                    }
                    registry.remove_instance(&instance.id)?;
                    removed += 1;
                } else if !instance.data_dir.exists() {
                    // Instance data directory is missing
                    registry.remove_instance(&instance.id)?;
                    removed += 1;
                }
            }

            eprintln!("Removed {} instance(s)", removed);
            Ok(())
        }
    }
}

fn find_instance(registry: &Registry, id_or_name: &str) -> anyhow::Result<Instance> {
    // Try as ID first
    let instance_id = InstanceId(id_or_name.to_string());
    if let Some(instance) = registry.get_instance(&instance_id) {
        return Ok(instance);
    }

    // Try as name
    if let Some(instance) = registry.find_instance_by_name(id_or_name) {
        return Ok(instance);
    }

    anyhow::bail!("Instance not found: {}", id_or_name);
}

fn find_available_port() -> anyhow::Result<u16> {
    use std::net::TcpListener;

    // Try ports starting from 5432
    for port in 5432..65535 {
        if TcpListener::bind(format!("127.0.0.1:{}", port)).is_ok() {
            // Port is available
            return Ok(port);
        }
    }

    anyhow::bail!("No available port found");
}

#[cfg(target_os = "macos")]
async fn enable_launchd_for_instance(
    _registry: &Registry,
    _instance_id: &InstanceId,
    instance: &Instance,
) -> anyhow::Result<()> {
    use std::env;

    let label = format!("dev.postgel.{}", instance.id.0);
    let service = LaunchdService::new(label.clone());

    // Get the binary path
    let binary_path = env::current_exe()?;

    // Get Postgres bin dir from instance
    // This is a bit of a hack - we need to reconstruct it
    let pg_install = PgInstall::get_or_install(&instance.postgres_version)?;

    service.install(
        &binary_path,
        &pg_install.bin_dir,
        &instance.data_dir,
        &instance.run_dir,
        instance.port,
        600,
    )?;

    eprintln!("Launchd service enabled: {}", label);
    Ok(())
}
