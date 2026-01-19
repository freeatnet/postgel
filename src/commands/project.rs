use clap::{Args, Subcommand};
use postgel::instance::env::{DEFAULT_DB_NAME, DEFAULT_DB_USER};
use postgel::instance::postgres::{normalize_pg_version, resolve_pg_version};
use postgel::instance::{CreateInstanceRequest, format_env_output};
use postgel::project::{ProjectConfig, ProjectRoot};
use postgel::state::{Link, LinkId, Registry};

#[derive(Subcommand)]
pub enum ProjectCommand {
    /// Initialize a new project
    Init(ProjectInitArgs),
    /// Link the current project to an existing instance
    Link(ProjectLinkArgs),
    /// Unlink the current project
    Unlink(ProjectUnlinkArgs),
    /// Show project information
    Info,
    /// Output connection environment variables
    Env(ProjectEnvArgs),
}

#[derive(Args)]
pub struct ProjectInitArgs {
    /// Instance slug (alpha-numeric, dash, underscore)
    pub slug: String,
    /// Postgres version (default: 18)
    #[arg(long, alias = "instance-pg-version")]
    pub pg_version: Option<String>,
    /// Listening port
    #[arg(long, alias = "instance-port")]
    pub port: Option<u16>,
    /// Don't enable launchd (default: enabled on macOS)
    #[arg(long, alias = "instance-no-launchd")]
    pub no_launchd: bool,
}

#[derive(Args)]
pub struct ProjectLinkArgs {
    /// Instance slug
    pub slug: String,
}

#[derive(Args)]
pub struct ProjectUnlinkArgs {
    /// Also destroy the linked instance
    #[arg(long)]
    pub destroy_instance: bool,
}

#[derive(Args)]
pub struct ProjectEnvArgs {
    /// Output format
    #[arg(long, default_value = "sh")]
    pub format: String,
}

pub async fn handle(cmd: ProjectCommand) -> anyhow::Result<()> {
    match cmd {
        ProjectCommand::Init(args) => {
            let cwd = std::env::current_dir()?;
            let root = match ProjectRoot::find(&cwd) {
                Ok(found) => found,
                Err(_) => ProjectRoot::from_path(cwd),
            };

            let registry = Registry::load()?;
            if registry.get_link_by_path(root.path()).is_some() {
                anyhow::bail!("Project already linked. Use 'postgel project unlink' first.");
            }

            let config_path = root.config_path();
            let (postgres_version, wrote_config) = if config_path.exists() {
                let config = root.load_config()?;
                let config_version = normalize_pg_version(&config.require_postgres_version()?)?;
                if let Some(flag_version) = args.pg_version.as_deref() {
                    let normalized = normalize_pg_version(flag_version)?;
                    if normalized != config_version {
                        anyhow::bail!(
                            "Config requires PostgreSQL {}, but {} was requested",
                            config_version,
                            normalized
                        );
                    }
                }
                (config_version, false)
            } else {
                let version = resolve_pg_version(args.pg_version.as_deref())?;
                let config = ProjectConfig::new(version.clone());
                root.save_config(&config)?;
                (version, true)
            };

            let instance = postgel::instance::create_instance(
                &registry,
                CreateInstanceRequest {
                    slug: args.slug,
                    pg_version: Some(postgres_version),
                    port: args.port,
                    no_launchd: args.no_launchd,
                },
            )?;

            let link = Link {
                id: LinkId::new(),
                project_path: root.path().to_path_buf(),
                instance_slug: instance.slug.clone(),
                db_name: DEFAULT_DB_NAME.to_string(),
                db_user: DEFAULT_DB_USER.to_string(),
                created_at: chrono::Utc::now(),
            };
            registry.add_link(link)?;

            if wrote_config {
                eprintln!("Created postgel.toml");
            }
            eprintln!("Project initialized!");
            eprintln!("Instance: {} (port {})", instance.slug, instance.port);
            Ok(())
        }
        ProjectCommand::Link(args) => {
            let cwd = std::env::current_dir()?;
            let root = ProjectRoot::find(&cwd)?;
            let config = root.load_config()?;
            let required_version = normalize_pg_version(&config.require_postgres_version()?)?;

            let registry = Registry::load()?;
            if registry.get_link_by_path(root.path()).is_some() {
                anyhow::bail!("Project already linked. Use 'postgel project unlink' first.");
            }

            let instance = postgel::instance::find_instance_by_slug(&registry, &args.slug)?;
            let instance_version = normalize_pg_version(&instance.postgres_version)?;
            if instance_version != required_version {
                anyhow::bail!(
                    "Instance PostgreSQL {} does not match project requirement {}",
                    instance.postgres_version,
                    required_version
                );
            }

            let link = Link {
                id: LinkId::new(),
                project_path: root.path().to_path_buf(),
                instance_slug: instance.slug.clone(),
                db_name: DEFAULT_DB_NAME.to_string(),
                db_user: DEFAULT_DB_USER.to_string(),
                created_at: chrono::Utc::now(),
            };
            registry.add_link(link)?;
            eprintln!("Project linked to instance {}", instance.slug);
            Ok(())
        }
        ProjectCommand::Unlink(args) => {
            let cwd = std::env::current_dir()?;
            let root = ProjectRoot::find(&cwd)?;
            let registry = Registry::load()?;
            let link = registry
                .get_link_by_path(root.path())
                .ok_or_else(|| anyhow::anyhow!("Project not linked"))?;

            if args.destroy_instance {
                postgel::instance::delete_instance_by_slug(&registry, &link.instance_slug)?;
            } else {
                registry.remove_link(&link.id)?;
            }

            eprintln!("Project unlinked");
            Ok(())
        }
        ProjectCommand::Info => {
            let cwd = std::env::current_dir()?;
            let root = ProjectRoot::find(&cwd)?;
            let registry = Registry::load()?;
            let link = registry
                .get_link_by_path(root.path())
                .ok_or_else(|| anyhow::anyhow!("Project not linked"))?;
            let instance = registry
                .get_instance(&link.instance_slug)
                .ok_or_else(|| anyhow::anyhow!("Instance not found"))?;

            println!("Project root: {}", root.path().display());
            println!("Linked instance: {}", instance.slug);
            println!("PostgreSQL version: {}", instance.postgres_version);
            println!("Port: {}", instance.port);
            println!("Database: {}", link.db_name);
            println!("User: {}", link.db_user);
            println!("Data directory: {}", instance.data_dir.display());
            println!("Run directory: {}", instance.run_dir.display());
            Ok(())
        }
        ProjectCommand::Env(args) => {
            let cwd = std::env::current_dir()?;
            let root = ProjectRoot::find(&cwd)?;
            let registry = Registry::load()?;
            let link = registry
                .get_link_by_path(root.path())
                .ok_or_else(|| anyhow::anyhow!("Project not linked"))?;
            let instance = registry
                .get_instance(&link.instance_slug)
                .ok_or_else(|| anyhow::anyhow!("Instance not found"))?;

            let output =
                format_env_output(&args.format, instance.port, &link.db_user, &link.db_name)?;
            println!("{}", output);
            Ok(())
        }
    }
}
