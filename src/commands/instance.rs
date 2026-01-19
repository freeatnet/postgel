use clap::{Args, Subcommand};
use postgel::instance::env::{DEFAULT_DB_NAME, DEFAULT_DB_USER};
use postgel::instance::{CreateInstanceRequest, format_env_output};
use postgel::state::Registry;

#[derive(Subcommand)]
pub enum InstanceCommand {
    /// Create a new instance
    Create(InstanceCreateArgs),
    /// List all instances
    List,
    /// Delete an instance
    Delete(InstanceDeleteArgs),
    /// Run an instance in the foreground or via launchd
    Run(InstanceRunArgs),
    /// Output connection environment variables
    Env(InstanceEnvArgs),
}

#[derive(Args)]
pub struct InstanceCreateArgs {
    /// Instance slug (alpha-numeric, dash, underscore)
    pub slug: String,
    /// Postgres version (default: 18)
    #[arg(long)]
    pub pg_version: Option<String>,
    /// Listening port
    #[arg(long)]
    pub port: Option<u16>,
    /// Don't enable launchd (default: enabled on macOS)
    #[arg(long)]
    pub no_launchd: bool,
}

#[derive(Args)]
pub struct InstanceDeleteArgs {
    /// Instance slug
    pub slug: String,
}

#[derive(Args)]
pub struct InstanceRunArgs {
    /// Instance slug
    pub slug: String,
    /// Use launchd socket activation
    #[arg(long)]
    pub from_launchd: bool,
}

#[derive(Args)]
pub struct InstanceEnvArgs {
    /// Instance slug
    pub slug: String,
    /// Output format
    #[arg(long, default_value = "sh")]
    pub format: String,
}

pub async fn handle(cmd: InstanceCommand) -> anyhow::Result<()> {
    match cmd {
        InstanceCommand::Create(args) => {
            let registry = Registry::load()?;
            let request = CreateInstanceRequest {
                slug: args.slug,
                pg_version: args.pg_version,
                port: args.port,
                no_launchd: args.no_launchd,
            };
            let instance = postgel::instance::create_instance(&registry, request)?;
            eprintln!(
                "Instance created: {} (port {})",
                instance.slug, instance.port
            );
            Ok(())
        }
        InstanceCommand::List => {
            let registry = Registry::load()?;
            let instances = registry.list_instances();
            let links = registry.list_links();

            if instances.is_empty() {
                println!("No instances found");
                return Ok(());
            }

            println!("Instances:");
            for instance in instances {
                let link_count = links
                    .iter()
                    .filter(|link| link.instance_slug == instance.slug)
                    .count();
                println!(
                    "  {} - port {}, {} link(s)",
                    instance.slug, instance.port, link_count
                );
            }
            Ok(())
        }
        InstanceCommand::Delete(args) => {
            let registry = Registry::load()?;
            postgel::instance::delete_instance_by_slug(&registry, &args.slug)?;
            eprintln!("Instance deleted: {}", args.slug);
            Ok(())
        }
        InstanceCommand::Run(args) => {
            let registry = Registry::load()?;
            postgel::instance::run_instance(&registry, &args.slug, args.from_launchd).await
        }
        InstanceCommand::Env(args) => {
            let registry = Registry::load()?;
            let instance = postgel::instance::find_instance_by_slug(&registry, &args.slug)?;
            let output = format_env_output(
                &args.format,
                instance.port,
                DEFAULT_DB_USER,
                DEFAULT_DB_NAME,
            )?;
            println!("{}", output);
            Ok(())
        }
    }
}
