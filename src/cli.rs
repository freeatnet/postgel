use clap::{Parser, Subcommand};

use crate::commands::{
    instance::InstanceCommand, project::ProjectCommand, projects::ProjectsCommand,
};

#[derive(Parser)]
#[command(name = "postgel")]
#[command(about = "PostgreSQL project manager for local development")]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Manage instances
    Instance {
        #[command(subcommand)]
        command: InstanceCommand,
    },
    /// Manage the current project
    Project {
        #[command(subcommand)]
        command: ProjectCommand,
    },
    /// Manage all projects
    Projects {
        #[command(subcommand)]
        command: ProjectsCommand,
    },
}

pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Instance { command } => crate::commands::instance::handle(command).await,
        Commands::Project { command } => crate::commands::project::handle(command).await,
        Commands::Projects { command } => crate::commands::projects::handle(command),
    }
}
