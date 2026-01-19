use clap::Subcommand;
use postgel::state::Registry;

#[derive(Subcommand)]
pub enum ProjectsCommand {
    /// List all project links
    List,
    /// Prune dead project links
    Prune,
}

pub fn handle(cmd: ProjectsCommand) -> anyhow::Result<()> {
    match cmd {
        ProjectsCommand::List => {
            let registry = Registry::load()?;
            let mut links = registry.list_links();
            if links.is_empty() {
                println!("No projects linked");
                return Ok(());
            }

            links.sort_by(|a, b| a.project_path.cmp(&b.project_path));

            println!("Projects:");
            for link in links {
                let instance = registry.get_instance(&link.instance_slug);
                match instance {
                    Some(instance) => {
                        println!(
                            "  {} -> {} (pg {}, port {})",
                            link.project_path.display(),
                            instance.slug,
                            instance.postgres_version,
                            instance.port
                        );
                    }
                    None => {
                        println!("  {} -> <missing instance>", link.project_path.display());
                    }
                }
            }
            Ok(())
        }
        ProjectsCommand::Prune => {
            let registry = Registry::load()?;
            let removed = registry.prune_dead_links()?;
            eprintln!("Removed {} dead link(s)", removed.len());
            Ok(())
        }
    }
}
