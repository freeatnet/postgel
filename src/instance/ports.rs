use crate::state::Registry;
use anyhow::Result;

pub const DEFAULT_PORT: u16 = 15432;

pub fn resolve_port(registry: &Registry, requested: Option<u16>) -> Result<u16> {
    let instances = registry.list_instances();
    if let Some(port) = requested {
        if instances.iter().any(|inst| inst.port == port) {
            anyhow::bail!("Port {} is already in use", port);
        }
        return Ok(port);
    }

    let max_port = instances.iter().map(|inst| inst.port).max();
    match max_port {
        None => Ok(DEFAULT_PORT),
        Some(port) => {
            if port == u16::MAX {
                anyhow::bail!("No available port found");
            }
            Ok(port + 1)
        }
    }
}
