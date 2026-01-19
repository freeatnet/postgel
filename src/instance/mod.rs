pub mod env;
pub mod lifecycle;
pub mod ports;
pub mod postgres;
pub mod proxy;
pub mod service_manager;

pub struct CreateInstanceRequest {
    pub slug: String,
    pub pg_version: Option<String>,
    pub port: Option<u16>,
    pub no_launchd: bool,
}

pub use env::format_env_output;
pub use lifecycle::{
    create_instance, delete_instance_by_slug, find_instance_by_slug, run_instance,
};
