use anyhow::Result;

pub const DEFAULT_DB_USER: &str = "postgres";
pub const DEFAULT_DB_NAME: &str = "postgres";

pub fn format_env_output(format: &str, port: u16, db_user: &str, db_name: &str) -> Result<String> {
    let database_url = format!("postgres://{}@127.0.0.1:{}/{}", db_user, port, db_name);

    let output = match format {
        "sh" => format!(
            "export PGHOST=127.0.0.1\nexport PGPORT={}\nexport PGUSER={}\nexport PGDATABASE={}\nexport PGSSLMODE=disable\nexport DATABASE_URL=\"{}\"",
            port, db_user, db_name, database_url
        ),
        "dotenv" => format!(
            "PGHOST=127.0.0.1\nPGPORT={}\nPGUSER={}\nPGDATABASE={}\nPGSSLMODE=disable\nDATABASE_URL={}",
            port, db_user, db_name, database_url
        ),
        "json" => {
            let json = serde_json::json!({
                "PGHOST": "127.0.0.1",
                "PGPORT": port,
                "PGUSER": db_user,
                "PGDATABASE": db_name,
                "PGSSLMODE": "disable",
                "DATABASE_URL": database_url,
            });
            serde_json::to_string_pretty(&json)?
        }
        _ => anyhow::bail!("Unknown format: {}. Supported: sh, dotenv, json", format),
    };

    Ok(output)
}
