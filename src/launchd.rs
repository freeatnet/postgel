use anyhow::{Context, Result};
use plist::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

pub struct LaunchdService {
    label: String,
    plist_path: PathBuf,
}

impl LaunchdService {
    pub fn new(label: String) -> Self {
        let plist_path = PathBuf::from(std::env::var("HOME").unwrap())
            .join("Library/LaunchAgents")
            .join(format!("{}.plist", label));
        Self { label, plist_path }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn install(
        &self,
        binary_path: &Path,
        socket_name: &str,
        postgres_bin_dir: &Path,
        postgres_data_dir: &Path,
        postgres_run_dir: &Path,
        port: u16,
        idle_timeout_secs: u64,
    ) -> Result<()> {
        // Create LaunchAgents directory if it doesn't exist
        if let Some(parent) = self.plist_path.parent() {
            fs::create_dir_all(parent).context("Failed to create LaunchAgents directory")?;
        }

        let mut dict = plist::Dictionary::new();

        // Label
        dict.insert("Label".to_string(), Value::String(self.label.clone()));

        // ProgramArguments
        let args = vec![
            binary_path.to_string_lossy().to_string(),
            "proxy".to_string(),
            "--socket-name".to_string(),
            socket_name.to_string(),
            "--postgres-bin-dir".to_string(),
            postgres_bin_dir.to_string_lossy().to_string(),
            "--postgres-data-dir".to_string(),
            postgres_data_dir.to_string_lossy().to_string(),
            "--postgres-run-dir".to_string(),
            postgres_run_dir.to_string_lossy().to_string(),
            "--idle-timeout-secs".to_string(),
            idle_timeout_secs.to_string(),
        ];

        dict.insert(
            "ProgramArguments".to_string(),
            Value::Array(args.into_iter().map(Value::String).collect()),
        );

        // Sockets
        let mut socket_array = Vec::new();

        // IPv4 socket
        let mut ipv4_socket = plist::Dictionary::new();
        ipv4_socket.insert(
            "SockNodeName".to_string(),
            Value::String("127.0.0.1".to_string()),
        );
        ipv4_socket.insert(
            "SockServiceName".to_string(),
            Value::String(port.to_string()),
        );
        ipv4_socket.insert("SockType".to_string(), Value::String("stream".to_string()));
        ipv4_socket.insert("SockFamily".to_string(), Value::String("IPv4".to_string()));
        socket_array.push(Value::Dictionary(ipv4_socket));

        // IPv6 socket
        let mut ipv6_socket = plist::Dictionary::new();
        ipv6_socket.insert("SockNodeName".to_string(), Value::String("::1".to_string()));
        ipv6_socket.insert(
            "SockServiceName".to_string(),
            Value::String(port.to_string()),
        );
        ipv6_socket.insert("SockType".to_string(), Value::String("stream".to_string()));
        ipv6_socket.insert("SockFamily".to_string(), Value::String("IPv6".to_string()));
        socket_array.push(Value::Dictionary(ipv6_socket));

        let mut socket_dict = plist::Dictionary::new();
        socket_dict.insert(socket_name.to_string(), Value::Array(socket_array));
        dict.insert("Sockets".to_string(), Value::Dictionary(socket_dict));

        // RunAtLoad: false (don't start on login)
        dict.insert("RunAtLoad".to_string(), Value::Boolean(false));

        // KeepAlive: false (don't restart automatically)
        dict.insert("KeepAlive".to_string(), Value::Boolean(false));

        // StandardOutPath and StandardErrorPath
        let log_dir = PathBuf::from("/tmp");
        dict.insert(
            "StandardOutPath".to_string(),
            Value::String(
                log_dir
                    .join(format!("{}.stdout.log", self.label))
                    .to_string_lossy()
                    .to_string(),
            ),
        );
        dict.insert(
            "StandardErrorPath".to_string(),
            Value::String(
                log_dir
                    .join(format!("{}.stderr.log", self.label))
                    .to_string_lossy()
                    .to_string(),
            ),
        );

        let plist = Value::Dictionary(dict);

        // Write plist to file
        plist::to_file_xml(&self.plist_path, &plist).context("Failed to write plist file")?;

        // Load the service
        self.load()
    }

    pub fn load(&self) -> Result<()> {
        // Unload first if it exists
        let _ = self.unload();

        let output = Command::new("launchctl")
            .arg("bootstrap")
            .arg(format!("gui/{}", users::get_current_uid()))
            .arg(&self.plist_path)
            .output()
            .context("Failed to run launchctl bootstrap")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Failed to load LaunchAgent: {}", stderr);
        }

        Ok(())
    }

    pub fn unload(&self) -> Result<()> {
        let output = Command::new("launchctl")
            .arg("bootout")
            .arg(format!("gui/{}", users::get_current_uid()))
            .arg(&self.label)
            .output();

        match output {
            Ok(output) if output.status.success() => Ok(()),
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                // Ignore "no such service" errors
                if stderr.contains("No such process") || stderr.contains("Could not find") {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Failed to unload LaunchAgent: {}", stderr))
                }
            }
            Err(e) => Err(anyhow::anyhow!("Failed to run launchctl bootout: {}", e)),
        }
    }

    pub fn remove(&self) -> Result<()> {
        self.unload()?;
        if self.plist_path.exists() {
            fs::remove_file(&self.plist_path).context("Failed to remove plist file")?;
        }
        Ok(())
    }

    pub fn plist_path(&self) -> &Path {
        &self.plist_path
    }
}
