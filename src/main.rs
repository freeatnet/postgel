use clap::Parser;
use futures::FutureExt;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use std::os::unix::io::{FromRawFd, RawFd};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixStream};
use tokio::process::Command;
use tokio::sync::{Notify, broadcast, watch};
use tokio::time::{Duration, sleep};

#[derive(Parser, Debug)]
#[command(name = "postgel-proxy")]
#[command(about = "PostgreSQL proxy with launchd socket activation")]
struct Cli {
    /// Socket name for launchd activation
    #[arg(short, long)]
    socket_name: String,

    /// Backend Unix socket path (required unless using managed Postgres)
    #[arg(short, long)]
    backend_socket: Option<PathBuf>,

    /// Postgres binaries directory (enables managed Postgres mode)
    /// Should contain 'postgres' and 'pg_isready' binaries
    #[arg(long)]
    postgres_bin_dir: Option<PathBuf>,

    /// Postgres data directory (required if --postgres-bin-dir is set)
    #[arg(long)]
    postgres_data_dir: Option<PathBuf>,

    /// Postgres run directory for Unix sockets (required if --postgres-bin-dir is set)
    #[arg(long)]
    postgres_run_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();

    // Determine if we're in managed-postgres mode
    let managed_postgres = cli.postgres_bin_dir.is_some()
        || cli.postgres_data_dir.is_some()
        || cli.postgres_run_dir.is_some();

    if managed_postgres {
        // Validate all Postgres args are provided
        if cli.postgres_bin_dir.is_none()
            || cli.postgres_data_dir.is_none()
            || cli.postgres_run_dir.is_none()
        {
            eprintln!(
                "Error: --postgres-bin-dir, --postgres-data-dir, and --postgres-run-dir must all be provided together"
            );
            std::process::exit(1);
        }
    }

    // Determine backend socket path
    let backend_path = if let Some(ref path) = cli.backend_socket {
        path.clone()
    } else if managed_postgres {
        // Default to <run_dir>/.s.PGSQL.5432
        let run_dir = cli.postgres_run_dir.as_ref().unwrap();
        run_dir.join(".s.PGSQL.5432")
    } else {
        eprintln!("Error: --backend-socket is required when not using managed Postgres");
        std::process::exit(1);
    };

    // We leak `backend_path` instead of wrapping it in an Arc to share it with future tasks since
    // `backend_path` is going to live for the lifetime of the server in all cases.
    // (This reduces MESI/MOESI cache traffic between CPU cores.)
    let backend_path_str = backend_path.to_string_lossy().to_string();
    let backend_path: &str = Box::leak(backend_path_str.into_boxed_str());

    // Check if running on macOS (launchd is macOS-only)
    #[cfg(not(target_os = "macos"))]
    {
        eprintln!("Error: launchd socket activation is only available on macOS");
        std::process::exit(1);
    }

    // Activate the socket using raunch
    #[cfg(target_os = "macos")]
    {
        let fds = raunch::activate_socket(&cli.socket_name)
            .map_err(|e| format!("Failed to activate socket '{}': {}", cli.socket_name, e))?;

        if fds.is_empty() {
            eprintln!(
                "No file descriptors returned for socket '{}'",
                cli.socket_name
            );
            std::process::exit(1);
        }

        // Create shutdown signal and active connection tracking
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let active_connections = Arc::new(AtomicUsize::new(0));
        let active_changed = Arc::new(Notify::new());
        let postgres_exited_unexpectedly = Arc::new(AtomicBool::new(false));

        // Spawn Postgres if in managed mode
        let mut postgres_child = None;
        if managed_postgres {
            let postgres_bin_dir = cli.postgres_bin_dir.as_ref().unwrap();
            let postgres_data_dir = cli.postgres_data_dir.as_ref().unwrap();
            let postgres_run_dir = cli.postgres_run_dir.as_ref().unwrap();

            // Construct paths to postgres binaries
            let postgres_bin = postgres_bin_dir.join("postgres");
            let pg_isready_bin = postgres_bin_dir.join("pg_isready");

            eprintln!("Starting managed Postgres instance...");

            // Set locale environment variables to prevent Postgres multithreading error
            // Postgres requires these to be set when spawned from a multithreaded process
            let locale = std::env::var("LC_ALL")
                .or_else(|_| std::env::var("LANG"))
                .unwrap_or_else(|_| "C".to_string());

            let child = Command::new(&postgres_bin)
                .arg("-D")
                .arg(postgres_data_dir)
                .arg("-c")
                .arg("listen_addresses=")
                .arg("-c")
                .arg(format!(
                    "unix_socket_directories={}",
                    postgres_run_dir.display()
                ))
                .arg("-k")
                .arg(postgres_run_dir)
                .env("LC_ALL", &locale)
                .env("LANG", &locale)
                .stderr(std::process::Stdio::inherit())
                .stdout(std::process::Stdio::inherit())
                .spawn()
                .map_err(|e| format!("Failed to spawn Postgres: {}", e))?;

            let pid = child.id().expect("Postgres child should have a PID");
            eprintln!("Postgres started with PID {}", pid);

            // Wait for Postgres to become ready before accepting connections
            eprintln!("Waiting for Postgres to become ready...");
            wait_for_postgres_ready(&pg_isready_bin, postgres_run_dir, shutdown_rx.clone()).await?;
            eprintln!("Postgres is ready, starting to accept connections");

            // Spawn supervision task
            let postgres_supervisor_shutdown_rx = shutdown_rx.clone();
            let postgres_supervisor_shutdown_tx = shutdown_tx.clone();
            let postgres_supervisor_exited = postgres_exited_unexpectedly.clone();
            tokio::spawn(async move {
                supervise_postgres(
                    child,
                    postgres_supervisor_shutdown_rx,
                    postgres_supervisor_shutdown_tx,
                    postgres_supervisor_exited,
                )
                .await;
            });

            postgres_child = Some(pid);
        }

        // Spawn idle monitor task
        let idle_monitor_active = active_connections.clone();
        let idle_monitor_notify = active_changed.clone();
        let idle_monitor_shutdown = shutdown_tx.clone();
        tokio::spawn(async move {
            idle_monitor(
                idle_monitor_active,
                idle_monitor_notify,
                idle_monitor_shutdown,
            )
            .await;
        });

        // Spawn a task for each file descriptor
        let mut handles = Vec::new();
        for fd in fds {
            let listener_shutdown_rx = shutdown_rx.clone();
            let listener_active = active_connections.clone();
            let listener_notify = active_changed.clone();
            handles.push(tokio::spawn(handle_listener(
                fd,
                backend_path,
                listener_shutdown_rx,
                listener_active,
                listener_notify,
            )));
        }

        // Wait for all listener tasks (they will exit when shutdown is signaled)
        for handle in handles {
            if let Err(e) = handle.await {
                eprintln!("Listener task error: {:?}", e);
            }
        }

        // Gracefully shutdown Postgres if it was started
        if let Some(pid) = postgres_child {
            eprintln!("Shutting down Postgres (PID {})...", pid);
            shutdown_postgres(pid).await;
        }

        // Check if Postgres exited unexpectedly
        if postgres_exited_unexpectedly.load(Ordering::Relaxed) {
            eprintln!("Postgres exited unexpectedly, proxy shutting down");
            return Err("Postgres exited unexpectedly".into());
        }
    }

    Ok(())
}

/// Wait for Postgres to become ready by polling pg_isready.
/// Returns Ok(()) when Postgres is ready, or an error if shutdown is requested.
async fn wait_for_postgres_ready(
    pg_isready_bin: &PathBuf,
    postgres_run_dir: &PathBuf,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let check_interval = Duration::from_millis(100);
    let mut attempt = 0u64;

    loop {
        // Check if shutdown was requested
        if *shutdown_rx.borrow() {
            return Err("Shutdown requested before Postgres became ready".into());
        }

        // Run pg_isready to check if Postgres is ready
        // For Unix sockets, pg_isready uses PGHOST or -h with the socket directory
        let output = Command::new(pg_isready_bin)
            .arg("-h")
            .arg(postgres_run_dir)
            .env("PGHOST", postgres_run_dir)
            .output()
            .await;

        match output {
            Ok(output) if output.status.success() => {
                // Postgres is ready
                eprintln!(
                    "Postgres readiness check succeeded (attempt {})",
                    attempt + 1
                );
                return Ok(());
            }
            Ok(_) => {
                // pg_isready returned non-zero, Postgres not ready yet
                attempt += 1;
                if attempt.is_multiple_of(50) {
                    // Log every 5 seconds (50 * 100ms)
                    eprintln!(
                        "Postgres not ready yet (attempt {}), continuing to poll...",
                        attempt
                    );
                }
            }
            Err(e) => {
                // Error running pg_isready, log but continue polling
                eprintln!("Error running pg_isready: {}, continuing to poll...", e);
            }
        }

        // Wait before next check, but also check for shutdown
        tokio::select! {
            _ = sleep(check_interval) => {
                // Continue polling
            }
            _ = shutdown_rx.changed() => {
                return Err("Shutdown requested before Postgres became ready".into());
            }
        }
    }
}

/// Supervise Postgres process: if it exits unexpectedly (before shutdown is requested),
/// trigger proxy shutdown and mark the exit as unexpected.
async fn supervise_postgres(
    mut child: tokio::process::Child,
    mut shutdown_rx: watch::Receiver<bool>,
    shutdown_tx: watch::Sender<bool>,
    exited_unexpectedly: Arc<AtomicBool>,
) {
    loop {
        tokio::select! {
            result = child.wait() => {
                match result {
                    Ok(status) => {
                        // Check if shutdown was already requested
                        if *shutdown_rx.borrow() {
                            // Shutdown was requested, this is expected
                            eprintln!("Postgres exited (expected shutdown)");
                        } else {
                            // Unexpected exit
                            eprintln!("Postgres exited unexpectedly with status: {:?}", status);
                            exited_unexpectedly.store(true, Ordering::Relaxed);
                            let _ = shutdown_tx.send(true);
                        }
                        break;
                    }
                    Err(e) => {
                        eprintln!("Error waiting for Postgres: {}", e);
                        exited_unexpectedly.store(true, Ordering::Relaxed);
                        let _ = shutdown_tx.send(true);
                        break;
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                // Shutdown requested, check if Postgres already exited
                if let Ok(Some(status)) = child.try_wait() {
                    eprintln!("Postgres already exited with status: {:?}", status);
                    break;
                }
                // Continue waiting for Postgres to exit (we'll send SIGTERM from main)
            }
        }
    }
}

/// Gracefully shutdown Postgres by sending SIGTERM, waiting up to 10 seconds,
/// then sending SIGKILL if still running.
async fn shutdown_postgres(pid: u32) {
    let nix_pid = Pid::from_raw(pid as i32);

    // Send SIGTERM
    match signal::kill(nix_pid, Signal::SIGTERM) {
        Ok(()) => {
            eprintln!(
                "Sent SIGTERM to Postgres (PID {}), waiting up to 10 seconds...",
                pid
            );
        }
        Err(e) => {
            eprintln!("Failed to send SIGTERM to Postgres (PID {}): {}", pid, e);
            return;
        }
    }

    // Wait up to 10 seconds, checking every 100ms
    let timeout = Duration::from_secs(10);
    let check_interval = Duration::from_millis(100);
    let mut elapsed = Duration::from_secs(0);

    while elapsed < timeout {
        sleep(check_interval).await;
        elapsed += check_interval;

        // Check if process still exists by sending signal 0
        match signal::kill(nix_pid, None) {
            Ok(()) => {
                // Process still exists, continue waiting
            }
            Err(nix::errno::Errno::ESRCH) => {
                // Process doesn't exist, it exited
                eprintln!("Postgres (PID {}) exited gracefully", pid);
                return;
            }
            Err(e) => {
                // Some other error occurred, log and continue
                eprintln!("Error checking Postgres (PID {}) status: {}", pid, e);
            }
        }
    }

    // Still running after timeout, send SIGKILL
    eprintln!(
        "Postgres (PID {}) did not exit within timeout, sending SIGKILL",
        pid
    );
    match signal::kill(nix_pid, Signal::SIGKILL) {
        Ok(()) => {
            eprintln!("Sent SIGKILL to Postgres (PID {})", pid);
        }
        Err(e) => {
            eprintln!("Failed to send SIGKILL to Postgres (PID {}): {}", pid, e);
        }
    }
}

async fn handle_listener(
    fd: RawFd,
    backend_path: &'static str,
    mut shutdown_rx: watch::Receiver<bool>,
    active_connections: Arc<AtomicUsize>,
    active_changed: Arc<Notify>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Convert raw FD to std::net::TcpListener
    let listener = unsafe { std::net::TcpListener::from_raw_fd(fd) };
    listener.set_nonblocking(true)?;

    // Convert to Tokio TcpListener
    let listener = TcpListener::from_std(listener)?;

    // Accept connections in a loop until shutdown is signaled
    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => {
                        // Increment active connection count
                        active_connections.fetch_add(1, Ordering::Relaxed);

                        // Create guard that will decrement on drop
                        let guard = ActiveConnGuard {
                            counter: active_connections.clone(),
                            notify: active_changed.clone(),
                        };

                        // Spawn a task to handle this connection
                        tokio::spawn(async move {
                            let _guard = guard; // Move guard into task
                            if let Err(e) = handle_connection(stream, backend_path).await {
                                eprintln!("Connection error: {}", e);
                            }
                            // guard is dropped here, decrementing the counter
                        });
                    }
                    Err(e) => {
                        eprintln!("Failed to accept connection: {}", e);
                        // Continue accepting even on error
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                // Shutdown signal received, break out of loop
                eprintln!("Shutdown signal received, stopping listener");
                break;
            }
        }
    }

    Ok(())
}

const BUF_SIZE: usize = 1024;
const IDLE_TIMEOUT_SECS: u64 = 10;

/// RAII guard that tracks active connections
/// Increments counter on creation, decrements on drop
struct ActiveConnGuard {
    counter: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl Drop for ActiveConnGuard {
    fn drop(&mut self) {
        let prev = self.counter.fetch_sub(1, Ordering::Relaxed);
        if prev == 1 {
            // Transitioned from 1 to 0, notify idle monitor
            self.notify.notify_one();
        }
    }
}

/// Idle monitor task that triggers shutdown after 10 seconds of no active connections
async fn idle_monitor(
    active_connections: Arc<AtomicUsize>,
    active_changed: Arc<Notify>,
    shutdown_tx: watch::Sender<bool>,
) {
    loop {
        // Wait until active connections reach zero
        while active_connections.load(Ordering::Relaxed) > 0 {
            active_changed.notified().await;
        }

        // All connections closed, start idle timer
        eprintln!(
            "No active connections, starting {} second idle timer",
            IDLE_TIMEOUT_SECS
        );

        // Now wait for 10 seconds, but abort if a connection arrives
        tokio::select! {
            _ = sleep(Duration::from_secs(IDLE_TIMEOUT_SECS)) => {
                // Timer completed - check if still idle
                if active_connections.load(Ordering::Relaxed) == 0 {
                    // Still idle after timeout, trigger shutdown
                    eprintln!("Idle timer expired, shutting down");
                    let _ = shutdown_tx.send(true);
                    break;
                }
                // A connection arrived during the timeout, restart the loop
                eprintln!("Idle timer cancelled, new connection arrived");
            }
            _ = active_changed.notified() => {
                // A connection arrived, restart the loop
                eprintln!("Idle timer cancelled, new connection arrived");
            }
        }
    }
}

async fn handle_connection(
    mut client: tokio::net::TcpStream,
    backend_path: &'static str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Extract client address from the stream
    let client_addr = client.peer_addr()?;

    // Establish connection to upstream Unix socket for each incoming client connection
    let mut backend = match UnixStream::connect(backend_path).await {
        Ok(result) => result,
        Err(e) => {
            eprintln!(
                "Error establishing upstream connection to {}: {}",
                backend_path, e
            );
            return Ok(()); // Drop client connection gracefully
        }
    };

    eprintln!(
        "Proxy connection opened: client {} -> backend {}",
        client_addr, backend_path
    );

    // Split both streams into read/write halves
    let (mut client_read, mut client_write) = client.split();
    let (mut backend_read, mut backend_write) = backend.split();

    // Create a broadcast channel for cancellation
    let (cancel, _) = broadcast::channel::<()>(1);

    // Run both copy operations concurrently, canceling the other when one finishes
    let (backend_copied, client_copied) = tokio::join!(
        copy_with_abort(&mut backend_read, &mut client_write, cancel.subscribe()).then(|r| {
            let _ = cancel.send(());
            async { r }
        }),
        copy_with_abort(&mut client_read, &mut backend_write, cancel.subscribe()).then(|r| {
            let _ = cancel.send(());
            async { r }
        })
    );

    // Log results (errors are already handled in copy_with_abort)
    match client_copied {
        Ok(count) => {
            eprintln!(
                "Transferred {} bytes from client {} to backend",
                count, client_addr
            );
        }
        Err(err) => {
            eprintln!(
                "Error writing bytes from client {} to backend: {}",
                client_addr, err
            );
        }
    }

    match backend_copied {
        Ok(count) => {
            eprintln!(
                "Transferred {} bytes from backend to client {}",
                count, client_addr
            );
        }
        Err(err) => {
            eprintln!(
                "Error writing bytes from backend to client {}: {}",
                client_addr, err
            );
        }
    }

    eprintln!(
        "Proxy connection closed: client {} -> backend {}",
        client_addr, backend_path
    );

    Ok(())
}

async fn copy_with_abort<R, W>(
    read: &mut R,
    write: &mut W,
    mut abort: broadcast::Receiver<()>,
) -> tokio::io::Result<usize>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut copied = 0;
    let mut buf = [0u8; BUF_SIZE];
    loop {
        let bytes_read;
        tokio::select! {
            biased;

            result = read.read(&mut buf) => {
                use std::io::ErrorKind::{ConnectionReset, ConnectionAborted};
                bytes_read = result.or_else(|e| match e.kind() {
                    // Consider these to be part of the proxy life, not errors
                    ConnectionReset | ConnectionAborted => Ok(0),
                    _ => Err(e)
                })?;
            },
            _ = abort.recv() => {
                break;
            }
        }

        if bytes_read == 0 {
            break;
        }

        // While we ignore some read errors above, any error writing data we've already read to
        // the other side is always treated as exceptional.
        write.write_all(&buf[0..bytes_read]).await?;
        copied += bytes_read;
    }

    Ok(copied)
}
