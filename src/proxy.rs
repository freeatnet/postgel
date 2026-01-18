use anyhow::{Context, Result};
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

const PROXY_SOCKET_NAME: &str = "postgel-proxy";

pub struct ProxyConfig {
    pub postgres_bin_dir: PathBuf,
    pub postgres_data_dir: PathBuf,
    pub postgres_run_dir: PathBuf,
    pub idle_timeout_secs: u64,
    pub use_launchd: bool,
    pub port: Option<u16>, // For foreground mode
}

impl ProxyConfig {
    pub fn backend_socket_path(&self) -> PathBuf {
        self.postgres_run_dir.join(".s.PGSQL.5432")
    }
}

pub async fn run_proxy(config: ProxyConfig) -> Result<()> {
    let backend_path = config.backend_socket_path();
    let backend_path_str = backend_path.to_string_lossy().to_string();
    let backend_path: &str = Box::leak(backend_path_str.into_boxed_str());

    if config.use_launchd {
        #[cfg(not(target_os = "macos"))]
        {
            anyhow::bail!("launchd socket activation is only available on macOS");
        }

        #[cfg(target_os = "macos")]
        {
            run_proxy_launchd(config, backend_path).await?;
        }
    } else {
        run_proxy_foreground(config, backend_path).await?;
    }

    Ok(())
}

#[cfg(target_os = "macos")]
async fn run_proxy_launchd(config: ProxyConfig, backend_path: &'static str) -> Result<()> {
    let fds = raunch::activate_socket(PROXY_SOCKET_NAME)
        .context(format!("Failed to activate socket '{}'", PROXY_SOCKET_NAME))?;

    if fds.is_empty() {
        anyhow::bail!(
            "No file descriptors returned for socket '{}'",
            PROXY_SOCKET_NAME
        );
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let active_connections = Arc::new(AtomicUsize::new(0));
    let active_changed = Arc::new(Notify::new());
    let postgres_exited_unexpectedly = Arc::new(AtomicBool::new(false));

    let postgres_bin_dir = &config.postgres_bin_dir;
    let postgres_data_dir = &config.postgres_data_dir;
    let postgres_run_dir = &config.postgres_run_dir;

    let postgres_bin = postgres_bin_dir.join("postgres");
    let pg_isready_bin = postgres_bin_dir.join("pg_isready");

    eprintln!("Starting managed Postgres instance...");

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
        .context("Failed to spawn Postgres")?;

    let pid = child.id().expect("Postgres child should have a PID");
    eprintln!("Postgres started with PID {}", pid);

    eprintln!("Waiting for Postgres to become ready...");
    wait_for_postgres_ready(&pg_isready_bin, postgres_run_dir, shutdown_rx.clone()).await?;
    eprintln!("Postgres is ready, starting to accept connections");

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

    let postgres_child = Some(pid);

    let idle_monitor_active = active_connections.clone();
    let idle_monitor_notify = active_changed.clone();
    let idle_monitor_shutdown = shutdown_tx.clone();
    let idle_timeout_secs = config.idle_timeout_secs;
    tokio::spawn(async move {
        idle_monitor(
            idle_monitor_active,
            idle_monitor_notify,
            idle_monitor_shutdown,
            idle_timeout_secs,
        )
        .await;
    });

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

    for handle in handles {
        if let Err(e) = handle.await {
            eprintln!("Listener task error: {:?}", e);
        }
    }

    if let Some(pid) = postgres_child {
        eprintln!("Shutting down Postgres (PID {})...", pid);
        shutdown_postgres(pid).await;
    }

    if postgres_exited_unexpectedly.load(Ordering::Relaxed) {
        anyhow::bail!("Postgres exited unexpectedly");
    }

    Ok(())
}

async fn run_proxy_foreground(config: ProxyConfig, backend_path: &'static str) -> Result<()> {
    use std::net::SocketAddr;
    use tokio::net::TcpListener as TokioTcpListener;

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let active_connections = Arc::new(AtomicUsize::new(0));
    let active_changed = Arc::new(Notify::new());
    let postgres_exited_unexpectedly = Arc::new(AtomicBool::new(false));

    let postgres_bin_dir = &config.postgres_bin_dir;
    let postgres_data_dir = &config.postgres_data_dir;
    let postgres_run_dir = &config.postgres_run_dir;

    let postgres_bin = postgres_bin_dir.join("postgres");
    let pg_isready_bin = postgres_bin_dir.join("pg_isready");

    eprintln!("Starting managed Postgres instance...");

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
        .context("Failed to spawn Postgres")?;

    let pid = child.id().expect("Postgres child should have a PID");
    eprintln!("Postgres started with PID {}", pid);

    eprintln!("Waiting for Postgres to become ready...");
    wait_for_postgres_ready(&pg_isready_bin, postgres_run_dir, shutdown_rx.clone()).await?;
    eprintln!("Postgres is ready, starting to accept connections");

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

    let postgres_child = Some(pid);

    // In foreground mode, bind to a TCP port
    let port = config.port.unwrap_or(5432);
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
    let listener = TokioTcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    eprintln!("Proxy listening on {}", local_addr);

    let idle_monitor_active = active_connections.clone();
    let idle_monitor_notify = active_changed.clone();
    let idle_monitor_shutdown = shutdown_tx.clone();
    let idle_timeout_secs = config.idle_timeout_secs;
    tokio::spawn(async move {
        idle_monitor(
            idle_monitor_active,
            idle_monitor_notify,
            idle_monitor_shutdown,
            idle_timeout_secs,
        )
        .await;
    });

    let listener_shutdown_rx = shutdown_rx.clone();
    let listener_active = active_connections.clone();
    let listener_notify = active_changed.clone();

    tokio::select! {
        result = handle_foreground_listener(
            listener,
            backend_path,
            listener_shutdown_rx,
            listener_active,
            listener_notify,
        ) => {
            result?;
        }
        _ = shutdown_rx.changed() => {
            eprintln!("Shutdown signal received");
        }
    }

    if let Some(pid) = postgres_child {
        eprintln!("Shutting down Postgres (PID {})...", pid);
        shutdown_postgres(pid).await;
    }

    if postgres_exited_unexpectedly.load(Ordering::Relaxed) {
        anyhow::bail!("Postgres exited unexpectedly");
    }

    Ok(())
}

async fn handle_foreground_listener(
    listener: tokio::net::TcpListener,
    backend_path: &'static str,
    mut shutdown_rx: watch::Receiver<bool>,
    active_connections: Arc<AtomicUsize>,
    active_changed: Arc<Notify>,
) -> Result<()> {
    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => {
                        active_connections.fetch_add(1, Ordering::Relaxed);
                        let guard = ActiveConnGuard {
                            counter: active_connections.clone(),
                            notify: active_changed.clone(),
                        };
                        tokio::spawn(async move {
                            let _guard = guard;
                            if let Err(e) = handle_connection(stream, backend_path).await {
                                eprintln!("Connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("Failed to accept connection: {}", e);
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                break;
            }
        }
    }
    Ok(())
}

async fn wait_for_postgres_ready(
    pg_isready_bin: &PathBuf,
    postgres_run_dir: &PathBuf,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let check_interval = Duration::from_millis(100);
    let mut attempt = 0u64;

    loop {
        if *shutdown_rx.borrow() {
            anyhow::bail!("Shutdown requested before Postgres became ready");
        }

        let output = Command::new(pg_isready_bin)
            .arg("-h")
            .arg(postgres_run_dir)
            .env("PGHOST", postgres_run_dir)
            .output()
            .await;

        match output {
            Ok(output) if output.status.success() => {
                eprintln!(
                    "Postgres readiness check succeeded (attempt {})",
                    attempt + 1
                );
                return Ok(());
            }
            Ok(_) => {
                attempt += 1;
                if attempt.is_multiple_of(50) {
                    eprintln!(
                        "Postgres not ready yet (attempt {}), continuing to poll...",
                        attempt
                    );
                }
            }
            Err(e) => {
                eprintln!("Error running pg_isready: {}, continuing to poll...", e);
            }
        }

        tokio::select! {
            _ = sleep(check_interval) => {}
            _ = shutdown_rx.changed() => {
                anyhow::bail!("Shutdown requested before Postgres became ready");
            }
        }
    }
}

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
                        if *shutdown_rx.borrow() {
                            eprintln!("Postgres exited (expected shutdown)");
                        } else {
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
                if let Ok(Some(status)) = child.try_wait() {
                    eprintln!("Postgres already exited with status: {:?}", status);
                    break;
                }
            }
        }
    }
}

async fn shutdown_postgres(pid: u32) {
    let nix_pid = Pid::from_raw(pid as i32);

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

    let timeout = Duration::from_secs(10);
    let check_interval = Duration::from_millis(100);
    let mut elapsed = Duration::from_secs(0);

    while elapsed < timeout {
        sleep(check_interval).await;
        elapsed += check_interval;

        match signal::kill(nix_pid, None) {
            Ok(()) => {}
            Err(nix::errno::Errno::ESRCH) => {
                eprintln!("Postgres (PID {}) exited gracefully", pid);
                return;
            }
            Err(e) => {
                eprintln!("Error checking Postgres (PID {}) status: {}", pid, e);
            }
        }
    }

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
) -> Result<()> {
    let listener = unsafe { std::net::TcpListener::from_raw_fd(fd) };
    listener.set_nonblocking(true)?;
    let listener = TcpListener::from_std(listener)?;

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => {
                        active_connections.fetch_add(1, Ordering::Relaxed);
                        let guard = ActiveConnGuard {
                            counter: active_connections.clone(),
                            notify: active_changed.clone(),
                        };
                        tokio::spawn(async move {
                            let _guard = guard;
                            if let Err(e) = handle_connection(stream, backend_path).await {
                                eprintln!("Connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("Failed to accept connection: {}", e);
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                eprintln!("Shutdown signal received, stopping listener");
                break;
            }
        }
    }

    Ok(())
}

struct ActiveConnGuard {
    counter: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl Drop for ActiveConnGuard {
    fn drop(&mut self) {
        let prev = self.counter.fetch_sub(1, Ordering::Relaxed);
        if prev == 1 {
            self.notify.notify_one();
        }
    }
}

async fn idle_monitor(
    active_connections: Arc<AtomicUsize>,
    active_changed: Arc<Notify>,
    shutdown_tx: watch::Sender<bool>,
    idle_timeout_secs: u64,
) {
    loop {
        while active_connections.load(Ordering::Relaxed) > 0 {
            active_changed.notified().await;
        }

        eprintln!(
            "No active connections, starting {} second idle timer",
            idle_timeout_secs
        );

        tokio::select! {
            _ = sleep(Duration::from_secs(idle_timeout_secs)) => {
                if active_connections.load(Ordering::Relaxed) == 0 {
                    eprintln!("Idle timer expired, shutting down");
                    let _ = shutdown_tx.send(true);
                    break;
                }
                eprintln!("Idle timer cancelled, new connection arrived");
            }
            _ = active_changed.notified() => {
                eprintln!("Idle timer cancelled, new connection arrived");
            }
        }
    }
}

async fn handle_connection(
    mut client: tokio::net::TcpStream,
    backend_path: &'static str,
) -> Result<()> {
    let client_addr = client.peer_addr()?;

    let mut backend = match UnixStream::connect(backend_path).await {
        Ok(result) => result,
        Err(e) => {
            eprintln!(
                "Error establishing upstream connection to {}: {}",
                backend_path, e
            );
            return Ok(());
        }
    };

    eprintln!(
        "Proxy connection opened: client {} -> backend {}",
        client_addr, backend_path
    );

    let (mut client_read, mut client_write) = client.split();
    let (mut backend_read, mut backend_write) = backend.split();

    let (cancel, _) = broadcast::channel::<()>(1);

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
    const BUF_SIZE: usize = 1024;
    let mut copied = 0;
    let mut buf = [0u8; BUF_SIZE];
    loop {
        let bytes_read;
        tokio::select! {
            biased;
            result = read.read(&mut buf) => {
                use std::io::ErrorKind::{ConnectionReset, ConnectionAborted};
                bytes_read = result.or_else(|e| match e.kind() {
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

        write.write_all(&buf[0..bytes_read]).await?;
        copied += bytes_read;
    }

    Ok(copied)
}
