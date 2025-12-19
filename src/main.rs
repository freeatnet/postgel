use futures::FutureExt;
use std::env;
use std::os::unix::io::{FromRawFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixStream};
use tokio::sync::{Notify, broadcast, watch};
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!(
            "Usage: {} <socket-name> <backend-unix-socket-path>",
            args[0]
        );
        std::process::exit(1);
    }
    let socket_name = &args[1];
    // We leak `backend_path` instead of wrapping it in an Arc to share it with future tasks since
    // `backend_path` is going to live for the lifetime of the server in all cases.
    // (This reduces MESI/MOESI cache traffic between CPU cores.)
    let backend_path: &str = Box::leak(args[2].clone().into_boxed_str());

    // Check if running on macOS (launchd is macOS-only)
    #[cfg(not(target_os = "macos"))]
    {
        eprintln!("Error: launchd socket activation is only available on macOS");
        std::process::exit(1);
    }

    // Activate the socket using raunch
    #[cfg(target_os = "macos")]
    {
        let fds = raunch::activate_socket(socket_name)
            .map_err(|e| format!("Failed to activate socket '{}': {}", socket_name, e))?;

        if fds.is_empty() {
            eprintln!("No file descriptors returned for socket '{}'", socket_name);
            std::process::exit(1);
        }

        // Create shutdown signal and active connection tracking
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let active_connections = Arc::new(AtomicUsize::new(0));
        let active_changed = Arc::new(Notify::new());

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
    }

    Ok(())
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
