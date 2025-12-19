use futures::FutureExt;
use std::env;
use std::os::unix::io::{FromRawFd, RawFd};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixStream};
use tokio::sync::broadcast;

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
    let backend_path: Arc<str> = Arc::from(args[2].clone());

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

        // Spawn a task for each file descriptor
        let mut handles = Vec::new();
        for fd in fds {
            let backend_path_clone = backend_path.clone();
            handles.push(tokio::spawn(handle_listener(fd, backend_path_clone)));
        }

        // Wait for all listener tasks (they run forever, so this will block indefinitely)
        // In practice, if a task panics, we'll see the error
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
    backend_path: Arc<str>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Convert raw FD to std::net::TcpListener
    let listener = unsafe { std::net::TcpListener::from_raw_fd(fd) };
    listener.set_nonblocking(true)?;

    // Convert to Tokio TcpListener
    let listener = TcpListener::from_std(listener)?;

    // Accept connections in a loop (serve forever)
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let backend_path_clone = backend_path.clone();
                // Spawn a task to handle this connection
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, backend_path_clone).await {
                        eprintln!("Connection error: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Failed to accept connection: {}", e);
                // Continue accepting even on error
            }
        }
    }
}

const BUF_SIZE: usize = 1024;

async fn handle_connection(
    mut client: tokio::net::TcpStream,
    backend_path: Arc<str>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Extract client address from the stream
    let client_addr = client.peer_addr()?;

    // Establish connection to upstream Unix socket for each incoming client connection
    let mut backend = match UnixStream::connect(&*backend_path).await {
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
