# postgel-proxy

This tool simplifies managing PostgreSQL servers in development environments by:

- automatically **starting Postgres when a new client connection is attempted** (via `launchd` socket activation)
- shutting Postgres down after an idle timeout

Benefits:
- No need to run anything constantly in the background.
- No need to run anything on startup.
- No need to manually launch the right PostgreSQL instance for each project.

## How it works

- `launchd` owns the listening TCP port (ex: `55432`) and hands the listening socket FD(s) to this process (socket activation).
- `postgel-proxy` starts (and supervises) a Postgres instance, waits for readiness, then accepts connections.
- Each incoming TCP connection is proxied to Postgres’s Unix domain socket (ex: `.s.PGSQL.5432`).
- When active connections drop to zero, an idle shutdown timer starts; on expiry, the proxy shuts down (and Postgres is terminated).

## Requirements

- **macOS** (uses `launchd` socket activation APIs)
- Rust toolchain (for building): `rustup` + stable
- Postgres binaries: `postgres` + `pg_isready` (for example via Homebrew)

## Build

```bash
cargo build --release
```

Binary will be at `target/release/postgel-proxy`.

## CLI

`postgel-proxy` is intended to be started by `launchd` (because it requires an activated socket).

Required:

- `--socket-name <NAME>`: must match the socket key name in the plist’s `Sockets` dictionary

Managed Postgres:

- `--postgres-bin-dir <DIR>` (must contain `postgres` and `pg_isready`)
- `--postgres-data-dir <DIR>`
- `--postgres-run-dir <DIR>`
- `--backend-socket` is optional; defaults to `<postgres-run-dir>/.s.PGSQL.5432`

Optional:

- `--idle-timeout-secs <SECONDS>` (default `600`)

## Launchd setup (recommended)

Create `~/Library/LaunchAgents/dev.postgel.your-project-slug.plist` with the following contents (edit paths to match your machine):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>dev.postgel.your-project-slug</string>

    <key>ProgramArguments</key>
    <array>
        <!-- Path to the compiled binary -->
        <string>/path/to/postgel-proxy</string>

        <!-- Socket name for launchd activation (must match the Sockets key below) -->
        <string>--socket-name</string>
        <string>postgres-proxy</string>

        <!-- Managed Postgres mode -->
        <!-- Directory containing postgres and pg_isready binaries -->
        <string>--postgres-bin-dir</string>
        <string>/opt/homebrew/opt/postgresql@18/bin</string>

        <string>--postgres-data-dir</string>
        <string>/path/to/.postgel/data</string>

        <string>--postgres-run-dir</string>
        <string>/path/to/.postgel/run</string>

        <!-- Optional: override backend socket path (defaults to <run-dir>/.s.PGSQL.5432) -->
        <!-- <string>--backend-socket</string> -->
        <!-- <string>/custom/path/to/socket</string> -->

        <!-- Optional: idle timeout before shutting down (default 600) -->
        <!-- <string>--idle-timeout-secs</string> -->
        <!-- <string>600</string> -->
    </array>

    <key>Sockets</key>
    <dict>
        <key>postgres-proxy</key>
        <array>
          <dict>
            <key>SockNodeName</key><string>127.0.0.1</string>
            <key>SockServiceName</key><string>55432</string>
            <key>SockType</key><string>stream</string>
            <key>SockFamily</key><string>IPv4</string>
          </dict>
          <dict>
            <key>SockNodeName</key><string>::1</string>
            <key>SockServiceName</key><string>55432</string>
            <key>SockType</key><string>stream</string>
            <key>SockFamily</key><string>IPv6</string>
          </dict>
        </array>
    </dict>

    <!-- No need to launch on startup! -->
    <key>RunAtLoad</key>
    <false/>

    <!-- No need to run it all the time! -->
    <key>KeepAlive</key>
    <false/>

    <key>StandardOutPath</key>
    <string>/tmp/postgel-proxy.stdout.log</string>

    <key>StandardErrorPath</key>
    <string>/tmp/postgel-proxy.stderr.log</string>
</dict>
</plist>
```

### 2) Load the LaunchAgent

```bash
launchctl unload ~/Library/LaunchAgents/dev.postgel.your-project-slug.plist 2>/dev/null || true
launchctl load ~/Library/LaunchAgents/dev.postgel.your-project-slug.plist
```

### 3) Connect

```bash
psql -h 127.0.0.1 -p 55432
```

## Notes / troubleshooting

- **macOS only**: on other OSes the program exits with an error (no `launchd` activation).
- **Managed Postgres locale**: the proxy sets `LC_ALL`/`LANG` for the spawned Postgres using your current environment (falls back to `C`) to avoid Postgres complaining when spawned from a multithreaded process.
- **Backend socket path**: defaults to `<postgres-run-dir>/.s.PGSQL.5432`.
- **Idle shutdown**: when active connections drop to zero, the proxy starts the idle timer; if no new connections arrive before it expires, the proxy triggers shutdown.

## Development

```bash
cargo fmt
cargo clippy
cargo test
```

## In loving memory of Gel

This tool is built in loving memory of [Gel database (formerly EdgeDB),](https://github.com/geldata/gel) which featured a delightfully simple DX.

## License

MIT. See `LICENSE`.


