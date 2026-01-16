# postgel

`postgel` provides an `edgedb project`-like workflow for **local development PostgreSQL instances**.

Key properties:

- **Project linking**: a working directory tree is linked to a managed Postgres instance.
- **Driver-agnostic connection handoff**: emits standard **libpq environment variables** (and a `DATABASE_URL`) so apps don’t need custom drivers or config parsing.
- **Two ways to run**:
  - **launchd socket activation (macOS)**: on-demand startup with idle shutdown
  - **foreground mode**: run in a terminal pane / Procfile-style runner

## Requirements

- **macOS** for `launchd` socket activation
- Rust toolchain (for building): `rustup` + stable
- Homebrew (required by the current “bottle-like” Postgres installation approach)

## Build

```bash
cargo build --release
```

Binary: `target/release/postgel`

## Quick start (recommended: project-managed + launchd)

From your project root:

```bash
postgel project init
eval "$(postgel project env --format=sh)"
psql
```

What this does:
- Creates `postgel.toml` if missing (portable marker/config; safe to commit).
- Creates a **tool-managed Postgres instance** and links this working copy to it (non-portable).
- Installs/enables a **LaunchAgent** on macOS by default (use `postgel project init --no-launchd` to skip).

## Instance lifecycle (how the managed instance behaves)

### Initialization

`postgel project init`:

- Finds/creates the project root by locating/creating `postgel.toml`.
- Creates a new instance with:
  - a private **data directory** and **run directory**
  - a fixed TCP port on `127.0.0.1`
- Records the linkage by project path so multiple working copies don’t collide.

### Running (launchd socket activation)

When launchd is enabled:

- `launchd` owns the TCP port and starts `postgel proxy ...` on first connection attempt.
- `postgel proxy` starts `postgres` (managed mode), waits for readiness, then proxies each TCP connection to Postgres’ unix socket.
- When active connections drop to zero, an idle timer starts; on expiry the proxy shuts down and Postgres is terminated.

### Running (foreground / Procfile-style)

`postgel instance run <id-or-name>` runs the proxy in the foreground (no launchd):

- Binds the instance TCP port on `127.0.0.1:<port>`
- Starts Postgres and proxies connections
- Exits on Ctrl+C / process termination

### Unlinking / deleting / pruning

- `postgel project unlink`: removes the project→instance link (instance remains unless `--destroy-instance`).
- `postgel instance delete <id-or-name>`: removes LaunchAgent (if installed) and deletes instance directories; works even if the original project dir is gone.
- `postgel project prune`: removes links whose project directories no longer exist.
- `postgel instance prune`: removes instances with missing dirs; optionally remove orphaned instances via `--orphaned`.

## CLI overview

### Project commands

- `postgel project init [--no-launchd]`
- `postgel project info`
- `postgel project env --format=sh|dotenv|json` (stdout only)
- `postgel project unlink [--destroy-instance]`
- `postgel project prune`
- `postgel project enable-launchd`
- `postgel project disable-launchd`

### Instance commands

- `postgel instance list`
- `postgel instance info <id-or-name>`
- `postgel instance run <id-or-name>` (foreground proxy)
- `postgel instance delete <id-or-name> [--force]`
- `postgel instance prune [--orphaned]`

### Proxy command

`postgel proxy ...` is primarily intended to be started by `launchd` (socket activation). `postgel project init` manages this for you on macOS.

## Where state is stored

- **Portable (in repo)**: `postgel.toml`
- **Non-portable (per machine)**: registry and instance files under your OS user config/data dirs (currently managed via `directories` crate; see `Registry::load()` in code).

## Notes / troubleshooting

- **macOS only for socket activation**: on other OSes, `launchd` mode is unavailable.
- **Idle shutdown**: when active connections drop to zero, the idle timer starts; on expiry the proxy triggers shutdown and terminates Postgres.

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


