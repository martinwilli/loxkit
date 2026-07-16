# loxkit workspace

loxkit is a Rust Cargo workspace for a generic Loxone to TimescaleDB bridge: an
ingest daemon plus a config-driven dashboard server. See [README.md](README.md)
for the overview.

## Layout

- `loxtap/` — the ingest daemon crate (see [loxtap/CLAUDE.md](loxtap/CLAUDE.md)).
- `loxweb/` — the dashboard server crate (see [loxweb/CLAUDE.md](loxweb/CLAUDE.md)).
- `debian/` — packaging: one `.deb` ships both daemons and their unit files.
- `doc/` — public setup and config guides.
- `local/` — if present, may hold site-specific information and instructions.

## Build & test

It's a Cargo workspace:

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace
```

Run test and clippy before each commit; each commit must build and pass tests on
its own.

## Commit-prefix scheme

Double-prefix: name the sub-project, then the subsystem.

- `tap/<sub>:` for the loxtap daemon (transport/crypto/auth/structure/sink/events/
  schema/config/supervisor); a bare `tap:` is fine for scaffold or housekeeping.
- `web/<sub>:` for loxweb (config/dashboard/http/query/frontend); bare `web:`
  likewise.
- `debian:` (single umbrella) for the Debian packaging.
- `doc:` (umbrella) for the public documentation (README, `doc/`, these
  CLAUDE.md files).

Use imperative mood; explain the *why*; American spelling; refer to code as
func_name(); no backticks or Markdown in the message body.

## Public-release hygiene

The Rust crates are a generic, publicly-releasable Loxone to TimescaleDB bridge.
Keep public source, commit messages, and the public docs free of
operator-specific installation details and any private predecessor or migration
context.
