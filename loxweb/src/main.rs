//! loxweb: a config-driven dashboard engine over a loxtap TimescaleDB store.
//!
//! A pure read client. loxtap owns the Miniserver relationship and writes the
//! uuid-keyed series; loxweb only queries that store and serves a config-driven
//! set of charts. The two are decoupled: loxweb needs the shared database, not
//! the loxtap process. It reuses the bridge crate as a library (its TimescaleDB
//! connection config and wire model) rather than redefining them.
//!
//! This binary loads the static config and the reloadable dashboards, opens its
//! own read pool, installs the file-watch live-reload task, and serves the HTTP
//! app until a Ctrl-C or SIGTERM asks it to stop.

use std::path::PathBuf;

use anyhow::Context as _;
use clap::Parser;
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

use loxweb::config::Config;
use loxweb::dashboard::{self, DashboardStore};
use loxweb::{http, query};

/// Command-line arguments.
#[derive(Debug, Parser)]
#[command(name = "loxweb", about = "config-driven dashboards over a loxtap TimescaleDB store")]
struct Cli {
    /// Path to the YAML configuration file.
    #[arg(long, value_name = "FILE")]
    config: PathBuf,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = Config::load(&cli.config)?;
    init_tracing(&config.log_level);

    debug!(?config, "loaded configuration");

    // Fail-fast: a bad dashboards file must not start the service.
    let store = DashboardStore::load(&config.dashboards)?;
    let pool = query::build_pool(&config.timescale)?;

    // The live-reload task runs under the server now, so a save to the dashboards
    // file re-reads it without dropping the socket. The watcher never terminates;
    // the shutdown future below only wakes on Ctrl-C / SIGTERM.
    dashboard::spawn_reload_watch(store.clone());

    let app = http::router(store.clone(), pool, &config.server.base_path);

    let listener = tokio::net::TcpListener::bind(&config.server.bind)
        .await
        .with_context(|| format!("binding {}", config.server.bind))?;
    let local = listener
        .local_addr()
        .context("reading the bound address")?;
    info!(
        addr = %local,
        base_path = %config.server.base_path,
        views = store.current().views.len(),
        "loxweb serving"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serving HTTP")
}

/// Set up `tracing`, honoring `RUST_LOG` and falling back to the config level.
fn init_tracing(fallback_level: &str) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(fallback_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

/// Complete on the first Ctrl-C or (on unix) SIGTERM — the only signals that stop
/// the server. The dashboards reload watcher runs off this path, not on it.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(e) => {
                warn!("cannot install SIGTERM handler, using Ctrl-C only: {e}");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
