//! loxweb: a config-driven dashboard engine over a loxtap TimescaleDB store.
//!
//! A pure read client. loxtap owns the Miniserver relationship and writes the
//! uuid-keyed series; loxweb only queries that store and serves a config-driven
//! set of charts. The two are decoupled: loxweb needs the shared database, not
//! the loxtap process. It reuses the bridge crate as a library (its TimescaleDB
//! connection config and wire model) rather than redefining them.
//!
//! This binary loads the static config and the dashboard set and logs a
//! summary. The query layer and the HTTP server (which will wire the SIGHUP
//! reload under the running server) land in later commits.

use std::path::PathBuf;

use clap::Parser;
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

use loxweb::config::Config;
use loxweb::dashboard::DashboardStore;

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
    info!(
        bind = %config.server.bind,
        dashboards = %config.dashboards.display(),
        views = store.current().views.len(),
        "loxweb configuration loaded; the HTTP server lands in a later commit"
    );
    Ok(())
}

/// Set up `tracing`, honoring `RUST_LOG` and falling back to the config level.
fn init_tracing(fallback_level: &str) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(fallback_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
