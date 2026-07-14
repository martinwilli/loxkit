//! loxweb: a config-driven dashboard engine over a loxtap TimescaleDB store.
//!
//! A pure read client. loxtap owns the Miniserver relationship and writes the
//! uuid-keyed series; loxweb only queries that store and serves a config-driven
//! set of charts. The two are decoupled: loxweb needs the shared database, not
//! the loxtap process. It reuses the bridge crate as a library (its TimescaleDB
//! connection config and wire model) rather than redefining them.
//!
//! This is the crate skeleton: it parses its arguments and sets up logging. The
//! config model, the query layer, and the HTTP server land in later commits.

use std::path::PathBuf;

use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

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
    init_tracing("info");
    info!(config = %cli.config.display(), "loxweb starting");
    Ok(())
}

/// Set up `tracing`, honoring `RUST_LOG` and falling back to `fallback_level`.
fn init_tracing(fallback_level: &str) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(fallback_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
