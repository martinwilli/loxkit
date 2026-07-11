//! loxtap daemon binary.
//!
//! Loads its configuration, sets up logging, and hands off to the connection
//! supervisor, which keeps a Miniserver session alive: reconnect with backoff,
//! keepalive, proactive token refresh, and a clean token kill on shutdown.

use std::path::PathBuf;

use clap::Parser;
use tracing::debug;
use tracing_subscriber::EnvFilter;

use loxtap::config::Config;
use loxtap::supervisor;

/// Command-line arguments.
#[derive(Debug, Parser)]
#[command(name = "loxtap", about = "uuid-stable Loxone to TimescaleDB bridge")]
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
    supervisor::run(&config).await
}

/// Set up `tracing`, honoring `RUST_LOG` and falling back to the config level.
fn init_tracing(fallback_level: &str) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(fallback_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
