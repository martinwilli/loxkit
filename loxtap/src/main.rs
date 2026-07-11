//! loxtap daemon binary.
//!
//! Loads its configuration, sets up logging, and (in later commits) drives the
//! connection supervisor. For now it validates that the config parses and
//! exits clean.

use std::path::PathBuf;

use clap::Parser;
use tracing::debug;
use tracing_subscriber::EnvFilter;

use loxtap::config::Config;

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

    // Nothing to connect to yet: the transport and the supervisor land in
    // later commits. Exit clean once the config is validated.
    Ok(())
}

/// Set up `tracing`, honoring `RUST_LOG` and falling back to the config level.
fn init_tracing(fallback_level: &str) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(fallback_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
