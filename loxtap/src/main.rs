//! loxtap daemon binary.
//!
//! Loads its configuration, sets up logging, and (in later commits) drives the
//! connection supervisor. For now it validates that the config parses and
//! exits clean.

use std::path::PathBuf;

use clap::Parser;
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

use loxtap::auth::authenticate;
use loxtap::config::Config;
use loxtap::crypto::handshake;
use loxtap::events::run_events;
use loxtap::sink;
use loxtap::structure::fetch_structure;
use loxtap::transport::Transport;

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
    run(&config).await
}

/// Run one connected session end to end.
///
/// This is the single ingest path: open the socket, run the pin-gated crypto
/// handshake, authenticate for a token, load the structure map, then stream and
/// decode value events into the configured sink until the socket drops or the
/// process is asked to stop. A later commit wraps this in a supervisor that adds
/// reconnect, keepalive, and token refresh.
async fn run(config: &Config) -> anyhow::Result<()> {
    let mut transport = Transport::connect(&config.host, config.port).await?;
    let mut session = handshake(
        &mut transport,
        &config.host,
        config.port,
        Some(&config.server_key_pin),
    )
    .await?;
    info!("handshake complete: server key pinned, encryption active");

    let token = authenticate(&mut session, &mut transport, config).await?;
    info!(valid_until = ?token.valid_until(), "authenticated");

    let structure = fetch_structure(&mut transport).await?;
    info!(series = structure.len(), "structure map loaded");

    let sink = sink::build(config.sink);

    // The event loop owns the socket; race it against Ctrl-C so shutdown closes
    // the WebSocket cleanly. Token teardown (killtoken) is a later commit.
    tokio::select! {
        result = run_events(&mut transport, &structure, sink.as_ref()) => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => info!("shutdown requested"),
    }

    transport.close().await?;
    info!("connection closed");
    Ok(())
}

/// Set up `tracing`, honoring `RUST_LOG` and falling back to the config level.
fn init_tracing(fallback_level: &str) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(fallback_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
