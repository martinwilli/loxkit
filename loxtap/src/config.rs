//! Daemon configuration, parsed from a YAML file.
//!
//! The struct mirrors the connection parameters and operational knobs the
//! daemon needs. Secrets (the password) never appear in the [`fmt::Debug`]
//! output so that logging the whole config stays safe.

use std::fmt;
use std::path::Path;

use anyhow::Context as _;
use serde::Deserialize;

/// TimescaleDB (Postgres) connection parameters for the Timescale sink.
///
/// The password is redacted from [`fmt::Debug`] just as the Miniserver one is,
/// so logging the whole config stays safe.
#[derive(Clone, Deserialize)]
pub struct TimescaleConfig {
    /// Database host.
    pub host: String,
    /// Database port.
    #[serde(default = "default_pg_port")]
    pub port: u16,
    /// Login user.
    pub user: String,
    /// Login password.
    pub password: String,
    /// Database name.
    pub dbname: String,
    /// TLS mode, carried for forward-compat but **not yet enforced**: the sink
    /// connects with NoTls (a trusted LAN is assumed; TLS is a later concern).
    #[serde(default)]
    pub sslmode: Option<String>,
}

impl fmt::Debug for TimescaleConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimescaleConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .field("dbname", &self.dbname)
            .field("sslmode", &self.sslmode)
            .finish()
    }
}

/// Reconnect and keepalive timing.
///
/// All fields have defaults so a minimal config need not mention them.
#[derive(Clone, Debug, Deserialize)]
pub struct ReconnectConfig {
    /// First backoff delay after a drop, in seconds; doubles up to the max.
    #[serde(default = "default_backoff_base_secs")]
    pub backoff_base_secs: u64,
    /// Ceiling for the exponential backoff, in seconds.
    #[serde(default = "default_backoff_max_secs")]
    pub backoff_max_secs: u64,
    /// Keepalive cadence in seconds; must stay under the server idle cutoff.
    #[serde(default = "default_keepalive_secs")]
    pub keepalive_secs: u64,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            backoff_base_secs: default_backoff_base_secs(),
            backoff_max_secs: default_backoff_max_secs(),
            keepalive_secs: default_keepalive_secs(),
        }
    }
}

/// Miniserver connection settings.
///
/// The password is redacted from [`fmt::Debug`] just as the Timescale one is,
/// so logging the whole config stays safe.
#[derive(Clone, Deserialize)]
pub struct MiniserverConfig {
    /// Miniserver hostname or IP.
    pub host: String,
    /// Miniserver port (carries no default field on the wire, so we require it).
    #[serde(default = "default_port")]
    pub port: u16,
    /// Login user.
    pub user: String,
    /// Login password.
    pub password: String,
    /// SHA-256 pin of the server's public key as `sha256:<hex>`, enforced by
    /// the crypto handshake: the daemon refuses to start on a mismatch.
    pub server_key_pin: String,
    /// Reconnect and keepalive knobs.
    #[serde(default)]
    pub reconnect: ReconnectConfig,
}

impl fmt::Debug for MiniserverConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MiniserverConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .field("server_key_pin", &self.server_key_pin)
            .field("reconnect", &self.reconnect)
            .finish()
    }
}

/// The full daemon configuration.
///
/// Both connection sections redact their own passwords in [`fmt::Debug`], so a
/// derived one on the whole config stays safe to log.
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// Miniserver connection settings.
    pub miniserver: MiniserverConfig,
    /// TimescaleDB connection. Its presence selects the Timescale sink;
    /// without it, readings go to the debug sink.
    #[serde(default)]
    pub timescale: Option<TimescaleConfig>,
    /// Fallback log level when `RUST_LOG` is unset.
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

impl Config {
    /// Parse a config from a YAML string.
    pub fn from_yaml_str(yaml: &str) -> anyhow::Result<Self> {
        yaml_serde::from_str(yaml).context("parsing YAML configuration")
    }

    /// Load and parse a config from a file path.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        Self::from_yaml_str(&text)
    }
}

fn default_port() -> u16 {
    80
}

fn default_pg_port() -> u16 {
    5432
}

fn default_log_level() -> String {
    "info".to_owned()
}

fn default_backoff_base_secs() -> u64 {
    2
}

fn default_backoff_max_secs() -> u64 {
    300
}

fn default_keepalive_secs() -> u64 {
    240
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_config() {
        let yaml = r#"
miniserver:
  host: miniserver.example
  port: 8080
  user: logger
  password: s3cret
  server_key_pin: "sha256:deadbeef"
  reconnect:
    backoff_base_secs: 5
    backoff_max_secs: 120
    keepalive_secs: 200
log_level: debug
"#;
        let cfg = Config::from_yaml_str(yaml).unwrap();
        assert_eq!(cfg.miniserver.host, "miniserver.example");
        assert_eq!(cfg.miniserver.port, 8080);
        assert_eq!(cfg.miniserver.user, "logger");
        assert_eq!(cfg.miniserver.password, "s3cret");
        assert_eq!(cfg.miniserver.server_key_pin, "sha256:deadbeef");
        assert!(cfg.timescale.is_none());
        assert_eq!(cfg.log_level, "debug");
        assert_eq!(cfg.miniserver.reconnect.backoff_base_secs, 5);
        assert_eq!(cfg.miniserver.reconnect.backoff_max_secs, 120);
        assert_eq!(cfg.miniserver.reconnect.keepalive_secs, 200);
    }

    #[test]
    fn applies_defaults_for_optional_knobs() {
        let yaml = r#"
miniserver:
  host: miniserver.example
  user: logger
  password: s3cret
  server_key_pin: "sha256:deadbeef"
"#;
        let cfg = Config::from_yaml_str(yaml).unwrap();
        assert_eq!(cfg.miniserver.port, 80);
        assert!(cfg.timescale.is_none());
        assert_eq!(cfg.log_level, "info");
        assert_eq!(cfg.miniserver.reconnect.backoff_base_secs, 2);
        assert_eq!(cfg.miniserver.reconnect.backoff_max_secs, 300);
        assert_eq!(cfg.miniserver.reconnect.keepalive_secs, 240);
    }

    #[test]
    fn debug_redacts_password() {
        let cfg = Config {
            miniserver: MiniserverConfig {
                host: "miniserver.example".to_owned(),
                port: 80,
                user: "logger".to_owned(),
                password: "s3cret".to_owned(),
                server_key_pin: "sha256:deadbeef".to_owned(),
                reconnect: ReconnectConfig::default(),
            },
            timescale: None,
            log_level: "info".to_owned(),
        };
        let rendered = format!("{cfg:?}");
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("s3cret"));
    }

    #[test]
    fn parses_timescale_section_and_redacts_its_password() {
        let yaml = r#"
miniserver:
  host: miniserver.example
  user: logger
  password: s3cret
  server_key_pin: "sha256:deadbeef"
timescale:
  host: db.example
  port: 5433
  user: loxtap
  password: pgs3cret
  dbname: loxtap
"#;
        let cfg = Config::from_yaml_str(yaml).unwrap();
        let ts = cfg.timescale.expect("timescale section present");
        assert_eq!(ts.host, "db.example");
        assert_eq!(ts.port, 5433);
        assert_eq!(ts.user, "loxtap");
        assert_eq!(ts.password, "pgs3cret");
        assert_eq!(ts.dbname, "loxtap");
        assert_eq!(ts.sslmode, None);

        // The section's password must not leak through its Debug either.
        let rendered = format!("{ts:?}");
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("pgs3cret"));
    }

    #[test]
    fn timescale_port_defaults_to_5432() {
        let yaml = r#"
miniserver:
  host: miniserver.example
  user: logger
  password: s3cret
  server_key_pin: "sha256:deadbeef"
timescale:
  host: db.example
  user: loxtap
  password: pgs3cret
  dbname: loxtap
"#;
        let cfg = Config::from_yaml_str(yaml).unwrap();
        assert_eq!(cfg.timescale.unwrap().port, 5432);
    }
}
