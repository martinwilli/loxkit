//! loxweb server configuration, parsed from a YAML file at startup.
//!
//! This is the *static* config: the listen socket, the sub-path mount, and the
//! read-only database connection. It changes only across restarts. The churny,
//! agent-edited artifact (the dashboards) lives in its own file, pointed at by
//! [`Config::dashboards`], and reloads without a restart (see [`crate::dashboard`]).
//!
//! The database section reuses loxtap's [`TimescaleConfig`] verbatim, so the wire
//! parameters and the password redaction match the daemon; the operator simply
//! points it at a read-only role.

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use loxtap::config::TimescaleConfig;
use serde::Deserialize;

/// The full loxweb configuration.
///
/// Derives `Debug`; the database password stays redacted because
/// [`TimescaleConfig`] redacts its own.
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// HTTP server settings (the listen socket and sub-path mount).
    pub server: ServerConfig,
    /// Read-only TimescaleDB connection. Reuses loxtap's type; the operator
    /// points it at a read-only role, decoupled from the daemon's write pool.
    pub timescale: TimescaleConfig,
    /// Path to the reloadable `dashboards.yaml`; re-read whenever the file changes.
    pub dashboards: PathBuf,
    /// Fallback log level when `RUST_LOG` is unset.
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

/// HTTP server settings.
#[derive(Clone, Debug, Deserialize)]
pub struct ServerConfig {
    /// Listen socket, e.g. `127.0.0.1:8080`. Plain HTTP; a reverse proxy
    /// terminates TLS.
    pub bind: String,
    /// Sub-path the app is mounted under, e.g. `/loxweb`. Empty (the default)
    /// mounts at the root.
    #[serde(default)]
    pub base_path: String,
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

fn default_log_level() -> String {
    "info".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_config_with_defaults() {
        let yaml = r#"
server:
  bind: "127.0.0.1:8080"
timescale:
  host: db.example
  user: loxweb_ro
  password: pgs3cret
  dbname: loxtap
dashboards: /etc/loxkit/dashboards.yaml
"#;
        let cfg = Config::from_yaml_str(yaml).unwrap();
        assert_eq!(cfg.server.bind, "127.0.0.1:8080");
        assert_eq!(cfg.server.base_path, "");
        assert_eq!(cfg.timescale.host, "db.example");
        assert_eq!(cfg.timescale.user, "loxweb_ro");
        assert_eq!(cfg.timescale.port, 5432);
        assert_eq!(cfg.dashboards, PathBuf::from("/etc/loxkit/dashboards.yaml"));
        assert_eq!(cfg.log_level, "info");
    }

    #[test]
    fn parses_base_path_and_log_level() {
        let yaml = r#"
server:
  bind: "0.0.0.0:9090"
  base_path: /loxweb
timescale:
  host: db.example
  user: loxweb_ro
  password: pgs3cret
  dbname: loxtap
dashboards: dashboards.yaml
log_level: debug
"#;
        let cfg = Config::from_yaml_str(yaml).unwrap();
        assert_eq!(cfg.server.base_path, "/loxweb");
        assert_eq!(cfg.log_level, "debug");
    }

    #[test]
    fn debug_redacts_db_password() {
        let yaml = r#"
server:
  bind: "127.0.0.1:8080"
timescale:
  host: db.example
  user: loxweb_ro
  password: pgs3cret
  dbname: loxtap
dashboards: dashboards.yaml
"#;
        let cfg = Config::from_yaml_str(yaml).unwrap();
        let rendered = format!("{cfg:?}");
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("pgs3cret"));
    }
}
