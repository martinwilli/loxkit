//! Keep the TimescaleDB extensions in step with their installed packages.
//!
//! A package upgrade swaps an extension's shared library on disk, but each
//! database keeps its old extension version until someone runs `ALTER EXTENSION
//! … UPDATE` in it. The toolkit package ships only its current library, so a
//! lagging toolkit fails every call into it right after the upgrade, the
//! continuous-aggregate refresh jobs among them (silently, as real-time
//! aggregation papers over the gap). The core package keeps the libraries of a
//! few older releases, so a lagging core survives some upgrades before every
//! hypertable access breaks the same way.
//!
//! So the sink checks both: it updates the toolkit itself when its role owns
//! the extension, and warns with the exact remedy otherwise. Core is never
//! updated here: its update must be the first command of a fresh session, takes
//! heavy locks, and leaves every other session on the old library until it
//! reconnects, so that stays an operator step. Nothing here is fatal; ingestion
//! is the daemon's job.

use tokio_postgres::Client;
use tracing::{info, warn};

const CORE: &str = "timescaledb";
const TOOLKIT: &str = "timescaledb_toolkit";

/// The installed and the packaged version of both extensions, plus whether the
/// current role may alter them. `pg_has_role(extowner, 'USAGE')` holds when the
/// role has the owner's privileges, the very check `ALTER EXTENSION` applies
/// (and always true for a superuser). An extension that is not installed yet
/// has no row; the migrations create it at the packaged version.
const STATE_SQL: &str = "\
SELECT e.extname, e.extversion, a.default_version, pg_has_role(e.extowner, 'USAGE') \
FROM pg_extension e JOIN pg_available_extensions a ON a.name = e.extname \
WHERE e.extname IN ('timescaledb', 'timescaledb_toolkit')";

const TOOLKIT_UPDATE_SQL: &str = "ALTER EXTENSION timescaledb_toolkit UPDATE";

const TOOLKIT_REMEDY: &str = "its owner must run ALTER EXTENSION timescaledb_toolkit UPDATE \
    (rollup refreshes fail until then), or hand the extension to the daemon's role";

const CORE_REMEDY: &str = "run ALTER EXTENSION timescaledb UPDATE as the first command of a \
    fresh session (psql -X), then restart loxtap and loxweb";

/// What to do about one installed extension.
#[derive(Debug, PartialEq, Eq)]
enum Action {
    /// The installed version is the packaged one.
    Current,
    /// Lagging, and the daemon may and should update it.
    Update,
    /// Lagging, and up to the operator; carries the remedy for the warning.
    Lagging(&'static str),
}

/// Decide what to do about extension `name`, installed at `installed` while its
/// package ships `available`, given whether the daemon's role `owned` it.
fn action(name: &str, installed: &str, available: &str, owned: bool) -> Action {
    if installed == available {
        return Action::Current;
    }
    match (name, owned) {
        (TOOLKIT, true) => Action::Update,
        (TOOLKIT, false) => Action::Lagging(TOOLKIT_REMEDY),
        (CORE, _) => Action::Lagging(CORE_REMEDY),
        // STATE_SQL selects no other extension.
        _ => Action::Current,
    }
}

/// Check both extensions against their packages, update the toolkit when the
/// daemon may, and warn about any lag it cannot fix. Logs every failure instead
/// of returning it.
pub(super) async fn maintain(client: &Client) {
    let rows = match client.query(STATE_SQL, &[]).await {
        Ok(rows) => rows,
        Err(e) => {
            warn!("could not check the TimescaleDB extension versions: {e:#}");
            return;
        }
    };
    for row in &rows {
        let name: String = row.get(0);
        let installed: String = row.get(1);
        let owned: bool = row.get(3);
        // A control file without a default_version leaves nothing to compare.
        let Some(available) = row.get::<_, Option<String>>(2) else {
            continue;
        };
        match action(&name, &installed, &available, owned) {
            Action::Current => {}
            Action::Update => match client.batch_execute(TOOLKIT_UPDATE_SQL).await {
                Ok(()) => info!("updated extension {name} from {installed} to {available}"),
                Err(e) => {
                    warn!("could not update extension {name} from {installed} to {available}: {e:#}")
                }
            },
            Action::Lagging(remedy) => {
                warn!("extension {name} is at {installed}, but its package ships {available}: {remedy}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_current_extension_needs_nothing() {
        for owned in [false, true] {
            assert_eq!(action(CORE, "2.31.0", "2.31.0", owned), Action::Current);
            assert_eq!(action(TOOLKIT, "1.27.0", "1.27.0", owned), Action::Current);
        }
    }

    #[test]
    fn an_owned_lagging_toolkit_is_updated() {
        assert_eq!(action(TOOLKIT, "1.23.0", "1.27.0", true), Action::Update);
    }

    #[test]
    fn a_foreign_lagging_toolkit_is_left_to_its_owner() {
        assert_eq!(action(TOOLKIT, "1.23.0", "1.27.0", false), Action::Lagging(TOOLKIT_REMEDY));
    }

    #[test]
    fn a_lagging_core_is_never_updated() {
        // Even when owned: its update needs a fresh session and a restart of
        // every client, which the daemon cannot arrange on its own.
        for owned in [false, true] {
            assert_eq!(action(CORE, "2.29.1", "2.31.0", owned), Action::Lagging(CORE_REMEDY));
        }
    }
}
