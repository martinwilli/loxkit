//! Schema migrations, embedded in the binary and run at daemon startup.
//!
//! The store's DDL lives as versioned SQL under `migrations/` and is compiled
//! into the binary by [`refinery::embed_migrations!`]. A sink runs [`run`] once
//! when it connects: refinery tracks the applied versions in its own history
//! table, so applying an already-migrated database is a no-op.
//!
//! Embedding plain SQL and applying it at runtime deliberately avoids any
//! build-time database: nothing here needs a live server to compile, which
//! keeps a packaged build self-contained. The trade-off is no compile-time
//! query checking, which the crate does not rely on.

/// The embedded migration set, generated from the `migrations/` directory.
mod embedded {
    refinery::embed_migrations!("migrations");
}

/// Apply all pending schema migrations to `client`.
///
/// Idempotent: refinery skips versions already recorded in its history table,
/// so this is safe to call on every startup.
pub async fn run(client: &mut tokio_postgres::Client) -> anyhow::Result<()> {
    embedded::migrations::runner().run_async(client).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The embedded set must be non-empty and its versions contiguous from 1,
    /// so refinery never sees a gap it would refuse to apply. Runs offline.
    #[test]
    fn embedded_versions_are_contiguous_from_one() {
        let runner = embedded::migrations::runner();
        let mut versions: Vec<_> = runner.get_migrations().iter().map(|m| m.version()).collect();
        assert!(!versions.is_empty(), "no embedded migrations found");
        versions.sort_unstable();
        let expected: Vec<_> = (1..=versions.len() as i32).collect();
        assert_eq!(versions, expected, "migration versions must be 1..=N with no gaps");
    }

    /// Apply the schema against a real database and confirm it is coherent:
    /// readings is a hypertable and the two side tables exist. Skips cleanly
    /// when TEST_DATABASE_URL is unset, so the offline suite stays green.
    #[tokio::test]
    async fn migrations_apply_to_a_live_database() {
        let Ok(dsn) = std::env::var("TEST_DATABASE_URL") else {
            eprintln!("TEST_DATABASE_URL unset, skipping live migration test");
            return;
        };

        let (mut client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test database");
        // The connection drives the protocol; it must run on its own task.
        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Idempotent, so an already-migrated database is fine.
        run(&mut client).await.expect("run migrations");

        let hypertable = client
            .query_one(
                "SELECT count(*) FROM timescaledb_information.hypertables \
                 WHERE hypertable_name = 'readings'",
                &[],
            )
            .await
            .expect("query hypertables");
        assert_eq!(hypertable.get::<_, i64>(0), 1, "readings must be a hypertable");

        for table in ["series_meta", "series_state"] {
            let row = client
                .query_one(
                    "SELECT count(*) FROM information_schema.tables \
                     WHERE table_name = $1",
                    &[&table],
                )
                .await
                .expect("query information_schema.tables");
            assert_eq!(row.get::<_, i64>(0), 1, "{table} must exist");
        }
    }
}
