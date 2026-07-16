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

    /// The labeled views must label a resolved series and still surface an
    /// unresolved one. Both views LEFT JOIN series_meta, so a uuid written to
    /// readings/series_state with no series_meta row yet appears under its uuid
    /// with NULL labels rather than being silently dropped. Uses only
    /// clearly-synthetic uuids and cleans them up. Skips when TEST_DATABASE_URL
    /// is unset, so the offline suite stays green.
    #[tokio::test]
    async fn views_label_resolved_and_surface_unresolved_uuids() {
        use std::time::{Duration, SystemTime};

        let Ok(dsn) = std::env::var("TEST_DATABASE_URL") else {
            eprintln!("TEST_DATABASE_URL unset, skipping live view test");
            return;
        };

        let (mut client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test database");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Building the schema is idempotent, so an already-migrated DB is fine.
        run(&mut client).await.expect("run migrations");

        // A resolved series (has a series_meta row) and an unresolved one (a
        // readings + series_state row, but deliberately no series_meta row).
        let resolved: uuid::Uuid =
            "deadbeef-0006-0000-0000-000000000001".parse().unwrap();
        let unresolved: uuid::Uuid =
            "deadbeef-0006-0000-0000-000000000002".parse().unwrap();
        let ts = SystemTime::UNIX_EPOCH + Duration::from_secs(1_600_000_000);

        /// Remove the synthetic rows from every table so re-runs stay clean.
        async fn cleanup(client: &tokio_postgres::Client, ids: [uuid::Uuid; 2]) {
            for id in ids {
                for table in ["readings", "series_meta", "series_state"] {
                    client
                        .execute(&format!("DELETE FROM {table} WHERE uuid = $1"), &[&id])
                        .await
                        .expect("cleanup delete");
                }
            }
        }

        // Clear leftovers from any aborted run before asserting.
        cleanup(&client, [resolved, unresolved]).await;

        // Resolved: reading + labels + state.
        client
            .execute(
                "INSERT INTO readings (uuid, ts, value) VALUES ($1, $2, $3)",
                &[&resolved, &ts, &21.5f64],
            )
            .await
            .expect("insert resolved reading");
        client
            .execute(
                "INSERT INTO series_meta \
                 (uuid, control, name, room, category, sub, unit, agg_mode) \
                 VALUES ($1, 'InfoOnlyAnalog', 'Synthetic View', 'Test Room', \
                         'Test Category', 'value', 'kWh', 'counter')",
                &[&resolved],
            )
            .await
            .expect("insert resolved meta");
        client
            .execute(
                "INSERT INTO series_state (uuid, last_seen, last_value) VALUES ($1, $2, $3)",
                &[&resolved, &ts, &21.5f64],
            )
            .await
            .expect("insert resolved state");

        // Unresolved: reading + state, but no series_meta row.
        client
            .execute(
                "INSERT INTO readings (uuid, ts, value) VALUES ($1, $2, $3)",
                &[&unresolved, &ts, &7.0f64],
            )
            .await
            .expect("insert unresolved reading");
        client
            .execute(
                "INSERT INTO series_state (uuid, last_seen, last_value) VALUES ($1, $2, $3)",
                &[&unresolved, &ts, &7.0f64],
            )
            .await
            .expect("insert unresolved state");

        // readings_labeled: the resolved reading carries its labels.
        let row = client
            .query_one(
                "SELECT value, name, room, category, agg_mode \
                 FROM readings_labeled WHERE uuid = $1",
                &[&resolved],
            )
            .await
            .expect("query readings_labeled resolved");
        assert_eq!(row.get::<_, f64>(0), 21.5);
        assert_eq!(row.get::<_, Option<String>>(1).as_deref(), Some("Synthetic View"));
        assert_eq!(row.get::<_, Option<String>>(2).as_deref(), Some("Test Room"));
        assert_eq!(row.get::<_, Option<String>>(3).as_deref(), Some("Test Category"));
        assert_eq!(row.get::<_, Option<String>>(4).as_deref(), Some("counter"));

        // readings_labeled: the unresolved reading still appears, NULL labels.
        let row = client
            .query_one(
                "SELECT value, name, agg_mode FROM readings_labeled WHERE uuid = $1",
                &[&unresolved],
            )
            .await
            .expect("query readings_labeled unresolved");
        assert_eq!(row.get::<_, f64>(0), 7.0);
        assert_eq!(row.get::<_, Option<String>>(1), None, "unresolved has no name");
        assert_eq!(row.get::<_, Option<String>>(2), None, "unresolved has no agg_mode");

        // series_current: both series listed with last_seen/last_value; labels
        // populated for the resolved one, NULL for the unresolved one.
        let row = client
            .query_one(
                "SELECT last_seen, last_value, name, agg_mode \
                 FROM series_current WHERE uuid = $1",
                &[&resolved],
            )
            .await
            .expect("query series_current resolved");
        assert_eq!(row.get::<_, SystemTime>(0), ts);
        assert_eq!(row.get::<_, f64>(1), 21.5);
        assert_eq!(row.get::<_, Option<String>>(2).as_deref(), Some("Synthetic View"));
        assert_eq!(row.get::<_, Option<String>>(3).as_deref(), Some("counter"));

        let row = client
            .query_one(
                "SELECT last_seen, last_value, name, agg_mode \
                 FROM series_current WHERE uuid = $1",
                &[&unresolved],
            )
            .await
            .expect("query series_current unresolved");
        assert_eq!(row.get::<_, SystemTime>(0), ts);
        assert_eq!(row.get::<_, f64>(1), 7.0);
        assert_eq!(row.get::<_, Option<String>>(2), None, "unresolved has no name");
        assert_eq!(row.get::<_, Option<String>>(3), None, "unresolved has no agg_mode");

        cleanup(&client, [resolved, unresolved]).await;
    }
}
