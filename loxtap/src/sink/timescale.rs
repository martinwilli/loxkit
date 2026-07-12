//! The TimescaleDB (Postgres) sink.
//!
//! The daemon is a single writer at home-automation rate, well under a percent
//! of the store's capacity, so a connection pool buys nothing: one
//! `tokio-postgres` connection is enough, and the sink owns its reconnect the
//! same way the supervisor owns the websocket one. The `readings` hot path
//! writes via `COPY … (BINARY)`, the clean bulk append for a narrow table;
//! labels go through an idempotent `ON CONFLICT` upsert on `series_meta`.
//!
//! Reconnect is deliberately bounded. [`write`](TimescaleSink::write) runs
//! inline on the websocket read path (the event loop awaits it), so a long or
//! looping reconnect here would stall the keepalive and get the socket dropped.
//! So a dropped connection is retried a couple of times with a tiny delay, and
//! if that fails the batch just errors and the caller logs it; the event stream
//! re-offers the next value table, which reconnects then. Bounded in-memory
//! buffering across a longer DB outage is deferred to a later commit.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Context as _;
use futures_util::pin_mut;
use tokio::sync::Mutex as AsyncMutex;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls};
use tracing::warn;

use super::Sink;
use crate::config::TimescaleConfig;
use crate::migrate;
use crate::model::{LoxUuid, Reading, SeriesMeta};

/// Reconnect attempts before a single operation gives up, and the delay between
/// them. Kept tiny on purpose (see the module doc): the write path is inline on
/// the websocket read loop, so this must never approach the keepalive budget.
const RECONNECT_ATTEMPTS: usize = 3;
const RECONNECT_DELAY: Duration = Duration::from_millis(200);

/// The idempotent label upsert: insert the row, or update every label column on
/// a uuid conflict. A rename is exactly this update, never a new series.
const UPSERT_META_SQL: &str = "\
INSERT INTO series_meta (uuid, control, name, room, category, sub, unit, agg_mode) \
VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
ON CONFLICT (uuid) DO UPDATE SET \
    control = EXCLUDED.control, \
    name = EXCLUDED.name, \
    room = EXCLUDED.room, \
    category = EXCLUDED.category, \
    sub = EXCLUDED.sub, \
    unit = EXCLUDED.unit, \
    agg_mode = EXCLUDED.agg_mode";

/// A sink that persists readings and labels into a TimescaleDB store.
pub struct TimescaleSink {
    /// Connection parameters, kept so a dropped connection can be rebuilt.
    pg_config: tokio_postgres::Config,
    /// The live client. Behind an async mutex so a reconnect can replace it and
    /// the single writer serializes its COPY and upserts.
    client: AsyncMutex<Client>,
    /// The last labels written per uuid, so a no-op upsert is skipped. The event
    /// loop re-offers labels on every value table, but they change rarely.
    labels: Mutex<HashMap<LoxUuid, SeriesMeta>>,
}

impl TimescaleSink {
    /// Build the sink from the daemon's [`TimescaleConfig`].
    ///
    /// Uses the programmatic config builder rather than a hand-built DSN so the
    /// password and other fields never need escaping. TLS is not enforced yet
    /// (NoTls on a trusted LAN); `sslmode` is carried in the config but ignored.
    pub async fn connect(config: &TimescaleConfig) -> anyhow::Result<Self> {
        let mut pg_config = tokio_postgres::Config::new();
        pg_config
            .host(&config.host)
            .port(config.port)
            .user(&config.user)
            .password(&config.password)
            .dbname(&config.dbname);
        Self::from_pg_config(pg_config).await
    }

    /// Build the sink from an already-assembled Postgres config: connect, spawn
    /// the connection task, run the startup migrations once, and return it.
    ///
    /// The seam the integration test uses, parsing a DSN into a
    /// [`tokio_postgres::Config`] and handing it straight in.
    async fn from_pg_config(pg_config: tokio_postgres::Config) -> anyhow::Result<Self> {
        let mut client = Self::open(&pg_config).await?;
        migrate::run(&mut client).await.context("running startup migrations")?;
        Ok(Self {
            pg_config,
            client: AsyncMutex::new(client),
            labels: Mutex::new(HashMap::new()),
        })
    }

    /// Open one connection and spawn its driver task.
    ///
    /// `tokio-postgres` splits the client from a `Connection` future that must
    /// be polled on its own task to drive the protocol; when that future
    /// resolves the connection has dropped, so log it — the next operation sees
    /// [`Client::is_closed`] and reconnects.
    async fn open(pg_config: &tokio_postgres::Config) -> anyhow::Result<Client> {
        let (client, connection) = pg_config
            .connect(NoTls)
            .await
            .context("connecting to Postgres")?;
        tokio::spawn(async move {
            match connection.await {
                Ok(()) => warn!("Postgres connection closed"),
                Err(e) => warn!("Postgres connection error: {e}"),
            }
        });
        Ok(client)
    }

    /// Replace `client` with a fresh connection if the current one is closed.
    ///
    /// Bounded (see the module doc): a couple of quick attempts, then an error
    /// so the batch is dropped rather than blocking the event loop.
    async fn ensure_connected(&self, client: &mut Client) -> anyhow::Result<()> {
        if !client.is_closed() {
            return Ok(());
        }
        for attempt in 0..RECONNECT_ATTEMPTS {
            match Self::open(&self.pg_config).await {
                Ok(fresh) => {
                    *client = fresh;
                    warn!("Postgres connection re-established");
                    return Ok(());
                }
                Err(e) => {
                    warn!(attempt = attempt + 1, "Postgres reconnect failed: {e:#}");
                    if attempt + 1 < RECONNECT_ATTEMPTS {
                        tokio::time::sleep(RECONNECT_DELAY).await;
                    }
                }
            }
        }
        anyhow::bail!("Postgres reconnect failed after {RECONNECT_ATTEMPTS} attempts")
    }
}

#[async_trait::async_trait]
impl Sink for TimescaleSink {
    async fn write(&self, batch: &[Reading]) -> anyhow::Result<()> {
        // An empty batch never opens a COPY.
        if batch.is_empty() {
            return Ok(());
        }
        let mut client = self.client.lock().await;
        self.ensure_connected(&mut client).await?;

        // Dedup lands in a later commit; here the whole batch is written.
        let copy = client
            .copy_in("COPY readings (uuid, ts, value) FROM STDIN WITH (FORMAT binary)")
            .await
            .context("opening COPY into readings")?;
        let writer = BinaryCopyInWriter::new(copy, &[Type::UUID, Type::TIMESTAMPTZ, Type::FLOAT8]);
        pin_mut!(writer);
        for r in batch {
            let id = uuid::Uuid::from_bytes(*r.uuid.as_bytes());
            writer
                .as_mut()
                .write(&[&id, &r.ts, &r.value])
                .await
                .context("writing a readings row")?;
        }
        writer.finish().await.context("finishing the readings COPY")?;
        Ok(())
    }

    async fn upsert_meta(&self, meta: &[SeriesMeta]) -> anyhow::Result<()> {
        // The event loop calls this on every value table, so drop any meta that
        // matches what we last wrote for its uuid before touching the DB.
        // SeriesMeta is PartialEq, so equality is direct. The std lock is never
        // held across an await.
        let changed: Vec<SeriesMeta> = {
            let cache = self.labels.lock().expect("labels lock poisoned");
            meta.iter()
                .filter(|m| cache.get(&m.uuid) != Some(*m))
                .cloned()
                .collect()
        };
        if changed.is_empty() {
            return Ok(());
        }

        let mut client = self.client.lock().await;
        self.ensure_connected(&mut client).await?;
        // Per-row execution is fine: labels change rarely and the batch is one
        // table's worth of series.
        for m in &changed {
            let id = uuid::Uuid::from_bytes(*m.uuid.as_bytes());
            let agg = m.agg_mode.as_str();
            client
                .execute(
                    UPSERT_META_SQL,
                    &[&id, &m.control, &m.name, &m.room, &m.category, &m.sub, &m.unit, &agg],
                )
                .await
                .context("upserting series_meta")?;
        }
        drop(client);

        let mut cache = self.labels.lock().expect("labels lock poisoned");
        for m in changed {
            cache.insert(m.uuid, m);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;
    use crate::model::AggMode;

    /// A clearly-synthetic uuid no live series carries.
    fn synthetic_uuid() -> LoxUuid {
        "deadbeef-0003-0000-0000000000000001".parse().unwrap()
    }

    /// Remove the synthetic rows from every table so re-runs stay clean.
    async fn cleanup(client: &Client, id: &uuid::Uuid) {
        for table in ["readings", "series_meta", "series_state"] {
            client
                .execute(&format!("DELETE FROM {table} WHERE uuid = $1"), &[id])
                .await
                .expect("cleanup delete");
        }
    }

    /// Round-trip a batch and a meta upsert against a live store, then prove the
    /// uuid/label split: a rename updates one row, never adds a series. Skips
    /// cleanly when TEST_DATABASE_URL is unset, so the offline suite stays green.
    #[tokio::test]
    async fn round_trips_readings_and_a_rename() {
        let Ok(dsn) = std::env::var("TEST_DATABASE_URL") else {
            eprintln!("TEST_DATABASE_URL unset, skipping live Timescale sink test");
            return;
        };
        let pg_config: tokio_postgres::Config = dsn.parse().expect("parse TEST_DATABASE_URL");

        // A second connection, independent of the sink, drives verification.
        let (verify, connection) = pg_config.connect(NoTls).await.expect("connect verify client");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let uuid = synthetic_uuid();
        let pg_uuid = uuid::Uuid::from_bytes(*uuid.as_bytes());
        // Clear any leftovers from an aborted run before asserting counts.
        cleanup(&verify, &pg_uuid).await;

        // Building the sink runs the (idempotent) migrations.
        let sink = TimescaleSink::from_pg_config(pg_config).await.expect("build sink");

        let meta = SeriesMeta {
            uuid,
            control: "InfoOnlyAnalog".to_owned(),
            name: "Synthetic Original".to_owned(),
            room: Some("Test Room".to_owned()),
            category: Some("Test Category".to_owned()),
            sub: "value".to_owned(),
            unit: Some("kWh".to_owned()),
            agg_mode: AggMode::Counter,
        };
        sink.upsert_meta(std::slice::from_ref(&meta)).await.unwrap();

        let base = UNIX_EPOCH + Duration::from_secs(1_600_000_000);
        let batch: Vec<Reading> = (0u32..3)
            .map(|i| Reading {
                uuid,
                ts: base + Duration::from_secs(u64::from(i)),
                value: f64::from(i),
            })
            .collect();
        sink.write(&batch).await.unwrap();

        let count: i64 = verify
            .query_one("SELECT count(*) FROM readings WHERE uuid = $1", &[&pg_uuid])
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 3, "the whole batch landed");

        let row = verify
            .query_one("SELECT name, agg_mode FROM series_meta WHERE uuid = $1", &[&pg_uuid])
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "Synthetic Original");
        assert_eq!(row.get::<_, String>(1), "counter");

        // A rename: same uuid, new name. Exactly one row, updated in place.
        let renamed = SeriesMeta { name: "Synthetic Renamed".to_owned(), ..meta.clone() };
        sink.upsert_meta(std::slice::from_ref(&renamed)).await.unwrap();

        let meta_count: i64 = verify
            .query_one("SELECT count(*) FROM series_meta WHERE uuid = $1", &[&pg_uuid])
            .await
            .unwrap()
            .get(0);
        assert_eq!(meta_count, 1, "a rename updates one row, never adds a series");
        let name: String = verify
            .query_one("SELECT name FROM series_meta WHERE uuid = $1", &[&pg_uuid])
            .await
            .unwrap()
            .get(0);
        assert_eq!(name, "Synthetic Renamed");

        cleanup(&verify, &pg_uuid).await;
    }
}
