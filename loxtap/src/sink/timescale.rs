//! The TimescaleDB (Postgres) sink.
//!
//! The daemon is a single writer at home-automation rate, well under a percent
//! of the store's capacity, so a connection pool buys nothing: one
//! `tokio-postgres` connection is enough, and the sink owns its reconnect the
//! same way the supervisor owns the websocket one. The `readings` hot path
//! writes via `COPY … (BINARY)`, the clean bulk append for a narrow table;
//! labels go through an idempotent `ON CONFLICT` upsert on `series_meta`.
//!
//! Two extra jobs ride the write path. Bit-identical repeats are dropped before
//! the COPY (see [`retain_changed`]): every reconnect re-dumps the current value
//! of all series at a fresh receive time, and storing those repeats would bias
//! time-unaware aggregates toward reconnect instants and bury the real
//! change-point under a wrong timestamp; compression handles the bytes, this
//! handles the analytics. And `series_state` is upserted for every observed
//! reading, changed or not, so it answers liveness and latest-value in O(1) on a
//! fixed-size table; the dedup cache is seeded from it at startup so the first
//! post-restart dump dedups too.
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
use std::time::{Duration, SystemTime};

use anyhow::Context as _;
use futures_util::pin_mut;
use tokio::sync::Mutex as AsyncMutex;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls};
use tracing::{info, warn};

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

/// Advance the liveness/latest-value cache for a whole batch in one round-trip.
///
/// The arrays are unnested into rows and upserted; `last_seen` is the observed
/// receive time (advances on every event, changed or not) and `last_value` the
/// current value. The caller must first collapse the batch to one row per uuid,
/// because a duplicate conflict key in a single statement errors with "ON
/// CONFLICT DO UPDATE command cannot affect row a second time".
const UPSERT_STATE_SQL: &str = "\
INSERT INTO series_state (uuid, last_seen, last_value) \
SELECT * FROM unnest($1::uuid[], $2::timestamptz[], $3::float8[]) \
ON CONFLICT (uuid) DO UPDATE SET \
    last_seen = EXCLUDED.last_seen, \
    last_value = EXCLUDED.last_value";

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
    /// The last-*written* value per uuid, so a reading identical to it is
    /// dropped before the COPY. Seeded from `series_state` at startup so even
    /// the first post-restart dump dedups. Distinct from `labels`.
    dedup: Mutex<HashMap<LoxUuid, f64>>,
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
        let sink = Self::from_pg_config(pg_config).await?;
        sink.apply_timezone(config.timezone.as_deref()).await?;
        Ok(sink)
    }

    /// Pin the database's default timezone if the config requested one.
    ///
    /// Runs `ALTER DATABASE … SET TimeZone`, which is what makes the calendar-aware
    /// bucketing (daily/monthly dashboard rollups) align to local time; the stored
    /// data is untouched, every `ts` is a UTC instant regardless. It takes effect on
    /// connections opened *after* it, so the daemon's own session (which does no
    /// calendar math) need not care, but a querying client that reconnects picks it
    /// up. The zone name is checked against `pg_timezone_names` first so a config
    /// typo fails loud here instead of silently breaking every later session, and the
    /// statement is assembled by Postgres's own `format(%I, %L)` because ALTER
    /// DATABASE takes no bind parameter for the identifier or the value. A privilege
    /// failure on the ALTER (the role must own the database) is logged, not fatal:
    /// ingestion is the daemon's real job and must not hinge on a display setting.
    async fn apply_timezone(&self, timezone: Option<&str>) -> anyhow::Result<()> {
        let Some(tz) = timezone else {
            return Ok(());
        };
        let client = self.client.lock().await;

        let known: bool = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_timezone_names WHERE name = $1)",
                &[&tz],
            )
            .await
            .context("checking the configured timezone against pg_timezone_names")?
            .get(0);
        anyhow::ensure!(known, "configured timezone {tz:?} is not a known IANA zone name");

        let stmt: String = client
            .query_one(
                "SELECT format('ALTER DATABASE %I SET TimeZone TO %L', current_database(), $1::text)",
                &[&tz],
            )
            .await
            .context("building the ALTER DATABASE statement")?
            .get(0);

        match client.batch_execute(&stmt).await {
            Ok(()) => info!("database default timezone set to {tz:?} (effective on new connections)"),
            Err(e) => warn!("could not set database timezone to {tz:?} (own the database?): {e:#}"),
        }
        Ok(())
    }

    /// Build the sink from an already-assembled Postgres config: connect, spawn
    /// the connection task, run the startup migrations once, and return it.
    ///
    /// The seam the integration test uses, parsing a DSN into a
    /// [`tokio_postgres::Config`] and handing it straight in.
    async fn from_pg_config(pg_config: tokio_postgres::Config) -> anyhow::Result<Self> {
        let mut client = Self::open(&pg_config).await?;
        migrate::run(&mut client).await.context("running startup migrations")?;
        let dedup = Self::seed_dedup(&client).await.context("seeding the dedup cache")?;
        Ok(Self {
            pg_config,
            client: AsyncMutex::new(client),
            labels: Mutex::new(HashMap::new()),
            dedup: Mutex::new(dedup),
        })
    }

    /// Seed the dedup cache from `series_state`'s durable last values.
    ///
    /// `series_state` mirrors the last-written value per uuid, so loading it
    /// here means the first post-restart state dump dedups against the last
    /// known values instead of re-writing a full row per series. It is a
    /// rebuildable cache (derivable from `readings`), never a source of truth.
    async fn seed_dedup(client: &Client) -> anyhow::Result<HashMap<LoxUuid, f64>> {
        let rows = client
            .query("SELECT uuid, last_value FROM series_state", &[])
            .await
            .context("reading series_state for the dedup seed")?;
        let mut cache = HashMap::with_capacity(rows.len());
        for row in &rows {
            let id: uuid::Uuid = row.get(0);
            let value: f64 = row.get(1);
            cache.insert(LoxUuid::from_bytes(*id.as_bytes()), value);
        }
        Ok(cache)
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
        // An empty batch touches nothing.
        if batch.is_empty() {
            return Ok(());
        }

        // Drop exact repeats against the last-written value per uuid. The std
        // lock is never held across an await.
        let kept: Vec<Reading> = {
            let mut cache = self.dedup.lock().expect("dedup lock poisoned");
            retain_changed(&mut cache, batch)
        };

        let mut client = self.client.lock().await;
        self.ensure_connected(&mut client).await?;

        // COPY only the changed rows; a fully-deduped reconnect dump skips the
        // COPY entirely (but still advances series_state below).
        if !kept.is_empty() {
            let copy = client
                .copy_in("COPY readings (uuid, ts, value) FROM STDIN WITH (FORMAT binary)")
                .await
                .context("opening COPY into readings")?;
            let writer =
                BinaryCopyInWriter::new(copy, &[Type::UUID, Type::TIMESTAMPTZ, Type::FLOAT8]);
            pin_mut!(writer);
            for r in &kept {
                let id = uuid::Uuid::from_bytes(*r.uuid.as_bytes());
                writer
                    .as_mut()
                    .write(&[&id, &r.ts, &r.value])
                    .await
                    .context("writing a readings row")?;
            }
            writer.finish().await.context("finishing the readings COPY")?;
        }

        // Every observed reading advances series_state, even a fully-deduped
        // reconnect dump, so last_seen is true last-heard-from liveness.
        update_series_state(&client, batch).await?;
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

/// Keep only readings whose value differs from the last-written value cached
/// for their uuid, updating the cache for each kept reading.
///
/// The dominant redundancy is the full state dump on every (re)subscribe: a
/// reconnect re-sends the current value of every series at a fresh receive time,
/// almost all bit-identical to what is stored. Dropping those exact repeats here
/// keeps `readings` change-only, which is the analytic fix (compression already
/// handles the bytes).
///
/// Equality is exact bit-equality with no epsilon: the server already applied
/// its deadband, so any value that reached us is a real reported change, and a
/// repeat re-emits the exact source f64 (confirmed live). Re-quantizing here
/// would hide genuine small moves. A uuid not yet in the cache is always kept
/// (first sighting).
fn retain_changed(cache: &mut HashMap<LoxUuid, f64>, batch: &[Reading]) -> Vec<Reading> {
    let mut kept = Vec::new();
    for r in batch {
        let changed = match cache.get(&r.uuid) {
            Some(prev) => prev.to_bits() != r.value.to_bits(),
            None => true,
        };
        if changed {
            cache.insert(r.uuid, r.value);
            kept.push(*r);
        }
    }
    kept
}

/// Upsert `series_state` for every observed reading in `batch`, one round-trip.
///
/// The batch is first collapsed to one row per uuid, keeping the last
/// occurrence: Postgres rejects a second conflict on the same key in a single
/// statement ("ON CONFLICT DO UPDATE command cannot affect row a second time").
/// MS1 found no uuid repeats within a value table, so this is defensive but
/// cheap. The three parallel arrays bind as `uuid[]`/`timestamptz[]`/`float8[]`.
async fn update_series_state(client: &Client, batch: &[Reading]) -> anyhow::Result<()> {
    let mut latest: HashMap<LoxUuid, (SystemTime, f64)> = HashMap::with_capacity(batch.len());
    for r in batch {
        // Insert overwrites, so the last occurrence of a uuid wins.
        latest.insert(r.uuid, (r.ts, r.value));
    }

    let mut uuids: Vec<uuid::Uuid> = Vec::with_capacity(latest.len());
    let mut times: Vec<SystemTime> = Vec::with_capacity(latest.len());
    let mut values: Vec<f64> = Vec::with_capacity(latest.len());
    for (id, (ts, value)) in latest {
        uuids.push(uuid::Uuid::from_bytes(*id.as_bytes()));
        times.push(ts);
        values.push(value);
    }

    client
        .execute(UPSERT_STATE_SQL, &[&uuids, &times, &values])
        .await
        .context("upserting series_state")?;
    Ok(())
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

    /// A family of clearly-synthetic uuids for the dedup tests, keyed by index.
    fn dedup_uuid(n: u64) -> LoxUuid {
        format!("deadbeef-0004-0000-{n:016x}").parse().unwrap()
    }

    /// A reading at `secs` past the epoch, so tests read at a glance.
    fn reading(uuid: LoxUuid, secs: u64, value: f64) -> Reading {
        Reading { uuid, ts: UNIX_EPOCH + Duration::from_secs(secs), value }
    }

    #[test]
    fn constant_series_keeps_only_the_first_across_batches() {
        // The reconnect-dump case: the same value re-offered batch after batch
        // must collapse to a single stored reading.
        let mut cache = HashMap::new();
        let u = dedup_uuid(1);
        let mut kept = 0;
        for secs in 0..5 {
            kept += retain_changed(&mut cache, &[reading(u, secs, 21.5)]).len();
        }
        assert_eq!(kept, 1, "a constant series stores exactly one reading");
    }

    #[test]
    fn changing_series_keeps_every_change() {
        let mut cache = HashMap::new();
        let u = dedup_uuid(1);
        let batch: Vec<Reading> = [1.0, 2.0, 3.0, 4.0]
            .into_iter()
            .enumerate()
            .map(|(i, v)| reading(u, i as u64, v))
            .collect();
        assert_eq!(retain_changed(&mut cache, &batch).len(), 4);
    }

    #[test]
    fn preseeded_cache_drops_a_repeat_of_the_seed() {
        // Models the startup seed: a first batch that merely repeats the last
        // known value must be dropped, not written.
        let u = dedup_uuid(1);
        let mut cache = HashMap::from([(u, 21.5)]);
        assert!(retain_changed(&mut cache, &[reading(u, 0, 21.5)]).is_empty());
    }

    #[test]
    fn interleaved_uuids_dedup_independently() {
        let mut cache = HashMap::new();
        let a = dedup_uuid(1);
        let b = dedup_uuid(2);
        let batch = vec![
            reading(a, 0, 1.0), // a: first sighting, kept
            reading(b, 1, 1.0), // b: first sighting, kept
            reading(a, 2, 1.0), // a: repeat, dropped
            reading(b, 3, 2.0), // b: change, kept
        ];
        assert_eq!(retain_changed(&mut cache, &batch).len(), 3);
    }

    #[test]
    fn a_change_back_and_forth_keeps_all_three() {
        // a -> b -> a: each differs from the immediately prior written value,
        // so none is a repeat.
        let mut cache = HashMap::new();
        let u = dedup_uuid(1);
        let batch = vec![reading(u, 0, 1.0), reading(u, 1, 2.0), reading(u, 2, 1.0)];
        assert_eq!(retain_changed(&mut cache, &batch).len(), 3);
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

    /// How many readings rows exist across a set of uuids.
    async fn count_readings(client: &Client, ids: &[uuid::Uuid]) -> i64 {
        let ids = ids.to_vec();
        client
            .query_one("SELECT count(*) FROM readings WHERE uuid = ANY($1)", &[&ids])
            .await
            .unwrap()
            .get(0)
    }

    /// The recorded last-observed time for one series.
    async fn last_seen(client: &Client, id: &uuid::Uuid) -> SystemTime {
        client
            .query_one("SELECT last_seen FROM series_state WHERE uuid = $1", &[id])
            .await
            .unwrap()
            .get(0)
    }

    /// Prove the dedup + series_state contract against a live store: repeats add
    /// no readings rows but still advance last_seen, a real change adds exactly
    /// one row, and a fresh sink instance seeds its cache so a post-restart dump
    /// dedups too. Skips cleanly when TEST_DATABASE_URL is unset.
    #[tokio::test]
    async fn dedups_repeats_but_advances_series_state() {
        let Ok(dsn) = std::env::var("TEST_DATABASE_URL") else {
            eprintln!("TEST_DATABASE_URL unset, skipping live dedup/series_state test");
            return;
        };
        let pg_config: tokio_postgres::Config = dsn.parse().expect("parse TEST_DATABASE_URL");

        let (verify, connection) = pg_config.connect(NoTls).await.expect("connect verify client");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Four clearly-synthetic series.
        let uuids: Vec<LoxUuid> = (1..=4).map(dedup_uuid).collect();
        let pg: Vec<uuid::Uuid> =
            uuids.iter().map(|u| uuid::Uuid::from_bytes(*u.as_bytes())).collect();

        // Clear any leftovers before asserting counts.
        for id in &pg {
            cleanup(&verify, id).await;
        }

        let batch_at = |values: &[f64], ts: SystemTime| -> Vec<Reading> {
            uuids
                .iter()
                .zip(values)
                .map(|(u, v)| Reading { uuid: *u, ts, value: *v })
                .collect()
        };

        // A fresh sink seeds its dedup cache from an (empty-for-us) series_state.
        let sink = TimescaleSink::from_pg_config(pg_config.clone()).await.expect("build sink");
        let metas: Vec<SeriesMeta> = uuids
            .iter()
            .map(|u| SeriesMeta {
                uuid: *u,
                control: "InfoOnlyAnalog".to_owned(),
                name: "Synthetic Dedup".to_owned(),
                room: None,
                category: None,
                sub: "value".to_owned(),
                unit: None,
                agg_mode: AggMode::Gauge,
            })
            .collect();
        sink.upsert_meta(&metas).await.unwrap();

        let base = UNIX_EPOCH + Duration::from_secs(1_600_000_000);
        let values = [1.0, 2.0, 3.0, 4.0];

        // ts1: first sighting, the whole batch lands and last_seen == ts1.
        let ts1 = base;
        sink.write(&batch_at(&values, ts1)).await.unwrap();
        assert_eq!(count_readings(&verify, &pg).await, 4, "the whole first batch lands");
        for id in &pg {
            assert_eq!(last_seen(&verify, id).await, ts1, "last_seen is the first observation");
        }

        // ts2: identical values re-offered (a reconnect dump). No new readings,
        // but last_seen advances for every series.
        let ts2 = base + Duration::from_secs(60);
        sink.write(&batch_at(&values, ts2)).await.unwrap();
        assert_eq!(count_readings(&verify, &pg).await, 4, "an unchanged dump adds no readings");
        for id in &pg {
            assert_eq!(last_seen(&verify, id).await, ts2, "last_seen still advances");
        }

        // ts3: exactly one value changes; exactly one new readings row.
        let ts3 = base + Duration::from_secs(120);
        let changed = [100.0, 2.0, 3.0, 4.0];
        sink.write(&batch_at(&changed, ts3)).await.unwrap();
        assert_eq!(count_readings(&verify, &pg).await, 5, "one change adds exactly one row");

        // A second sink instance seeds its cache from series_state, so re-offering
        // the current values dedups every one of them.
        let sink2 = TimescaleSink::from_pg_config(pg_config.clone()).await.expect("build sink2");
        let ts4 = base + Duration::from_secs(180);
        sink2.write(&batch_at(&changed, ts4)).await.unwrap();
        assert_eq!(count_readings(&verify, &pg).await, 5, "the startup seed dedups the dump");

        for id in &pg {
            cleanup(&verify, id).await;
        }
    }
}
