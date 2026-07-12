//! The sink abstraction: where resolved readings are written.
//!
//! Decoupling ingestion from storage is the whole point of the crate, so the
//! sink is a trait from day one. A sink receives two kinds of input, kept apart
//! so it stays storage-agnostic:
//!
//! - [`Sink::write`] takes a batch of uuid-keyed [`Reading`]s (the hot path).
//! - [`Sink::upsert_meta`] takes the mutable human labels for a set of uuids.
//!
//! Splitting the labels out of the readings is deliberate: a reading is just
//! `{uuid, ts, value}`, and a rename in Loxone Config changes only the labels,
//! never the uuid. A storage backend records readings by uuid and joins the
//! labels in separately, so a rename is a label upsert, never a new series.
//!
//! Milestone 1 ships only the [`debug`] sink; a later milestone plugs a real
//! store in behind this same trait, additively.

use crate::config::{Config, SinkKind};
use crate::model::{Reading, SeriesMeta};

pub mod debug;
pub mod timescale;

/// A destination for resolved readings and their labels.
///
/// `Send + Sync` so a single sink can be shared across the runtime's tasks; the
/// methods take `&self` and use interior mutability where they must, so a sink
/// is cheap to share behind a `Box`/`Arc`.
#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    /// Persist a batch of readings.
    async fn write(&self, batch: &[Reading]) -> anyhow::Result<()>;

    /// Upsert the human labels for a set of series uuids.
    async fn upsert_meta(&self, meta: &[SeriesMeta]) -> anyhow::Result<()>;
}

/// Construct the sink named by the config selector.
///
/// The single seam the daemon uses to pick a backend. It is async and fallible
/// because a real store connects (and runs its startup migrations) here, either
/// of which can fail.
pub async fn build(config: &Config) -> anyhow::Result<Box<dyn Sink>> {
    match config.sink {
        SinkKind::Debug => Ok(Box::new(debug::DebugSink::new())),
        SinkKind::Timescale => {
            let ts = config.timescale.as_ref().ok_or_else(|| {
                anyhow::anyhow!("sink 'timescale' selected but no 'timescale:' config section present")
            })?;
            Ok(Box::new(timescale::TimescaleSink::connect(ts).await?))
        }
    }
}
