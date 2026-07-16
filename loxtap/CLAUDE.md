# loxtap (ingest daemon)

Connects to a Loxone Miniserver and writes readings into TimescaleDB.

## Module map

- `config` — YAML config.
- `transport` — websocket framing.
- `crypto` — RSA/AES handshake plus server-key pin.
- `auth` — token auth and refresh.
- `identity` — host-derived client id.
- `structure` — LoxAPP3.json to labels/units/kind.
- `events` — binary status stream to readings.
- `model` — Reading / SeriesMeta types.
- `sink` — `sink/timescale.rs`, `sink/debug.rs`; a `Sink` trait.
- `migrate` — embedded refinery migrations.
- `supervisor` — session lifecycle, backoff.

## Key invariants

- Every series is keyed on its 128-bit UUID; the human name is a mutable label
  (a Loxone rename is a `series_meta` UPDATE, never a new series).
- Writes are change-only (bit-identical repeats dropped).
- The daemon owns its schema — SQL lives in `loxtap/migrations/` and is applied on
  first connect.
- The server-key pin is fail-closed.

## Config

See [../doc/LOXTAP-CONFIG.md](../doc/LOXTAP-CONFIG.md); sample `config.example.yaml`.
