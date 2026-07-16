# loxkit

loxkit bridges a [Loxone](https://www.loxone.com/) Miniserver into
[TimescaleDB](https://www.timescale.com/) and serves config-driven dashboards
over the same store. Every series is keyed on its stable 128-bit UUID, so
renaming a room or a control in Loxone is a label change here, never a lost
series. It is two decoupled daemons — the loxtap ingest bridge and the loxweb web
reader — that share only the database. MIT-licensed.

```
Miniserver ──ws──> loxtap ──> TimescaleDB <── loxweb ──> browser
```

## Components

| Component | Role | Binary | Config | Service |
|-----------|------|--------|--------|---------|
| `loxtap` | Ingest daemon: connects to the Miniserver and writes readings | `/usr/bin/loxtap` | `/etc/loxkit/loxtap.yaml` | `loxtap.service` |
| `loxweb` | Dashboard reader: a pure reader over the shared store | `/usr/bin/loxweb` | `/etc/loxkit/loxweb.yaml` + `/etc/loxkit/dashboards.yaml` | `loxweb.service` |

The two never talk to each other. loxtap fills the database; loxweb reads it.
Either can be restarted, upgraded, or run on a different host without touching
the other.

## How it talks to the Miniserver

loxtap opens a plain websocket to `ws://host:port/ws/rfc6455` with the
`remotecontrol` subprotocol and runs the Miniserver's RSA/AES handshake over it.
The server's public key is pinned: its SHA-256 fingerprint must match the
`server_key_pin` in the config, and the daemon refuses to start on a mismatch,
so the pin fails closed. Once the session key is established, loxtap
authenticates with a token it acquires and refreshes over the same channel.

The Miniserver's structure file (`LoxAPP3.json`) supplies the human-readable
side of each series — its label, unit, and whether it is a gauge or a counter.
After that, a binary status stream pushes `{uuid, f64}` deltas as values change;
loxtap stamps each with its receive time and hands it to the sink.

## How ingestion works

Everything lands under a UUID key, split across three tables:

- `readings` — a TimescaleDB hypertable of `(uuid, ts, value)`, the raw samples.
- `series_meta` — the mutable labels (name, room, unit, kind) for each UUID. A
  Loxone rename is a single `UPDATE` here; the readings never move.
- `series_state` — a per-UUID liveness and latest-value cache.

Writes are change-only: a value bit-identical to the last one stored for that
UUID is dropped, so a sensor that reads flat for an hour costs one row, not
thousands. Surviving samples go in over a binary `COPY`. Older chunks are
compressed into the columnstore after seven days, and hierarchical continuous
aggregates roll the data up — `readings_1h` from the raw samples, a daily
aggregate from the hourly one, both real-time — so a multi-year query reads a
handful of rolled-up rows instead of scanning years of raw points.

The daemon owns its schema. On first connect it applies its embedded migrations,
creating the tables, hypertable, compression policy, and aggregates itself;
there is no separate migration step to run.

## How the web frontend works

loxweb is a pure reader: it needs the shared database, not the loxtap process. It
serves a small REST API plus a single-page app built on
[Apache ECharts](https://echarts.apache.org/), both compiled into the one
binary — there are no static assets to deploy.

The dashboards are *data*, not code. A `dashboards.yaml` file describes
`views → panels → axes + series`: each series is either *bound* to a store UUID
or *derived* from its siblings by an expression. The query engine tier-routes
bucketing by zoom — a wide window reads a coarse aggregate, a zoom re-queries a
finer one — so panels stay responsive from a live view out to multiple years.
Editing the file hot-reloads the views automatically within a couple of seconds,
without dropping the service and with no signal or command needed; an invalid
edit is logged and the previous set stays live.

## Quick start

Build both binaries from the workspace:

```sh
cargo build --release --workspace
```

or install the Debian package, which ships both daemons and their unit files.
Then fill in the two config files — start from the samples:

- `loxtap/config.example.yaml` — the Miniserver connection, the server-key pin,
  and the TimescaleDB sink.
- `loxweb/config.example.yaml` — the HTTP bind, a read-only database connection,
  and the path to the dashboards file.

A package install instead ships these as `/etc/loxkit/*.example.yaml`; copy each
to its real name (`loxtap.yaml`, `loxweb.yaml`, `dashboards.yaml`) — the config
guides give the owner and mode. Its units are already enabled and stay skipped
until the config exists.

With TimescaleDB reachable and both configs in place, start the daemons:

```sh
systemctl start loxtap loxweb
```

loxtap applies its schema on first run and begins ingesting; loxweb serves the
dashboards you describe.

## License

MIT. loxweb vendors Apache ECharts, distributed under Apache-2.0 (see
`loxweb/web/echarts.LICENSE`).
