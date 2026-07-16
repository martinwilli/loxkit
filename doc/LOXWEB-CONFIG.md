# loxweb configuration

loxweb is a pure reader over the store loxtap fills: it needs the shared database,
not the loxtap process. It has two config files — a small server config and a
dashboards file that describes the views. This guide covers the first in full and
gives a short tour of the second.

## Part A — the loxweb config

Lives at `/etc/loxkit/loxweb.yaml`, mode `0640`, owned `root:loxweb` (it holds a
database password). loxweb takes `--config <FILE>`; the packaged `loxweb.service`
unit passes that path. The Debian package ships a template at
`/etc/loxkit/loxweb.example.yaml`; copy it to the real name with that owner and
mode:

```sh
sudo install -o root -g loxweb -m 0640 \
    /etc/loxkit/loxweb.example.yaml /etc/loxkit/loxweb.yaml
```

The unit is already enabled but stays skipped until the config exists, so
`sudo systemctl start loxweb` brings it up once the file is in place. From a
source build, start instead from the in-tree `loxweb/config.example.yaml`.

```yaml
server:
  bind: "127.0.0.1:8080"
  base_path: ""

timescale:
  host: timescale.example
  port: 5432
  user: loxweb
  password: password
  dbname: loxtap
  # timezone: "Region/City"

dashboards: /etc/loxkit/dashboards.yaml

log_level: info
```

### `server:`

- **`bind`** — the listen address. loxweb serves plain HTTP; terminate TLS at a
  reverse proxy and keep this on the loopback (or a private interface) behind that
  proxy.
- **`base_path`** (optional) — a sub-path mount for a reverse proxy, e.g.
  `/loxweb`. Empty serves at the root. The frontend uses relative URLs, so this is
  the only place the sub-path is set.

### `timescale:`

Same keys as loxtap's sink (`host`, `port` default `5432`, `user`, `password`,
`dbname`, optional `timezone`), but point it at a **read-only** role: loxweb only
ever issues `SELECT`. See the read-only role in
[doc/TIMESCALEDB-SETUP.md](TIMESCALEDB-SETUP.md). Plain Postgres over a trusted
LAN (no TLS).

### `dashboards:` and `log_level:`

- **`dashboards`** — path to the dashboards file (below). The package ships an
  example at `/etc/loxkit/dashboards.example.yaml`; it holds no secrets, so a
  plain `sudo cp /etc/loxkit/dashboards.example.yaml /etc/loxkit/dashboards.yaml`
  is enough.
- **`log_level`** — fallback tracing level (`error`/`warn`/`info`/`debug`/
  `trace`) when `RUST_LOG` is unset.

### Reloading

Edit the dashboards file and loxweb hot-reloads the views on its own, within a
couple of seconds, without dropping the socket — no signal or command needed. An
invalid edit is logged and the previous set stays live. Startup, by contrast, is
fail-fast: a broken config or dashboards file stops loxweb from coming up at all.

## Part B — the dashboards file

The dashboards file is *data*, not code. Its shape is a nesting of three levels:

- a file is `views[]` — each view is a tab;
- a view is `panels[]` — each panel is a chart;
- a panel is `axes[]` plus `series[]`.

A **series** is either *bound* — a store `uuid` plus an `agg` telling loxweb how
to aggregate it — or *derived* — an `expr` computed over sibling series `id`s in
the same panel.

```yaml
# yaml-language-server: $schema=./dashboard.schema.yaml
views:
  - id: overview
    title: Overview
    range: now-2d
    panels:
      - title: Climate
        axes:
          - {id: temp, side: left, unit: "°C", min: 0, max: 40}
        series:
          - id: t_in
            label: Indoor
            uuid: 00000000-0000-0000-0000-000000000001   # a bound series
            agg: avg
            type: line
            axis: temp
          - label: Doubled
            expr: "t_in * 2"                              # a derived series
            type: line
            axis: temp
```

Replace the placeholder uuid with one from your store
(`SELECT uuid, name, room, unit FROM series_meta;`).

**Aggregation.** `agg` is one of `avg`, `max`, `min`, `median`, `last` for a
plain gauge, or `delta` / `integral` for a counter (`integral` is time-weighted).
Omit it and loxweb resolves a default from the series' stored kind.

**Bucketing.** The query engine tier-routes bucket width by the window span: a
wide window reads a coarse aggregate, a zoom re-queries a finer one, so panels
stay responsive from a live view out to years. A `bucket` on a view or panel
overrides that router — e.g. `bucket: month` pins real calendar months in the
store's local time.

### Full reference

This is only a taste. The complete key-by-key reference lives in the
**dashboards schema** section of [loxweb/README.md](../loxweb/README.md), and the
accepted keys are pinned by a machine-readable JSON Schema at
[loxweb/dashboard.schema.yaml](../loxweb/dashboard.schema.yaml). Wire your editor
to it — for completion and typo-flagging as you edit — with a first-line comment
in the dashboards file:

```yaml
# yaml-language-server: $schema=./dashboard.schema.yaml
```
