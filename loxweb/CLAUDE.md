# loxweb (dashboard server)

A config-driven, read-only dashboard server over the store loxtap fills.

## Module map

- `config` — server plus read-only DB config.
- `dashboard` — the `dashboards.yaml` model and validation (`deny_unknown_fields`;
  hot-reload via ArcSwap on file change).
- `http` — axum router; `/api/views`, `/api/series`, `/api/extent`; serves the SPA.
- `query` — tiered `time_bucket_gapfill` SQL.

The frontend under `web/` (index.html, app.js, style.css, vendored
echarts.min.js) is compiled into the binary with `include_str!`.

## Key facts

- Pure reader: needs only the shared DB, never writes.
- Dashboards are *data*, not code.
- The config strictly rejects unknown keys.
- A bad dashboards edit is logged and the previous set stays live.

## Docs

The dashboards schema is documented in [README.md](README.md) with a
machine-readable [dashboard.schema.yaml](dashboard.schema.yaml); server config in
[../doc/LOXWEB-CONFIG.md](../doc/LOXWEB-CONFIG.md).
