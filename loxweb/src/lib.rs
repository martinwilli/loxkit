//! loxweb: a config-driven dashboard engine over a loxtap TimescaleDB store.
//!
//! A pure read client. loxtap owns the Miniserver relationship and writes the
//! uuid-keyed series; loxweb only queries that store and serves a set of charts
//! described entirely by configuration. Because it ships publicly the same way
//! loxtap does, no view is baked into code: the dashboards are *data*.
//!
//! This library exposes the two building blocks the binary wires together:
//! [`config`] (the server + database settings, loaded once at startup) and
//! [`dashboard`] (the reloadable view model, its validation, and the hot-swap
//! holder behind a SIGHUP). The query layer and the HTTP server land on top of
//! these in later commits.

pub mod config;
pub mod dashboard;
pub mod query;
