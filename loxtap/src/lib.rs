//! loxtap: a generic, uuid-stable Loxone -> TimescaleDB bridge.
//!
//! The Miniserver websocket only ever sends `{128-bit uuid, 64-bit float}`;
//! the uuid is the one stable identity, so every series is keyed on it and the
//! human name is a mutable label. This library exposes the building blocks
//! (config, wire model, transport, crypto, auth, structure, events, sinks);
//! the `loxtap` binary wires them into a long-running daemon.

pub mod auth;
pub mod config;
pub mod crypto;
pub mod events;
pub mod migrate;
pub mod model;
pub mod sink;
pub mod structure;
pub mod supervisor;
pub mod transport;
