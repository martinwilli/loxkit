//! Rebuild when the embedded migration set changes.
//!
//! `embed_migrations!` globs the `migrations/` directory at macro-expansion
//! time. rustc's own dependency tracking then only re-triggers when an
//! already-embedded file's contents change (through the `include_str!` the
//! macro emits), so *adding* a new migration file is invisible until some
//! unrelated `.rs` recompiles and the macro re-globs. Watching the whole
//! directory closes that gap: adding, editing, or removing anything under
//! `migrations/` re-runs the build and re-expands the macro.

fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
