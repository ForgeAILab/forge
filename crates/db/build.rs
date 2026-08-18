fn main() {
    // migration.rs embeds crates/db/migrations via include_dir!, which Cargo
    // does not reliably track; without this, a newly added migration can be
    // silently missing from an incremental build.
    println!("cargo:rerun-if-changed=migrations");
}
