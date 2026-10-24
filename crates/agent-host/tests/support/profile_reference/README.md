# Profiling reference

Copied from `agent-runtime` tag `v0.9.0`, commit
`8c19be5b226c2fdaeb6708ec5d39e91c235857a5`, under
`crates/agent-runtime-journal-bench/src/`.

This test-only model compresses data and models nonce/tag lengths; it does not
perform production AEAD. It shares equal bodies between a session's domains.
The profiling test supplies Forge's unchanged fixture, conservative projection,
128 MiB memo budget, per-turn S2/collection and final collection.

Adaptations: `agent_runtime_core` imports use the facade's identical
`agent_runtime::core` types; `lib.rs` is named `mod.rs`; unused re-exports and the
separate `packed_tests.rs` module registration are omitted. Formatting follows
the host repository. No storage algorithm or persisted codec was changed.
