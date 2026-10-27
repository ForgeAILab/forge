# Archived-main writer parity fixtures

Both corpora were captured from archived main
`9460941117b4e326c5f090c0e8f48b8f8d142c25` (`94609411`). The single harness is
[`../../writer_fixture.rs`](../../writer_fixture.rs). Branch tests only compare
against these records; the harness has no fixture-writing mode.

| Corpus | Records | Comparison |
| --- | ---: | --- |
| `main-94609411.jsonl` | 110 | Per-field byte length and SHA-256 after the original identity/time substitutions; `provenance.tsv` records each case's archived writer locations |
| `writer-main-94609411.jsonl` | 256 | Six stored columns verbatim after the original substitutions, plus a projection SHA-256; includes two exact `/raw` merge annotations |

The fingerprint corpus compares `error_annotation`, `blocked_json`, `failed_json`,
`entry_barrier_json`, `metadata_json`, `condition_json`, `public`, `read`,
`failure_kind`, `operations_text`, `offers`, `attention_digest`, and
`attention_incident_digest`. Each fingerprint covers the serialized field value,
including the JSON quoting around stored strings.

The verbatim corpus names the same six stored columns `annotation`, `blocked`,
`failed`, `barrier`, `metadata`, and `condition`. Their inner JSON strings retain
member order, nulls, and escaping. `projection_sha256` hashes compact, recursively
key-sorted JSON containing `read`, `public`, `offers`, and `event_digest`.
Only `plan-wait` omits `event_digest`, as in its original archived-main capture.
The `merge-budget/raw` and `manual-merge/raw` records compare annotation strings
directly, without the snapshot projection hash.

## Options and callers

All options live in `writer_fixture.rs`. The comparison profile selects its
original corpus, identity substitutions, snapshot encoding, metadata-write time,
and phase names together; no caller gains extra substitutions.

| Option | Behavior and callers |
| --- | --- |
| `Comparison::FieldFingerprints` (default) | Used by `demo`, `task_dispatcher/workspace_blocking`, and `task_service/{actions,dependencies,subtask}` condition tests. Replaces Task/Project/primary Repo IDs, `/coordination_review_pending_id`, `/queued_recovery/id`, and `/placement_refusal/eligibility_key` with their original fixture tokens. Phases are `first`, `following-unconverted-write`, and conditional `queue-overlay`; the metadata write consumes the frozen clock. Queue overlays retain the additional literal merge assertion. |
| `Comparison::VerbatimWithProjectionHash` | Used by every `writer_parity_tests` module and the queue, owner-failure, and executor-failure cases using `case!`. Replaces Task/Project IDs, `awaiting_human_marker_id`, transition IDs in row order, and generated step IDs in sequence order (excluding the claimed step). Phases are `first`, `second`, and conditional `queue`; the metadata write uses `2026-10-25T00:00:00Z`. Also selected explicitly by the two raw merge assertions. |
| `ClockMode::RestartAfterSetup` (default constructor) | The fingerprint callers above initialize the DB before starting their own frozen sequence. |
| `ClockMode::PreserveCaller` | Every `case!` caller, `workflow/engine/durable::writer_parity_tests::blocked_entry_barrier`, and `workflow/actions/merge::writer_parity_tests::merge_annotations`. DB setup uses a nested clock and restores the caller's sequence, including clock calls preceding fixture creation. |
| `metadata_times` (default empty) | `task_service/dependencies::condition_tests`: `review-retry`, `admission-owner-wait`, and `admission-expired` replace `/owner_wait/started_at` with `fixture-time/owner_wait/started_at` and `/deferred_dispatch/not_before` with `fixture-time/deferred_dispatch/not_before`. Every present timestamp must parse as RFC3339. |
| `condition_times` (default empty) | Only `admission-expired` replaces `/blocked_at` and `/created_at` in annotation/interruption strings with `fixture-admission-time`, after RFC3339 validation. |
| `workflow_offers` (default `None`) | `demo::condition_tests::demo_annotations_match_main_literal_bytes` selects workflow offers and asserts `[]` for both `demo-reset` and `demo-error`. All other snapshots retain `condition_offers`, including its nonempty-offer assertion. |
| `ClockNormalization::None` (default) | Every snapshot except the two cases below; no post-event clock substitutions. The explicit `metadata_times` / `condition_times` substitutions still apply where configured. |
| `ClockNormalization::PlanWait` | Only `plan_artifact/transport::writer_parity_tests::plan_wait` (`plan-wait`). Validates that `plan_settlement_wait.retry_at - owner_wait.started_at` is exactly five seconds, replaces those times with `<transport-retry>` / `<transport-now>`, and omits the event digest that embeds the uncontrolled times. Runs after canonical event construction, as before. |
| `ClockNormalization::ExecutionRetry` | Only `task_service/execution/cascade/failure::condition_tests::unavailable_and_retry` (`retry-metadata`). Validates RFC3339 round-tripping and replaces `deferred_dispatch.not_before` with `<execution-retry-deadline>`, after canonical event construction. The caller still asserts its real deadline lies within the ten-second retry window. |

The frozen clock starts at `2026-10-25T00:00:00.123456+00:00` and advances one
microsecond per call. These options preserve the existing comparisons; they do
not replace arbitrary timestamps, truncate strings, or regenerate expectations.

## Regeneration: archived main only

Never derive expected records from the branch under test. Use archived main
`9460941117b4e326c5f090c0e8f48b8f8d142c25`, keeping its production writers
unchanged. Install the matching test harness/cases and frozen clock there;
adapt only test plumbing to the archived APIs (in particular, retain main's
`Value` input for executor attempts).

For `main-94609411.jsonl`, add a test-only capture at the fingerprint assertion,
emit each complete `{"case": ..., "fields": ...}` record, and run the fingerprint
callers listed above with `--test-threads=1`. Sort records by `case` and retain
exactly one provenance row per case. The provenance file identifies the original
writer locations and the later archived-main captures that completed this corpus.

For `writer-main-94609411.jsonl`, add a test-only JSONL capture at
`assert_main_parity`, set `FORGE_WRITER_ORACLE_OUT` in that capture hook, and run
the `writer_parity_tests::` cases plus `queue_channels_and_overlays`, the
owner-failure `owner_recovery` and executor-failure `unavailable_and_retry`
cases with `--test-threads=1`. Emit complete `{"name": ..., "raw": ...}` records
and sort by `name`. The original preparation script and command log are retained
with the gate review report in `31g2a2/`.

Capture through the original normalization and comparison encoding described
above. Check record counts, unique keys, and SHA-256 hashes before bringing an
intentional fixture update back for review. The harness consolidation moves
the verbatim corpus byte-for-byte and leaves the fingerprint corpus and
`provenance.tsv` unchanged; it does not perform regeneration.
