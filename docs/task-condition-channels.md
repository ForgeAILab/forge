# Condition channel encodings

`db::task_condition::channels` defines storage values for new writers. The four
columns and the 24 condition metadata keys stay in place. `map_legacy_condition`
continues to produce `condition_json`. No existing row, restore, reader, backfill
or mapper calls the strict decoders in this module.

`ErrorAnnotation` has an `AnnotationShape` and one optional `StepFailureOverlay`.
Failure kind is a field of each shape. Decoding selects a shape by its field set;
`merge_conflict`, `ci_failed` and `dispatch_failed` each occur in multiple shapes.
The queue overlay replaces its previous value. It cannot contain another
annotation or another overlay.
An overlay on a detected-with-state payload has the same fields as a native
step failure; construction and decoding both use the step-failure form. Value
equality compares payload and overlay, excluding the stored field order.

The annotation shapes are blocking struct, hold, timed block, execution recovery,
remote cancellation, message literal, reason, review candidate, detected,
detected with state, budget, step failure, placement refusal, plan wait and daemon
upgrade dispatch. `plan_settlement_wait` is its own closed stored tag, outside
the public `FailureKind` vocabulary. Interruption kind likewise explicitly
includes `integration_admission_failed`. Unknown failure kinds, fields and tags
are rejected by strict decoding. Optional presence retains the difference
between missing and null. An interruption reason can be null, as in the queue's
parked-loop writer.

## Bytes

These values retain strings as strings, including timestamp spelling. They do
not parse and reformat timestamps. `json!` and `Value` use sorted object keys in
this workspace. `TaskBlockingAnnotation` writes declaration order, including its
artifact; demo message literals put `type` first. `ErrorAnnotation` retains the
encoding of those writer families when decoded. `ErrorAnnotation::new` uses
declaration order for blocking and message shapes; `ErrorAnnotation::sorted`
selects the `Value` encoding. Applying a step overlay sorts the entire value,
including nested objects, just as the queue does.

The metadata types use the sorted encoding of their producers, including nested
machines, placement candidates and saved annotation snapshots. The enum
`ConditionMetadataValue` serializes just the value. `decode(key, raw)` chooses the
type from the exact metadata key; it does not add a stored discriminator. Scalar
keys remain booleans, strings or integers. `planning_state_entry_token` also
accepts explicit null, which its producer writes when no entry exists.

SQL `json_set` preserves existing member positions and appends absent members.
It must not be replaced with a blanket parse/struct/serialize pass over an old
row. During gate 2, keep the existing SQL patch and envelope ordering for:

- `sqlite/task/action_scope.rs`: probe-error `environment_wait.last_error` and
  `deferred_dispatch.reason` edits, and the two whole-marker insertions.
- `sqlite/task/dispatch.rs`: provision-reason edits and placement, owner and plan
  marker insertions.
- `sqlite/task/operator.rs`: review-window `started_at`, review annotation
  `blocking_reason`, deferral and owner-marker edits.
- `sqlite/task/workflow.rs` and `sqlite/task/roles.rs`: pause-generation updates,
  deferred markers and carried `paused_integration.state`.

Use typed values for marker payloads, leave their enclosing `json_set` operations
in place, and use typed-value comparisons for fences where a stored patched
value's order differs from a fresh struct. A whole-value rewrite is valid only
when its bytes match the original writer for that row. In particular,
`operator.rs`'s review-annotation update requires the old `blocking_reason` in its
predicate, so it replaces that member rather than adding one; its entry-window
update can append `started_at`. Historical, noncanonical and non-text values
retain the old path.

## Caller-supplied JSON

Known hook payloads are closed types: dependency-gate identity and cancelled
dependency IDs, or the before-work script result (including its string-valued
environment dictionary). Known interruption details are also typed: executor
route attempts, retry time, assignment guidance and dependency snapshots. Local
and remote route-attempt writers both emit `candidate_key` and `outcome` strings;
remote outcome strings come from the terminal notification.

Opaque JSON is confined to copied snapshots, bounded to 32 KiB and 32 container
levels, and never truncated:

- `workflow/engine/mod.rs::annotate_upgrade_dispatch_refusal` copies the Task's
  `dispatch_disposition`, `deferred_dispatch` and retained annotation into its
  refusal fence. Those stored inputs can have a schema unknown to this build.
- `task_service/dependencies.rs::block_cancelled_dependencies` passes an
  existing `details.superseded` object back to `write_dependency_block`. Fresh
  snapshots use `SupersededCondition`; carried objects use
  `BoundedConditionObject`. Saved channel bytes inside a fresh snapshot remain
  opaque strings, including malformed historical text.

There is no open JSON fallback for an ordinary hook or interruption-details
object. A copied historical row that exceeds these construction bounds must
continue through the existing raw restore path in gate 2; these types do not
authorize truncation, eager decoding or a change to legacy mapping.

## Verification

The channel tests use literals generated by real writers in an archive of main
`13bc2ae1`, plus the exact fixed demo literals. The fixture directory records
their provenance. Tests compare serialized bytes, exercise shared tags and
overlays, reject unknown fields and tags, bound opaque snapshots, and compare
legacy mapping with an independent archived-main reference.
