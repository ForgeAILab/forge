# Main condition-channel literals

Baseline: main `13bc2ae1041137d53f9372d93a2ba08d8f5b0ead`.

`main-13bc2ae1.jsonl` contains 118 byte literals: 66 annotations, 15
interruptions, three entry barriers and 34 metadata values. The tests preserve
the raw string, including key order, nulls, escaping and timestamp spelling.
The corpus covers 15 annotation variants and all 24 metadata codecs.

The main driver was compiled and run in a `git archive` of that commit. It
called production service writers under a claimed Task step, reread their
stored columns, and invoked the actual queue writer for overlay samples.
`db::test_clock::freeze` stayed held on the current-thread runtime. The two
`demo-*` samples instead copy the exact fixed literals from `demo.rs:107,116`.
The main driver produced 103 records and the guard/admission extension produced
seven; the opaque-boundary extension produced five. The unrelated `queued_recovery` record is excluded from this corpus.

The verify barrier was captured after `set_entry_barrier` committed its real
transaction. Its timestamp call held the frozen clock in a synchronous block,
so check-runner leases used the real clock before that point. The archived
end-to-end driver subsequently failed its original lifecycle assertion: the
frozen timestamp predated the live entry and the Task moved on. The committed
barrier literal is retained; that broader archived flow is **not a passing
smoke test**. No lifecycle assertion was removed or weakened.

`executor_unavailable_execution_id` has no active setting producer on this
baseline. Its `String` codec uses the literal emitted by the real
`planning_execution_id` producer; it is marked as codec reuse in the provenance
table. It is not claimed as a newly observed write of that historical key.

`legacy-main-13bc2ae1.jsonl` independently records the baseline mapper's output
for malformed JSON, unknown kind, historical `agent_timeout`, missing kind/time,
deep legacy JSON, superseded text and all five non-text channel classifications.
The branch calls the unchanged lenient mapper for those cases. Non-text input is
represented by the mapper's existing `non_text` boundary, including BLOB and
invalid-UTF8 classifications; no strict channel decoder is involved.

`provenance.tsv` maps every literal to its producer. The external generation
drivers, archive, source audit and command logs are retained with the gate's
report. The archive widens test visibility and adds test-only clock/capture
hooks; it does not replace the channel constructors or serialization code.
Its disposable Git fixture was built without commit/add/checkout/reset commands.

SQL patches to existing JSON are a separate boundary: a patch can append a
member after the sorted fields. Gate 2 must preserve those SQL edits and use
typed-value comparisons where needed, as documented in
`docs/task-condition-channels.md`. These fixtures do not authorize
canonicalizing historical or patched rows through a strict decoder.
