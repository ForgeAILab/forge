# Pre-queue upgrade fixtures

`pre_queue.tar.xz` holds 17 installs as the last build before the merge-queue
cutover left them, each stopped in one in-flight merge shape. They were not
written by hand: real Tasks were driven through the old merge path and the
database and directory were copied at each shape.

- Generating commit: `9b0151af` (`scratch/pre-queue-fixtures`, never merged) on
  top of `next/v0.14` `f7605a2b`.
- Generating test: `model_workflow::generate_pre_queue_fixtures` (api crate,
  ignored), which uses fault-injection seams of that scratch commit to stop
  the old merge between its durable writes.
- The old code no longer exists on this branch, so the fixtures cannot be
  regenerated here.

Each directory in the archive has:

- `db.sql`: a dump of the checkpointed SQLite database (schema of `f7605a2b`,
  so the test also runs every later migration on real rows).
- `manifest.json`: the shape, the absolute `root` the rows and Git link files
  name, the Project, the Tasks with their state at the copy, and `main`.
- `repo/` and `workspaces/`: the Project's checkout and the Task worktrees.

Two edits were made when packing, nothing else:

- The daemon's machine id names `fixture-host` instead of the generating
  host. The test replaces it by the running host's embedded machine id.
- The dump creates the semantic-memory FTS table with its `CREATE VIRTUAL
  TABLE` statement and leaves its index empty (a dump of the shadow tables
  cannot be loaded through a plain connection). Nothing in a merge reads it.

The `pre_queue_*` cases in `crates/api/tests/model_workflow/upgrade_cases.rs`
use the helpers in `crates/api/tests/model_workflow/upgrade.rs` to unpack one
fixture into a temporary directory, replace `root` by that directory in every
text file (the dump, the manifest, Git's worktree link files), load the dump and
start this build on it the way `RuntimeSupervisor::start` does. The
`pre_queue_bare_*` cases do the same after deleting every integration attempt
and queue row: that is what an install that never ran the passive queue stages has.

Shapes (design section 5 rows): p01 outcome recorded, Task still merging; p02
merge intent with and without the merge landed; p04 entry hooks pending; p05
rebase started, no outcome; p06 clean-rebase bounce in `merge_failed` and in
`review`; p07 conflict handed off, before and during the coder repair; p09
back at the merge gate after the bounce; p10 / p11 Project paused, and
resumed with nothing continued; p13 owner Hold (with and without a paused
Project); p14 entry step ran against a locked index; p19 dirty worktree and
dirty target. Not reached by real execution, covered by row-level tests only:
P8, P12, P15, P16, P17, P18, P20.
