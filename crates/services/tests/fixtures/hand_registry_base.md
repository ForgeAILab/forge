# Slice E2 literal hand-path checkpoint

`hand_registry_base.sqlite` and `hand_registry_base.json` preserve the hand-path
command receipts and responses from `33e2a9740adfd69e29850dcde9c9cde28575d874`.
Each operation was captured before its registry entry was added. The database
contains only synthetic test identities and artifacts. Inspect it with SQLite's
`immutable=1` URI option so inspection does not create WAL sidecars here.

The setup uses `main_project_create_command`'s real Genesis, immutable Profile,
Charter approval and atomic Project/create/handoff helpers. Its milestone is
created through the native definition command, with a complete acceptance check
and matching evidence requirement. Adoption setup uses a separate legacy
Project; the approved Charter is never deleted or detached. Main draft starts a
new Genesis; Genesis start cancels that draft's session and uses the fixture's
leased user-message turn. No external service is called.

Responses and receipt inputs are not scrubbed or renumbered. Replay tests copy
the checkpoint into `tempfile::tempdir()` and compare receipt digests, event IDs,
outcome JSON and literal result JSON. Genesis has a nested `replayed` marker;
its first and repeated responses are both recorded. The first evidence capture
record predated that helper and its expected outer/inner replay markers are
explicit in the test.

The Project authority tables intentionally retain successful immutable receipt
retrieval after the identity is paused or its effective ceiling is reduced.
Registry admission would narrow these hand paths, so adoption and evidence
remain unregistered. The Main tests separately replace the active Main binding:
the former identity can retrieve its exact receipt but cannot perform a fresh
command. The existing `main_account_id` gate for Project-create and selection
is unchanged, and the two Main command handlers retain their domain authorizer.

## Why a binary checkpoint, and its size

The replay tests prove that a receipt written by the hand path is returned
unchanged by the registered path: same receipt id, event id, input digest and
outcome JSON. That needs rows the hand path wrote. State rebuilt in code by the
current tree would only prove the new path agrees with itself, and the ids,
digests and timestamps would differ on every run. A data-only SQL dump loaded
into a freshly migrated database is the text alternative; it was not attempted
because the schema's insert triggers and FTS shadow tables make the load order
fragile.

The file is 1,660,928 bytes (about 190 KB compressed in Git). It was captured
at 4,587,520 bytes and rewritten once with `PRAGMA journal_mode=DELETE`,
`PRAGMA page_size=512` and `VACUUM`: the size is the schema (1,163 objects,
one page each at minimum), not data (about 150 rows). The sorted `.dump` of the
file is byte-identical before and after (SHA-1
`6580c89a285548c6c6661f82af165d799d4ecfe7`). Regenerate it only from the base
commit named above, with `FORGE_E2_CAPTURE_DIR` set.

Limits of this fixture: its schema is frozen at the base commit, so a later
migration that changes a table these commands read needs the checkpoint
migrated or recaptured. The recorded responses cannot by themselves show which
tree wrote them, because both paths return the same handler JSON; the capture
on the base commit is asserted by this file and by the `base` key in the JSON,
not verified by a test.
