## ADDED Requirements

### Requirement: Check steps report failing tests
The check executor SHALL extract failure signatures from each step's full output while the step runs. It strips ANSI codes first and recognises:
- cargo test (`test <path> ... FAILED`);
- cargo nextest (`FAIL [ <duration>] <binary> <test>`, optionally with a progress counter, and the timed-out, signalled and final retried statuses);
- vitest and jest (`FAIL <file> > <suite> > <test>`, and failure glyphs only inside a recognised runner block, ending at its summary);
- pytest (`FAILED <file>::<test>`);
- go test (`--- FAIL: <Test>`);
- failed and errored cases in JUnit XML files a step writes under `FORGE_CI_REPORT_DIR`.

Signatures are normalised as follows:
- durations, absolute Task-root prefixes and ANSI codes are removed;
- each is cut to 300 characters;
- at most 200 are kept per step, with `failures_truncated` set when more existed.

They SHALL be stored as optional `failures` on `CheckCommandOutcome` and `StepResultEntry`, exported to the generated TypeScript and documented in `docs/api.md`. A step result without `failures` SHALL read as an empty list.

#### Scenario: Vitest suite with two failures
- **WHEN** `pnpm test` exits 1 and its output contains `FAIL src/a.test.ts > parser > empty input` and `FAIL src/b.test.ts > cli > json flag`
- **THEN** the step result's `failures` holds exactly those two normalised signatures

#### Scenario: Linter and progress glyphs
- **WHEN** a linter or progress renderer emits `×`, `✗` or `●` outside a recognised vitest/jest output block
- **THEN** those lines produce no test signatures, including after a test runner's summary

#### Scenario: Nextest repeats a failure
- **WHEN** nextest reports a binary and failing test with durations, repeats libtest output in a stdout block and lists the same failure in its summary
- **THEN** one stable binary/test identity is retained without durations or progress counters

#### Scenario: Abandoned report directories
- **WHEN** a check starts in a worktree with abandoned Forge-generated `ci-reports-*` directories under its reserved `.forge-task` directory
- **THEN** those directories are removed before the command runs, without following symlinks or removing unrelated entries

#### Scenario: JUnit report
- **WHEN** a step runs `vitest --reporter=junit --outputFile=$FORGE_CI_REPORT_DIR/web.xml` and two cases fail
- **THEN** both cases appear in `failures`, even if the console output was cut

#### Scenario: Unknown runner
- **WHEN** a step fails with output no extractor recognises
- **THEN** `failures` is empty and the step is reported as one failure, as today

### Requirement: Stored output tails are free of ANSI codes
Forge SHALL strip ANSI escape sequences from every `output_tail` and `stderr_tail` before storing a check step result. The 4096-byte tail budget then holds text only.

#### Scenario: Coloured vitest summary
- **WHEN** a step prints `\x1b[31m4 failed\x1b[39m | \x1b[32m316 passed\x1b[39m`
- **THEN** the stored tail contains `4 failed | 316 passed`

### Requirement: Each failing test is its own finding
In the finding ledger, a failed review CI step with signatures SHALL produce one finding per signature, with id `CI-<step>#<first 8 hex of sha256(signature)>`. The same test failing again carries the same id. A failed step without signatures SHALL stay one finding, `CI-<step>`.

A complete signature list resolves earlier signatures that no longer fail. A
passing step resolves every finding for that step. A missing or truncated
signature list SHALL NOT be treated as evidence that an omitted test passed.

The repair brief and Task comments SHALL list at most 20 test findings per step, followed by "+N more". The ledger keeps all of them.

#### Scenario: Progress from 28 to 26
- **WHEN** review 1's `pnpm test` fails 28 tests and review 2's fails 26 of those same tests
- **THEN** the ledger shows 2 `CI-1#…` findings resolved and 26 still open, and the open count dropped
