## ADDED Requirements

### Requirement: Projects choose how check failures are triaged
Every Project SHALL have a `review_ci_triage` setting:
- `full` (the default) re-runs and compares, and blocks only on failures the Task introduced.
- `report` re-runs and compares, records the classification, and blocks as before this change.
- `off` does neither.

The setting SHALL be readable and writable through the Project settings API and the web Project settings page. It SHALL be documented in `docs/api.md`.

#### Scenario: Owner opts out
- **WHEN** the owner sets `review_ci_triage: "off"`
- **THEN** a failed review CI step routes exactly as before this change, with no re-run and no base run

#### Scenario: Report keeps classified findings blocking
- **WHEN** `report` classifies signatures as flaky or pre-existing
- **THEN** they remain open blocking findings in the ledger and repair brief, with their classifications shown

### Requirement: A failing step is re-run once to separate flakes
When a review CI step fails and triage is not `off`, Forge SHALL re-run the failing steps once on the same candidate, in the same workspace. When the re-run fails again with at least one of the same signatures, a signature that does not fail in both runs SHALL be classified `flaky`. A step that passes on the re-run SHALL be classified wholly flaky. A step whose re-run fails with only other signatures is a consistent failure: none of its signatures is `flaky`, and all of them go to the base comparison.

Durable consumer request keys SHALL admit at most one rerun bundle and base comparison per review attempt or queue-head check, including after restart or redelivery. There SHALL be no volatile triage budget. It SHALL NOT create a new Review attempt or charge the review budget.

#### Scenario: Timeout flake
- **WHEN** `pnpm test` fails with `AgentAuthorityHttpTransportError` timeouts in two tests and passes on the re-run
- **THEN** the step is classified flaky, the two signatures are `flaky`, and under `full` the review continues as if the step passed

#### Scenario: Re-run fails with another test
- **WHEN** a fail-fast run fails `a` and its re-run fails `b`
- **THEN** neither is `flaky`; both go to the base comparison

#### Scenario: Consistent failure
- **WHEN** the same three tests fail in the run and the re-run
- **THEN** all three are consistent failures, and they go to the base comparison

### Requirement: Consistent failures are compared with the base commit
For consistent failures, Forge SHALL run a base check on the base commit that is comparable to the candidate run. The base is the merge base of the candidate with its target branch.

The base check:
- runs on the same owner as the candidate run, with the same Project env, assets and per-run build environment;
- uses a clean `ExactCommit` checkout under `canonical-ci/1`;
- runs the effective review config's `setup_steps` first, then the review CI steps up to the last failing one;
- is reused by every request for the same base commit, setup steps and CI steps.

When the effective review config has no `setup_steps`, or the Task is on a daemon placement, the base comparison SHALL be `base_unavailable`. Consistent failures then stay unclassified and block as under `report`, and the
brief and attention say why. Independently proved rerun flakes remain non-blocking
under `full`.

The base run SHALL be `base_run_invalid`, and classify nothing, when either of these holds:
- its setup steps fail;
- it fails with no signatures where the candidate run had some.

In that case every consistent failure blocks as under `report`, and a `base_check_failing` attention item names the invalid run.

Consistent signatures:
- that also fail on a valid base run SHALL be `pre_existing`;
- that do not SHALL be `introduced`.

A failing step without signatures that also fails on a valid base run SHALL be `pre_existing_unverified`. It keeps blocking, and the brief names the base failure.

#### Scenario: Red base
- **WHEN** 26 tests fail consistently on the candidate and 26 of the same signatures fail on the base
- **THEN** all 26 are `pre_existing`, none reaches the coder, and under `full` the review continues to verify and the reviewer

#### Scenario: One introduced failure
- **WHEN** 27 tests fail consistently and 26 of them also fail on the base
- **THEN** the remaining one is `introduced` and becomes the coder's only CI finding

#### Scenario: Base checkout without dependencies
- **WHEN** the Project has no `setup_steps` and `pnpm test` fails consistently
- **THEN** the base comparison is `base_unavailable`, the failures block as today, and the attention says to set `setup_steps` to enable base comparison

#### Scenario: Invalid base run
- **WHEN** the base run's `pnpm install` setup step fails
- **THEN** the run is `base_run_invalid`, nothing is classified `pre_existing`, every consistent failure blocks, and a `base_check_failing` item names the invalid run

#### Scenario: Shared base run
- **WHEN** two Tasks on the same base commit both need a base run of the same steps
- **THEN** one base run executes and both use its result

#### Scenario: Project assets on a shared base
- **WHEN** two Tasks need the same base check with Forge-materialized Project assets
- **THEN** the base gets the same assets and their untracked paths do not invalidate sharing; tracked changes still do

### Requirement: Triage never certifies a failed check
A check result that triage treats as non-blocking SHALL keep its real stored outcome (`fail`) in `check_result`. Only the Review settlement, or the queue-head settle, SHALL treat the step as non-blocking. A triaged result SHALL never be reused as a certified pass.

#### Scenario: Unchanged commit at the queue head
- **WHEN** a Task's review CI passed only through triage, and its unchanged commit reaches the queue head
- **THEN** the head runs its own check rather than reusing the review result as a certified pass

### Requirement: Triage applies at the merge-queue head
A queue-head check that fails SHALL be triaged the same way, with the target tip as the base. Under `full`, a head whose failures are all `flaky` or `pre_existing` SHALL NOT be sent back to the coder.

#### Scenario: Merge on a red target
- **WHEN** a Task that passed review reaches the queue head, and its checks fail only on tests that also fail on the target tip
- **THEN** the head is not sent back, and the merge proceeds

#### Scenario: Triage outlives the original head wait
- **WHEN** a queue-head triage rerun or base check is pending or running past the original 35-minute head check wait
- **THEN** the head wait is extended, bounded by the check run's own deadline

#### Scenario: Triage infrastructure cannot produce a verdict
- **WHEN** either queue-head triage stage exhausts its infrastructure retries
- **THEN** the head parks with the existing infrastructure reason and Attention rather than a timeout

### Requirement: Flaky and pre-existing failures are reported to the Project
Forge SHALL raise deduplicated Project attention for triaged failures:
- `flaky_check`, keyed by step command and signature, carrying a count and the Tasks that hit it;
- `base_check_failing`, keyed by Project and step command, carrying the latest failing base commit and signatures. A newer red base updates the same item; it resolves only when a newer queue-head target-tip base run of that command passes. Review-entry merge-base observations may open/update it but never resolve it.

Both SHALL be new `AttentionCategory` values, shown in Mission Control and documented in `docs/api.md`.

#### Scenario: Base failure visible to the owner
- **WHEN** triage classifies 26 failures as `pre_existing` on base commit `abc123`
- **THEN** Mission Control shows one `base_check_failing` item for `pnpm test` at `abc123` listing them, and no Task is blocked by them under `full`
