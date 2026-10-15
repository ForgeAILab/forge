## ADDED Requirements

### Requirement: An unchanged candidate is not reviewed again
Forge SHALL NOT start review CI, verify or a reviewer when a coder repair round ends with a commit (and clean worktree) equal to the one the latest Review in the window judged, only when that latest Review failed. In that case it creates no Review row.

- **First time:** Forge sends the Task back to the coder with a brief headed "No change since review N". This is charged to the persistent `repair_no_change` budget (Review window, limit 1).
- **Second time:** the Task parks with `review_needs_owner` and the message `no change after repair round`.

Neither case charges the review budget. A re-review the owner starts is exempt.

#### Scenario: Coder changes nothing
- **WHEN** after failed review 2 at commit `c2`, the coder's repair round ends with HEAD still `c2`
- **THEN** no CI or reviewer runs, the coder is sent back once with "No change since review 2", and the review budget is unchanged

#### Scenario: Later pass
- **WHEN** a failed Review is followed by a passed Review of the same commit, and a repair entry is unchanged
- **THEN** the entry runs normal review without a reminder or no-change budget charge

#### Scenario: Entry suspended before the gate shipped
- **WHEN** an entry resumes with entry checkpoints or `awaited_consumer_id` but no recorded gate decision
- **THEN** it records `Continue` and resumes normal entry without evaluating the gate

#### Scenario: Repair entry retries after reconnect
- **WHEN** an offline owner caused a typed `review_ci_infrastructure` wait before a repair entry could compare HEAD, and the entry retries after reconnect
- **THEN** the retry retains the original source state and actor, and an unchanged candidate gets the reminder instead of another review, clearing the resolved infrastructure blocker before returning to the coder

#### Scenario: Still nothing
- **WHEN** the next repair round also ends at `c2`
- **THEN** the Task parks with `review_needs_owner` and the message `no change after repair round`

### Requirement: The repair brief carries attempt memory
The repair brief SHALL replace its "Previous round" section with an "Attempts" section. For each of the last three repair rounds in the window it SHALL list:
- the commit range;
- up to 20 changed files, from the diff between the reviewed commits;
- the coder's completion summary, cut to 1 KiB;
- the findings that round resolved, left open and opened.

The section SHALL end with: "Do not repeat an approach that left a finding open; if you keep one, say why it failed before."

When the brief is over budget, Attempts SHALL be cut after the reviewer report, keeping the most recent round longest.

#### Scenario: Third repair round
- **WHEN** the coder starts repair round 3
- **THEN** the brief lists rounds 1 and 2 with their files, summaries and finding outcomes, plus the closing instruction

### Requirement: The repair brief shows requirement status and shared causes
The brief SHALL include a "Requirement status" line for each frozen requirement:
- `unmet` when an open blocking finding cites it;
- `addressed` when only resolved findings cite it;
- otherwise `no open finding`.

When findings are linked by `same_cause_as`, the brief SHALL group each family under "Pattern", with its locations, and say: "These share a cause. Fix it at every call site and list the sites you changed."

Neither section is cut for budget.

#### Scenario: NK-28 style family
- **WHEN** F3 has `same_cause_as: "F1"` (stale apply after async validation) and F1 was resolved in round 1
- **THEN** the brief's Pattern section lists F1 and F3 with their locations and asks for a fix at every call site

### Requirement: A carried finding changes the strategy
A repair round that carries a reviewer finding open in two consecutive reviews SHALL start a fresh coder execution with the full brief, instead of resuming the previous coder thread. The brief SHALL open with: "Before editing, write a short root-cause note for each carried finding."

When the Project setting `repair_switch_model_when_stuck` is `true` (default `false`), that round SHALL run on the coder Agent's next fallback candidate when one exists. Otherwise it runs on the same candidate.

The setting SHALL be in the Project settings API, the web settings page and `docs/api.md`.

#### Scenario: Fresh eyes
- **WHEN** F1 is open in reviews 1 and 2 and a progress round is granted
- **THEN** the coder runs as a new execution with no resumed thread, and the brief opens with the root-cause instruction

#### Scenario: Model switch opted in
- **WHEN** the same happens with `repair_switch_model_when_stuck: true` and the coder Agent's chain has a second candidate
- **THEN** that round runs on the second candidate, and later rounds return to the first

### Requirement: Progress counts only the Task's own blocking findings
The open blocking count used by the round policy of `add-structured-repair-and-verify` SHALL exclude findings that are `flaky`, `pre_existing`, `minor` or `out_of_scope`. Under `review_ci_triage: full`, `pre_existing_unverified` findings SHALL NOT trigger repeat parks.

#### Scenario: Base noise does not stall progress
- **WHEN** review 2 resolves two reviewer findings, and 26 `pre_existing` test failures remain
- **THEN** the open blocking count drops by two, and a progress round can be granted
