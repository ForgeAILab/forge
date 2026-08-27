## ADDED Requirements

### Requirement: Reversible Provider, CLI Runtime, and Agent Availability

Forge SHALL let an authorized account user enable or disable one configured
provider entry, one discovered CLI runtime instance, or one Agent identity
without disconnecting credentials, deleting profiles, rewriting bindings, or
removing history. Policy SHALL be versioned/account-scoped, existing records
SHALL default enabled, and disconnect/archive SHALL remain distinct operations.

#### Scenario: User disables a provider entry

- **WHEN** an authorized user disables an enabled provider entry using its
  current version
- **THEN** Forge preserves credentials/configuration and reports every
  dependent Agent effectively unavailable for new work
- **AND** it shows dependent Agents/bindings before mutation and never silently
  rebinds them

#### Scenario: User disables a CLI runtime instance

- **WHEN** an authorized user disables one `(daemon, executor type)` runtime
  source
- **THEN** only Agents whose current profile resolves to that exact source
  become effectively unavailable
- **AND** Forge neither imports CLI credentials nor disables another host's
  runtime with the same executor type

#### Scenario: User disables an Agent

- **WHEN** an authorized user disables an Agent identity with its current
  version
- **THEN** the identity and profiles/history remain inspectable but the Agent
  is excluded from new selection, admission, fallback, and Task leases
- **AND** re-enabling restores eligibility only if its source, profile, health,
  and capabilities also pass

#### Scenario: Availability update is stale or cross-account

- **WHEN** a request carries a stale version or targets configuration outside
  the caller's account
- **THEN** Forge changes nothing and returns a typed conflict or denial before
  disclosing dependent identities, bindings, health, or counts

### Requirement: One Effective Agent Availability Decision

Forge SHALL resolve eligibility through one server-owned decision combining
Agent enabled state, selected profile, provider/CLI source enabled state,
credential/runtime/daemon health, and capability compatibility. Genesis
selection, Main/Project binding health, Agent Settings, chat admission/retry,
Project setup, fallback, and Task claim/lease/recovery SHALL consume the same
decision and stable reason code; clients SHALL NOT infer eligibility.

#### Scenario: Disabled configuration is excluded everywhere

- **WHEN** an Agent or its provider/CLI source is disabled
- **THEN** every new selection, chat turn, retry, fallback, Task claim, and
  Workspace lease rejects it with the same bounded availability reason
- **AND** inventory still shows the configuration and a re-enable action

#### Scenario: Bound Agent becomes disabled

- **WHEN** a source or identity behind a Main/Project binding is disabled
- **THEN** Forge preserves the explicit binding, reports setup-required with
  the exact recovery action, and refuses new admission through it
- **AND** it never substitutes another Agent without an explicit binding change

#### Scenario: Source is disabled while work exists

- **WHEN** disable commits while work is queued, in retry-wait, or running
- **THEN** queued/retry-wait work does not start a new attempt, lease, renewal,
  retry, or continuation
- **AND** an already running bounded attempt may finish once, after which the
  disabled configuration cannot be used again

#### Scenario: All candidates for a required Task role are disabled

- **WHEN** no effectively available configured Agent remains for the role
- **THEN** Forge reports the exact missing role and recovery action instead of
  selecting a disabled Agent
- **AND** source disable does not consume the Task execution retry budget
