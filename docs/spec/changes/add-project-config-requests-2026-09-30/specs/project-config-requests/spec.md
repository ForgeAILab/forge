## ADDED Requirements

### Requirement: Project configuration items
A Project SHALL hold named configuration items. Each item has a `name` (a valid environment variable name that is not reserved: no `FORGE_*`, no `PWD`, and not a name already in `environment.env`), a `kind` of `value` or `secret`, a `description`, a `status` of `requested` or `provided`, the requester, and the blocked Task ids. `value` items are stored as plain text and returned by the API. `secret` values SHALL be sealed with the existing protected store cipher, and SHALL never be returned by any API, event, log, prompt, or transcript. Responses show only `provided: true` and `updated_at`.

#### Scenario: Secret is never echoed
- **WHEN** the owner provides `NOVELKIT_LLM_API_KEY` as a secret
- **THEN** `GET /api/v1/projects/{id}/config` lists it with `kind: secret, status: provided` and no value field

#### Scenario: Reserved name refused
- **WHEN** a request names `FORGE_TOKEN`
- **THEN** it is refused as invalid

### Requirement: Provided items are injected like environment variables
Every provided item SHALL be set as an environment variable on each executor process, review setup and CI step, conformance check, Project environment check, and lifecycle hook, with the same precedence rules as `environment.env`. Secret values SHALL be added to the log redaction set of that execution, so any echo of the value is masked.

#### Scenario: Review check uses the endpoint
- **WHEN** `NOVELKIT_LLM_BASE_URL` and `NOVELKIT_LLM_API_KEY` are provided and NK-5's review runs `npm run test:live-summary`
- **THEN** the check process sees both variables, and the key never appears unmasked in the step output

### Requirement: Agents request configuration and never set values
The Project Agent SHALL be able to create a `requested` item through `project.config.request {name, kind, description, why, task_ids}`. A coder SHALL be able to do the same through a `config_request` outbox entry, and a reviewer through `needs_config: [{name, kind, why}]` in its result block. A request for a name that is already provided SHALL be refused with "already provided" and the item's `updated_at`, so agents do not re-ask. Agents SHALL NOT be able to write, read, or delete values.

#### Scenario: Reviewer asks for a live endpoint
- **WHEN** NK-5's reviewer ends with `{"result": "fail", "reason": "live summary timing not verifiable", "fixable_by": "owner", "needs_config": [{"name": "NOVELKIT_LLM_BASE_URL", "kind": "value", "why": "3000-char summary under 60s"}]}`
- **THEN** a `requested` item `NOVELKIT_LLM_BASE_URL` exists, listing NK-5 as blocked
- **AND** NK-5 parks as `needs_config` without dispatching the coder or spending retry budget

#### Scenario: Agent tries to set a value
- **WHEN** an agent calls the owner-only `PUT /config/{name}`
- **THEN** the call is refused as forbidden

### Requirement: Owner provides values and blocked work resumes
Only the Project owner SHALL be able to provide, replace, or delete a value (`PUT` and `DELETE /api/v1/projects/{id}/config/{name}`, and forge-ctl `project config set`). Once every item a parked `needs_config` Task depends on is provided, the system SHALL clear the park and re-run the blocked step. That is the reviewer attempt for a review park, and the coder execution for a coder park. The system SHALL publish a `project.config.provided` event.

#### Scenario: Owner pastes the endpoint
- **WHEN** the owner provides `NOVELKIT_LLM_BASE_URL`, the last item NK-5 was waiting on
- **THEN** NK-5's park is cleared and a new review attempt starts with the variable set

#### Scenario: Partial fill keeps waiting
- **WHEN** NK-5 waits on two items and only one is provided
- **THEN** NK-5 stays parked, and its card shows "1 of 2 settings provided"

### Requirement: Configuration requests are visible
Each pending request SHALL create an Attention item and a Notification for the owner. The Project header SHALL show a "N settings needed" badge. A Project **Configuration** page SHALL list every item with its reason, requester, and blocked Tasks, and a paste field; secret fields are masked. Task cards parked on `needs_config` SHALL name the missing items.

#### Scenario: Owner sees what is needed
- **WHEN** NK-5 is parked on `NOVELKIT_LLM_BASE_URL`
- **THEN** the NovelKit header shows "1 setting needed", and the Configuration page shows the item with "requested by NK-5 reviewer: 3000-char summary under 60s"
