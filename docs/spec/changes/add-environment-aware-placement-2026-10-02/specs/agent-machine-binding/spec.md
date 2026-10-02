## ADDED Requirements

### Requirement: Agents are not bound to a machine
An Agent SHALL be defined by its executor, model, configuration and credentials, and SHALL have no machine unless an admin sets a pin. Creating an Agent, through the API, the UI, `forge-ctl`, genesis, or default-Agent seeding, SHALL NOT set a pin on its own. An unpinned Agent SHALL be runnable on any machine where its executor is installed, authenticated, and enabled, subject to placement. CLI credentials are not moved: a CLI Agent runs only where its CLI is installed and logged in.

#### Scenario: Agent set up from a second machine
- **WHEN** a user creates a Codex Agent while Codex is available on the server and on daemon D
- **THEN** the Agent has no pin
- **AND** its Tasks are placed by the placement rules, which prefer the machine that has the repository and passes the Project's environment checks

#### Scenario: Executor exists on one machine only
- **WHEN** an unpinned Agent's executor is logged in only on daemon D
- **THEN** placement considers only D for that Agent and reports `executor_unavailable` for the other machines

### Requirement: The machine pin is an explicit, visible constraint
An admin SHALL be able to pin an Agent to one daemon and to clear the pin, through the Agent API, the Agent settings page, and `forge-ctl`. A pinned Agent SHALL run only on that daemon. The Agent settings page SHALL show the pin and SHALL warn when the pinned daemon does not have the Agent's executor. Existing pins SHALL be preserved on upgrade.

#### Scenario: Admin clears a pin
- **WHEN** an admin clears the pin of an Agent pinned to daemon D
- **THEN** the Agent's next Task is placed by the placement rules and may run on another machine

#### Scenario: Non-admin cannot pin
- **WHEN** a non-admin user sends an Agent update that sets a pin
- **THEN** the request is refused with `admin_required`

### Requirement: An Agent reports where it can run
The Agent response SHALL include `runnable_on`: the machines where the Agent's executor is currently installed, authenticated, and enabled, restricted to the pinned daemon when a pin is set, computed from the same facts placement uses. Machine identities SHALL be returned to admins; other users SHALL receive the count. The Agent list and settings page SHALL show it and SHALL flag an Agent that can run nowhere.

#### Scenario: Agent runnable on two machines
- **WHEN** an admin opens an unpinned Codex Agent and Codex is logged in on the server and on daemon D
- **THEN** the page shows "Runs on: server, D"

#### Scenario: Agent runnable nowhere
- **WHEN** an Agent's executor is not logged in on any reachable machine
- **THEN** `runnable_on` is empty and the Agent is flagged as unable to run
