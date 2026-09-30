## ADDED Requirements

### Requirement: No blind recovery during an environment pause
While a Project is environment-paused, `task.recover` on any of its Tasks SHALL be refused with a typed `environment_paused` error. The error carries the failing checks, their output tail, and `next_check_at`. The Project Agent SHALL receive one project-level wake per pause, not one wake per Task.

#### Scenario: Agent tries to re-execute during a disk pause
- **WHEN** NovelKit is paused for `disk` and the Project Agent calls `reexecute` on NK-37
- **THEN** the call is refused with `environment_paused`, `checks: ["disk"]`, and the next check time, and NK-37 is unchanged

### Requirement: Blocker wakes end in action or escalation
A Project Agent turn woken by a blocker SHALL end in one of three ways: an accepted unblocking action (a recovery action, a config request, or an environment re-check); an owner escalation through `project.escalate {need, task_ids}`, which creates a Notification and an Attention item naming the exact need; or a declaration that the blocker is already resolved, with evidence. A blocker is an execution failure, a `review_needs_owner`, `needs_config` or `review_blocked` park, or an environment pause. If the turn ends while its blocker persists and none of those outcomes was recorded, the system SHALL create the escalation itself from the blocker detail. The operating doctrine SHALL tell the agent to verify that a recovery took effect, and never to retry an unchanged blocker.

#### Scenario: Disk too low, agent cannot fix it
- **WHEN** the Project Agent is woken for the NovelKit disk pause
- **THEN** it escalates "Forge host root has 7G free; the disk check needs ≥8G. Free disk or lower the threshold", and the owner receives a Notification

#### Scenario: Silent turn is escalated by Forge
- **WHEN** the agent replies "no user action needed" while NK-24 is still blocked and records no action or escalation
- **THEN** Forge creates the owner escalation from NK-24's blocker detail

### Requirement: Agent is woken when a blocker can move
The system SHALL wake the Project Agent when an environment pause clears, when a config item it or its Tasks requested is provided, and when the owner answers an escalation. The wake carries the affected Tasks, so the agent can verify that they resumed.

#### Scenario: Disk recovers
- **WHEN** the environment pause clears at 19:40Z
- **THEN** the Project Agent gets a wake listing the six previously affected Tasks and confirms that each re-dispatched

### Requirement: Blocker wakes are not starved by follow-ups
Wake budget admission SHALL reserve a share of each budget window for blocker wakes (execution failed, parked for owner, environment pause, config provided). Delivery follow-up wakes (`reconcile_delivery`, readiness) SHALL NOT consume that share. When a delivery follow-up fails with the same error as the previous attempt on the same incident, it SHALL be suppressed as `repeated_failure` rather than re-admitted.

#### Scenario: Readiness spam cannot block recovery
- **WHEN** seven delivery follow-ups in one window each fail readiness on NK-2 metadata
- **THEN** repeats after the first are suppressed as `repeated_failure`, and a later `execution_failed` wake in the same window is still admitted
