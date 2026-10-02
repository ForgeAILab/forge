## ADDED Requirements
### Requirement: Every machine has a run cap
The system SHALL limit the number of concurrent runs on each machine, where a machine is the server host or one daemon, and a run is a Running Task execution, a live workspace reservation (including a ready placement whose launch has not started) that has no Running execution yet, or a leased or running Agent Chat turn, counted on the machine that executes it. A cap of `0` SHALL mean unlimited. When no cap is configured the cap SHALL be automatic: half the machine's logical cores, rounded down, and never less than 2.

#### Scenario: Automatic cap on a machine with no configuration
- **WHEN** a server host with 8 logical cores has no `server.max_concurrent_runs` configured
- **THEN** its effective cap is 4

#### Scenario: Small machine
- **WHEN** a machine has 2 logical cores and no configured cap
- **THEN** its effective cap is 2

#### Scenario: Unlimited
- **WHEN** a machine's cap is configured as `0`
- **THEN** placement never rejects that machine for capacity

### Requirement: Server host cap is a setting
The system SHALL expose the server host cap as `server.max_concurrent_runs` in the configuration file, the environment, a CLI flag, the Settings API and the Forge Settings page, following the usual precedence. A change made through the Settings API SHALL take effect for the next admission without a restart, and the settings response SHALL report the value in effect, including the automatic value when the setting is unset.

#### Scenario: Administrator lowers the cap
- **WHEN** an administrator sets `server.max_concurrent_runs` to 2 in Settings while 3 runs are active on the server host
- **THEN** no running work is stopped
- **AND** no new run starts on the server host until fewer than 2 are active
- **AND** the setting is not reported as requiring a restart

#### Scenario: Non-administrator
- **WHEN** a user who is not an administrator updates the setting
- **THEN** the request is refused

### Requirement: Daemon cap is reported as a typed field
A daemon SHALL have a run cap in its own configuration with the same automatic default computed on its machine, and SHALL report it as a typed field at registration and in status reports. The server SHALL store the reported cap on the daemon record and SHALL NOT derive a cap from daemon labels. A daemon that does not report the field SHALL keep the cap last recorded for it.

#### Scenario: Daemon reports its cap
- **WHEN** a daemon configured with a cap of 3 registers
- **THEN** the daemon record carries a cap of 3
- **AND** a fourth run is not placed on that daemon while three are active

#### Scenario: Label cap carried over on upgrade
- **WHEN** a database is migrated in which a daemon's labels held a positive session cap
- **THEN** that value is the daemon's recorded cap after the migration

#### Scenario: Labels no longer set a cap
- **WHEN** a daemon reports a `max_sessions` label and no typed cap after the migration
- **THEN** the label does not change the recorded cap

### Requirement: Administrators can limit a daemon
The system SHALL let an administrator set or clear a run limit for a daemon through the API and the Machines page. The effective cap of a daemon SHALL be the lower of its reported cap and the administrator's limit, treating an absent limit as no limit and a reported cap of `0` as unlimited.

#### Scenario: Admin limit below the daemon's cap
- **WHEN** a daemon reports a cap of 6 and an administrator sets a limit of 2
- **THEN** its effective cap is 2

#### Scenario: Admin limit above the daemon's cap
- **WHEN** a daemon reports a cap of 3 and an administrator sets a limit of 8
- **THEN** its effective cap is 3

#### Scenario: Limit cleared
- **WHEN** the administrator clears the limit
- **THEN** the effective cap is the daemon's reported cap

### Requirement: The cap is enforced at admission and at execution start
The system SHALL reject a machine whose active runs have reached its effective cap as a placement candidate with the filter code `machine_capacity`, in the same transaction that records the reservation, and SHALL check again in the transaction that inserts the Running execution. The filter code `daemon_capacity` SHALL no longer be produced. A launch SHALL retain its reservation until its Running execution is inserted or the launch is abandoned. A valid slot holder SHALL NOT be refused at start because a chat turn or a lowered cap has filled the machine; an idle ready placement SHALL hold no slot. Expired reservations SHALL not count. Work that is already running SHALL never be stopped or failed because of the cap.

#### Scenario: Server host at its cap, another machine free
- **WHEN** the server host is at its cap and a connected daemon that satisfies every other filter has a free slot
- **THEN** the Task is placed on the daemon

#### Scenario: Two admissions race for the last slot
- **WHEN** two Tasks are admitted concurrently for a machine with one free slot
- **THEN** exactly one of them is placed there

#### Scenario: A launch holds its slot until it starts
- **WHEN** a launch has reserved the last slot and its placement becomes ready
- **THEN** it retains that slot until its Running execution is inserted or it is abandoned
- **AND** a chat turn arriving during preparation does not refuse that launch at start
- **AND** after insertion the reservation is not counted a second time

### Requirement: Tasks wait visibly when every machine is full
When no machine is eligible for a Task and at least one otherwise eligible machine was rejected only for `machine_capacity`, the system SHALL leave the Task in its state with a queued dispatch reason that names machine capacity, SHALL NOT record a failure, a blocking annotation or an Attention item, SHALL NOT consume a retry budget, and SHALL start the Task once a run on an eligible machine ends.

#### Scenario: Waiting and resuming
- **WHEN** a Task is ready to start while the only eligible machine is at its cap
- **THEN** the Task shows a waiting reason for machine capacity
- **AND** when a run on that machine ends the Task starts without operator action

#### Scenario: A machine-capacity waiter does not hold a Project slot
- **WHEN** a Task enters an active state but dispatch waits only for machine capacity
- **THEN** it is counted as parked rather than active in both Project slot projections
- **AND** its wait marker uses its final Task version and invalidates Project slot memos
- **AND** queued recovery ticks preserve the same marker, Task version and event stream until a slot frees

### Requirement: Machine occupancy is visible to operators
The operations status SHALL list the server host and every daemon with the number of active runs, the effective cap and whether the machine is at capacity.

#### Scenario: Server host in operations status
- **WHEN** an administrator reads the operations status while 2 runs are active on a server host with a cap of 4
- **THEN** the server host is listed with 2 active runs, a cap of 4 and not at capacity
