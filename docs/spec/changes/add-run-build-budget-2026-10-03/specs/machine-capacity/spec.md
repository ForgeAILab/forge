## ADDED Requirements

### Requirement: Each machine supplies a build budget to run processes
Every CLI executor, native tool command, review CI step, Project hook, environment check and setup process SHALL receive `CARGO_BUILD_JOBS=k`, `RUST_TEST_THREADS=k`, `MAKEFLAGS=-j<k>`, `CMAKE_BUILD_PARALLEL_LEVEL=k` and `GOFLAGS=-p=<k>`, including inherited child processes, unless the Project environment or operator process environment already sets that variable. Project environment SHALL have highest precedence, followed by operator environment, then Forge. Automatic k SHALL be `max(1, logical_cores / effective_run_cap)`; a positive configured machine run cap SHALL be the divisor, while an unset or unlimited cap SHALL use the automatic run cap. Unset `build_jobs_per_run` SHALL mean automatic, zero SHALL disable Forge budget variables, and a positive value SHALL set exactly that value.

#### Scenario: Automatic and unlimited run caps
- **WHEN** an 8-core machine has no explicit build budget and a run cap of 2
- **THEN** k is 4
- **AND** changing the run cap to 0 uses the automatic cap of 4 and k is 2

#### Scenario: Project and operator precedence
- **WHEN** the Project sets CARGO_BUILD_JOBS to 7 and the operator sets RUST_TEST_THREADS to 3
- **THEN** run children preserve those values and receive the machine budget for the remaining variables

#### Scenario: Disabled budget
- **WHEN** build_jobs_per_run is 0
- **THEN** Forge inserts no budget defaults and preserves Project and operator values

### Requirement: Run children have lower CPU priority on Unix
Run processes SHALL start with a child-only niceness increment of `run_nice`, defaulting to 10, with allowed values from 0 through 19. Children SHALL inherit the priority. Zero SHALL leave priority unchanged; failure SHALL be logged once and SHALL NOT fail a run. Windows SHALL apply no priority change.

#### Scenario: Priority applies only to children
- **WHEN** a Unix host with niceness 0 starts a run configured with run_nice 5
- **THEN** the run process has niceness 5 and the host remains at niceness 0

#### Scenario: Priority disabled
- **WHEN** run_nice is 0
- **THEN** run process priority is unchanged

### Requirement: Machine build and priority policy is configurable and visible
Server build_jobs_per_run and run_nice SHALL be available in YAML, environment, CLI flags, Settings API and Forge Settings, with file less than environment less than flag precedence. Settings updates SHALL apply to subsequent child launches without restart and display cores, effective run cap, build jobs and configured niceness. Daemons SHALL use their local YAML/flag policy without a protocol change. Operator machine entries SHALL add locally available facts next to the existing cap; unavailable remote facts SHALL be null.

#### Scenario: Live update and reset
- **WHEN** an administrator sets build_jobs_per_run to 3 and run_nice to 7 in Settings
- **THEN** Settings reads return effective values 3 and 7 without requiring restart
- **AND** resetting build_jobs_per_run to null restores automatic calculation for subsequent processes

#### Scenario: Daemon policy remains local
- **WHEN** a daemon receives a workspace command from the server
- **THEN** its run child uses the daemon's configured budget and priority with the Project environment taking precedence
