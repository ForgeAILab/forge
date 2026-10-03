## ADDED Requirements

### Requirement: Machine probe
A daemon that advertises the capability fact `machine_probe.v1` SHALL accept `machine.probe`: a list of named commands with per-command timeouts and an environment map, and optionally a repository location to run in. Without a location it SHALL run each command in a new empty directory inside the runtime's `workspace_root` and remove the directory afterwards. With a location it SHALL run each command in that location's checkout and SHALL NOT modify the checkout. It SHALL return, per command, the exit status, whether it timed out, and a bounded output tail. The daemon SHALL run a probe only when its local run policy allows the purpose `environment_probe`; otherwise it SHALL refuse with `run_purpose_denied`. Check commands SHALL be read-only and SHALL inherit the daemon environment with Project env overrides, as workspace.run does; scratch directories are not a filesystem sandbox. Timeout/cancellation SHALL kill the process group and startup SHALL sweep abandoned probe scratch directories. A probe SHALL NOT take a workspace lock, create a workspace, or write to the daemon journal. The server SHALL treat a daemon without the capability as unable to be probed: for a machine with a ready location it falls back to the launch-time check, and the daemon is never a provisioning candidate.

#### Scenario: Probe on a machine with no checkout
- **WHEN** the server sends `machine.probe` with `cargo --version` and no location to a daemon whose policy allows `environment_probe`
- **THEN** the command runs in an empty directory under `workspace_root`, the result carries its exit status and output tail, and the directory is removed

#### Scenario: Purpose not allowed
- **WHEN** the daemon's run policy does not list `environment_probe`
- **THEN** `machine.probe` is refused with `run_purpose_denied` and no command runs

#### Scenario: Command exceeds its timeout
- **WHEN** a probe command runs longer than its timeout
- **THEN** the daemon terminates it and reports it as timed out

### Requirement: Managed clone provisioning
A daemon that advertises `repo_provision.v1` SHALL accept `repo_location.provision` with the repository identity, owner-configured remote URL, requested default branch and timeout budget (1–86400 seconds). It SHALL clone the remote into `<workspace_root>/repos/<repository id>` using the machine's own Git configuration and credentials, and SHALL return its canonical workspace root, authoritative clone path and requested default branch. A fresh clone SHALL select that branch; reuse SHALL fetch a missing local branch before verification. The operation SHALL be idempotent: an existing clone of the same remote at that path is reused, and a path holding anything else is refused with `path_conflict` without modifying it. The daemon SHALL run it only when its local run policy allows the purpose `repo_provision`. The server SHALL record the result as a `managed_clone` location in `unverified` state and SHALL make it eligible only after `repo_location.verify` succeeds. The server SHALL NOT add credentials beyond the owner-configured remote URL. Both sides SHALL redact URL userinfo and token-like query values from errors, logs, receipts and wait reasons. A failed clone SHALL leave no partial directory and SHALL be reported with the daemon's bounded error; the server SHALL retry with exponential backoff up to five attempts for unchanged settings and connection, then report deterministic provision_failed Task Attention with the redacted last error until reconnect or a relevant settings edit.

#### Scenario: Clone onto a verified machine
- **WHEN** the server sends `repo_location.provision` for a repository to a daemon that allows `repo_provision`
- **THEN** the daemon clones it under `workspace_root/repos/` and the server records an `unverified` `managed_clone` location that becomes `ready` after verification

#### Scenario: Repeat request
- **WHEN** the same `repo_location.provision` is sent again after a lost response
- **THEN** the existing clone is reused and one location exists

#### Scenario: The machine cannot authenticate to the remote
- **WHEN** the clone fails because the machine has no credentials for the remote
- **THEN** no directory is left behind, the location is `unavailable` with the daemon's error, and placement rejects the machine with `location_not_ready`

#### Scenario: Advertised root is a symlink
- **WHEN** the advertised workspace root is a symlink to the daemon's canonical root
- **THEN** the server records the returned path under the returned canonical root and verification succeeds

#### Scenario: Default branch differs from remote HEAD
- **WHEN** the requested default branch differs from the remote HEAD branch
- **THEN** the daemon clones or fetches that branch and verification resolves it locally

#### Scenario: Probe cancellation and restart
- **WHEN** a machine probe times out or is cancelled, or the daemon restarts with abandoned scratch directories
- **THEN** probe descendants are killed on timeout/cancellation and abandoned UUID probe directories are swept on startup
