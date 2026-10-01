## ADDED Requirements

### Requirement: Repository location records
The system SHALL record a Project repository's physical checkouts as repository locations, separate from the repository's logical identity. Each location SHALL have an owner (the server host, or one daemon runtime), a machine-local path, a kind (`primary_checkout`, `managed_clone`, or `shared_mount`), an `is_default` flag, a status (`unverified`, `ready`, `unavailable`, `invalid`), the last verification time and error, and an optimistic `version`. Only `ready` locations SHALL be eligible for placement.

#### Scenario: Server checkout backfilled
- **WHEN** Forge migrates a database that has a repository with `local_path`
- **THEN** a server-owned `primary_checkout` location with that path exists for the repository

#### Scenario: Unverified location is not placeable
- **WHEN** a daemon location has been registered but not verified
- **THEN** placement admission rejects it with filter `location_not_ready`

### Requirement: Register and verify a daemon location
An owner or admin SHALL be able to register a daemon-owned location for a repository by giving the daemon, runtime, and machine-local path. The server SHALL ask the owning daemon to verify it (`repo_location.verify`). The daemon SHALL accept the location only if the path is inside its runtime's `workspace_root`, is a git work tree, and resolves the repository's default branch, and if its remote, when present, matches the repository. Verification SHALL be retried on daemon reconnect, and a failure SHALL set the status to `invalid` or `unavailable` with the daemon's error.

#### Scenario: Register Mac checkout
- **WHEN** a user registers `/Volumes/Data/codes/ai/framerill` on the Mac daemon for the FrameRill repository
- **THEN** the daemon verifies it and the location becomes `ready`
- **AND** the server never reads that path itself

#### Scenario: Path outside the daemon root
- **WHEN** a user registers a path outside the daemon runtime's `workspace_root`
- **THEN** the location is `invalid` with error `outside_workspace_root`

### Requirement: Shared-mount locations are verified
A daemon whose runtime root is the same filesystem as the server's worktree root MAY be used as the execution provider for server-owned placements only through a `shared_mount` location. The server SHALL verify that location by writing a probe file that the daemon reads back at the same path. Without a verified `shared_mount`, a daemon SHALL NOT receive a server worktree path.

#### Scenario: Container with shared mount
- **WHEN** a daemon in a container mounts the server worktree root at the same path and the probe round-trips
- **THEN** its `shared_mount` location is `ready` and server-owned placements may execute on it

#### Scenario: Unverified shared path
- **WHEN** no verified `shared_mount` exists for a daemon
- **THEN** placement never pairs a server-owned workspace with that daemon as execution provider

### Requirement: Location API surface
The REST API SHALL expose `GET/POST /api/v1/repos/{id}/locations`, `PATCH/DELETE /api/v1/repos/{id}/locations/{location_id}`, and `POST .../{location_id}/verify`, returning `items` with opaque cursors. `forge-ctl` SHALL provide matching `repo location list|add|verify|set-default|remove` commands. A location referenced by a non-`cleaned` placement SHALL NOT be deletable.

#### Scenario: Delete in-use location
- **WHEN** a user deletes a location that a ready placement references
- **THEN** the request fails with HTTP 409 and names the placement's Task
