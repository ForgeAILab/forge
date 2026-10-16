# Public demo instance

Forge is local-first. The demo instance is the one deliberate exception: a
throwaway copy on a public URL, so that people (and an automated test suite)
can look at the product without installing it. It is hosted on
[Dokploy](https://dokploy.com) as a Docker Compose application.

Everything for it lives in [`deploy/demo/`](../deploy/demo/):

| File | Purpose |
|---|---|
| `docker-compose.yml` | The Dokploy Compose application: one `forge` service. |
| `Dockerfile` | The demo image: the Forge image plus the two scripts below. |
| `forge-demo-entrypoint.sh` | Entrypoint of the demo image. Creates the accounts, then starts Forge. |
| `forge-demo-healthcheck.sh` | Container health check of the demo image. |
| `demo.env.example` | The variables for Dokploy's Environment tab. |

The root `docker-compose.yml` is the ordinary self-host file and is unrelated.

Username/password sign-in now returns HttpOnly `forge_session` and
`forge_refresh` cookies as well as the existing JSON tokens. TestSprite can
capture the session cookie after signing in and continue with authenticated
requests, including when the browser starts with empty localStorage. The demo's
`FORGE_PUBLIC_BASE_URL=https://…` makes both cookies `Secure` even though
Cloudflare and Traefik forward HTTP to the container. Cookie-authenticated
writes must satisfy the same-origin CSRF rule described in
[API authentication](api.md#authentication). This behavior is covered by local
API and web tests; a live TestSprite run is not part of this change's verification.

## Live URL

<!-- OWNER: fill in once the instance is up. Do not publish a URL here before
     reading "What a visitor can and cannot do". -->

_Not published yet._

## What the demo is

- **Seeded demo data.** The server runs with `--demo`: one Project named
  `Demo`, one repository record that points at `https://example.com/demo.git`,
  a placeholder Agent (`Null Default`), a placeholder daemon record and 14
  Tasks spread over `backlog`, `todo`, `in_progress`, `review`, `done` and
  `blocked`.
- **No agent daemon.** `--no-embedded-daemon`: nothing on the host launches a
  coding agent.
- **No MCP endpoint.** `--no-mcp`: `POST /mcp` is not served.
- **No provider credentials.** The instance is started without any model
  provider key.
- **Disposable.** There is no volume. `/data` is a memory-backed filesystem
  with a size cap, and the entrypoint empties it at every start. Every deploy,
  every restart and every crash recovery is a new instance with the same seed
  and the same two accounts. Anything a visitor creates is gone at that point.
- **Limited, not isolated.** The container has a read-only root filesystem,
  all Linux capabilities dropped, `no-new-privileges`, and memory, CPU and
  process limits. It **can open outbound connections**: see the limits below.

### How the accounts get there

Dokploy starts the container itself, so there is no host script. The demo
image's entrypoint does the work at every container start:

1. empties `/data`;
2. starts Forge on `127.0.0.1:8080`, which nothing outside the container can
   reach, and waits for `GET /healthz`;
3. registers the administrator (the first registered user of a Forge instance
   is its administrator), then a test account without administrator rights,
   signs in as each to check that, and adds the test account to the seeded
   Project;
4. stops that Forge and writes a marker file;
5. starts Forge again on `0.0.0.0:8080`, the address the proxy reaches.

Forge does not listen on a reachable address before step 5. That, and not the
proxy's treatment of an unhealthy container, is what keeps a visitor from
registering first and becoming the administrator. If anything in steps 1 to 4
fails, the entrypoint exits non-zero, the marker is never written, and Forge
never listens publicly. Docker then starts the container again, and a failure
with a lasting cause (a refused password, a Forge that cannot start) repeats
at every start: the container keeps restarting, stays unhealthy and the URL
stays offline until somebody reads the log and fixes the cause. Every request
the entrypoint makes is bounded (15 seconds; `DEMO_SEED_TIMEOUT_SECONDS` for
the wait on `/healthz`), so a Forge that accepts a connection and never
answers ends the same way instead of hanging.

The container health check passes only when the marker exists and `/healthz`
answers 200.

The passwords reach the container as environment variables. The entrypoint
moves them into shell variables and removes them from the environment before
it starts any process, so the Forge process does not carry them; it sends them
only inside the two HTTP requests, and never prints them.

The first registered user also becomes the owner of the seeded `Demo` Project,
and a Project is visible to its owner and members only. That is why the test
account is added to it (`DEMO_TEST_PROJECT_ROLE`, default `member`). **Any
other account does not see the seeded Project**: to show the demo data to a
person, give them the test account, or add their account to the Project as
the administrator. The first registered user also takes over the seeded Agent
and daemon record, so the test account most likely sees neither (not checked
against a running instance).

## What a visitor can and cannot do

Registration is open: anyone who reaches the URL can create an account and sign
in. Through the product, a visitor's account is an ordinary one.

A visitor who registers their own account starts with no Projects and can use
the product as an ordinary user: create Projects and Tasks of their own.
Nothing they create runs an agent, because no daemon is attached and no
provider is configured.

**Known limit — read before publishing the URL.** `--no-embedded-daemon` stops
agent runs. It does not remove every place where the server process itself
starts a shell command: Project lifecycle hooks and review check steps are
`bash -lc` commands run by the server
(`crates/services/src/lifecycle/runner.rs` and the review check runner under
`crates/review/`), and any signed-in user can create a Project
and edit its settings. Whether a visitor can make one of those commands run on
an instance without a daemon has not been tested. Until it is, treat a
signed-in visitor as someone who may be able to run a command **inside the
`forge` container**, as the user the server runs as. Such a visitor can:

- read and change the instance's database and signing key, and so make any
  account an administrator and read or change every other visitor's data;
- **open connections from the container.** The service sits on Dokploy's
  shared proxy network, which has a route to the internet and to every other
  application on that network. A command in the container can send data out,
  fetch tools, scan or call the other applications that Dokploy hosts, and
  reach services of the host that listen on Docker's bridge addresses. The
  earlier design of this demo had no outbound network; with Dokploy's proxy
  that is not possible, because the proxy must reach the service over a
  shared network. This makes the limit above worse than it was.

What the container still keeps from them: the host's filesystem and Docker
socket (nothing is mounted), Linux capabilities, and more memory, CPU or
processes than the limits allow. Storage: the root filesystem is read-only,
and the only writable places are memory-backed filesystems with a size cap
each (`/data`, `/data/logs`, `/tmp`, and Docker's own 64 MB `/dev/shm`), all
counted against the memory limit. That list was drawn up by reading where
Forge writes (data directory, log directory, the system temp directory,
`HOME`, which the entrypoint points into `/tmp`); it was not confirmed on a
running container, where a write to any other path now fails instead of
filling the host's disk.

**Consequences for where to host it.** Do not put the demo on a Dokploy server
that also runs anything you care about. Use a server (or at least a Dokploy
instance) that hosts only the demo, and keep services on it from listening on
all interfaces.

Other limits:

- **No rate limits.** Registration and sign-in are not rate-limited. Anyone
  can create accounts in bulk or guess passwords. Use long random passwords
  for the two demo accounts and reuse them nowhere.
- **Storage is capped, and it is memory.** `/data` is limited to
  `DEMO_DATA_SIZE` (default 512 MB). A visitor can fill it; Forge then fails
  until the next restart.
- **Log files.** Forge always writes a daily log file under `/data/logs`
  besides its standard error, with no size limit and no switch to turn it
  off. The demo gives that directory its own cap (`DEMO_LOG_SIZE`, default
  64 MB) so that a full log cannot take the database's space. Once it is
  full the file stops growing; the container log (standard error) carries
  on, and may then also carry a write-error line per log event.
- **No privacy between visitors.** Do not put real code, real names or real
  credentials into the demo.
- **The account passwords are visible to anyone who can inspect the container
  on the host** (`docker inspect`) and to anyone who can read the Dokploy
  application's environment. Inside the container, the Forge process does not
  carry them, but the init process (`init: true`, there to reap orphaned
  child processes) keeps the environment it was started with: a visitor who
  can run commands in the container can read both passwords from it.

A visitor cannot use MCP, cannot use a model provider that the instance owner
paid for (there is none), and cannot keep anything across a restart.

## Setting it up in Dokploy

Requirements: a Dokploy server whose Traefik answers on ports 80 and 443, and
a DNS `A` record for the demo host name that points at it.

**Order matters.** Dokploy reads `deploy/demo/docker-compose.yml` from the
branch chosen in step 2, and GitHub offers **Run workflow** only for a
workflow file that is on the default branch. Merge the change that adds
`deploy/demo/` and `.github/workflows/demo-testsprite.yml` to `main` first;
steps 2, 6 and 8 cannot work before that.

1. **Project.** In Dokploy, create a project (for example `forge-demo`).
2. **Compose application.** In the project: **Create Service → Compose**,
   compose type **Docker Compose** (not Stack). Under **Provider**, choose
   GitHub (or Git), this repository, branch `main`, compose path
   `./deploy/demo/docker-compose.yml`. Leave **Auto Deploy** off: the
   workflow triggers deploys. Dokploy reads the compose file from that
   branch; a pull request changes what runs through the image, not through
   this file.
3. **Environment.** Paste [`demo.env.example`](../deploy/demo/demo.env.example)
   into the **Environment** tab and fill it in. The passwords must be 8 to 128
   characters from `A-Z a-z 0-9 . _ ~ + = , -` (for example the output of
   `openssl rand -hex 24`): Dokploy writes the tab to an env file that Compose
   interpolates, where `$`, `#` and quotes change a value. The entrypoint
   refuses any other character instead of creating an account with a password
   nobody knows.
4. **Domain.** In the **Domains** tab add the host name, service `forge`,
   container port `8080`, HTTPS on, certificate provider Let's Encrypt.
   Dokploy adds the Traefik labels and its `dokploy-network` to the service at
   deploy time; the compose file declares neither and publishes no port.
5. **API access.** In Dokploy, **Settings → Profile → API/CLI**, generate an
   API key. Find the application's `composeId` (it is in the address of the
   application's page, or in the answer of `GET /api/project.all`). Put both
   into the repository settings listed below. Before the first workflow run,
   check the server's architecture with `uname -m` on the server: `x86_64`
   means `linux/amd64` (the default); `aarch64` means `linux/arm64`. Set the
   repository variable `DEMO_PLATFORM` to match. A deployment log saying
   `no matching manifest for linux/arm64/v8` also identifies an ARM64 server:
   set `DEMO_PLATFORM` to `linux/arm64` and run the workflow again.
6. **First image.** Run the **Demo TestSprite** workflow by hand
   (`Actions → Demo TestSprite → Run workflow`). It builds and pushes
   `ghcr.io/<owner>/<repo>-demo`, moves the `live` tag and asks Dokploy to
   deploy. The package may already be readable by Dokploy. If the instance
   does not come up, open the deployment log and the container log in Dokploy
   first: every failure to come up ends at "Wait for this build on the public
   URL". A pull error names the image; a container that did start prints
   `[demo-seed]` lines and stops at the one that failed (`[demo-seed] ERROR:
   …`), and a Forge that could not install its demo data prints
   `demo install failed`.
7. **Make the package public if needed** (see "Registry access" below):
   GitHub → the organisation's **Packages** → `<repo>-demo` → **Package settings** →
   **Change visibility** → Public.
8. **Deploy.** Re-run the failed job of the workflow, or press **Deploy** in
   Dokploy. In the container log the entrypoint reports each step with the
   prefix `[demo-seed]`.

### Registry access

The repository is public, and the demo image holds nothing that the repository
does not. Two ways to let Dokploy pull it:

- **Public GHCR package (recommended).** One click, no credential stored on
  the demo server, nothing to rotate. A new package is private until it is
  made public.
- **Registry credential in Dokploy.** **Settings → Registry**, URL `ghcr.io`,
  a GitHub user and a classic token. Dokploy's guide asks for
  `write:packages`; `read:packages` is enough for pulling. This stores a
  GitHub token on a server that, per the limit above, strangers may be able
  to run commands on, and it is not confirmed that Dokploy applies registry
  credentials to the image pulls of a Compose application.

## Resetting and taking it offline

**Reset.** Any start of the container resets the instance. In Dokploy:
**Stop**, then **Deploy** (or **Start**). A **Redeploy** with an unchanged
image may leave the running container in place, and then nothing is reset.

**Offline.** **Stop** in Dokploy. To remove it, delete the application.

**Putting a known-good build back** after a pull request was tested on the
instance: run the **Demo TestSprite** workflow by hand on `main`.

## Repository secrets and variables

Secrets (`Settings → Secrets and variables → Actions → Secrets`):

| Secret | Value |
|---|---|
| `DOKPLOY_URL` | Base URL of the Dokploy panel, with scheme, without `/api`. |
| `DOKPLOY_API_KEY` | API key generated in Dokploy. |
| `TESTSPRITE_API_KEY` | TestSprite API key. |

Variables (same page, `Variables` tab):

| Variable | Value |
|---|---|
| `DOKPLOY_COMPOSE_ID` | The `composeId` of the Dokploy Compose application. |
| `DEMO_URL` | Public URL of the demo, with scheme. The same URL as the domain in Dokploy, as `FORGE_PUBLIC_BASE_URL`, and as the URL the TestSprite project tests. |
| `DEMO_PLATFORM` | Optional demo server platform: `linux/amd64` (default when unset) or `linux/arm64`. Any other value fails the configuration check. |
| `TESTSPRITE_PROJECT_ID` | Id of the TestSprite project that holds the suite. |

The account emails and passwords are not repository secrets. They live only in
the Dokploy application's environment and in the TestSprite project.

**Who can read these secrets.** Anyone who can push a branch to this
repository can edit the workflow on that branch and make it print or send the
secrets. A Dokploy API key is not scoped to one application: it can do
whatever its user can do in Dokploy. Give the key a Dokploy user that can
reach only the demo project, or use a Dokploy instance that hosts only the
demo. To add an approval step, move the three secrets into a GitHub
Environment (`Settings → Environments`, for example `demo`) with **Required
reviewers** and add `environment: demo` to both jobs of the workflow; the
check then no longer runs unattended.

## How the pull-request check works

Workflow: [`.github/workflows/demo-testsprite.yml`](../.github/workflows/demo-testsprite.yml).
It runs for pull requests to `main` and `next/v0.14`, and by hand. Pull
requests that change only Markdown files or files under `docs/` do not start
it.

1. **Build.** The pull request's head commit (the code as pushed, not the
   result of merging it into its base) is built with the root `Dockerfile`, then
   `deploy/demo/Dockerfile` puts the entrypoint, the health check and a build
   id on top. The result is pushed to GHCR as
   `ghcr.io/<owner>/<repo>-demo:pr-<number>-<head sha>`. A newer push to the
   same pull request cancels its older build.
2. **Deploy.** The `live` tag is moved to that image. The workflow calls
   Dokploy's `POST /api/compose.stop`, waits until the public URL no longer
   serves a build, then calls `POST /api/compose.deploy`. Dokploy pulls
   `live` (`pull_policy: always`) and starts the container.
3. **Wait for this build.** The image carries its build id in a static file
   that Forge serves without sign-in, `/demo-build.txt`. The workflow waits
   until the public URL serves exactly the id of the image it built and
   `/healthz` answers. Because Forge listens publicly only after the accounts
   exist, that also means the instance is seeded.
4. **Suite.** `TestSprite/testsprite-action` runs every test of the TestSprite
   project. The job fails if a test fails or is skipped.
5. **Result.** A last job, **Demo TestSprite result**, is green only when the
   image was built and the suite passed. It is the check to require: the
   suite job is skipped when the build fails, and a skipped job would count
   as passing.

Things that follow from there being one demo instance:

- A full TestSprite run always tests the URL configured in the TestSprite
  project, so every pull request is deployed onto that same URL. While a check
  runs, the public demo shows that pull request's build, and it keeps showing
  it afterwards until the next deploy. The demo is offline from the stop until
  the new container has seeded itself.
- Deploy-and-test runs one at a time (`concurrency: demo-testsprite`). GitHub
  keeps a single waiting run per group: if a third run arrives while one is
  running and one is waiting, the waiting one is cancelled. Re-run a cancelled
  check from the Actions page.
- Pull requests from forks and from Dependabot are skipped: they receive no
  secrets. A skipped check does not block merging.
- Draft pull requests are skipped. Marking a draft ready for review starts
  the check.
- A pull request's changes to `deploy/demo/docker-compose.yml` are not
  tested: Dokploy uses the file from the branch configured in step 2 of the
  setup. Its changes to the entrypoint and the health check are, because they
  are part of the image.
- The suite starts from the same state every time: the seed plus the two
  accounts, with the test account a member of the seeded Project.
- Old `pr-…` and `base-…` tags accumulate in the GHCR package. Nothing
  removes them.

### Build time and cache

`DEMO_PLATFORM` selects a native build runner: `ubuntu-latest` for
`linux/amd64` (the default), or `ubuntu-24.04-arm` for `linux/arm64`. Both
images target that platform, without QEMU emulation. The layer cache scope
includes the platform, so amd64 and arm64 layers stay separate. The suite
still runs on `ubuntu-latest`; it names the image's platform when creating
the container to copy out the build id, without starting the container.

Every run compiles the release image. Two things decide how long that takes:

- **The Dockerfile's dependency layer does not cache today.** It copies the
  manifests of only some of the workspace's crates and builds them with
  `|| true`; Cargo cannot load the incomplete workspace, the error is
  swallowed, and the layer holds no compiled dependencies. Every build whose
  Rust sources changed compiles all dependencies again. Fixing that is a
  change to the root `Dockerfile`, which the release image shares, and is not
  part of this setup.
- **The layer cache is written by manual runs only.** A pull request can read
  the GitHub Actions cache of its base branch, not of other pull requests, so
  the workflow reads the cache on every run and writes it only on
  `workflow_dispatch`. Run the workflow by hand on `main` and on `next/v0.14`
  once, and again after large dependency or frontend changes. With the
  dependency layer as it is, the cache saves the base images, the frontend
  install and, when `web/` is unchanged, the frontend build; it does not save
  the Rust compile.

The 90-minute build timeout is an estimate, not a measurement.

### Making the check required

The workflow file does not make anything required. That is a branch protection
setting: `Settings → Branches` (or `Rules → Rulesets`) → the rule for `main`
and for `next/v0.14` → **Require status checks to pass** → add
**Demo TestSprite result** (not "Demo TestSprite suite", which is skipped, and
so counts as passing, when the build fails). The check appears in the list
after it has run once.

One consequence to decide on first: the workflow does not start for
documentation-only pull requests, and a required check that never starts stays
"expected" and blocks the merge. If the check is made required, either remove
the `paths-ignore` block from the workflow or merge documentation-only pull
requests with an administrator override.

## Not verified

The first GitHub Actions run reached Dokploy, which could read the image's
manifest list but could not pull it on the ARM64 server: the image was built
for amd64 only. That run did not verify a running demo container or the
TestSprite suite.

The ARM64 path then ran end to end (`DEMO_PLATFORM=linux/arm64`): the image
was built on `ubuntu-24.04-arm`, `compose.stop` and `compose.deploy` were
accepted, Dokploy pulled the image without a registry credential, the
container seeded itself and turned healthy, and the public URL served the
run's build id and `/healthz` to the workflow. The TestSprite step ran and
reported that the project had no tests yet.

**Cloudflare in front of the demo.** If the demo host name is proxied by
Cloudflare, Bot Fight Mode answers requests from cloud address ranges (GitHub
runners, TestSprite's browsers) with a challenge page (`403`,
`cf-mitigated: challenge`) on every page except static files. The workflow
then waits until its timeout: `/demo-build.txt` answers, `/healthz` does not.
Bot Fight Mode cannot exempt one host name on the free plan: turn it off for
the zone (Security → Settings → Bot traffic), or serve the demo from a host
name that Cloudflare does not proxy.

The following remain unverified:

- **Dokploy setup details taken from its documentation, not tried:** that
  the Domains tab adds the Traefik labels and `dokploy-network` to the
  chosen service; that the Environment tab becomes an env file Compose reads.
- **Not found in Dokploy's documentation:** whether `compose.stop` and
  `compose.deploy` return before the work is done (the workflow assumes they
  may, and waits on the public URL); whether a deploy recreates a container
  whose image did not change (the workflow does not depend on it: it stops
  first and gives every run a new image); the exact names of the UI controls
  in the setup steps.
- **Traefik and unhealthy containers.** Traefik's Docker provider
  documentation implies, in the description of `allowEmptyServices`, that by
  default no server is created for a container that is not healthy; it does
  not say so outright, and Dokploy's documentation does not say which Traefik
  provider serves Compose applications. The demo therefore does not rely on
  it: Forge is on loopback until the accounts exist.
- **In the container:** that Forge starts with all capabilities dropped, a
  read-only root filesystem and its data on memory filesystems; that Forge
  writes nowhere outside `/data`, `/tmp` and `HOME`; that Docker mounts
  `/data/logs` inside `/data` in that order.
- **In the workflow:** that the job's token may create the `-demo` package and
  move a tag in it; that an empty `cache-to` means "do not export".

## Suite coverage

<!-- OWNER: fill in after the TestSprite suite exists: which flows it covers,
     which account it signs in with, and what it deliberately leaves out. -->

_Not written yet._
