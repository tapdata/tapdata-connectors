# Connector distribution

Batch builds create an independent local clone of the already checked-out source
and detach at the immutable release-plan commit. This does not fetch from GitHub.
The normal `.git` directory supports legacy `git-commit-id-plugin`/JGit versions
used by enterprise connectors, which cannot read linked worktree `commondir`
layouts. Git reuses local objects via hardlinks where supported; temporary clones
are removed on success or failure without altering the source checkout.

## Transfer limits and progress

Every Nexus request logs its target name, method, artifact path, completion time,
and retries. Downloads report byte progress every ten seconds while data arrives.
Requests have a ten-minute wall-clock limit covering connection setup, transfer,
and all retries, in addition to socket inactivity timeouts. This uses POSIX timers
in the publisher's main thread on the Linux CI runner; continuously trickling
responses cannot extend the deadline. Upload steps default to 120 minutes and final inventory verification/BOM
publication to 180 minutes. Override these with positive integer Actions Variables
`CONNECTOR_UPLOAD_TIMEOUT_MINUTES` and `CONNECTOR_FINALIZE_TIMEOUT_MINUTES`
respectively (both must fit within the 480-minute job limit). A timeout fails the
run; rerun the workflow to resume its immutable Nexus release plan.

Final verification still downloads and compares the actual JAR bytes from both
Nexus installations. Once a remote JAR has been verified in that pass, checksum
repair does not download that same JAR again. Existing bytes are never replaced.

This directory contains the automated connector release workflow. CI compares
the selected source ref with the source commit recorded in the latest Nexus
BOM, finds connector modules affected by changes, versions them according to
their own PDK API generation, and publishes a new immutable BOM containing the
full connector set.

## Running the release

`.github/workflows/publish-connectors.yml` can be started with
`workflow_dispatch` or called with `workflow_call`. The `source_ref` input
defaults to the selected workflow branch; enterprise inputs are described below. Both the
workflow and caller must run on the explicitly trusted, protected `main` or
`develop` branch in `tapdata/tapdata-connectors`. The source may be any protected
branch in that repository. The reusable batch workflow independently checks the caller,
branch protection, and that each pinned commit belongs to its validated branch
history (scripts from the workflow branch, source from the selected source branch).
Tags, commit refs, and unprotected branches are rejected before checkout or
Nexus credential setup. CI resolves the validated source branch to a commit SHA
and checks out that exact commit.

For a manual release, select **main** (or **develop**) in the Actions **Run workflow**
branch selector. Leave `source_ref` blank to release that branch, or enter a
protected source branch such as `develop`. Changing `source_ref` alone does not
change the branch that supplies the workflow and publishing scripts.

Configure these repository/organization Actions variables and secrets:

### Include enterprise connectors

Enable `include_enterprise` when starting this same workflow to publish both
repositories, sequentially on the configured runner. Existing callers default to
OSS only. Set `enterprise_source_ref` to a protected branch in
`tapdata/tapdata-connectors-enterprise`; if blank it follows `source_ref`, or the
selected workflow branch. The enterprise branch is validated independently and
its exact commit is pinned before checkout. Publishing scripts always come from
the protected OSS workflow branch, never from the enterprise source checkout.

Add the Actions Secret `CONNECTOR_ENTERPRISE_READ_TOKEN` to this repository (or
an organization secret accessible here). Use a token with read access to repository
contents and metadata for `tapdata/tapdata-connectors-enterprise` and the public
OSS repository. The default `GITHUB_TOKEN` cannot read a different private repo.
Reusable callers must pass this secret or use `secrets: inherit`. Do not put a
token in workflow inputs. The token is scoped to validation and source checkout,
is not persisted by checkout, and is not passed to Maven.

Both distributions use the existing internal/external Nexus settings. Enterprise
JARs retain their declared `io.tapdata:<artifactId>` coordinates and use the same
PDK-based version rules and batches of at most ten. Their full inventory is
published as `io.tapdata:connectors-enterprise-bom`, with independent metadata and
`io.tapdata:connector-enterprise-release-plan` recovery plans. OSS continues to use
`connectors-bom` and `connector-release-plan`. A failed or resumed enterprise
release does not advance the OSS baseline (or vice versa). Initial enterprise
publication builds every connector declared in its `connectors/pom.xml`.

Example reusable invocation inputs:

```yaml
with:
  source_ref: main
  include_enterprise: true
  enterprise_source_ref: main
secrets: inherit
```

### Nexus settings

| Kind | Name | Purpose |
| --- | --- | --- |
| Variable | `NEXUS_INTERNAL_URL` | Full internal Maven releases repository URL, ending in `/repository/maven-releases/` |
| Variable | `NEXUS_EXTERNAL_URL` | Full external Maven releases repository URL |
| Variable | `NEXUS_INTERNAL_CONNECT_IP` | Optional internal TCP destination IP; e.g. `192.168.1.184` |
| Variable | `NEXUS_EXTERNAL_CONNECT_IP` | Optional external TCP destination IP; blank uses normal DNS/hosts resolution |
| Variable | `CONNECTOR_PUBLISH_RUNNER` | Optional runner label; defaults to `ubuntu-latest` |
| Secret | `NEXUS_INTERNAL_USERNAME`, `NEXUS_INTERNAL_PASSWORD` | Internal repository credentials |
| Secret | `NEXUS_EXTERNAL_USERNAME`, `NEXUS_EXTERNAL_PASSWORD` | External repository credentials |

The planning job validates these settings before source checkout and recovery tests.
If settings are missing or blank, it lists all missing names without printing values.
Configuration validation does not test Nexus connectivity or upload permissions.
Set them under **Settings → Secrets and variables → Actions**, using the Variables
and Secrets tabs respectively, or organization settings accessible to this repository.
These jobs do not select a GitHub Environment, so Environment-only settings are not
available. A missing URL is a configuration error, not an HTTPS server error.

Reusable workflow callers pass all four secrets explicitly (or use
`secrets: inherit`); URLs and the runner label come from the caller repository's
Actions variables. Both URLs must be HTTPS endpoints for Maven **hosted
release** repositories. Identical URLs are supported when the connection IP
settings differ. The selected runner must reach both environments;
configure a self-hosted runner label if GitHub-hosted runners cannot reach the
internal Nexus. Credentials need read/upload access, including Maven
metadata updates. Nexus must allow the custom `json` extension used by release
plans. CI deliberately fails if either environment is unavailable.

For two independent Nexus installations sharing `nexus.tapdata.net`, set:

```text
NEXUS_INTERNAL_URL=https://nexus.tapdata.net/repository/maven-releases/
NEXUS_EXTERNAL_URL=https://nexus.tapdata.net/repository/maven-releases/
NEXUS_INTERNAL_CONNECT_IP=192.168.1.184
NEXUS_EXTERNAL_CONNECT_IP=
```

Internal requests connect directly to `192.168.1.184` using the URL's port,
while retaining `nexus.tapdata.net` for HTTP Host, TLS SNI, and certificate
verification. IPv4 and IPv6 literals are accepted; hostnames and port suffixes
are rejected. This setting applies to all plan, artifact, checksum, and metadata
reads/writes, including retries. It does not modify global DNS or hosts files.
Pinned requests bypass environment HTTP(S) proxies and reject redirects; use the
final repository URL. Each installation needs a certificate trusted by the runner
and valid for the URL hostname; certificate verification remains enabled.

Use a runner that can reach both the public Nexus and `192.168.1.184`. With the
external IP blank, ensure its DNS/hosts resolves `nexus.tapdata.net` to the public
installation (remove the internal hosts override from that runner). Alternatively,
set `NEXUS_EXTERNAL_CONNECT_IP` to a stable public IP to pin both destinations.

Publishing scripts are checked out from a pinned commit of the protected workflow
branch (`main` or `develop`), where this workflow is installed. Planning, batch
preparation, upload, and BOM publication run separately from Maven. Each batch builds on a disposable
GitHub-hosted `ubuntu-latest` runner without Nexus secrets; the upload job checks
out the trusted scripts again. Source builds never execute on the internal Nexus
runner. The build runner needs access to the Maven dependencies used for packaging.

The immutable release plan travels directly from the planning job to upload jobs
by its Actions artifact ID. Upload also compares it to the saved plan in **both**
Nexus installations before making changes. Build output is downloaded into a
separate data directory and is never imported or executed as Python code.

The first run publishes all connector modules. Each connector starts at
`2.<api-generation>.0` (for example, PDK API `2.11` or `2.0.11-SNAPSHOT`
maps to connector version `2.11.0`). CI records each connector's PDK API
version and the checked out Git commit inside the generated BOM. Later runs
compare that commit with the selected source ref. Previous BOMs must use this
version scheme and record the source commit and each connector's PDK API version;
there is no migration support for earlier formats.

## Change detection and versions

CI checks changed paths since the last BOM source commit. Connector source
changes select that connector. Changes in shared Maven modules, such as
`sql-core`, propagate through Maven dependency declarations to every dependent
connector, including transitive dependents. Root build POM changes
conservatively select all connectors. Documentation and workflow-only changes
do not create a connector release.

Each selected connector gets a version based on its own PDK API generation.
Within the same generation, the patch number increments (`2.11.0` becomes
`2.11.1`). When that connector moves to a higher PDK API generation, its
version starts at `2.<new-generation>.0` (`2.11.3` becomes `2.12.0`). The
accepted API spellings are `2.<n>` and `2.0.<n>`; for example, `2.11` and
`2.0.11-SNAPSHOT` both map to generation `11`. Unsupported formats and API
generation downgrades fail the workflow. Unchanged connectors retain their
exact version from the previous BOM. The BOM itself has an independent version:
later releases increment its final numeric segment; the first BOM uses the
default PDK API base version, UTC date, and `.1` (for example,
`2.0.11.20261022.1`).

Before changing local POMs, building connectors, or uploading artifacts, CI
checks the planned BOM version's exact POM URL in Nexus. An existing BOM aborts
the release; only HTTP 404 allows it to continue. Authentication, server, and
network failures stop the release as well. This detects conflicts even if Nexus
metadata has not yet listed the version. The preflight check does not reserve
the version, so Nexus must still reject concurrent redeployment.

## Batches and retry recovery

A release processes selected connectors in serial batches of **at most 10**.
Each batch prepares its missing-JAR list, builds without credentials, then uploads
to both repositories in a separate job before the next batch starts.
For 23 selected connectors the batches are 10, 10, and 3. There is one complete
BOM for the release, published after every connector JAR and POM is present in
both environments. A single workflow run continues through all batches; the
limit is per batch, not a limit on the complete BOM's contents. Maven may build
shared dependencies through `-am`; only the selected connectors are uploaded.
Tests remain skipped during packaging.

Before building, CI persists an identical immutable release plan in both
repositories under:

```
io/tapdata/connector-release-plan/<previous-bom-version-or-bootstrap>/
  connector-release-plan-<previous-bom-version-or-bootstrap>.json
```

The plan fixes the source commit, target BOM version, connector versions, and
complete release inventory. Do not delete these plans while a release is
incomplete. If plan replication fails, no connectors are built or uploaded.
The existing BOM conflict preflight runs before creating a new plan. A retry
with a saved plan may reuse its own already published BOM, but must match its
bytes exactly; it cannot overwrite a different BOM.

Re-run the workflow after a failure. It reads the saved plan and:

- Skips building/uploading connectors already present; repairs missing JARs,
  POMs, and checksum files in either environment.
- Downloads an existing JAR and copies those exact bytes to the missing
  environment. Local/remote SHA-256 differences produce a warning; remote
  bytes remain authoritative. If both environments already contain different
  bytes for the same coordinate, CI stops because it cannot repair immutable
  releases by overwriting them.
- Rebuilds only missing connectors in the current batch and subsequent batches.
  Authentication and permission errors fail immediately; transient HTTP/network
  requests retry up to three times, with 1- and 2-second delays. A lost PUT
  response is checked against the stored file before treating it as failure.
- Completes a partially published BOM and its Maven metadata without bumping
  any version again. New plans still reject an existing target BOM.

If the protected source branch advanced, CI first finishes the saved source
commit in an isolated worktree, after verifying it is an ancestor of the
validated protected branch. Run the workflow again to release the newer changes.
Unrelated branch histories are rejected. First-release retries retain the same
BOM version even across UTC dates. Local POM version edits stay in the temporary
worktree; tracked source files in the caller checkout remain untouched.

BOM publication across two Nexus installations cannot be atomic. All connector
files are uploaded and verified in both environments before either BOM is
published. A network failure can still leave only one environment's BOM or
metadata visible temporarily; a retry completes the other using the saved plan.
Do not start separate concurrent release jobs outside this workflow's shared
`connector-publish` concurrency group. Nexus must prevent redeployment of release
coordinates, while allowing Maven metadata updates.
