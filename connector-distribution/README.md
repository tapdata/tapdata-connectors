# Connector distribution

This directory contains the automated connector release workflow. CI compares
the selected source ref with the source commit recorded in the latest Nexus
BOM, finds connector modules affected by changes, versions them according to
their own PDK API generation, and publishes a new immutable BOM containing the
full connector set.

## Running the release

`.github/workflows/publish-connectors.yml` can be started with
`workflow_dispatch` or called with `workflow_call`. The only optional input is
`source_ref`; when omitted, CI uses the selected workflow branch. Both the
workflow branch (including a reusable workflow's own branch), the caller branch,
and the source branch must be protected in `tapdata/tapdata-connectors`.
Tags, commit refs, and unprotected branches are rejected before checkout or
Nexus credential setup. CI resolves the validated source branch to a commit SHA
and checks out that exact commit.

Configure these repository/organization Actions variables and secrets:

| Kind | Name | Purpose |
| --- | --- | --- |
| Variable | `NEXUS_INTERNAL_URL` | Full internal Maven releases repository URL, ending in `/repository/maven-releases/` |
| Variable | `NEXUS_EXTERNAL_URL` | Full external Maven releases repository URL |
| Variable | `NEXUS_INTERNAL_CONNECT_IP` | Optional internal TCP destination IP; e.g. `192.168.1.184` |
| Variable | `NEXUS_EXTERNAL_CONNECT_IP` | Optional external TCP destination IP; blank uses normal DNS/hosts resolution |
| Variable | `CONNECTOR_PUBLISH_RUNNER` | Optional runner label; defaults to `ubuntu-latest` |
| Secret | `NEXUS_INTERNAL_USERNAME`, `NEXUS_INTERNAL_PASSWORD` | Internal repository credentials |
| Secret | `NEXUS_EXTERNAL_USERNAME`, `NEXUS_EXTERNAL_PASSWORD` | External repository credentials |

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
