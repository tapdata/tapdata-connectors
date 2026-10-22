# Connector distribution

This directory contains the automated connector release workflow. CI compares
the selected source ref with the source commit recorded in the latest Nexus
BOM, finds connector modules affected by changes, increments their versions,
and publishes a new immutable BOM containing the full connector set.

## Running the release

`.github/workflows/publish-connectors.yml` can be started with
`workflow_dispatch` or called with `workflow_call`. The only optional input is
`source_ref`; when omitted, CI uses the selected workflow ref. Configure the
`NEXUS_USERNAME` and `NEXUS_PASSWORD` Actions secrets.

The first run publishes all connector modules at `1.0.0` (or reuses matching
immutable coordinates already in Nexus). It records the checked out Git commit
inside the generated BOM. Later runs compare that commit with the selected
source ref. A run with no affected connector changes exits without publishing.

## Change detection and versions

CI checks changed paths since the last BOM source commit. Connector source
changes select that connector. Changes in shared Maven modules, such as
`sql-core`, propagate through Maven dependency declarations to every dependent
connector, including transitive dependents. Root build POM changes conservatively
select all connectors. Documentation and workflow-only changes do not create a
connector release.

Each selected connector gets an automatic patch increment: `1.0.0` becomes
`1.0.1`. A connector not previously listed starts at `1.0.0`. Unchanged
connectors retain their exact version from the previous BOM. The next BOM
version is generated automatically by incrementing its final numeric segment;
for the first BOM, CI uses the PDK API base version, UTC date, and `.1` (for
example, `2.0.11.20261022.1`).

CI builds only selected connectors, checks the Nexus SHA-256 for each
immutable coordinate, and skips an upload when the bytes already match. It
fails if a coordinate exists with different bytes or if a reused connector
artifact is missing. It then generates and deploys a BOM containing every
connector's exact version. The source commit and PDK API version are stored as
BOM properties so future runs can find the comparison baseline. The source
POMs in Git are not changed.

The workflow supports a single PDK API version in each BOM. If the PDK API
version changes, CI starts a complete connector set for that API version. Nexus
must prevent redeployment of release coordinates as a second immutability
guard.
