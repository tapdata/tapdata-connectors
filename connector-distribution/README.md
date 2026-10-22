# Connector distribution

This directory contains the automated connector release workflow. CI compares
the selected source ref with the source commit recorded in the latest Nexus
BOM, finds connector modules affected by changes, versions them according to
their own PDK API generation, and publishes a new immutable BOM containing the
full connector set.

## Running the release

`.github/workflows/publish-connectors.yml` can be started with
`workflow_dispatch` or called with `workflow_call`. The only optional input is
`source_ref`; when omitted, CI uses the selected workflow ref. Configure the
`NEXUS_USERNAME` and `NEXUS_PASSWORD` Actions secrets.

The first run publishes all connector modules. Each connector starts at
`1.<api-generation>.0` (for example, PDK API `2.11` or `2.0.11-SNAPSHOT`
maps to connector version `1.11.0`). CI records each connector's PDK API
version and the checked out Git commit inside the generated BOM. Later runs
compare that commit with the selected source ref. A run with no affected
connector changes exits without publishing.

## Change detection and versions

CI checks changed paths since the last BOM source commit. Connector source
changes select that connector. Changes in shared Maven modules, such as
`sql-core`, propagate through Maven dependency declarations to every dependent
connector, including transitive dependents. Root build POM changes
conservatively select all connectors. Documentation and workflow-only changes
do not create a connector release.

Each selected connector gets a version based on its own PDK API generation.
Within the same generation, the patch number increments (`1.11.0` becomes
`1.11.1`). When that connector moves to a higher PDK API generation, its
version starts at `1.<new-generation>.0` (`1.11.3` becomes `1.12.0`). The
accepted API spellings are `2.<n>` and `2.0.<n>`; for example, `2.11` and
`2.0.11-SNAPSHOT` both map to generation `11`. Unsupported formats and API
generation downgrades fail the workflow. Unchanged connectors retain their
exact version from the previous BOM. The BOM itself has an independent version:
later releases increment its final numeric segment; the first BOM uses the
default PDK API base version, UTC date, and `.1` (for example,
`2.0.11.20261022.1`).

CI builds only selected connectors, checks the Nexus SHA-256 for each
immutable coordinate, and skips an upload when the bytes already match. It
fails if a coordinate exists with different bytes or if a reused connector
artifact is missing. It then generates and deploys a BOM containing every
connector's exact version. The source commit and each connector's PDK API
version are stored as BOM properties so future runs can find the comparison
baseline. The source POMs in Git are not changed. Nexus must prevent
redeployment of release coordinates as a second immutability guard.
