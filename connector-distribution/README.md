# Connector distribution

This directory contains the connector release workflow and documents how Tapstate
consumes connector artifacts from Nexus through a PDK-specific BOM. Maven
consumes exact versions from the BOM. The release workflow selects the newest
connector releases built against the requested PDK API and generates a
new, immutable BOM.

## Current develop branch values

Both sample connectors inherit the same PDK API version from
`connectors/pom.xml`:

| Connector | Current Maven version | `tapdata.pdk.api.version` |
| --- | --- | --- |
| `mysql-connector` | `1.0-SNAPSHOT` | `2.0.11-SNAPSHOT` |
| `postgres-connector` | `1.0-SNAPSHOT` | `2.0.11-SNAPSHOT` |

A release BOM can contain both connectors, but each entry must use an immutable
connector release version. The source POMs currently use `1.0-SNAPSHOT`; the
workflow sets release versions in its build checkout and does not publish those
snapshot coordinates.

## How CI fits in

1. The connector build publishes only modules selected for release. A release
   must give the connector a new version; a version already present in Nexus
   is immutable. CI skips an existing coordinate when its SHA-256 matches and
   fails if the same coordinate has different bytes.
2. CI reads each published connector POM's `tapdata.pdk.api.version` property.
   It groups releases by that exact PDK API version and chooses the highest
   connector version for each `groupId:artifactId` in the group. Connector
   version and PDK API version are separate fields; they must not be compared
   with each other.
3. CI writes the selected exact coordinates into a new
   `connectors-bom:<bom-version>` POM and deploys it to Nexus. The BOM
   version is never overwritten. A new connector release creates a new BOM;
   unchanged connector JARs are simply referenced again.
4. Tapstate imports the BOM matching its PDK API and declares the connectors it
   needs without versions. Updating Tapstate's BOM version is the explicit
   action that adopts a newer connector set.

`generate_bom.py` implements the selection and POM generation step. CI can
export the immutable releases and their POM metadata from Nexus into a JSON
inventory, then run:

```sh
python3 connector-distribution/generate_bom.py \
  --inventory target/nexus-connector-releases.json \
  --pdk-api-version 2.0.11-SNAPSHOT \
  --bom-version 2.0.11.20261016.1 \
  --output target/generated-boms/pdk-2.0.11/pom.xml
```

Each inventory record contains `groupId`, `artifactId`, `version`,
`pdkApiVersion`, and `sha256`. The generator chooses the highest numeric
connector version per artifact, rejects snapshots and conflicting hashes for
the same coordinate, and writes exact versions. CI then deploys that POM as a
new BOM coordinate. Nexus credentials belong in CI settings/secrets, not in
this repository. `maven-deploy-plugin` uploads Maven artifacts; it does not
select compatible connectors or detect source changes by itself.

The generated POM is a build artifact, so CI writes it under `target/` and
deploys it from there. It is not kept in a versioned `boms/` source directory;
the immutable BOM versions and contents come from the release inventory.

## Files

- `generate_bom.py`: generates the immutable BOM POM from published release
  metadata; CI supplies the inventory and output path.
- `tapstate-consumer/pom.xml`: Tapstate consumer POM. Supply a published BOM
  version with `-Dconnectors.bom.version=<version>`; connector dependencies
  remain versionless.

## Artifact publishing workflow

`.github/workflows/publish-connectors.yml` is the separate artifact publishing
flow. It has no push or pull request trigger, so reviewing or merging this PR
does not publish anything. It can be started manually with `workflow_dispatch`
or called by another repository's release workflow with `workflow_call`.
Configure the `NEXUS_USERNAME` and `NEXUS_PASSWORD` Actions secrets.

The caller supplies the complete connector set for one PDK API version. For
example, to publish MySQL `1.1.0` and keep using Postgres `1.0.0`:

```json
[
  {
    "module": "connectors/mysql-connector",
    "groupId": "io.tapdata",
    "artifactId": "mysql-connector",
    "version": "1.1.0",
    "pdkApiVersion": "2.0.11-SNAPSHOT",
    "publish": true
  },
  {
    "module": "connectors/postgres-connector",
    "groupId": "io.tapdata",
    "artifactId": "postgres-connector",
    "version": "1.0.0",
    "pdkApiVersion": "2.0.11-SNAPSHOT",
    "publish": false
  }
]
```

The workflow checks each PDK API value against the connector POM. It builds
only entries with `publish: true`. For a new coordinate it deploys the shaded
JAR and a generated POM under the requested immutable release version. If the
coordinate already exists with the same SHA-256, it skips upload; if the bytes
differ, it fails. Entries with `publish: false` are fetched from Nexus and
hashed, so a missing reused version fails before BOM deployment. The generated
BOM contains the exact versions from the full input set and is deployed under
the requested new BOM version. This workflow supports a single PDK API group per
run and requires a new numeric BOM version, for example
`2.0.11.20261016.1` (format example; use a new version for each release).

The reusable workflow lets Tapdata's OpenSource Version Construction supply
the connector-set manifest and a new BOM version. That orchestration decides
which modules changed and sets `publish`; this workflow builds/uploads the selected
connectors, validates reused coordinates, and publishes the BOM. It sets each
selected module's project version in the runner's checkout before building,
so the JAR manifest and Maven coordinate use the requested release version;
the source POM in Git is not changed. Reusable callers also pass `source_ref`
to choose the connector branch or tag to build. Nexus must enforce release
redeploy prevention as a second immutability guard.

This workflow publishes connector releases and immutable BOMs. The sample
connector modules still declare `1.0-SNAPSHOT`; their shared PDK API version
is inherited from `connectors/pom.xml` as `2.0.11-SNAPSHOT`. The release
workflow publishes separately versioned artifacts without committing version
edits to source POMs.
