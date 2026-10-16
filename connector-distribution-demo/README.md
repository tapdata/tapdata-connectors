# Connector distribution demo

This directory shows how Tapstate can consume connectors from Nexus through a
PDK-specific BOM. Maven consumes exact versions from the BOM; the release job
is responsible for choosing the newest connector release built against the
requested PDK API and generating a new, immutable BOM.

## Current develop branch values

Both sample connectors inherit the same PDK API version from
`connectors/pom.xml`:

| Connector | Current Maven version | `tapdata.pdk.api.version` |
| --- | --- | --- |
| `mysql-connector` | `1.0-SNAPSHOT` | `2.0.11-SNAPSHOT` |
| `postgres-connector` | `1.0-SNAPSHOT` | `2.0.11-SNAPSHOT` |

This demo has one BOM containing both connectors. It uses the connectors'
current Maven coordinates so it can be inspected alongside the existing POMs.
Production releases should use independent, immutable connector versions (for
example, `1.1.0`, `1.0.0`) instead of `1.0-SNAPSHOT`.

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
   `connectors-bom:<pdk-api>-<release-id>` POM and deploys it to Nexus. The BOM
   version is never overwritten. A new connector release creates a new BOM;
   unchanged connector JARs are simply referenced again.
4. Tapstate imports the BOM matching its PDK API and declares the connectors it
   needs without versions. Updating Tapstate's BOM version is the explicit
   action that adopts a newer connector set.

`generate_bom.py` implements the selection and POM generation step. CI can
export the immutable releases and their POM metadata from Nexus into a JSON
inventory, then run:

```sh
python3 connector-distribution-demo/generate_bom.py \
  --inventory target/nexus-connector-releases.json \
  --pdk-api-version 2.0.11-SNAPSHOT \
  --bom-version 2.0.11-20261016.1 \
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
- `tapstate-consumer/pom.xml`: sample showing BOM import and versionless
  connector dependencies for PDK API `2.0.11-SNAPSHOT`.

This is an integration demo, not yet a production release job. The current
connector modules still use `1.0-SNAPSHOT`; their shared PDK API version is
inherited from `connectors/pom.xml` as `2.0.11-SNAPSHOT`.
