#!/usr/bin/env python3
"""Generate an immutable PDK-specific Maven BOM from a release inventory."""

import argparse
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path


VERSION_RE = re.compile(r"^\d+(?:\.\d+)*$")
SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
NS = "http://maven.apache.org/POM/4.0.0"
ET.register_namespace("", NS)


def version_key(value):
    if not VERSION_RE.fullmatch(value):
        raise ValueError(f"unsupported connector release version: {value!r}")
    return tuple(int(part) for part in value.split("."))


def sub(parent, name, value=None):
    element = ET.SubElement(parent, f"{{{NS}}}{name}")
    if value is not None:
        element.text = str(value)
    return element


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", required=True, type=Path,
                        help="JSON export of immutable connector releases in Nexus")
    parser.add_argument("--pdk-api-version", required=True)
    parser.add_argument("--bom-version", required=True,
                        help="new immutable BOM version, e.g. 2.0.8-20261016.1")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    inventory = json.loads(args.inventory.read_text(encoding="utf-8"))
    releases = inventory.get("releases")
    if not isinstance(releases, list):
        raise ValueError("inventory must contain a 'releases' array")

    coordinates = {}
    latest = {}
    for item in releases:
        fields = ("groupId", "artifactId", "version", "pdkApiVersion", "sha256")
        if any(not isinstance(item.get(field), str) or not item[field] for field in fields):
            raise ValueError(f"release is missing required metadata: {item!r}")
        if "SNAPSHOT" in item["version"].upper():
            raise ValueError(f"mutable snapshot cannot be included in a release BOM: {item}")
        version_key(item["version"])
        if not SHA256_RE.fullmatch(item["sha256"]):
            raise ValueError(f"invalid SHA-256 for {item['artifactId']}:{item['version']}")

        coordinate = (item["groupId"], item["artifactId"], item["version"])
        previous = coordinates.get(coordinate)
        if previous and previous["sha256"].lower() != item["sha256"].lower():
            raise ValueError(f"same Maven coordinate has different SHA-256 values: {coordinate}")
        coordinates[coordinate] = item

        if item["pdkApiVersion"] != args.pdk_api_version:
            continue
        key = (item["groupId"], item["artifactId"])
        current = latest.get(key)
        if current is None or version_key(item["version"]) > version_key(current["version"]):
            latest[key] = item

    if not latest:
        raise ValueError(f"no releases found for PDK API {args.pdk_api_version}")

    project = ET.Element(f"{{{NS}}}project")
    sub(project, "modelVersion", "4.0.0")
    sub(project, "groupId", "io.tapdata")
    sub(project, "artifactId", "connectors-bom")
    sub(project, "version", args.bom_version)
    sub(project, "packaging", "pom")
    sub(project, "name", f"Connectors for PDK API {args.pdk_api_version}")
    management = sub(project, "dependencyManagement")
    dependencies = sub(management, "dependencies")
    for key in sorted(latest):
        item = latest[key]
        dependency = sub(dependencies, "dependency")
        sub(dependency, "groupId", item["groupId"])
        sub(dependency, "artifactId", item["artifactId"])
        sub(dependency, "version", item["version"])

    args.output.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(project).write(args.output, encoding="UTF-8", xml_declaration=True)
    print(f"Wrote {args.output} with {len(latest)} connector(s) for PDK API {args.pdk_api_version}")


if __name__ == "__main__":
    main()
