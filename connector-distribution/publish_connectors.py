#!/usr/bin/env python3
"""Build selected connector JARs, publish immutable releases, and publish a BOM."""

import hashlib
import json
import os
import re
import subprocess
import tempfile
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RELEASE_REPO = "https://nexus.tapdata.net/repository/maven-releases/"
REPOSITORY_ID = "tapdata-releases"
VERSION_RE = re.compile(r"^\d+(?:\.\d+)*$")


def run(command, *, cwd=ROOT):
    print("+", " ".join(str(part) for part in command), flush=True)
    subprocess.run(command, cwd=cwd, check=True)


def artifact_url(group_id, artifact_id, version):
    group_path = group_id.replace(".", "/")
    return (f"{RELEASE_REPO}{group_path}/{artifact_id}/{version}/"
            f"{artifact_id}-{version}.jar")


def remote_sha256(url):
    username = os.environ["NEXUS_USERNAME"]
    password = os.environ["NEXUS_PASSWORD"]
    request = urllib.request.Request(url)
    import base64
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    request.add_header("Authorization", f"Basic {credentials}")
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            digest = hashlib.sha256()
            while chunk := response.read(1024 * 1024):
                digest.update(chunk)
            return digest.hexdigest()
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise RuntimeError(f"Nexus artifact read failed with HTTP {error.code}: {url}") from error


def module_metadata(item):
    module = Path(item["module"])
    if module.is_absolute() or ".." in module.parts or module.parts[:1] != ("connectors",):
        raise ValueError(f"module must be a connector path under connectors/: {module}")
    pom_path = ROOT / module / "pom.xml"
    if not pom_path.is_file():
        raise ValueError(f"connector POM not found: {module}/pom.xml")
    tree = ET.parse(pom_path)
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    artifact_id = tree.findtext("m:artifactId", namespaces=ns)
    if not artifact_id or artifact_id != item["artifactId"]:
        raise ValueError(f"artifactId does not match {module}/pom.xml: {item['artifactId']}")
    pdk_api_version = tree.findtext("m:properties/m:tapdata.pdk.api.version", namespaces=ns)
    if not pdk_api_version:
        connectors_pom = ET.parse(ROOT / "connectors/pom.xml")
        pdk_api_version = connectors_pom.findtext("m:properties/m:tapdata.pdk.api.version", namespaces=ns)
    if pdk_api_version != item["pdkApiVersion"]:
        raise ValueError(
            f"PDK API version for {item['artifactId']} is {pdk_api_version}, "
            f"not the manifest value {item['pdkApiVersion']}"
        )
    return module, artifact_id


def write_artifact_pom(output, item):
    ns = "http://maven.apache.org/POM/4.0.0"
    ET.register_namespace("", ns)

    def add(parent, name, value):
        node = ET.SubElement(parent, f"{{{ns}}}{name}")
        node.text = value

    project = ET.Element(f"{{{ns}}}project")
    add(project, "modelVersion", "4.0.0")
    add(project, "groupId", item["groupId"])
    add(project, "artifactId", item["artifactId"])
    add(project, "version", item["version"])
    add(project, "packaging", "jar")
    properties = ET.SubElement(project, f"{{{ns}}}properties")
    add(properties, "tapdata.pdk.api.version", item["pdkApiVersion"])
    ET.ElementTree(project).write(output, encoding="UTF-8", xml_declaration=True)


def set_build_version(module, version):
    pom_path = ROOT / module / "pom.xml"
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    tree = ET.parse(pom_path)
    version_node = tree.find("m:version", ns)
    if version_node is None:
        raise ValueError(f"connector POM needs a direct project version: {module}/pom.xml")
    version_node.text = version
    tree.write(pom_path, encoding="UTF-8", xml_declaration=True)


def deploy(file_path, pom_path, packaging):
    run([
        "mvn", "-B", "org.apache.maven.plugins:maven-deploy-plugin:3.1.4:deploy-file",
        f"-Dfile={file_path}", f"-DpomFile={pom_path}", f"-Dpackaging={packaging}",
        f"-DrepositoryId={REPOSITORY_ID}", f"-Durl={RELEASE_REPO}",
    ])


def main():
    items = json.loads(os.environ["CONNECTOR_RELEASES"])
    if not isinstance(items, list) or not items:
        raise ValueError("releases_json must be a non-empty JSON array")

    seen = set()
    modules_to_build = []
    for item in items:
        required = ("module", "groupId", "artifactId", "version", "pdkApiVersion", "publish")
        if not isinstance(item, dict) or any(key not in item for key in required):
            raise ValueError(f"each release needs {', '.join(required)}")
        if any(not isinstance(item[key], str) or not item[key] for key in required[:-1]):
            raise ValueError("module, coordinates, version, and PDK API version must be non-empty strings")
        if item["groupId"] != "io.tapdata":
            raise ValueError("this workflow only publishes io.tapdata connectors")
        if not VERSION_RE.fullmatch(item["version"]):
            raise ValueError(f"connector version must be an immutable numeric release: {item['version']}")
        if not isinstance(item["publish"], bool):
            raise ValueError(f"publish must be true or false for {item['artifactId']}")
        module, _ = module_metadata(item)
        coordinate = (item["groupId"], item["artifactId"])
        if coordinate in seen:
            raise ValueError(f"duplicate connector in release set: {coordinate}")
        seen.add(coordinate)
        if item["publish"]:
            modules_to_build.append(str(module))
            set_build_version(module, item["version"])

    api_versions = {item["pdkApiVersion"] for item in items}
    if len(api_versions) != 1:
        raise ValueError("one BOM can contain only one exact PDK API version")
    pdk_api_version = api_versions.pop()
    if not os.environ.get("BOM_VERSION") or not VERSION_RE.fullmatch(os.environ["BOM_VERSION"]):
        raise ValueError("bom_version must be a new immutable numeric release, e.g. 2.0.11.20261016.1")

    if modules_to_build:
        run(["mvn", "-B", "-pl", ",".join(modules_to_build), "-am", "package",
             "-DskipTests", "-DskipITs"])

    inventory = {"releases": []}
    with tempfile.TemporaryDirectory(prefix="connector-release-") as temp_dir:
        temp = Path(temp_dir)
        for item in items:
            jar_url = artifact_url(item["groupId"], item["artifactId"], item["version"])
            if item["publish"]:
                module = ROOT / item["module"]
                jars = [p for p in (module / "target").glob("*.jar")
                        if not p.name.startswith("original-")
                        and not p.name.endswith(("-sources.jar", "-javadoc.jar"))]
                if len(jars) != 1:
                    raise RuntimeError(f"expected one shaded connector JAR in {module / 'target'}, found {len(jars)}")
                jar = jars[0]
                digest = hashlib.sha256(jar.read_bytes()).hexdigest()
                existing_digest = remote_sha256(jar_url)
                if existing_digest is not None:
                    if existing_digest.lower() != digest.lower():
                        raise RuntimeError(f"immutable coordinate already exists with different bytes: {item['artifactId']}:{item['version']}")
                    print(f"Skip unchanged release: {item['artifactId']}:{item['version']}")
                else:
                    pom = temp / f"{item['artifactId']}-{item['version']}.pom"
                    write_artifact_pom(pom, item)
                    deploy(jar, pom, "jar")
            else:
                digest = remote_sha256(jar_url)
                if digest is None:
                    raise RuntimeError(f"reused release is missing from Nexus: {item['artifactId']}:{item['version']}")
                print(f"Reuse existing release: {item['artifactId']}:{item['version']}")

            record = {key: item[key] for key in ("groupId", "artifactId", "version", "pdkApiVersion")}
            record["sha256"] = digest
            inventory["releases"].append(record)

        inventory_path = temp / "nexus-connector-releases.json"
        inventory_path.write_text(json.dumps(inventory, indent=2), encoding="utf-8")
        bom_path = temp / "connectors-bom.pom"
        run([
            "python3", "connector-distribution/generate_bom.py",
            "--inventory", str(inventory_path),
            "--pdk-api-version", pdk_api_version,
            "--bom-version", os.environ["BOM_VERSION"],
            "--output", str(bom_path),
        ])
        deploy(bom_path, bom_path, "pom")


if __name__ == "__main__":
    main()
