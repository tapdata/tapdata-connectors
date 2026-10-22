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
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RELEASE_REPO = "https://nexus.tapdata.net/repository/maven-releases/"
REPOSITORY_ID = "tapdata-releases"
VERSION_RE = re.compile(r"^\d+(?:\.\d+)*$")
NS = {"m": "http://maven.apache.org/POM/4.0.0"}


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


def nexus_read(url):
    import base64
    credentials = base64.b64encode(
        f"{os.environ['NEXUS_USERNAME']}:{os.environ['NEXUS_PASSWORD']}".encode()
    ).decode()
    request = urllib.request.Request(url, headers={"Authorization": f"Basic {credentials}"})
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise RuntimeError(f"Nexus metadata read failed with HTTP {error.code}: {url}") from error


def pom_dependencies(pom_path):
    tree = ET.parse(pom_path)
    return {node.text for node in tree.findall(".//m:dependencies/m:dependency/m:artifactId", NS)
            if node.text and "${" not in node.text}


def pom_pdk_api_version(pom_contents, inherited_api=None):
    tree = ET.fromstring(pom_contents)
    return (tree.findtext("m:properties/m:tapdata.pdk.api.version", namespaces=NS)
            or inherited_api)


def source_file(commit, path):
    result = subprocess.run(
        ["git", "show", f"{commit}:{path.as_posix()}"],
        cwd=ROOT, check=False, text=True, capture_output=True,
    )
    return result.stdout if result.returncode == 0 else None


def connector_modules():
    connectors_pom = ET.parse(ROOT / "connectors/pom.xml")
    modules = []
    for node in connectors_pom.findall("m:modules/m:module", NS):
        if not node.text:
            continue
        module = Path("connectors") / node.text.strip()
        pom = ROOT / module / "pom.xml"
        if pom.is_file():
            modules.append((module, pom))
    if not modules:
        raise RuntimeError("no connector modules found in connectors/pom.xml")
    return modules


def latest_bom():
    metadata_url = f"{RELEASE_REPO}io/tapdata/connectors-bom/maven-metadata.xml"
    body = nexus_read(metadata_url)
    if body is None:
        return None
    tree = ET.fromstring(body)
    versions = [node.text for node in tree.iter()
                if node.tag.rsplit("}", 1)[-1] == "version"
                and node.text and VERSION_RE.fullmatch(node.text)]
    if not versions:
        return None
    version = max(versions, key=lambda value: tuple(int(part) for part in value.split(".")))
    pom_url = f"{RELEASE_REPO}io/tapdata/connectors-bom/{version}/connectors-bom-{version}.pom"
    pom = nexus_read(pom_url)
    if pom is None:
        raise RuntimeError(f"Nexus metadata lists BOM {version}, but its POM is missing")
    return version, ET.fromstring(pom)


def current_pdk_api_version():
    tree = ET.parse(ROOT / "connectors/pom.xml")
    value = tree.findtext("m:properties/m:tapdata.pdk.api.version", namespaces=NS)
    if not value:
        raise RuntimeError("could not read PDK API version from connectors/pom.xml")
    return value


def connector_pdk_api_version(pom):
    """Return the connector's declared PDK API version or inherited default."""
    tree = ET.parse(pom)
    value = tree.findtext("m:properties/m:tapdata.pdk.api.version", namespaces=NS)
    return value or current_pdk_api_version()


def api_generation(pdk_api_version):
    """Map 2.11 and 2.0.11[-SNAPSHOT] to connector API generation 11."""
    stable = pdk_api_version.split("-", 1)[0]
    match = re.fullmatch(r"2\.(?:0\.)?(\d+)", stable)
    if not match:
        raise ValueError(
            f"unsupported PDK API version {pdk_api_version}; expected 2.<n> or 2.0.<n>"
        )
    return int(match.group(1))


def changed_connectors(base_commit, current_commit):
    result = subprocess.run(
        ["git", "diff", "--name-only", base_commit, current_commit],
        cwd=ROOT, check=True, text=True, capture_output=True,
    )
    changed_paths = [Path(line) for line in result.stdout.splitlines() if line]
    if Path("connectors/pom.xml") in changed_paths:
        previous_parent = subprocess.check_output(
            ["git", "show", f"{base_commit}:connectors/pom.xml"],
            cwd=ROOT, text=True,
        )
        current_parent = (ROOT / "connectors/pom.xml").read_text(encoding="utf-8")
        api_property = re.compile(
            r"<tapdata\.pdk\.api\.version>.*?</tapdata\.pdk\.api\.version>", re.DOTALL
        )
        if api_property.sub("<tapdata.pdk.api.version>API_VERSION</tapdata.pdk.api.version>",
                            previous_parent) == api_property.sub(
                                "<tapdata.pdk.api.version>API_VERSION</tapdata.pdk.api.version>",
                                current_parent):
            changed_paths.remove(Path("connectors/pom.xml"))
    if not changed_paths:
        return set()

    modules = connector_modules()
    connector_by_artifact = {
        ET.parse(pom).findtext("m:artifactId", namespaces=NS): (module, pom)
        for module, pom in modules
    }
    all_poms = [pom for pom in ROOT.rglob("pom.xml")
                if not {".git", "target"}.intersection(pom.parts)]
    projects = {}
    for pom in all_poms:
        try:
            artifact = ET.parse(pom).findtext("m:artifactId", namespaces=NS)
        except ET.ParseError:
            continue
        if artifact:
            projects[artifact] = (pom.parent.relative_to(ROOT), pom)

    changed_artifacts = set()
    global_change = False
    for path in changed_paths:
        if path.parts[:1] == ("connectors",) and len(path.parts) > 2:
            module_name = path.parts[1]
            for artifact, (module, _) in connector_by_artifact.items():
                if module.parts[1] == module_name:
                    changed_artifacts.add(artifact)
        candidates = [(directory, artifact) for artifact, (directory, _) in projects.items()
                      if directory.parts and path.parts[:len(directory.parts)] == directory.parts]
        if candidates:
            directory, artifact = max(candidates, key=lambda pair: len(pair[0].parts))
            if artifact not in connector_by_artifact:
                changed_artifacts.add(artifact)
            if path.name == "pom.xml":
                # Maven child projects inherit parent POM properties and
                # dependency management even when they do not declare a
                # dependency on the parent artifact.
                changed_artifacts.update(
                    child_artifact for child_artifact, (child_directory, _) in projects.items()
                    if child_directory.parts[:len(directory.parts)] == directory.parts
                )
        elif path.name in ("pom.xml", "build.gradle", "settings.gradle"):
            global_change = True

    connectors_parent_changed = any(path.parts == ("connectors", "pom.xml") for path in changed_paths)
    if global_change or connectors_parent_changed:
        return set(connector_by_artifact)

    # Propagate changes through Maven project dependencies until the affected
    # connector set stops growing.
    dependency_map = {artifact: pom_dependencies(pom) for artifact, (_, pom) in projects.items()}
    impacted_artifacts = set(changed_artifacts)
    while True:
        newly_impacted = {artifact for artifact, dependencies in dependency_map.items()
                          if dependencies & impacted_artifacts}
        if newly_impacted <= impacted_artifacts:
            break
        impacted_artifacts.update(newly_impacted)
    return {artifact for artifact in connector_by_artifact if artifact in impacted_artifacts}


def next_connector_version(previous, old_api, new_api):
    generation = api_generation(new_api)
    if previous is None:
        return f"2.{generation}.0"

    if not VERSION_RE.fullmatch(previous):
        raise ValueError(f"invalid previous connector version: {previous}")
    parts = [int(part) for part in previous.split(".")]
    if len(parts) != 3:
        raise ValueError(f"previous connector version must have three numeric parts: {previous}")

    old_generation = api_generation(old_api) if old_api else None
    if old_generation is None:
        raise ValueError(f"cannot validate previous connector version {previous} without its PDK API version")
    if generation < old_generation:
        raise ValueError(
            f"PDK API generation for connector moved backwards: {old_generation} -> {generation}"
        )
    if parts[0] == 1:
        if parts[1] != old_generation:
            raise ValueError(
                f"previous connector version {previous} does not match PDK API generation {old_generation}"
            )
        # Migrate connector versions created by the previous 1.x policy.
        return f"2.{generation}.0"
    if parts[0] != 2 or parts[1] != old_generation:
        raise ValueError(
            f"previous connector version {previous} does not match PDK API generation {old_generation}"
        )
    if old_generation == generation:
        parts[2] += 1
        return ".".join(map(str, parts))
    return f"2.{generation}.0"


def next_patch(version):
    parts = [int(part) for part in version.split(".")]
    if len(parts) < 3:
        parts.append(1)
    else:
        parts[-1] += 1
    return ".".join(map(str, parts))


def next_bom_version(previous_version, pdk_api_version):
    if previous_version:
        return next_patch(previous_version)
    pdk_base = pdk_api_version.split("-", 1)[0]
    if not VERSION_RE.fullmatch(pdk_base):
        raise ValueError(f"cannot derive initial BOM version from PDK API version {pdk_api_version}")
    return f"{pdk_base}.{datetime.now(timezone.utc):%Y%m%d}.1"


def bom_releases(pom):
    records = []
    properties = pom.find("m:properties", NS)
    for dependency in pom.findall("m:dependencyManagement/m:dependencies/m:dependency", NS):
        group_id = dependency.findtext("m:groupId", namespaces=NS)
        artifact_id = dependency.findtext("m:artifactId", namespaces=NS)
        version = dependency.findtext("m:version", namespaces=NS)
        if group_id and artifact_id and version:
            if not VERSION_RE.fullmatch(version):
                raise ValueError(
                    f"invalid connector version in previous BOM for {artifact_id}: {version}"
                )
            api_version = None
            if properties is not None:
                api_version = properties.findtext(
                    f"m:tapdata.connector.{artifact_id}.pdk.api.version", namespaces=NS
                )
            records.append({"groupId": group_id, "artifactId": artifact_id,
                            "version": version, "pdkApiVersion": api_version})
    return records


def discover_releases(previous, source_commit):
    all_connectors = connector_modules()
    previous_by_artifact = {}
    base_commit = None
    if previous:
        _, previous_pom = previous
        properties = previous_pom.find("m:properties", NS)
        if properties is not None:
            base_commit = properties.findtext("m:tapdata.source.commit", namespaces=NS)
        previous_by_artifact = {item["artifactId"]: item for item in bom_releases(previous_pom)}

    current_api_by_artifact = {
        ET.parse(pom).findtext("m:artifactId", namespaces=NS): connector_pdk_api_version(pom)
        for _, pom in all_connectors
    }
    if base_commit:
        module_by_artifact = {
            ET.parse(pom).findtext("m:artifactId", namespaces=NS): module
            for module, pom in all_connectors
        }
        parent_contents = source_file(base_commit, Path("connectors/pom.xml"))
        parent_api = pom_pdk_api_version(parent_contents) if parent_contents else None
        for artifact, previous_release in previous_by_artifact.items():
            if previous_release.get("pdkApiVersion"):
                continue
            module = module_by_artifact.get(artifact)
            if module is None:
                continue
            pom_contents = source_file(base_commit, module / "pom.xml") if module else None
            recovered_api = pom_pdk_api_version(pom_contents, parent_api) if pom_contents else None
            if not recovered_api:
                raise RuntimeError(
                    f"cannot recover the PDK API version for legacy BOM connector {artifact} "
                    f"from source commit {base_commit}"
                )
            previous_release["pdkApiVersion"] = recovered_api
    current_artifacts = set(current_api_by_artifact)
    if not previous_by_artifact or not base_commit:
        selected = current_artifacts
    else:
        selected = changed_connectors(base_commit, source_commit)
        selected |= current_artifacts - set(previous_by_artifact)
        # A connector's API generation is independent of other connectors'.
        selected |= {
            artifact for artifact, api in current_api_by_artifact.items()
            if artifact in previous_by_artifact
            and previous_by_artifact[artifact].get("pdkApiVersion")
            and api_generation(api) != api_generation(previous_by_artifact[artifact]["pdkApiVersion"])
        }
        # Move every connector in an existing 1.x BOM to the aligned 2.x scheme.
        selected |= {
            artifact for artifact, release in previous_by_artifact.items()
            if artifact in current_artifacts
            and VERSION_RE.fullmatch(release["version"])
            and int(release["version"].split(".", 1)[0]) == 1
        }

    if not selected:
        return [], base_commit

    items = []
    module_by_artifact = {}
    for module, pom in all_connectors:
        tree = ET.parse(pom)
        artifact_id = tree.findtext("m:artifactId", namespaces=NS)
        module_by_artifact[artifact_id] = module.as_posix()
        if artifact_id not in selected:
            continue
        old = previous_by_artifact.get(artifact_id)
        api_version = current_api_by_artifact[artifact_id]
        version = next_connector_version(
            old["version"] if old else None,
            old.get("pdkApiVersion") if old else None,
            api_version,
        )
        items.append({
            "module": module.as_posix(),
            "groupId": "io.tapdata",
            "artifactId": artifact_id,
            "version": version,
            "pdkApiVersion": api_version,
            "publish": True,
        })
    for artifact_id, old in previous_by_artifact.items():
        if artifact_id not in selected:
            if artifact_id not in module_by_artifact:
                continue
            items.append({
                "module": module_by_artifact[artifact_id],
                "groupId": old["groupId"],
                "artifactId": artifact_id,
                "version": old["version"],
                "pdkApiVersion": old.get("pdkApiVersion") or current_api_by_artifact[artifact_id],
                "publish": False,
            })
    return items, base_commit


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
    source_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    previous = latest_bom()
    items, base_commit = discover_releases(previous, source_commit)
    if not items:
        print(f"No connector source changes since {base_commit}; no BOM release is needed.")
        return

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

    bom_version = next_bom_version(previous[0] if previous else None, current_pdk_api_version())
    print(f"Detected {sum(item['publish'] for item in items)} connector(s) to publish; "
          f"BOM version: {bom_version}")

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
            "--bom-version", bom_version,
            "--source-commit", source_commit,
            "--output", str(bom_path),
        ])
        deploy(bom_path, bom_path, "pom")


if __name__ == "__main__":
    main()
