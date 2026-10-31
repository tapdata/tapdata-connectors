#!/usr/bin/env python3
"""Build one batch on a runner without Nexus credentials."""

import json
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

BATCH_SIZE = 10
NS = {"m": "http://maven.apache.org/POM/4.0.0"}


def run(command, *, cwd):
    print("+", " ".join(str(part) for part in command), flush=True)
    subprocess.run(command, cwd=cwd, check=True)


def rewrite_reactor_versions(checkout, items):
    """Keep references to changed reactor GAVs aligned with their new versions."""
    ET.register_namespace("", NS["m"])
    trees = {}
    replacements = {}
    dirty = set()
    # Capture the original coordinates before changing any project version.
    for item in items:
        pom = checkout / item["module"] / "pom.xml"
        if not pom.resolve().is_relative_to(checkout.resolve()):
            raise ValueError("connector POM escapes checkout")
        tree = ET.parse(pom)
        version = tree.find("m:version", NS)
        original_version = version.text if version is not None else tree.findtext("m:parent/m:version", namespaces=NS)
        if not original_version:
            raise ValueError(f"connector POM needs a project or parent version: {pom}")
        if version is None:
            root = tree.getroot()
            artifact_id = root.find("m:artifactId", NS)
            if artifact_id is None:
                raise ValueError(f"connector POM needs a direct artifactId: {pom}")
            version = ET.Element(f"{{{NS['m']}}}version")
            version.text = item["version"]
            root.insert(list(root).index(artifact_id) + 1, version)
        else:
            version.text = item["version"]
        replacements[(item["groupId"], item["artifactId"], original_version)] = item["version"]
        trees[pom] = tree
        dirty.add(pom)
    # Also update consumers outside the batch: -am can add them through other
    # selected modules. Match the original full GAV, preserving pinned releases.
    for pom in checkout.rglob("pom.xml"):
        if not pom.resolve().is_relative_to(checkout.resolve()):
            raise ValueError("reactor POM escapes checkout")
        tree = trees.get(pom)
        if tree is None:
            tree = ET.parse(pom)
        for dependency in tree.findall(".//m:dependency", NS):
            version = dependency.find("m:version", NS)
            if version is None:
                continue
            key = (dependency.findtext("m:groupId", namespaces=NS),
                   dependency.findtext("m:artifactId", namespaces=NS), version.text)
            if key in replacements and version.text != replacements[key]:
                version.text = replacements[key]
                dirty.add(pom)
        trees[pom] = tree
    for pom in sorted(dirty):
        trees[pom].write(pom, encoding="UTF-8", xml_declaration=True)


def create_build_checkout(source_dir, checkout, source_commit):
    # Legacy git-commit-id-plugin/JGit cannot resolve linked worktree commondir.
    # A local clone has a normal .git directory and uses hardlinks where possible;
    # no network fetch or copy of the source working tree is needed.
    run(["git", "clone", "--local", "--no-checkout", str(source_dir), str(checkout)], cwd=source_dir)
    run(["git", "checkout", "--detach", source_commit], cwd=checkout)


def build(plan_path, source_dir, output_dir):
    source_dir = Path(source_dir).resolve()
    output_dir = Path(output_dir).resolve()
    plan = json.loads(Path(plan_path).read_text(encoding="utf-8"))
    items = [item for item in plan["items"] if item["publish"]]
    if len(items) > BATCH_SIZE:
        raise ValueError("a build batch may contain at most 10 connectors")
    # Validate every destination before writing any POM or executing Maven.
    for item in items:
        module = Path(item["module"])
        if module.is_absolute() or ".." in module.parts or module.parts[:1] != ("connectors",):
            raise ValueError(f"invalid connector module: {module}")
        artifact = item["artifactId"]
        if not artifact or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-" for c in artifact) or artifact in (".", ".."):
            raise ValueError("invalid artifactId")
    output_dir.mkdir(parents=True, exist_ok=True)
    # Keep an artifact even when the whole batch was already uploaded.
    (output_dir / "batch.json").write_text(json.dumps({"count": len(items)}))
    if not items:
        return
    with tempfile.TemporaryDirectory(prefix="connector-build-") as temp_dir:
        checkout = Path(temp_dir) / "source"
        create_build_checkout(source_dir, checkout, plan["sourceCommit"])
        rewrite_reactor_versions(checkout, items)
        run(["mvn", "-B", "-pl", ",".join(item["module"] for item in items),
             "-am", "clean", "package", "-DskipTests", "-DskipITs"], cwd=checkout)
        for item in items:
            target = checkout / item["module"] / "target"
            jars = [p for p in target.glob("*.jar") if not p.name.startswith("original-")
                    and not p.name.endswith(("-sources.jar", "-javadoc.jar"))]
            # Assembly-based connectors produce both Maven's thin
            # artifactId-version.jar and the distributable
            # artifactId-vversion-timestamp.jar. Prefer the latter; shade
            # based connectors generally leave just one candidate.
            distribution_prefix = f"{item['artifactId']}-v{item['version']}-"
            distribution_jars = [p for p in jars if p.name.startswith(distribution_prefix)]
            if len(distribution_jars) == 1:
                jars = distribution_jars
            if len(jars) != 1:
                names = ", ".join(sorted(p.name for p in jars)) or "none"
                raise RuntimeError(f"expected one distributable JAR in {target}, found {len(jars)}: {names}")
            destination = output_dir / item["artifactId"]
            destination.mkdir()
            shutil.copyfile(jars[0], destination / "connector.jar")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit("Usage: build_connectors.py <plan> <source> <output>")
    build(*sys.argv[1:])
