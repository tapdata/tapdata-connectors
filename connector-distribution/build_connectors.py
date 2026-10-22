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


def run(command, *, cwd):
    print("+", " ".join(str(part) for part in command), flush=True)
    subprocess.run(command, cwd=cwd, check=True)


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
        run(["git", "worktree", "add", "--detach", str(checkout), plan["sourceCommit"]], cwd=source_dir)
        try:
            for item in items:
                pom_path = checkout / item["module"] / "pom.xml"
                if not pom_path.resolve().is_relative_to(checkout.resolve()):
                    raise ValueError("connector POM escapes checkout")
                ET.register_namespace("", "http://maven.apache.org/POM/4.0.0")
                tree = ET.parse(pom_path)
                node = tree.find("{http://maven.apache.org/POM/4.0.0}version")
                if node is None:
                    raise ValueError("connector POM needs a direct project version")
                node.text = item["version"]
                tree.write(pom_path, encoding="UTF-8", xml_declaration=True)
            run(["mvn", "-B", "-pl", ",".join(item["module"] for item in items),
                 "-am", "clean", "package", "-DskipTests", "-DskipITs"], cwd=checkout)
            for item in items:
                target = checkout / item["module"] / "target"
                jars = [p for p in target.glob("*.jar") if not p.name.startswith("original-")
                        and not p.name.endswith(("-sources.jar", "-javadoc.jar"))]
                if len(jars) != 1:
                    raise RuntimeError(f"expected one shaded JAR in {target}, found {len(jars)}")
                destination = output_dir / item["artifactId"]
                destination.mkdir()
                shutil.copyfile(jars[0], destination / "connector.jar")
        finally:
            run(["git", "worktree", "remove", "--force", str(checkout)], cwd=source_dir)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit("Usage: build_connectors.py <plan> <source> <output>")
    build(*sys.argv[1:])
