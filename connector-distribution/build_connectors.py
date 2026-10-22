#!/usr/bin/env python3
"""Build connector JARs without credentials in the environment."""

import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(command, *, cwd=None):
    print("+", " ".join(str(part) for part in command), flush=True)
    subprocess.run(command, cwd=cwd or ROOT, check=True)


def set_build_version(module, version):
    """Update connector POM to the release version before building."""
    import xml.etree.ElementTree as ET
    pom_path = ROOT / module / "pom.xml"
    ET.register_namespace("", "http://maven.apache.org/POM/4.0.0")
    tree = ET.parse(pom_path)
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    version_node = tree.find("m:version", ns)
    if version_node is None:
        raise ValueError(f"connector POM needs a direct project version: {module}/pom.xml")
    version_node.text = version
    tree.write(pom_path, encoding="UTF-8", xml_declaration=True)


def main():
    if len(sys.argv) < 3:
        print("Usage: build_connectors.py <release_plan.json> <source_dir> <output_dir>", file=sys.stderr)
        sys.exit(1)

    plan_path = Path(sys.argv[1])
    source_dir = Path(sys.argv[2]).resolve()
    output_dir = Path(sys.argv[3]).resolve()

    plan = json.loads(plan_path.read_text(encoding="utf-8"))

    modules_to_build = [item for item in plan["items"] if item["publish"]]

    if not modules_to_build:
        print("No connectors require building.")
        return

    # Create isolated checkout to avoid affecting the working directory
    with tempfile.TemporaryDirectory(prefix="connector-build-") as temp_dir:
        temp = Path(temp_dir)
        checkout = temp / "source"
        run(["git", "worktree", "add", "--detach", str(checkout), plan["sourceCommit"]], cwd=source_dir)

        try:
            # Set versions in the isolated checkout
            for item in modules_to_build:
                module = checkout / item["module"]
                import xml.etree.ElementTree as ET
                pom_path = module / "pom.xml"
                ET.register_namespace("", "http://maven.apache.org/POM/4.0.0")
                tree = ET.parse(pom_path)
                ns = {"m": "http://maven.apache.org/POM/4.0.0"}
                version_node = tree.find("m:version", ns)
                if version_node is None:
                    raise ValueError(f"connector POM needs a direct project version: {item['module']}/pom.xml")
                version_node.text = item["version"]
                tree.write(pom_path, encoding="UTF-8", xml_declaration=True)

            # Build all connectors
            print(f"Building {len(modules_to_build)} connector(s) without credentials...", flush=True)
            run(["mvn", "-B", "-pl", ",".join(item["module"] for item in modules_to_build),
                 "-am", "clean", "package", "-DskipTests", "-DskipITs"], cwd=checkout)

            # Copy built JARs to the output directory
            for item in modules_to_build:
                source_target = checkout / item["module"] / "target"
                dest_target = output_dir / item["module"] / "target"
                dest_target.mkdir(parents=True, exist_ok=True)

                for jar in source_target.glob("*.jar"):
                    if not jar.name.startswith("original-"):
                        print(f"Copying {jar.name} to {dest_target}")
                        import shutil
                        shutil.copy2(jar, dest_target / jar.name)

            print("Build complete.")
        finally:
            run(["git", "worktree", "remove", "--force", str(checkout)], cwd=source_dir)


if __name__ == "__main__":
    main()
