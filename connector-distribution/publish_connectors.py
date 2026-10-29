#!/usr/bin/env python3
"""Build selected connector JARs, publish immutable releases, and publish a BOM."""

import argparse
import base64
import hashlib
import http.client
import ipaddress
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BATCH_SIZE = 10
ATTEMPTS = 3
VERSION_RE = re.compile(r"^\d+(?:\.\d+)*$")
NS = {"m": "http://maven.apache.org/POM/4.0.0"}


def run(command, *, cwd=None):
    print("+", " ".join(str(part) for part in command), flush=True)
    subprocess.run(command, cwd=cwd or ROOT, check=True)


def repositories():
    required = [f"NEXUS_{name}_{suffix}"
                for name in ("INTERNAL", "EXTERNAL")
                for suffix in ("URL", "USERNAME", "PASSWORD")]
    missing = [key for key in required if not os.environ.get(key, "").strip()]
    if missing:
        raise ValueError(
            "Missing Actions configuration: " + ", ".join(missing) +
            ". Configure *_URL under Settings > Secrets and variables > Actions > Variables "
            "and *_USERNAME/*_PASSWORD under Secrets (or grant repository access to "
            "organization configuration). Publishing jobs do not select an Environment.")
    targets = []
    for name in ("INTERNAL", "EXTERNAL"):
        prefix = f"NEXUS_{name}"
        url = os.environ[f"{prefix}_URL"].rstrip("/") + "/"
        parsed = urllib.parse.urlsplit(url)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.query or parsed.fragment):
            raise ValueError(f"{prefix}_URL must be an HTTPS repository URL without credentials, query or fragment")
        parsed.port  # Validate port before any requests or uploads.
        connect_ip = os.environ.get(f"{prefix}_CONNECT_IP", "").strip()
        if connect_ip:
            connect_ip = str(ipaddress.ip_address(connect_ip))
        username = os.environ[f"{prefix}_USERNAME"]
        password = os.environ[f"{prefix}_PASSWORD"]
        if not username or not password:
            raise ValueError(f"{prefix} credentials must not be empty")
        targets.append({"name": name.lower(), "url": url,
                        "username": username, "password": password,
                        "connect_ip": connect_ip})
    if (targets[0]["url"], targets[0]["connect_ip"]) == (targets[1]["url"], targets[1]["connect_ip"]):
        raise ValueError("internal and external Nexus targets must have distinct URLs or connection IPs")
    return targets


class PinnedHTTPSConnection(http.client.HTTPSConnection):
    """Change only the TCP destination; retain the URL host for TLS and HTTP."""

    def __init__(self, host, *, connect_ip, **kwargs):
        super().__init__(host, **kwargs)
        self._create_connection = lambda address, timeout, source_address=None: socket.create_connection(
            (connect_ip, address[1]), timeout, source_address)


class PinnedHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, connect_ip):
        super().__init__()
        self.connect_ip = connect_ip

    def https_open(self, req):
        def connection(host, **kwargs):
            return PinnedHTTPSConnection(host, connect_ip=self.connect_ip, **kwargs)
        return self.do_open(connection, req, context=self._context)


class RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, "Nexus redirects are not allowed", headers, fp)


def open_request(repo, req):
    # Large connector JARs need more time to send than metadata reads. This is
    # a socket inactivity timeout, not a limit on the whole release operation.
    timeout = 600 if req.get_method() == "PUT" else 45
    if not repo.get("connect_ip"):
        return urllib.request.urlopen(req, timeout=timeout)
    # Never let a proxy or redirect route a pinned request to another installation.
    target = urllib.parse.urlsplit(repo["url"])
    requested = urllib.parse.urlsplit(req.full_url)
    if (requested.scheme, requested.hostname, requested.port or 443) != (
            target.scheme, target.hostname, target.port or 443):
        raise ValueError("Pinned Nexus request must retain the configured HTTPS origin")
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}), PinnedHTTPSHandler(repo["connect_ip"]), RejectRedirects())
    return opener.open(req, timeout=timeout)


def artifact_url(group_id, artifact_id, version, repo, extension="jar"):
    group_path = group_id.replace(".", "/")
    return (f"{repo['url']}{group_path}/{artifact_id}/{version}/"
            f"{artifact_id}-{version}.{extension}")


def request(repo, url, *, data=None, destination=None):
    """Retry transient failures; only a GET 404 denotes an absent file."""
    credentials = base64.b64encode(
        f"{repo['username']}:{repo['password']}".encode()
    ).decode()
    for attempt in range(ATTEMPTS):
        req = urllib.request.Request(url, data=data,
                                     method="GET" if data is None else "PUT",
                                     headers={"Authorization": f"Basic {credentials}",
                                              "Content-Type": "application/octet-stream"})
        try:
            with open_request(repo, req) as response:
                if destination is None:
                    return response.read()
                received = 0
                with destination.open("wb") as output:
                    while chunk := response.read(1024 * 1024):
                        output.write(chunk)
                        received += len(chunk)
                expected = response.headers.get("Content-Length")
                if expected is not None and received != int(expected):
                    raise http.client.IncompleteRead(b"", int(expected) - received)
                return True
        except urllib.error.HTTPError as error:
            if data is None and error.code == 404:
                return None
            if error.code not in (408, 429, 500, 502, 503, 504) or attempt == ATTEMPTS - 1:
                raise RuntimeError(f"Nexus {repo['name']} HTTP {error.code}: {url}") from error
        except (urllib.error.URLError, http.client.HTTPException, TimeoutError, OSError):
            if attempt == ATTEMPTS - 1:
                raise
        time.sleep(2 ** attempt)


def nexus_read(url, repo):
    return request(repo, url)


def put_immutable(repo, url, content):
    existing = nexus_read(url, repo)
    if existing is not None:
        if existing != content:
            raise RuntimeError(f"Immutable file differs in {repo['name']}: {url}")
        return
    try:
        request(repo, url, data=content)
    except Exception:
        # PUT may have succeeded even if the response was lost.
        if nexus_read(url, repo) != content:
            raise
    if nexus_read(url, repo) != content:
        raise RuntimeError(f"Upload verification failed in {repo['name']}: {url}")


def upload_file(repo, url, path):
    content = path.read_bytes()
    put_immutable(repo, url, content)
    for algorithm in ("sha1", "sha256"):
        checksum = hashlib.new(algorithm, content).hexdigest().encode()
        put_immutable(repo, f"{url}.{algorithm}", checksum)


def pom_dependencies(pom_path):
    tree = ET.parse(pom_path)
    return {node.text for node in tree.findall(".//m:dependencies/m:dependency/m:artifactId", NS)
            if node.text and "${" not in node.text}


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


def bom_url(version, repo):
    return artifact_url("io.tapdata", "connectors-bom", version, repo, "pom")


def ensure_bom_available(version, repo):
    if nexus_read(bom_url(version, repo), repo) is not None:
        raise RuntimeError(
            f"BOM io.tapdata:connectors-bom:{version} already exists in {repo['name']}; "
            "stopping before connector builds or uploads."
        )


def latest_bom(repo):
    metadata_url = f"{repo['url']}io/tapdata/connectors-bom/maven-metadata.xml"
    body = nexus_read(metadata_url, repo)
    if body is None:
        return None
    tree = ET.fromstring(body)
    versions = [node.text for node in tree.iter()
                if node.tag.rsplit("}", 1)[-1] == "version"
                and node.text and VERSION_RE.fullmatch(node.text)]
    if not versions:
        return None
    version = max(versions, key=lambda value: tuple(int(part) for part in value.split(".")))
    pom = nexus_read(bom_url(version, repo), repo)
    if pom is None:
        raise RuntimeError(f"Nexus metadata lists BOM {version}, but its POM is missing")
    return version, ET.fromstring(pom)


def plan_url(base_version, repo):
    return artifact_url("io.tapdata", "connector-release-plan", base_version or "bootstrap",
                        repo, "json")


def release_plan(targets, source_commit):
    latest = [latest_bom(repo) for repo in targets]
    key = lambda entry: tuple(map(int, entry[0].split("."))) if entry else ()
    previous = min(latest, key=key)
    base_version = previous[0] if previous else None
    saved = [nexus_read(plan_url(base_version, repo), repo) for repo in targets]
    bodies = [body for body in saved if body is not None]
    if bodies:
        if any(body != bodies[0] for body in bodies):
            raise RuntimeError("Nexus environments contain different release plans")
        plan = json.loads(bodies[0])
        if plan["baseVersion"] != base_version:
            raise RuntimeError("Release plan baseline does not match Nexus")
        if plan["sourceCommit"] != source_commit:
            # A protected branch may advance between attempts. Finish the pinned
            # release before considering the newer changes on a subsequent run.
            subprocess.run(["git", "merge-base", "--is-ancestor", plan["sourceCommit"], source_commit],
                           cwd=ROOT, check=True)
            print(f"Resuming pinned source {plan['sourceCommit']} before {source_commit}")
        for entry in latest:
            if entry and entry[0] not in (base_version, plan["bomVersion"]):
                raise RuntimeError("Unexpected BOM divergence between Nexus environments")
    else:
        if key(latest[0]) != key(latest[1]):
            raise RuntimeError("Nexus BOM baselines differ without a saved release plan")
        if latest[0] and ET.tostring(latest[0][1]) != ET.tostring(latest[1][1]):
            raise RuntimeError("The same BOM version differs between Nexus environments")
        items, base_commit = discover_releases(previous, source_commit)
        if not items:
            # The final metadata PUT may have succeeded while its checksum PUT
            # failed. Repair that tail even when both latest BOMs already advanced.
            for repo in targets:
                repair_metadata_checksums(repo)
            print(f"No connector source changes since {base_commit}; no BOM release is needed.")
            return None
        version = next_bom_version(base_version, current_pdk_api_version())
        for repo in targets:
            ensure_bom_available(version, repo)
        plan = {"sourceCommit": source_commit, "baseVersion": base_version,
                "bomVersion": version, "items": items}
    # Persist on BOTH targets before any build or connector upload. The plan is
    # immutable and keyed by the previous BOM, so a new source cannot silently
    # reuse coordinates left behind by an interrupted release.
    content = json.dumps(plan, sort_keys=True, indent=2).encode()
    for repo in targets:
        put_immutable(repo, plan_url(base_version, repo), content)
    return plan


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
            if not api_version:
                raise ValueError(f"previous BOM is missing PDK API version for {artifact_id}")
            next_connector_version(version, api_version, api_version)
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
    if previous and not base_commit:
        raise ValueError("previous BOM is missing tapdata.source.commit")
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
                "pdkApiVersion": old["pdkApiVersion"],
                "publish": False,
            })
    return items, base_commit


def module_metadata(item):
    module = Path(item["module"])
    if module.is_absolute() or ".." in module.parts or module.parts[:1] != ("connectors",):
        raise ValueError(f"module must be a connector path under connectors/: {module}")
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*", item["artifactId"]):
        raise ValueError("invalid connector artifactId")
    pom_path = ROOT / module / "pom.xml"
    if not pom_path.resolve().is_relative_to(ROOT.resolve()):
        raise ValueError("connector POM escapes source checkout")
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
    if item["publish"] and pdk_api_version != item["pdkApiVersion"]:
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
        artifact_id = tree.find("m:artifactId", ns)
        if artifact_id is None:
            raise ValueError(f"connector POM needs a direct artifactId: {module}/pom.xml")
        version_node = ET.Element(f"{{{ns['m']}}}version")
        tree.getroot().insert(list(tree.getroot()).index(artifact_id) + 1, version_node)
    version_node.text = version
    tree.write(pom_path, encoding="UTF-8", xml_declaration=True)


def sync_connector(item, targets, temp, *, jar=None):
    """Use an existing remote JAR as canonical; repair missing files on either side."""
    existing = []
    for repo in targets:
        path = temp / f"{item['artifactId']}-{repo['name']}.jar"
        url = artifact_url(item["groupId"], item["artifactId"], item["version"], repo)
        if request(repo, url, destination=path) is not None:
            existing.append(path)
    if existing:
        canonical = existing[0]
        digest = hashlib.sha256(canonical.read_bytes()).hexdigest()
        if any(hashlib.sha256(path.read_bytes()).hexdigest() != digest for path in existing[1:]):
            raise RuntimeError(f"Immutable JAR differs between Nexus environments: {item['artifactId']}")
        if jar and hashlib.sha256(jar.read_bytes()).hexdigest() != digest:
            print(f"::warning::SHA-256 mismatch for {item['artifactId']}:{item['version']}; "
                  "reusing Nexus bytes in both environments.")
    elif jar:
        canonical = jar
        digest = hashlib.sha256(jar.read_bytes()).hexdigest()
    else:
        return None
    pom = temp / f"{item['artifactId']}.pom"
    write_artifact_pom(pom, item)
    for repo in targets:
        upload_file(repo, artifact_url(item["groupId"], item["artifactId"], item["version"], repo), canonical)
        upload_file(repo, artifact_url(item["groupId"], item["artifactId"], item["version"], repo, "pom"), pom)
    for path in existing:
        path.unlink()
    return {**{key: item[key] for key in ("groupId", "artifactId", "version", "pdkApiVersion")},
            "sha256": digest}


def repair_metadata_checksums(repo):
    url = f"{repo['url']}io/tapdata/connectors-bom/maven-metadata.xml"
    content = nexus_read(url, repo)
    if content is None:
        return
    for algorithm in ("sha1", "sha256"):
        checksum = hashlib.new(algorithm, content).hexdigest().encode()
        existing = nexus_read(f"{url}.{algorithm}", repo)
        if existing is None or existing.strip().lower() != checksum:
            request(repo, f"{url}.{algorithm}", data=checksum)
        actual = nexus_read(f"{url}.{algorithm}", repo)
        if actual is None or actual.strip().lower() != checksum:
            raise RuntimeError(f"BOM metadata checksum verification failed in {repo['name']}")


def update_bom_metadata(repo, version):
    url = f"{repo['url']}io/tapdata/connectors-bom/maven-metadata.xml"
    body = nexus_read(url, repo)
    metadata = ET.fromstring(body) if body else ET.Element("metadata")
    if body is None:
        ET.SubElement(metadata, "groupId").text = "io.tapdata"
        ET.SubElement(metadata, "artifactId").text = "connectors-bom"
    versioning = metadata.find("versioning")
    if versioning is None:
        versioning = ET.SubElement(metadata, "versioning")
    versions = versioning.find("versions")
    if versions is None:
        versions = ET.SubElement(versioning, "versions")
    values = {node.text for node in versions.findall("version")}
    if version not in values:
        ET.SubElement(versions, "version").text = version
    newest = max(values | {version}, key=lambda v: tuple(map(int, v.split("."))))
    for tag, value in (("latest", newest), ("release", newest),
                       ("lastUpdated", datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"))):
        node = versioning.find(tag)
        if node is None:
            node = ET.SubElement(versioning, tag)
        node.text = value
    content = ET.tostring(metadata, encoding="UTF-8", xml_declaration=True)
    request(repo, url, data=content)
    repair_metadata_checksums(repo)
    actual = nexus_read(url, repo)
    if actual is None or version not in [n.text for n in ET.fromstring(actual).findall("versioning/versions/version")]:
        raise RuntimeError(f"BOM metadata update failed in {repo['name']}")


def publish_plan(plan, targets, temp, artifacts_path, scripts_dir, *, batch_index=None, finalize=False):
    items = plan["items"]
    seen = set()
    for item in items:
        module, _ = module_metadata(item)
        coordinate = (item["groupId"], item["artifactId"])
        if coordinate in seen or item["groupId"] != "io.tapdata":
            raise ValueError(f"invalid or duplicate coordinate: {coordinate}")
        seen.add(coordinate)
        next_connector_version(item["version"], item["pdkApiVersion"], item["pdkApiVersion"])
        if item["publish"]:
            set_build_version(module, item["version"])

    inventory = {"releases": []}
    selected = [item for item in items if item["publish"]]
    if batch_index is not None:
        selected = batch_items(plan, batch_index)
    for start in range(0, len(selected), BATCH_SIZE):
        batch = selected[start:start + BATCH_SIZE]
        print(f"Batch {start // BATCH_SIZE + 1}: {len(batch)} connector(s)", flush=True)
        pending = []
        for item in batch:
            record = sync_connector(item, targets, temp)
            if record is None:
                pending.append(item)
            else:
                inventory["releases"].append(record)
        if pending:
            # Expect pre-built artifacts from the credential-free build phase
            for item in pending:
                # Look for JARs in the artifacts directory instead of ROOT
                artifact_target = artifacts_path / item["artifactId"]
                jars = [p for p in artifact_target.glob("*.jar")
                        if not p.name.startswith("original-")
                        and not p.name.endswith(("-sources.jar", "-javadoc.jar"))]
                if finalize:
                    raise RuntimeError(f"release missing before BOM publication: {item['artifactId']}")
                if len(jars) != 1:
                    raise RuntimeError(f"expected one shaded JAR in {artifact_target}, found {len(jars)}")
                inventory["releases"].append(sync_connector(item, targets, temp, jar=jars[0]))
    if batch_index is not None:
        return
    for item in items:
        if not item["publish"]:
            record = sync_connector(item, targets, temp)
            if record is None:
                raise RuntimeError(f"reused release missing from both Nexus environments: {item['artifactId']}")
            inventory["releases"].append(record)

    inventory_path = temp / "nexus-connector-releases.json"
    inventory_path.write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    bom = temp / "connectors-bom.pom"
    # Execute generate_bom.py from trusted scripts directory to prevent credential leakage
    trusted_bom_script = Path(scripts_dir).resolve() / "connector-distribution" / "generate_bom.py"
    run([sys.executable, "-I", str(trusted_bom_script), "--inventory", str(inventory_path),
         "--bom-version", plan["bomVersion"], "--source-commit", plan["sourceCommit"],
         "--output", str(bom)])
    # Both environments have every JAR and connector POM before publishing BOMs.
    for repo in targets:
        upload_file(repo, bom_url(plan["bomVersion"], repo), bom)
    for repo in targets:
        update_bom_metadata(repo, plan["bomVersion"])
    print(f"Published BOM {plan['bomVersion']} to both Nexus environments.")


def batch_items(plan, index):
    selected = [item for item in plan["items"] if item["publish"]]
    if index < 0 or index >= max(1, (len(selected) + BATCH_SIZE - 1) // BATCH_SIZE):
        raise ValueError("invalid batch index")
    return selected[index * BATCH_SIZE:(index + 1) * BATCH_SIZE]


def verify_saved_plan(plan, targets):
    for repo in targets:
        body = nexus_read(plan_url(plan["baseVersion"], repo), repo)
        if body is None or json.loads(body) != plan:
            raise RuntimeError("local release plan differs from immutable Nexus plan")


def main(source_dir=None, plan_path="release_plan.json"):
    global ROOT
    if source_dir is not None:
        ROOT = Path(source_dir).resolve()
    targets = repositories()
    source_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    plan = release_plan(targets, source_commit)
    plan_file = Path(plan_path)
    if plan is None:
        plan_file.unlink(missing_ok=True)
        return None
    plan_file.write_text(json.dumps(plan, indent=2), encoding="utf-8")
    print(f"Release plan written to {plan_file}")
    return plan


def prepare_batch(plan_path, batch_index, output):
    plan = json.loads(Path(plan_path).read_text())
    targets = repositories()
    verify_saved_plan(plan, targets)
    pending = []
    for item in batch_items(plan, batch_index):
        present = False
        for repo in targets:
            url = artifact_url(item["groupId"], item["artifactId"], item["version"], repo)
            if nexus_read(url, repo) is not None:
                present = True
        if not present:
            pending.append(item)
    Path(output).write_text(json.dumps({**plan, "items": pending}, indent=2))


def upload_artifacts(source_dir=".", artifacts_dir="build-output", scripts_dir=None,
                     *, plan_path="release_plan.json", batch_index=None, finalize=False):
    """Upload in a fresh runner; only trusted scripts execute with credentials."""
    global ROOT
    ROOT = Path(source_dir).resolve()
    artifacts_path = Path(artifacts_dir).resolve()
    scripts_dir = Path(scripts_dir).resolve() if scripts_dir else Path(__file__).resolve().parents[1]
    targets = repositories()
    plan = json.loads(Path(plan_path).read_text(encoding="utf-8"))
    verify_saved_plan(plan, targets)
    with tempfile.TemporaryDirectory(prefix="connector-release-") as temp_dir:
        temp = Path(temp_dir)
        original_root = ROOT
        checkout = temp / "source"
        run(["git", "worktree", "add", "--detach", str(checkout), plan["sourceCommit"]], cwd=ROOT)
        ROOT = checkout
        try:
            publish_plan(plan, targets, temp, artifacts_path, scripts_dir,
                         batch_index=batch_index, finalize=finalize)
        finally:
            ROOT = original_root
            run(["git", "worktree", "remove", "--force", str(checkout)], cwd=ROOT)


def cli():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["validate-config", "plan", "prepare", "upload", "finalize"])
    parser.add_argument("--source", default="source")
    parser.add_argument("--plan", default="release_plan.json")
    parser.add_argument("--artifacts", default="build-output")
    parser.add_argument("--batch", type=int)
    parser.add_argument("--output", default="build-request.json")
    args = parser.parse_args()
    if args.mode == "validate-config":
        try:
            repositories()
        except ValueError as error:
            parser.exit(1, str(error) + "\n")
        print("Nexus configuration is complete and valid; connectivity is not checked.")
    elif args.mode == "plan":
        plan = main(args.source, args.plan)
        count = sum(item["publish"] for item in plan["items"]) if plan else 0
        batches = list(range(max(1, (count + BATCH_SIZE - 1) // BATCH_SIZE))) if plan else []
        if os.environ.get("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a") as output:
                output.write("batches=" + json.dumps(batches) + "\n")
    elif args.mode == "prepare":
        prepare_batch(args.plan, args.batch, args.output)
    else:
        upload_artifacts(args.source, args.artifacts, plan_path=args.plan,
                         batch_index=args.batch, finalize=args.mode == "finalize")


if __name__ == "__main__":
    cli()
