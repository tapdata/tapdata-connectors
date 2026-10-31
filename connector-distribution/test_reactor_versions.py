"""Regression coverage for release versions on interdependent connectors."""
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from build_connectors import rewrite_reactor_versions, create_build_checkout, NS


class BuildCheckoutTests(unittest.TestCase):
    def test_standalone_checkout_keeps_pinned_commit_and_source_unmodified(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            source = root / 'source'
            source.mkdir()

            def git(*args, cwd=source):
                return subprocess.check_output(['git', *args], cwd=cwd, text=True,
                                               stderr=subprocess.DEVNULL).strip()

            git('init', '-q')
            git('config', 'user.email', 'test@example.com')
            git('config', 'user.name', 'Test')
            (source / 'data').write_text('pinned')
            git('add', '.')
            git('commit', '-qm', 'pinned source')
            pinned = git('rev-parse', 'HEAD')
            (source / 'data').write_text('newer')
            git('commit', '-qam', 'advance branch')
            head = git('rev-parse', 'HEAD')
            git('gc', '--prune=now')
            (source / 'data').write_text('local edits')
            checkout = root / 'build'
            create_build_checkout(source, checkout, pinned)
            self.assertTrue((checkout / '.git').is_dir())
            self.assertFalse((checkout / '.git/commondir').exists())
            # Packed objects must be copies, not hardlinks that require both
            # paths to reside on the same filesystem (workspace vs /tmp in CI).
            packs = list((source / '.git/objects/pack').glob('*.pack'))
            self.assertTrue(packs)
            for original in packs:
                copied = checkout / '.git/objects/pack' / original.name
                self.assertEqual(copied.read_bytes(), original.read_bytes())
                self.assertNotEqual((copied.stat().st_dev, copied.stat().st_ino),
                                    (original.stat().st_dev, original.stat().st_ino))
            self.assertFalse((checkout / '.git/objects/info/alternates').exists())
            self.assertEqual(git('rev-parse', 'HEAD', cwd=checkout), pinned)
            self.assertEqual((checkout / 'data').read_text(), 'pinned')
            self.assertEqual(git('rev-parse', 'HEAD'), head)
            self.assertEqual((source / 'data').read_text(), 'local edits')
            self.assertEqual(len(git('worktree', 'list', '--porcelain').split('worktree ')) - 1, 1)


class ReactorVersionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        self.items = []

    def module(self, artifact, dependencies="", *, selected=True, version="2.11.0"):
        relative = pathlib.Path("connectors") / artifact
        path = self.root / relative / "pom.xml"
        path.parent.mkdir(parents=True)
        path.write_text(f'''<project xmlns="{NS['m']}"><modelVersion>4.0.0</modelVersion>
<groupId>io.tapdata</groupId><artifactId>{artifact}</artifactId>
<version>1.0-SNAPSHOT</version><dependencies>{dependencies}</dependencies></project>''')
        if selected:
            self.items.append(dict(module=str(relative), groupId="io.tapdata",
                                   artifactId=artifact, version=version))
        return path

    def dependency(self, artifact="mongodb-connector", version="1.0-SNAPSHOT", group="io.tapdata"):
        return (f"<dependency><groupId>{group}</groupId><artifactId>{artifact}</artifactId>"
                f"<version>{version}</version></dependency>")

    def versions(self, path):
        return [d.findtext("m:version", namespaces=NS)
                for d in ET.parse(path).findall(".//m:dependency", NS)]

    def test_aligns_same_batch_dependency_with_dependency_release_version(self):
        mongo = self.module("mongodb-connector", version="2.11.1")
        atlas = self.module("mongodb-atlas-connector", self.dependency())
        rewrite_reactor_versions(self.root, self.items)
        self.assertEqual("2.11.1", ET.parse(mongo).findtext("m:version", namespaces=NS))
        self.assertEqual(["2.11.1"], self.versions(atlas))
        self.assertEqual("2.11.0", ET.parse(atlas).findtext("m:version", namespaces=NS))

    def test_updates_unselected_consumer_without_changing_its_project_version(self):
        self.module("mongodb-connector")
        consumer = self.module("mongodb-lower-connector", self.dependency(), selected=False)
        rewrite_reactor_versions(self.root, self.items)
        self.assertEqual(["2.11.0"], self.versions(consumer))
        self.assertEqual("1.0-SNAPSHOT", ET.parse(consumer).findtext("m:version", namespaces=NS))

    def test_preserves_pinned_release_and_other_group(self):
        self.module("mongodb-connector")
        consumer = self.module("atlas", self.dependency(version="1.5.0") +
                               self.dependency(group="com.example"))
        rewrite_reactor_versions(self.root, self.items)
        self.assertEqual(["1.5.0", "1.0-SNAPSHOT"], self.versions(consumer))

    def test_preserves_original_dependency_when_base_is_outside_batch(self):
        self.module("mongodb-connector", selected=False)
        consumer = self.module("atlas", self.dependency())
        rewrite_reactor_versions(self.root, self.items)
        self.assertEqual(["1.0-SNAPSHOT"], self.versions(consumer))

    @unittest.skipUnless(shutil.which("mvn"), "Maven required for reactor integration")
    def test_maven_selects_released_base_with_also_make(self):
        self.module("mongodb-connector")
        self.module("mongodb-atlas-connector", self.dependency())
        (self.root / "pom.xml").write_text(f'''<project xmlns="{NS['m']}">
<modelVersion>4.0.0</modelVersion><groupId>io.tapdata</groupId>
<artifactId>reactor</artifactId><version>1.0</version><packaging>pom</packaging>
<modules><module>connectors/mongodb-connector</module>
<module>connectors/mongodb-atlas-connector</module></modules></project>''')
        command = ["mvn", "-B", "-o", "-pl", "connectors/mongodb-atlas-connector", "-am", "validate"]
        originals = {p: p.read_bytes() for p in self.root.rglob("pom.xml")}
        # Reproduce the previous builder: project versions change, references do not.
        for item in self.items:
            pom = self.root / item["module"] / "pom.xml"
            tree = ET.parse(pom)
            tree.find("m:version", NS).text = item["version"]
            tree.write(pom)
        broken = subprocess.run(command, cwd=self.root, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=60)
        self.assertEqual(0, broken.returncode, broken.stdout)
        self.assertNotIn("Building mongodb-connector 2.11.0", broken.stdout)
        for pom, content in originals.items():
            pom.write_bytes(content)
        rewrite_reactor_versions(self.root, self.items)
        result = subprocess.run(command, cwd=self.root, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=60)
        self.assertEqual(0, result.returncode, result.stdout)
        self.assertIn("Building mongodb-connector 2.11.0", result.stdout)
        self.assertIn("Building mongodb-atlas-connector 2.11.0", result.stdout)


if __name__ == "__main__":
    unittest.main()
