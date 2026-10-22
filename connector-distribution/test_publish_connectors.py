"""Exercise batched dual-Nexus publishing and recovery without Maven or real Nexus."""

import contextlib
import hashlib
import io
import json
import shutil
import subprocess
import tempfile
import threading
import unittest
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import publish_connectors as publisher


class Nexus:
    def __init__(self):
        self.files = {}
        self.puts = []
        self.fail_path = None
        self.fail_status = 403
        self.fail_count = 0
        self.commit_before_failure = False
        self.truncate_path = None
        self.truncate_count = 0
        nexus = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                if self.headers.get('Authorization') != 'Basic dGVzdDp0ZXN0':
                    self.send_error(401)
                    return
                if self.path not in nexus.files:
                    self.send_error(404)
                    return
                self.send_response(200)
                content = nexus.files[self.path]
                self.send_header('Content-Length', str(len(content)))
                self.end_headers()
                if self.path == nexus.truncate_path and nexus.truncate_count:
                    nexus.truncate_count -= 1
                    self.wfile.write(content[:len(content) // 2])
                    self.close_connection = True
                else:
                    self.wfile.write(content)

            def do_PUT(self):
                if self.headers.get('Authorization') != 'Basic dGVzdDp0ZXN0':
                    self.send_error(401)
                    return
                data = self.rfile.read(int(self.headers['Content-Length']))
                if self.path == nexus.fail_path and nexus.fail_count:
                    nexus.fail_count -= 1
                    if nexus.commit_before_failure:
                        nexus.files[self.path] = data
                    self.send_error(nexus.fail_status)
                    return
                if self.path in nexus.files and 'maven-metadata.xml' not in self.path:
                    self.send_error(400, 'Release redeployment disabled')
                    return
                nexus.files[self.path] = data
                nexus.puts.append(self.path)
                self.send_response(201)
                self.end_headers()

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.repo = {'name': str(self.server.server_port),
                     'url': f'http://127.0.0.1:{self.server.server_port}/repository/releases/',
                     'username': 'test', 'password': 'test'}

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def path(self, artifact, version, extension):
        return f'/repository/releases/io/tapdata/{artifact}/{version}/{artifact}-{version}.{extension}'


class PublishingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / 'repo'
        self.root.mkdir()
        self.nexuses = [Nexus(), Nexus()]
        self.targets = [n.repo for n in self.nexuses]
        self.builds = []
        self.build_sources = []
        self.original_run = publisher.run
        self.root_patch = patch.object(publisher, 'ROOT', self.root)
        self.root_patch.start()
        self.addCleanup(self.root_patch.stop)
        self.addCleanup(self.temp.cleanup)
        for nexus in self.nexuses:
            self.addCleanup(nexus.close)
        self.git('init', '-q')
        self.git('config', 'user.email', 'test@example.com')
        self.git('config', 'user.name', 'Test')
        directory = self.root / 'connector-distribution'
        directory.mkdir()
        shutil.copy(Path(__file__).with_name('generate_bom.py'), directory)

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.root, text=True).strip()

    def seed(self, count=23):
        namespace = 'http://maven.apache.org/POM/4.0.0'
        connectors = self.root / 'connectors'
        connectors.mkdir()
        modules = ''.join(f'<module>c{i:02}</module>' for i in range(count))
        (connectors / 'pom.xml').write_text(
            f'<project xmlns="{namespace}"><artifactId>connectors</artifactId>'
            '<properties><tapdata.pdk.api.version>2.0.11</tapdata.pdk.api.version></properties>'
            f'<modules>{modules}</modules></project>')
        for i in range(count):
            module = connectors / f'c{i:02}'
            module.mkdir()
            (module / 'pom.xml').write_text(
                f'<project xmlns="{namespace}"><artifactId>c{i:02}</artifactId>'
                '<version>1.0-SNAPSHOT</version></project>')
            (module / 'source.txt').write_text('initial')
        self.git('add', '.')
        self.git('commit', '-qm', 'initial')
        return self.git('rev-parse', 'HEAD')

    def fake_run(self, command, **kwargs):
        if command[0] != 'mvn':
            return self.original_run(command, **kwargs)
        modules = command[command.index('-pl') + 1].split(',')
        self.builds.append(modules)
        self.build_sources.append(subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'], cwd=publisher.ROOT, text=True).strip())
        for name in modules:
            target = publisher.ROOT / name / 'target'
            target.mkdir(exist_ok=True)
            (target / 'connector.jar').write_bytes(f'build-{len(self.builds)}-{name}'.encode())

    def publish(self):
        with patch.object(publisher, 'repositories', return_value=self.targets), \
                patch.object(publisher, 'run', side_effect=self.fake_run), \
                contextlib.redirect_stdout(io.StringIO()):
            publisher.main()

    def assert_replicated(self):
        left, right = self.nexuses
        for path, data in left.files.items():
            if 'maven-metadata.xml' not in path:
                self.assertEqual(data, right.files.get(path), path)

    def test_23_connectors_build_in_batches_of_10_and_no_change_does_nothing(self):
        self.seed()
        self.publish()
        self.assertEqual([len(batch) for batch in self.builds], [10, 10, 3])
        self.assert_replicated()
        for nexus in self.nexuses:
            latest = publisher.latest_bom(nexus.repo)
            self.assertEqual(len(publisher.bom_releases(latest[1])), 23)
        count = [len(n.puts) for n in self.nexuses]
        self.publish()
        self.assertEqual([len(n.puts) for n in self.nexuses], count)
        self.assertEqual(len(self.builds), 3)

    def test_last_metadata_checksum_failure_is_repaired_even_when_no_source_changes(self):
        self.seed(1)
        external = self.nexuses[1]
        external.fail_path = '/repository/releases/io/tapdata/connectors-bom/maven-metadata.xml.sha256'
        external.fail_count = 1
        with self.assertRaisesRegex(RuntimeError, 'HTTP 403'):
            self.publish()
        self.assertEqual(publisher.latest_bom(self.targets[0])[0], publisher.latest_bom(self.targets[1])[0])
        self.publish()
        metadata = external.files[external.fail_path.removesuffix('.sha256')]
        self.assertEqual(external.files[external.fail_path], hashlib.sha256(metadata).hexdigest().encode())
        self.assertEqual(len(self.builds), 1)

    def test_partial_connector_failure_resumes_pinned_plan_even_after_branch_advances(self):
        source = self.seed()
        external = self.nexuses[1]
        external.fail_path = external.path('c10', '2.11.0', 'pom')
        external.fail_count = 1
        with self.assertRaisesRegex(RuntimeError, 'HTTP 403'):
            self.publish()
        self.assertEqual([len(b) for b in self.builds], [10, 10])
        self.assertFalse(any('/connectors-bom/' in p for n in self.nexuses for p in n.files))
        plans_before = [n.files[n.path('connector-release-plan', 'bootstrap', 'json')]
                        for n in self.nexuses]
        (self.root / 'connectors/c00/source.txt').write_text('changed')
        self.git('add', '.')
        self.git('commit', '-qm', 'newer source')
        with patch.object(publisher, 'next_bom_version', side_effect=AssertionError('must use saved version')):
            self.publish()
        self.assertEqual([len(b) for b in self.builds], [10, 10, 9, 3])
        self.assertTrue(all(sha == source for sha in self.build_sources))
        self.assert_replicated()
        for index, nexus in enumerate(self.nexuses):
            self.assertEqual(nexus.files[nexus.path('connector-release-plan', 'bootstrap', 'json')],
                             plans_before[index])
            self.assertEqual(nexus.puts.count(nexus.path('c00', '2.11.0', 'jar')), 1)
        self.publish()  # The next invocation releases the newer source.
        self.assertEqual(self.builds[-1], ['connectors/c00'])
        self.assert_replicated()

    def test_second_bom_upload_failure_reuses_existing_bom(self):
        self.seed(1)
        version = '2.0.11.20990101.1'
        external = self.nexuses[1]
        external.fail_path = external.path('connectors-bom', version, 'pom')
        external.fail_count = 1
        with patch.object(publisher, 'next_bom_version', return_value=version):
            with self.assertRaisesRegex(RuntimeError, 'HTTP 403'):
                self.publish()
        self.assertTrue(self.nexuses[0].files.get(self.nexuses[0].path('connectors-bom', version, 'pom')))
        self.publish()
        self.assertEqual(len(self.builds), 1)
        self.assert_replicated()

    def test_metadata_failure_after_first_environment_advances(self):
        self.seed(1)
        external = self.nexuses[1]
        external.fail_path = '/repository/releases/io/tapdata/connectors-bom/maven-metadata.xml'
        external.fail_count = 1
        with self.assertRaisesRegex(RuntimeError, 'HTTP 403'):
            self.publish()
        self.assertIsNotNone(publisher.latest_bom(self.targets[0]))
        self.assertIsNone(publisher.latest_bom(self.targets[1]))
        self.publish()
        self.assertEqual(len(self.builds), 1)
        self.assertEqual(publisher.latest_bom(self.targets[0])[0], publisher.latest_bom(self.targets[1])[0])
        self.assert_replicated()

    def test_bom_conflict_fails_before_plan_build_or_upload(self):
        self.seed(1)
        version = '2.0.11.20990101.1'
        self.nexuses[1].files[self.nexuses[1].path('connectors-bom', version, 'pom')] = b'conflict'
        with patch.object(publisher, 'next_bom_version', return_value=version):
            with self.assertRaisesRegex(RuntimeError, 'already exists'):
                self.publish()
        self.assertFalse(self.builds)
        self.assertTrue(all(not n.puts for n in self.nexuses))

    def test_plan_replication_failure_prevents_any_connector_upload(self):
        self.seed(1)
        external = self.nexuses[1]
        external.fail_path = external.path('connector-release-plan', 'bootstrap', 'json')
        external.fail_count = 1
        with self.assertRaisesRegex(RuntimeError, 'HTTP 403'):
            self.publish()
        self.assertFalse(self.builds)
        self.publish()
        self.assertEqual(len(self.builds), 1)
        self.assert_replicated()

    def test_put_response_lost_after_commit_and_transient_retries(self):
        nexus = self.nexuses[0]
        nexus.fail_path = '/repository/releases/test.json'
        nexus.fail_status = 503
        nexus.fail_count = 1
        nexus.commit_before_failure = True
        with patch.object(publisher.time, 'sleep'):
            publisher.put_immutable(nexus.repo, nexus.repo['url'] + 'test.json', b'payload')
        self.assertEqual(nexus.files[nexus.fail_path], b'payload')

    def test_truncated_jar_download_is_retried_from_the_start(self):
        nexus = self.nexuses[0]
        nexus.truncate_path = '/repository/releases/connector.jar'
        nexus.truncate_count = 1
        nexus.files[nexus.truncate_path] = b'complete artifact bytes'
        destination = Path(self.temp.name) / 'download.jar'
        with patch.object(publisher.time, 'sleep') as sleep:
            publisher.request(nexus.repo, nexus.repo['url'] + 'connector.jar', destination=destination)
        self.assertEqual(destination.read_bytes(), b'complete artifact bytes')
        self.assertEqual(sleep.call_count, 1)

    def test_build_failure_keeps_previous_batch_and_retry_skips_it(self):
        self.seed(21)
        original = self.fake_run
        failed = False

        def fail_second_build(command, **kwargs):
            nonlocal failed
            if command[0] == 'mvn' and len(self.builds) == 1 and not failed:
                failed = True
                raise subprocess.CalledProcessError(1, command)
            return original(command, **kwargs)

        with patch.object(self, 'fake_run', side_effect=fail_second_build):
            with self.assertRaises(subprocess.CalledProcessError):
                self.publish()
        self.publish()
        self.assertEqual([len(batch) for batch in self.builds], [10, 10, 1])
        self.assert_replicated()

    def test_existing_jar_is_canonical_and_missing_pom_is_repaired(self):
        item = {'groupId': 'io.tapdata', 'artifactId': 'c00', 'version': '2.11.0',
                'pdkApiVersion': '2.0.11'}
        nexus = self.nexuses[0]
        nexus.files[nexus.path('c00', '2.11.0', 'jar')] = b'original'
        jar = Path(self.temp.name) / 'local.jar'
        jar.write_bytes(b'rebuilt differently')
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            record = publisher.sync_connector(item, self.targets, Path(self.temp.name), jar=jar)
        self.assertIn('::warning::SHA-256 mismatch', output.getvalue())
        self.assertEqual(record['sha256'], hashlib.sha256(b'original').hexdigest())
        self.assert_replicated()

    def test_different_immutable_jars_across_environments_fail(self):
        item = {'groupId': 'io.tapdata', 'artifactId': 'c00', 'version': '2.11.0',
                'pdkApiVersion': '2.0.11'}
        for i, nexus in enumerate(self.nexuses):
            nexus.files[nexus.path('c00', '2.11.0', 'jar')] = str(i).encode()
        with self.assertRaisesRegex(RuntimeError, 'differs between'):
            publisher.sync_connector(item, self.targets, Path(self.temp.name))
        self.assertTrue(all(not n.puts for n in self.nexuses))

    def test_http_auth_and_network_errors_never_mean_absent(self):
        repo = {**self.targets[0], 'password': 'wrong'}
        with self.assertRaisesRegex(RuntimeError, 'HTTP 401'):
            publisher.nexus_read(repo['url'] + 'missing', repo)
        with patch.object(publisher.urllib.request, 'urlopen', side_effect=TimeoutError), \
                patch.object(publisher.time, 'sleep') as sleep:
            with self.assertRaises(TimeoutError):
                publisher.nexus_read(repo['url'] + 'missing', repo)
        self.assertEqual(sleep.call_count, 2)


if __name__ == '__main__':
    unittest.main()
