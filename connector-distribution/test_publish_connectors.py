"""Exercise batched dual-Nexus publishing and recovery without Maven or real Nexus."""

import contextlib
import hashlib
import io
import json
import os
import shutil
import subprocess
import tempfile
import threading
import unittest
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
import publish_connectors as publisher
import build_connectors as builder


class Nexus:
    def __init__(self, *, host='127.0.0.1', port=0, tls_context=None):
        self.files = {}
        self.puts = []
        self.gets = []
        self.heads = []
        self.fail_path = None
        self.fail_status = 403
        self.fail_count = 0
        self.commit_before_failure = False
        self.truncate_path = None
        self.truncate_count = 0
        self.hosts = []
        nexus = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                nexus.gets.append(self.path)
                nexus.hosts.append(self.headers.get('Host'))
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

            def do_HEAD(self):
                nexus.heads.append(self.path)
                if self.headers.get('Authorization') != 'Basic dGVzdDp0ZXN0':
                    self.send_error(401)
                    return
                if self.path not in nexus.files:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header('Content-Length', str(len(nexus.files[self.path])))
                self.end_headers()

            def do_PUT(self):
                nexus.hosts.append(self.headers.get('Host'))
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

        self.server = ThreadingHTTPServer((host, port), Handler)
        if tls_context is not None:
            self.server.socket = tls_context.wrap_socket(self.server.socket, server_side=True)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.repo = {'name': str(self.server.server_port),
                     'url': f'http://{host}:{self.server.server_port}/repository/releases/',
                     'username': 'test', 'password': 'test'}

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def path(self, artifact, version, extension):
        return f'/repository/releases/io/tapdata/{artifact}/{version}/{artifact}-{version}.{extension}'


class PublishingTests(unittest.TestCase):
    def setUp(self):
        # CI runs this suite in both distribution legs. Each fixture starts with
        # OSS coordinates; enterprise cases explicitly override this baseline.
        distribution = patch.dict(os.environ, {'CONNECTOR_DISTRIBUTION': 'opensource'})
        distribution.start()
        self.addCleanup(distribution.stop)
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
            ['git', 'rev-parse', 'HEAD'], cwd=kwargs['cwd'], text=True).strip())
        for name in modules:
            target = Path(kwargs['cwd']) / name / 'target'
            target.mkdir(exist_ok=True)
            (target / 'connector.jar').write_bytes(f'build-{len(self.builds)}-{name}'.encode())

    def publish(self):
        plan_file = Path(self.temp.name) / 'release_plan.json'
        artifacts = Path(self.temp.name) / 'artifacts'
        with patch.object(publisher, 'repositories', return_value=self.targets), \
                patch.object(builder, 'run', side_effect=self.fake_run), \
                contextlib.redirect_stdout(io.StringIO()):
            plan = publisher.main(self.root, plan_file)
            if plan is None:
                return
            count = sum(item['publish'] for item in plan['items'])
            for index in range(max(1, (count + publisher.BATCH_SIZE - 1) // publisher.BATCH_SIZE)):
                request = Path(self.temp.name) / 'request.json'
                publisher.prepare_batch(plan_file, index, request)
                shutil.rmtree(artifacts, ignore_errors=True)
                builder.build(request, self.root, artifacts)
                publisher.upload_artifacts(self.root, artifacts, plan_path=plan_file, batch_index=index)
            publisher.upload_artifacts(self.root, artifacts, plan_path=plan_file, finalize=True)

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

    def test_sync_reads_each_existing_jar_once_and_repairs_checksums(self):
        self.seed(1)
        self.publish()
        plan = json.loads((Path(self.temp.name) / 'release_plan.json').read_text())
        item = plan['items'][0]
        jar_path = self.nexuses[0].path(item['artifactId'], item['version'], 'jar')
        for nexus in self.nexuses:
            nexus.gets.clear()
            del nexus.files[jar_path + '.sha256']
        with tempfile.TemporaryDirectory() as temp:
            publisher.sync_connector(item, self.targets, Path(temp))
        for nexus in self.nexuses:
            self.assertEqual(nexus.gets.count(jar_path), 1)
            self.assertEqual(nexus.files[jar_path + '.sha256'],
                             hashlib.sha256(nexus.files[jar_path]).hexdigest().encode())

    def test_enterprise_plan_bom_metadata_and_resume_are_isolated(self):
        self.seed(1)
        # An existing OSS BOM/plan must not become the enterprise baseline.
        oss_metadata = '/repository/releases/io/tapdata/connectors-bom/maven-metadata.xml'
        oss_plan = '/repository/releases/io/tapdata/connector-release-plan/bootstrap/connector-release-plan-bootstrap.json'
        for nexus in self.nexuses:
            nexus.files[oss_metadata] = b'OSS metadata must remain untouched'
            nexus.files[oss_plan] = b'OSS plan must remain untouched'
        with patch.dict(os.environ, {'CONNECTOR_DISTRIBUTION': 'enterprise'}):
            self.publish()
            self.assert_replicated()
            for nexus in self.nexuses:
                version, bom = publisher.latest_bom(nexus.repo)
                self.assertEqual(bom.findtext('m:artifactId', namespaces=publisher.NS), 'connectors-enterprise-bom')
                self.assertIn('/connectors-enterprise-bom/', publisher.bom_url(version, nexus.repo))
                self.assertIn('/connector-enterprise-release-plan/', publisher.plan_url(None, nexus.repo))
                self.assertEqual(nexus.files[oss_metadata], b'OSS metadata must remain untouched')
                self.assertEqual(nexus.files[oss_plan], b'OSS plan must remain untouched')
            puts = [len(n.puts) for n in self.nexuses]
            self.publish()
            self.assertEqual([len(n.puts) for n in self.nexuses], puts)
            self.assertEqual(len(self.builds), 1)

    def test_enterprise_partial_upload_resumes_original_source(self):
        source = self.seed(2)
        external = self.nexuses[1]
        external.fail_path = external.path('c01', '2.11.0', 'pom')
        external.fail_count = 1
        with patch.dict(os.environ, {'CONNECTOR_DISTRIBUTION': 'enterprise'}):
            with self.assertRaisesRegex(RuntimeError, 'HTTP 403'):
                self.publish()
            self.assertTrue(all(publisher.latest_bom(n.repo) is None for n in self.nexuses))
            (self.root / 'connectors/c00/source.txt').write_text('newer source')
            self.git('add', '.')
            self.git('commit', '-qm', 'advance enterprise source')
            self.publish()
            self.assertTrue(all(sha == source for sha in self.build_sources))
            self.assertEqual(len(self.builds), 1)
            self.assert_replicated()
            for nexus in self.nexuses:
                bom = publisher.latest_bom(nexus.repo)[1]
                self.assertEqual(bom.findtext('m:properties/m:tapdata.source.commit', namespaces=publisher.NS), source)

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

    def test_same_generation_parent_spelling_change_preserves_reused_api(self):
        self.seed(2)
        pom = self.root / 'connectors/pom.xml'
        pom.write_text(pom.read_text().replace('2.0.11', '2.0.11-SNAPSHOT'))
        self.git('add', '.')
        self.git('commit', '-qm', 'initial snapshot API')
        self.publish()
        pom.write_text(pom.read_text().replace('2.0.11-SNAPSHOT', '2.0.11'))
        (self.root / 'connectors/c00/source.txt').write_text('updated')
        self.git('add', '.')
        self.git('commit', '-qm', 'same generation and connector update')
        self.publish()
        releases = {i['artifactId']: i for i in publisher.bom_releases(publisher.latest_bom(self.targets[0])[1])}
        self.assertEqual(releases['c00']['version'], '2.11.1')
        self.assertEqual(releases['c01']['version'], '2.11.0')
        self.assertEqual(releases['c01']['pdkApiVersion'], '2.0.11-SNAPSHOT')
        self.assert_replicated()

    def test_cross_generation_parent_change_rebuilds_inherited_connectors(self):
        self.seed(2)
        self.publish()
        pom = self.root / 'connectors/pom.xml'
        pom.write_text(pom.read_text().replace('2.0.11', '2.0.12'))
        self.git('add', '.')
        self.git('commit', '-qm', 'API generation upgrade')
        self.publish()
        releases = publisher.bom_releases(publisher.latest_bom(self.targets[0])[1])
        self.assertEqual({i['version'] for i in releases}, {'2.12.0'})
        self.assertEqual(len(self.builds[-1]), 2)

    def test_upload_rejects_tampered_plan_before_any_put(self):
        self.seed(1)
        plan = publisher.release_plan(self.targets, self.git('rev-parse', 'HEAD'))
        plan['items'][0]['version'] = '2.11.99'
        plan_file = Path(self.temp.name) / 'tampered.json'
        plan_file.write_text(json.dumps(plan))
        count = [len(n.puts) for n in self.nexuses]
        with patch.object(publisher, 'repositories', return_value=self.targets):
            with self.assertRaisesRegex(RuntimeError, 'differs from immutable'):
                publisher.upload_artifacts(self.root, plan_path=plan_file)
        self.assertEqual([len(n.puts) for n in self.nexuses], count)

    def test_build_rejects_path_traversal_and_oversized_batch_before_maven(self):
        self.seed(1)
        path = Path(self.temp.name) / 'build-plan.json'
        item = {'module': 'connectors/../../outside', 'artifactId': 'x', 'publish': True}
        with patch.object(builder, 'run') as run:
            for items, message in [([item], 'invalid connector module'), ([item] * 11, 'at most 10')]:
                path.write_text(json.dumps({'items': items}))
                with self.assertRaisesRegex(ValueError, message):
                    builder.build(path, self.root, Path(self.temp.name) / 'output')
            run.assert_not_called()

    def test_finalize_never_publishes_bom_if_a_jar_is_missing(self):
        self.seed(1)
        plan = publisher.release_plan(self.targets, self.git('rev-parse', 'HEAD'))
        path = Path(self.temp.name) / 'plan.json'
        path.write_text(json.dumps(plan))
        with patch.object(publisher, 'repositories', return_value=self.targets):
            with self.assertRaisesRegex(RuntimeError, 'missing before BOM'):
                publisher.upload_artifacts(self.root, plan_path=path, finalize=True)
        self.assertFalse(any('/connectors-bom/' in p for n in self.nexuses for p in n.puts))

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

    def seeded_sidecar_jar(self, jar):
        for nexus in self.nexuses:
            path = nexus.path('c00', '2.11.0', 'jar')
            nexus.files[path] = jar
            nexus.files[path + '.sha1'] = hashlib.sha1(jar).hexdigest().encode()
            nexus.files[path + '.sha256'] = hashlib.sha256(jar).hexdigest().encode()
            nexus.gets.clear()
            nexus.puts.clear()
        return path

    def test_matching_sidecars_verify_without_downloading_jars(self):
        item = {'groupId': 'io.tapdata', 'artifactId': 'c00', 'version': '2.11.0',
                'pdkApiVersion': '2.0.11'}
        jar = b'x' * 1024
        jar_path = self.seeded_sidecar_jar(jar)
        with contextlib.redirect_stdout(io.StringIO()):
            record = publisher.sync_connector(item, self.targets, Path(self.temp.name))
        self.assertEqual(record['sha256'], hashlib.sha256(jar).hexdigest())
        for nexus in self.nexuses:
            # Neither the JAR nor its bytes move: verification used the sidecars.
            self.assertNotIn(jar_path, nexus.gets)
            self.assertTrue(all('.pom' in path for path in nexus.puts))
        self.assert_replicated()

    def test_conflicting_sidecars_fall_back_to_jar_bytes(self):
        item = {'groupId': 'io.tapdata', 'artifactId': 'c00', 'version': '2.11.0',
                'pdkApiVersion': '2.0.11'}
        jar_path = self.seeded_sidecar_jar(b'x' * 1024)
        self.nexuses[1].files[jar_path + '.sha256'] = b'0' * 64
        # The JARs are identical, so the drift is a stale immutable sidecar and
        # repair must stay strict rather than overwrite it silently.
        with self.assertRaisesRegex(RuntimeError, 'Immutable file differs'):
            publisher.sync_connector(item, self.targets, Path(self.temp.name))
        self.assertIn(jar_path, self.nexuses[1].gets)

    def test_conflicting_sidecars_with_different_jars_fail(self):
        item = {'groupId': 'io.tapdata', 'artifactId': 'c00', 'version': '2.11.0',
                'pdkApiVersion': '2.0.11'}
        jar_path = self.seeded_sidecar_jar(b'x' * 1024)
        other = b'y' * 1024
        self.nexuses[1].files[jar_path] = other
        self.nexuses[1].files[jar_path + '.sha256'] = hashlib.sha256(other).hexdigest().encode()
        with self.assertRaisesRegex(RuntimeError, 'differs between'):
            publisher.sync_connector(item, self.targets, Path(self.temp.name))
        self.assertTrue(all(not n.puts for n in self.nexuses))

    def test_missing_sha1_sidecar_is_repaired_with_one_jar_download(self):
        item = {'groupId': 'io.tapdata', 'artifactId': 'c00', 'version': '2.11.0',
                'pdkApiVersion': '2.0.11'}
        jar = b'x' * 1024
        jar_path = self.seeded_sidecar_jar(jar)
        del self.nexuses[1].files[jar_path + '.sha1']
        with contextlib.redirect_stdout(io.StringIO()):
            record = publisher.sync_connector(item, self.targets, Path(self.temp.name))
        self.assertEqual(record['sha256'], hashlib.sha256(jar).hexdigest())
        # sha1 cannot be synthesized from a peer sidecar, so exactly one
        # environment downloads the JAR to rebuild it.
        self.assertEqual(sum(nexus.gets.count(jar_path) for nexus in self.nexuses), 1)
        self.assertEqual(self.nexuses[1].files[jar_path + '.sha1'],
                         hashlib.sha1(jar).hexdigest().encode())
        self.assert_replicated()

    def test_deleted_jar_is_reuploaded_despite_surviving_sidecars(self):
        item = {'groupId': 'io.tapdata', 'artifactId': 'c00', 'version': '2.11.0',
                'pdkApiVersion': '2.0.11'}
        jar = b'x' * 1024
        jar_path = self.seeded_sidecar_jar(jar)
        # Maven's detached checksums can outlive a deleted artifact.
        del self.nexuses[1].files[jar_path]
        with contextlib.redirect_stdout(io.StringIO()):
            record = publisher.sync_connector(item, self.targets, Path(self.temp.name))
        self.assertEqual(record['sha256'], hashlib.sha256(jar).hexdigest())
        # The missing JAR is re-uploaded from the verified peer, never skipped.
        self.assertIn(jar_path, self.nexuses[1].puts)
        self.assertEqual(self.nexuses[1].files[jar_path], jar)
        self.assertNotIn(jar_path, self.nexuses[0].puts)
        # Existence was probed with HEAD, and the repair downloaded the JAR
        # once from the surviving peer; the repaired side's reads belong to
        # put_immutable's pre-check and post-upload verification.
        self.assertIn(jar_path, self.nexuses[0].heads)
        self.assertEqual(self.nexuses[0].gets.count(jar_path), 1)
        self.assert_replicated()

    def test_deleted_jar_and_sha1_are_repaired_together(self):
        item = {'groupId': 'io.tapdata', 'artifactId': 'c00', 'version': '2.11.0',
                'pdkApiVersion': '2.0.11'}
        jar = b'x' * 1024
        jar_path = self.seeded_sidecar_jar(jar)
        del self.nexuses[1].files[jar_path]
        del self.nexuses[1].files[jar_path + '.sha1']
        with contextlib.redirect_stdout(io.StringIO()):
            record = publisher.sync_connector(item, self.targets, Path(self.temp.name))
        self.assertEqual(record['sha256'], hashlib.sha256(jar).hexdigest())
        self.assertIn(jar_path, self.nexuses[1].puts)
        # The JAR comes back before its sha1, never a checksum without a JAR.
        self.assertEqual(self.nexuses[1].files[jar_path], jar)
        self.assertEqual(self.nexuses[1].files[jar_path + '.sha1'],
                         hashlib.sha1(jar).hexdigest().encode())
        self.assertEqual(self.nexuses[0].gets.count(jar_path), 1)
        self.assert_replicated()

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
