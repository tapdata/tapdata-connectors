"""Verify per-repository routing with real TLS and no DNS/hosts changes."""

import os
import ssl
import signal
import time
import tempfile
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import patch, MagicMock

import publish_connectors as publisher
from test_publish_connectors import Nexus


class RequestTimeoutTests(unittest.TestCase):
    def test_deadline_interrupts_blocked_put(self):
        repo = {'name': 'test', 'url': 'https://nexus.example/',
                'username': 'test', 'password': 'test'}
        with patch.object(publisher, 'REQUEST_TOTAL_TIMEOUT', 0.08), \
             patch.object(publisher, 'open_request', side_effect=lambda *_: time.sleep(5)):
            with self.assertRaises(publisher.RequestDeadlineExceeded):
                publisher.request(repo, repo['url'] + 'connector.jar', data=b'jar')

    def test_deadline_interrupts_blocked_open_and_trickling_body(self):
        repo = {'name': 'test', 'url': 'https://nexus.example/',
                'username': 'test', 'password': 'test'}
        previous_handler = signal.getsignal(signal.SIGALRM)
        for blocked_open in (True, False):
            with self.subTest(blocked_open=blocked_open):
                response = MagicMock()
                response.__enter__.return_value = response
                response.headers = {}

                def trickle(_):
                    time.sleep(0.02)
                    return b'x'

                response.read1.side_effect = trickle

                def open_response(*_):
                    if blocked_open:
                        time.sleep(5)
                    return response

                started = time.monotonic()
                with patch.object(publisher, 'REQUEST_TOTAL_TIMEOUT', 0.08), \
                     patch.object(publisher, 'open_request', side_effect=open_response):
                    with self.assertRaises(publisher.RequestDeadlineExceeded):
                        publisher.request(repo, repo['url'] + 'connector.jar')
                self.assertLess(time.monotonic() - started, 1)
                self.assertEqual(signal.getsignal(signal.SIGALRM), previous_handler)
                self.assertEqual(signal.getitimer(signal.ITIMER_REAL)[0], 0)

    def test_upload_timeout_for_dns_and_pinned_routes(self):
        for pinned in (False, True):
            for method, timeout in (("GET", 45), ("PUT", 600)):
                with self.subTest(pinned=pinned, method=method):
                    repo = {'url': 'https://nexus.example/repository/releases/'}
                    if pinned:
                        repo['connect_ip'] = '127.0.0.1'
                    req = urllib.request.Request(repo['url'] + 'connector.jar', method=method)
                    with patch.object(publisher.urllib.request, 'urlopen') as direct, \
                         patch.object(publisher.urllib.request, 'build_opener') as factory:
                        publisher.open_request(repo, req)
                        transport = factory.return_value.open if pinned else direct
                        transport.assert_called_once_with(req, timeout=timeout)


class RepositoryConfigurationTests(unittest.TestCase):
    def setUp(self):
        environment = {}
        for name in ('INTERNAL', 'EXTERNAL'):
            environment.update({f'NEXUS_{name}_URL': 'https://nexus.example/repository/releases/',
                                f'NEXUS_{name}_USERNAME': 'test',
                                f'NEXUS_{name}_PASSWORD': 'test'})
        self.environment = patch.dict(os.environ, environment, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def test_missing_configuration_lists_all_required_names(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, 'Missing Actions configuration') as failure:
                publisher.repositories()
        for name in ('INTERNAL', 'EXTERNAL'):
            for suffix in ('URL', 'USERNAME', 'PASSWORD'):
                self.assertIn(f'NEXUS_{name}_{suffix}', str(failure.exception))
        self.assertNotIn('CONNECT_IP', str(failure.exception))

    def test_empty_configuration_names_only_without_secret_values(self):
        os.environ['NEXUS_INTERNAL_URL'] = '  '
        os.environ['NEXUS_EXTERNAL_PASSWORD'] = ''
        os.environ['NEXUS_INTERNAL_PASSWORD'] = 'private-test-value'
        with self.assertRaises(ValueError) as failure:
            publisher.repositories()
        message = str(failure.exception)
        self.assertIn('NEXUS_INTERNAL_URL', message)
        self.assertIn('NEXUS_EXTERNAL_PASSWORD', message)
        self.assertNotIn('private-test-value', message)
        self.assertNotIn('NEXUS_INTERNAL_PASSWORD', message)

    def test_same_url_with_internal_pin_and_external_dns(self):
        os.environ['NEXUS_INTERNAL_CONNECT_IP'] = '192.168.1.184'
        internal, external = publisher.repositories()
        self.assertEqual(internal['url'], external['url'])
        self.assertEqual(internal['connect_ip'], '192.168.1.184')
        self.assertEqual(external['connect_ip'], '')

    def test_same_destination_rejected(self):
        for value in ('', '192.168.1.184'):
            with self.subTest(value=value):
                os.environ['NEXUS_INTERNAL_CONNECT_IP'] = value
                os.environ['NEXUS_EXTERNAL_CONNECT_IP'] = value
                with self.assertRaisesRegex(ValueError, 'distinct'):
                    publisher.repositories()

    def test_ip_literals_validated_and_ipv6_normalized(self):
        os.environ['NEXUS_INTERNAL_CONNECT_IP'] = ' 2001:0db8::1 '
        self.assertEqual(publisher.repositories()[0]['connect_ip'], '2001:db8::1')
        for value in ('internal.example', '192.168.1.184:443', '[::1]', '192.168.1.999'):
            with self.subTest(value=value):
                os.environ['NEXUS_INTERNAL_CONNECT_IP'] = value
                with self.assertRaises(ValueError):
                    publisher.repositories()


class PinnedTransportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Public test-only key/certificate: never used outside loopback test servers.
        directory = Path(__file__).with_name('test-certs')
        cls.cert = directory / 'nexus.example.pem'
        key = directory / 'nexus.example-key.pem'
        cls.server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        cls.server_context.load_cert_chain(cls.cert, key)

    def setUp(self):
        self.sni = []
        self.server_context.set_servername_callback(lambda sock, name, context: self.sni.append(name))
        self.internal = Nexus(tls_context=self.server_context)
        self.addCleanup(self.internal.close)
        self.external = Nexus(host='127.0.0.2', port=self.internal.server.server_port,
                              tls_context=self.server_context)
        self.addCleanup(self.external.close)
        self.targets = []
        for name, nexus, ip in (('internal', self.internal, '127.0.0.1'),
                                ('external', self.external, '127.0.0.2')):
            nexus.repo.update(url=f'https://nexus.example:{nexus.server.server_port}/repository/releases/',
                              name=name, connect_ip=ip)
            self.targets.append(nexus.repo)
        context = ssl.create_default_context(cafile=str(self.cert))
        self.context_patch = patch.object(ssl, '_create_default_https_context', return_value=context)
        self.context_patch.start()
        self.addCleanup(self.context_patch.stop)

    def test_same_url_separate_storage_host_sni_and_proxy_bypass(self):
        with patch.dict(os.environ, {'https_proxy': 'http://127.0.0.1:1',
                                     'HTTPS_PROXY': 'http://127.0.0.1:1', 'NO_PROXY': '', 'no_proxy': ''}):
            for repo, payload in zip(self.targets, (b'internal', b'external')):
                publisher.put_immutable(repo, repo['url'] + 'file.jar', payload)
            for repo, payload in zip(self.targets, (b'internal', b'external')):
                self.assertEqual(publisher.nexus_read(repo['url'] + 'file.jar', repo), payload)
        self.assertTrue(self.sni)
        self.assertEqual(set(self.sni), {'nexus.example'})
        for nexus in (self.internal, self.external):
            self.assertEqual(set(nexus.hosts), {f'nexus.example:{nexus.server.server_port}'})

    def test_partial_upload_retry_keeps_destinations_and_bytes(self):
        item = {'groupId': 'io.tapdata', 'artifactId': 'c00', 'version': '2.11.0',
                'pdkApiVersion': '2.0.11'}
        self.external.fail_path = self.external.path('c00', '2.11.0', 'pom')
        self.external.fail_count = 1
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            jar = temp / 'local.jar'
            jar.write_bytes(b'canonical JAR')
            with self.assertRaisesRegex(RuntimeError, 'HTTP 403'):
                publisher.sync_connector(item, self.targets, temp, jar=jar)
            jar.unlink()  # Retry recovers bytes from Nexus without rebuilding.
            publisher.sync_connector(item, self.targets, temp)
        self.assertEqual(self.internal.files, self.external.files)
        self.assertEqual(self.internal.puts.count(self.internal.path('c00', '2.11.0', 'jar')), 1)

    def test_transient_failure_retries_pinned_destination(self):
        repo = self.targets[0]
        self.internal.fail_path = '/repository/releases/retry.jar'
        self.internal.fail_status = 503
        self.internal.fail_count = 1
        with patch.object(publisher.time, 'sleep') as sleep:
            publisher.put_immutable(repo, repo['url'] + 'retry.jar', b'payload')
        self.assertEqual(sleep.call_count, 1)
        self.assertFalse(self.external.files)

    def test_certificate_hostname_and_trust_still_required(self):
        repo = self.targets[0]
        wrong_host = {**repo, 'url': repo['url'].replace('nexus.example', 'wrong.example')}
        with patch.object(publisher.time, 'sleep'):
            with self.assertRaises(urllib.error.URLError) as failure:
                publisher.nexus_read(wrong_host['url'] + 'file.jar', wrong_host)
        self.assertIsInstance(failure.exception.reason, ssl.SSLCertVerificationError)
        with patch.object(ssl, '_create_default_https_context', return_value=ssl.create_default_context()), \
                patch.object(publisher.time, 'sleep'):
            with self.assertRaises(urllib.error.URLError) as failure:
                publisher.nexus_read(repo['url'] + 'file.jar', repo)
        self.assertIsInstance(failure.exception.reason, ssl.SSLCertVerificationError)

    def test_origin_change_and_redirect_rejected(self):
        repo = self.targets[0]
        for url in ('https://other.example/file', 'http://nexus.example/file'):
            with self.assertRaisesRegex(ValueError, 'origin'):
                publisher.nexus_read(url, repo)
        req = publisher.urllib.request.Request(repo['url'] + 'file')
        with self.assertRaises(urllib.error.HTTPError):
            publisher.RejectRedirects().redirect_request(
                req, None, 302, 'Found', {}, 'https://other.example/file')


if __name__ == '__main__':
    unittest.main()
