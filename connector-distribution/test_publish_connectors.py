"""BOM conflict preflight tests; no Maven or Nexus access required."""

import unittest
import urllib.error
from unittest.mock import patch

import publish_connectors as publisher


class BomPreflightTests(unittest.TestCase):
    def test_missing_bom_allows_release(self):
        error = urllib.error.HTTPError(publisher.bom_url("2.11.1"), 404, "missing", {}, None)
        with patch.dict(publisher.os.environ, NEXUS_USERNAME="test", NEXUS_PASSWORD="test"), \
                patch.object(publisher.urllib.request, "urlopen", side_effect=error):
            publisher.ensure_bom_available("2.11.1")

    def test_existing_empty_pom_is_still_a_conflict(self):
        with patch.object(publisher, "nexus_read", return_value=b"") as read:
            with self.assertRaisesRegex(RuntimeError, "already exists"):
                publisher.ensure_bom_available("2.11.1")
        read.assert_called_once_with(
            publisher.RELEASE_REPO + "io/tapdata/connectors-bom/2.11.1/connectors-bom-2.11.1.pom"
        )

    def test_http_errors_do_not_allow_release(self):
        for status in (401, 403, 500):
            with self.subTest(status=status):
                error = urllib.error.HTTPError(publisher.bom_url("2.11.1"), status, "failed", {}, None)
                with patch.dict(publisher.os.environ, NEXUS_USERNAME="test", NEXUS_PASSWORD="test"), \
                        patch.object(publisher.urllib.request, "urlopen", side_effect=error):
                    with self.assertRaisesRegex(RuntimeError, f"HTTP {status}"):
                        publisher.ensure_bom_available("2.11.1")

    def test_network_error_does_not_allow_release(self):
        with patch.dict(publisher.os.environ, NEXUS_USERNAME="test", NEXUS_PASSWORD="test"), \
                patch.object(publisher.urllib.request, "urlopen", side_effect=TimeoutError):
            with self.assertRaises(TimeoutError):
                publisher.ensure_bom_available("2.11.1")

    def test_main_stops_before_pom_changes_builds_or_uploads(self):
        with patch.object(publisher.subprocess, "check_output", return_value="source-sha"), \
                patch.object(publisher, "latest_bom", return_value=None), \
                patch.object(publisher, "discover_releases", return_value=([{"publish": True}], None)), \
                patch.object(publisher, "current_pdk_api_version", return_value="2.0.11"), \
                patch.object(publisher, "nexus_read", return_value=b"existing BOM"), \
                patch.object(publisher, "set_build_version") as set_version, \
                patch.object(publisher, "run") as run, \
                patch.object(publisher, "deploy") as deploy:
            with self.assertRaisesRegex(RuntimeError, "already exists"):
                publisher.main()
            set_version.assert_not_called()
            run.assert_not_called()
            deploy.assert_not_called()

    def test_no_changes_skips_preflight(self):
        with patch.object(publisher.subprocess, "check_output", return_value="source-sha"), \
                patch.object(publisher, "latest_bom", return_value=None), \
                patch.object(publisher, "discover_releases", return_value=([], "source-sha")), \
                patch.object(publisher, "ensure_bom_available") as preflight:
            publisher.main()
            preflight.assert_not_called()


if __name__ == "__main__":
    unittest.main()
