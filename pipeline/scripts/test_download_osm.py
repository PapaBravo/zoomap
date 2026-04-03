"""Unit tests for download_osm.py – specifically fetch_overpass resilience."""

import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import tempfile
import os

# ---------------------------------------------------------------------------
# Minimal stubs so the module can be imported without duckdb installed
# ---------------------------------------------------------------------------
if "duckdb" not in sys.modules:
    duckdb_stub = types.ModuleType("duckdb")
    duckdb_stub.connect = MagicMock()
    sys.modules["duckdb"] = duckdb_stub

# We also need to avoid the module-level CACHE_DIR.mkdir() from failing.
# Patch it before import via environment variable pointing to a temp location.
_tmp_cache = tempfile.mkdtemp()
os.environ.setdefault("CACHE_DIR", _tmp_cache)

import pipeline.scripts.download_osm as dl  # noqa: E402


class TestFetchOverpassCachePath(unittest.TestCase):
    """fetch_overpass should return cached data without hitting the network."""

    def test_returns_cached_json_when_cache_file_exists(self):
        with tempfile.TemporaryDirectory() as td:
            cache_file = Path(td) / "osm_test.json"
            expected = {"elements": [{"id": 1, "type": "node"}]}
            cache_file.write_text(json.dumps(expected))

            result = dl.fetch_overpass("irrelevant query", cache_path=str(cache_file))
            self.assertEqual(result, expected)

    def test_does_not_contact_network_when_cache_exists(self):
        with tempfile.TemporaryDirectory() as td:
            cache_file = Path(td) / "osm_test.json"
            cache_file.write_text(json.dumps({"elements": []}))

            with patch("pipeline.scripts.download_osm.make_session") as mock_session:
                dl.fetch_overpass("query", cache_path=str(cache_file))
                mock_session.assert_not_called()

    def test_ignores_cache_path_when_file_does_not_exist(self):
        """If cache_path is given but missing, it should fall through to Overpass."""
        fake_response = MagicMock()
        fake_response.json.return_value = {"elements": []}
        fake_response.raise_for_status = MagicMock()

        fake_session = MagicMock()
        fake_session.post.return_value = fake_response

        with patch("pipeline.scripts.download_osm.make_session", return_value=fake_session):
            result = dl.fetch_overpass(
                "query", cache_path="/nonexistent/path/osm.json"
            )
        self.assertEqual(result, {"elements": []})


class TestFetchOverpassMirrorRotation(unittest.TestCase):
    """fetch_overpass should rotate through mirrors on failure."""

    def _make_failing_session(self, fail_count: int, success_data: dict):
        """Return a mock session that raises RequestException *fail_count* times
        then succeeds."""
        import requests

        fail_resp = MagicMock()
        fail_resp.raise_for_status.side_effect = requests.exceptions.ConnectionError(
            "simulated timeout"
        )

        ok_resp = MagicMock()
        ok_resp.json.return_value = success_data
        ok_resp.raise_for_status = MagicMock()

        session = MagicMock()
        # First N calls raise, then succeed
        session.post.side_effect = (
            [requests.exceptions.ConnectionError("timeout")] * fail_count
            + [ok_resp]
        )
        return session

    def test_succeeds_on_second_mirror(self):
        import requests

        ok_resp = MagicMock()
        ok_resp.json.return_value = {"elements": [{"id": 99}]}
        ok_resp.raise_for_status = MagicMock()

        session = MagicMock()
        session.post.side_effect = [
            requests.exceptions.ConnectionError("first mirror down"),
            ok_resp,
        ]

        with patch("pipeline.scripts.download_osm.make_session", return_value=session), \
             patch("time.sleep"):  # don't actually sleep in tests
            result = dl.fetch_overpass("query")

        self.assertEqual(result["elements"][0]["id"], 99)
        self.assertEqual(session.post.call_count, 2)

    def test_raises_runtime_error_when_all_mirrors_fail(self):
        import requests

        session = MagicMock()
        session.post.side_effect = requests.exceptions.ConnectionError("all down")

        with patch("pipeline.scripts.download_osm.make_session", return_value=session), \
             patch("time.sleep"):
            with self.assertRaises(RuntimeError) as ctx:
                dl.fetch_overpass("query")

        self.assertIn("all mirrors", str(ctx.exception).lower())
        # Should have tried every endpoint
        self.assertEqual(session.post.call_count, len(dl.OVERPASS_ENDPOINTS))

    def test_all_configured_endpoints_are_tried(self):
        """Verify the endpoints list contains all four required mirrors."""
        endpoints = dl.OVERPASS_ENDPOINTS
        self.assertGreaterEqual(len(endpoints), 4)
        self.assertIn("https://overpass-api.de/api/interpreter", endpoints)
        self.assertIn("https://lz4.overpass-api.de/api/interpreter", endpoints)
        self.assertIn("https://overpass.kumi.systems/api/interpreter", endpoints)
        self.assertIn("https://overpass.openstreetmap.ru/api/interpreter", endpoints)


class TestMakeSession(unittest.TestCase):
    def test_returns_requests_session(self):
        import requests
        session = dl.make_session()
        self.assertIsInstance(session, requests.Session)

    def test_has_retry_adapter_mounted(self):
        from requests.adapters import HTTPAdapter
        session = dl.make_session(retries=3, backoff_factor=0.5)
        adapter = session.get_adapter("https://overpass-api.de/")
        self.assertIsInstance(adapter, HTTPAdapter)
        self.assertEqual(adapter.max_retries.total, 3)


if __name__ == "__main__":
    unittest.main()
