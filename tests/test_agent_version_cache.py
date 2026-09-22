#!/usr/bin/env python3
"""Tests for HGRC-015 agent version display convergence."""
import json
import os
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add webui root to path
WEBUI_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(WEBUI_ROOT))

from api import updates


class TestAgentVersionCache(unittest.TestCase):
    """Test that get_agent_version returns live-health-first with provenance."""

    def test_live_health_first(self):
        """When gateway health responds, provenance should be 'live'."""
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "version": "0.21.3",
            "status": "ok",
            "platform": "hermes-agent",
        }).encode()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("api.updates._AGENT_DIR", None), \
             patch("api.updates.urllib.request.urlopen", return_value=mock_response), \
             patch.dict(os.environ, {"GATEWAY_HEALTH_URL": "http://localhost:8642"}):
            # Clear cache
            updates._AGENT_VERSION_CACHE['cached_at'] = 0
            result = updates.get_agent_version()

        self.assertEqual(result["version"], "0.21.3")
        self.assertEqual(result["provenance"], "live")
        self.assertEqual(result["source"], "gateway_health")

    def test_disk_fallback_when_gateway_unreachable(self):
        """When gateway unreachable, fall back to disk with provenance 'disk'."""
        import urllib.error

        agent_dir = MagicMock()
        agent_dir.__truediv__ = MagicMock(return_value=MagicMock(exists=MagicMock(return_value=False)))

        with patch("api.updates._AGENT_DIR", "/nonexistent"), \
             patch("api.updates.Path") as mock_path, \
             patch("api.updates._detect_agent_version_from_gateway_health", return_value=None):
            mock_path.return_value = agent_dir
            # Clear cache
            updates._AGENT_VERSION_CACHE['cached_at'] = 0
            result = updates.get_agent_version()

        # Should be unknown or disk depending on what's available
        self.assertIn(result["provenance"], ("disk", "unknown"))

    def test_unknown_when_no_source(self):
        """When neither gateway nor disk works, provenance should be 'unknown'."""
        with patch("api.updates._AGENT_DIR", None), \
             patch("api.updates._detect_agent_version_from_gateway_health", return_value=None):
            updates._AGENT_VERSION_CACHE['cached_at'] = 0
            result = updates.get_agent_version()

        self.assertEqual(result["provenance"], "unknown")
        self.assertIn("error", result)

    def test_cache_refresh_after_ttl(self):
        """Cache should refresh after TTL expires."""
        call_count = 0

        def mock_detect():
            nonlocal call_count
            call_count += 1
            return {
                "version": f"0.21.{call_count}",
                "provenance": "live",
                "source": "gateway_health",
                "cached_at": 0.0,
                "ttl_seconds": 60,
                "warning": None,
                "agent_dir": None,
                "gateway_url": None,
                "error": None,
            }

        with patch("api.updates._detect_agent_version", side_effect=mock_detect):
            # Force cache expired
            updates._AGENT_VERSION_CACHE['cached_at'] = 0
            updates._AGENT_VERSION_CACHE['ttl_seconds'] = 0
            r1 = updates.get_agent_version()
            r2 = updates.get_agent_version()
            # After first call, cache should be fresh (ttl=60), so no third call
            r3 = updates.get_agent_version()

        self.assertEqual(r1["version"], "0.21.1")
        self.assertEqual(r2["version"], "0.21.1")
        self.assertEqual(r3["version"], "0.21.1")
        self.assertEqual(call_count, 1, "Only one probe expected within TTL")

    def test_cache_expires_after_ttl(self):
        """After TTL passes, a new probe should fire."""
        call_count = 0

        def mock_detect():
            nonlocal call_count
            call_count += 1
            return {
                "version": f"0.21.{call_count}",
                "provenance": "live",
                "source": "gateway_health",
                "cached_at": 0.0,
                "ttl_seconds": 0.01,  # 10ms TTL
                "warning": None,
                "agent_dir": None,
                "gateway_url": None,
                "error": None,
            }

        with patch("api.updates._detect_agent_version", side_effect=mock_detect):
            updates._AGENT_VERSION_CACHE['cached_at'] = 0
            updates._AGENT_VERSION_CACHE['ttl_seconds'] = 0.01
            r1 = updates.get_agent_version()
            time.sleep(0.02)  # Wait for TTL to expire
            r2 = updates.get_agent_version()

        self.assertEqual(call_count, 2, "Two probes expected: initial + after TTL expiry")


class TestGetAgentVersionPayload(unittest.TestCase):
    """Test the payload structure returned by get_agent_version."""

    def test_payload_has_required_fields(self):
        """Payload must have version, provenance, source, and ttl_seconds."""
        updates._AGENT_VERSION_CACHE['cached_at'] = 0
        with patch("api.updates._AGENT_DIR", None), \
             patch("api.updates._detect_agent_version_from_gateway_health", return_value=None):
            result = updates.get_agent_version()

        required_fields = ('version', 'provenance', 'source', 'ttl_seconds')
        for field in required_fields:
            self.assertIn(field, result, f"Missing required field: {field}")

    def test_provenance_is_valid(self):
        """Provenance must be one of: live, disk, unknown."""
        updates._AGENT_VERSION_CACHE['cached_at'] = 0
        with patch("api.updates._AGENT_DIR", None), \
             patch("api.updates._detect_agent_version_from_gateway_health", return_value=None):
            result = updates.get_agent_version()

        self.assertIn(result["provenance"], ("live", "disk", "unknown"))


if __name__ == "__main__":
    unittest.main()
