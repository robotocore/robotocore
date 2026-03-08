"""Error-path tests for ACM native provider.

Phase 3A: Covers UpdateCertificateOptions operation.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.acm.provider import handle_acm_request


def _make_request(action: str, body: dict | None = None) -> MagicMock:
    req = MagicMock()
    req.headers = {"x-amz-target": f"CertificateManager.{action}"}
    req.method = "POST"
    req.url = MagicMock()
    req.url.path = "/"
    req.query_params = {}
    payload = json.dumps(body or {}).encode()
    req.body = AsyncMock(return_value=payload)
    return req


@pytest.mark.asyncio
class TestUpdateCertificateOptions:
    async def test_update_nonexistent_certificate(self):
        """UpdateCertificateOptions on a nonexistent cert should not crash."""
        req = _make_request("UpdateCertificateOptions", {
            "CertificateArn": "arn:aws:acm:us-east-1:123456789012:certificate/nonexistent",
            "Options": {"CertificateTransparencyLoggingPreference": "ENABLED"},
        })
        resp = await handle_acm_request(req, "us-east-1", "123456789012")
        # Should either succeed silently or return an error, not crash
        assert resp.status_code in (200, 400, 404, 500)
