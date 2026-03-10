"""Tests for X-Ray anomaly detection and insights.

Covers rolling statistics, latency spike detection, error rate increase
detection, and the GetInsightSummaries provider endpoint.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.xray import provider as xray_provider
from robotocore.services.xray.provider import handle_xray_request
from robotocore.services.xray.trace_correlation import TraceCorrelationEngine, reset_engine


@pytest.fixture(autouse=True)
def _reset_state():
    xray_provider._sampling_rules.clear()
    xray_provider._groups.clear()
    xray_provider._tags.clear()
    xray_provider._encryption_config.clear()
    reset_engine()
    yield
    reset_engine()


def _make_request(method: str, path: str, body: dict | None = None) -> MagicMock:
    req = MagicMock()
    req.method = method
    req.url = MagicMock()
    req.url.path = path
    req.headers = {}
    req.query_params = {}
    payload = json.dumps(body or {}).encode() if body else b""
    req.body = AsyncMock(return_value=payload)
    return req


class TestAnomalyDetection:
    def test_no_anomaly_with_stable_latency(self):
        """Stable latency produces no insights."""
        engine = TraceCorrelationEngine()
        # 20 segments with consistent latency
        for i in range(20):
            engine.add_segment(
                {
                    "trace_id": f"t{i}",
                    "id": f"s{i}",
                    "name": "stable-svc",
                    "start_time": float(i),
                    "end_time": float(i) + 1.0,
                }
            )
        insights = engine.detect_anomalies(0, 100)
        assert len(insights) == 0

    def test_latency_spike_detected(self):
        """Sudden latency increase triggers LATENCY anomaly."""
        engine = TraceCorrelationEngine()
        # 20 normal segments (latency ~1.0)
        for i in range(20):
            engine.add_segment(
                {
                    "trace_id": f"t{i}",
                    "id": f"s{i}",
                    "name": "spike-svc",
                    "start_time": float(i),
                    "end_time": float(i) + 1.0,
                }
            )
        # 3 very slow segments (latency ~10.0)
        for i in range(20, 23):
            engine.add_segment(
                {
                    "trace_id": f"t{i}",
                    "id": f"s{i}",
                    "name": "spike-svc",
                    "start_time": float(i),
                    "end_time": float(i) + 10.0,
                }
            )
        insights = engine.detect_anomalies(0, 100)
        svc_insights = [i for i in insights if i["RootCauseServiceId"]["Name"] == "spike-svc"]
        assert len(svc_insights) == 1
        assert "LATENCY" in svc_insights[0]["Categories"]

    def test_error_rate_spike_detected(self):
        """Sudden error rate increase triggers ERROR_RATE anomaly."""
        engine = TraceCorrelationEngine()
        # 20 healthy segments
        for i in range(20):
            engine.add_segment(
                {
                    "trace_id": f"t{i}",
                    "id": f"s{i}",
                    "name": "err-svc",
                    "start_time": float(i),
                    "end_time": float(i) + 1.0,
                }
            )
        # 3 segments all with errors
        for i in range(20, 23):
            engine.add_segment(
                {
                    "trace_id": f"t{i}",
                    "id": f"s{i}",
                    "name": "err-svc",
                    "start_time": float(i),
                    "end_time": float(i) + 1.0,
                    "error": True,
                }
            )
        insights = engine.detect_anomalies(0, 100)
        svc_insights = [i for i in insights if i["RootCauseServiceId"]["Name"] == "err-svc"]
        assert len(svc_insights) >= 1
        categories = svc_insights[0]["Categories"]
        assert "ERROR_RATE" in categories

    def test_no_anomaly_with_too_few_samples(self):
        """Anomaly detection needs at least 5 samples."""
        engine = TraceCorrelationEngine()
        for i in range(3):
            engine.add_segment(
                {
                    "trace_id": f"t{i}",
                    "id": f"s{i}",
                    "name": "few-svc",
                    "start_time": 0,
                    "end_time": 100.0,  # Very high latency
                }
            )
        insights = engine.detect_anomalies(0, 200)
        assert len(insights) == 0

    def test_insight_structure(self):
        """Verify insight dict has all required AWS-compatible fields."""
        engine = TraceCorrelationEngine()
        # Create conditions for anomaly
        for i in range(20):
            engine.add_segment(
                {
                    "trace_id": f"t{i}",
                    "id": f"s{i}",
                    "name": "struct-svc",
                    "start_time": 0,
                    "end_time": 1.0,
                }
            )
        for i in range(20, 23):
            engine.add_segment(
                {
                    "trace_id": f"t{i}",
                    "id": f"s{i}",
                    "name": "struct-svc",
                    "start_time": 0,
                    "end_time": 50.0,
                }
            )
        insights = engine.detect_anomalies(0, 100)
        if insights:
            insight = insights[0]
            assert "InsightId" in insight
            assert "RootCauseServiceId" in insight
            assert "Categories" in insight
            assert "State" in insight
            assert insight["State"] == "ACTIVE"
            assert "Summary" in insight
            assert "TopAnomalousServices" in insight
            assert "ClientRequestImpactStatistics" in insight

    def test_rolling_window_bounded(self):
        """Rolling stats are bounded to max window size."""
        engine = TraceCorrelationEngine()
        for i in range(200):
            engine.add_segment(
                {
                    "trace_id": f"t{i}",
                    "id": f"s{i}",
                    "name": "bounded-svc",
                    "start_time": 0,
                    "end_time": 1.0,
                }
            )
        assert len(engine._rolling_stats["bounded-svc"]) == engine._max_rolling_window


@pytest.mark.asyncio
class TestInsightSummariesEndpoint:
    async def test_empty_insights(self):
        """GetInsightSummaries returns empty list with no data."""
        req = _make_request("POST", "/InsightSummaries", {"StartTime": 0, "EndTime": 100})
        resp = await handle_xray_request(req, "us-east-1", "123456789012")
        body = json.loads(resp.body)
        assert "InsightSummaries" in body
        assert isinstance(body["InsightSummaries"], list)

    async def test_insights_with_anomaly_data(self):
        """GetInsightSummaries returns insights when anomalies exist."""
        from robotocore.services.xray.trace_correlation import get_engine

        engine = get_engine()
        # Create latency anomaly
        for i in range(20):
            engine.add_segment(
                {
                    "trace_id": f"t{i}",
                    "id": f"s{i}",
                    "name": "anomaly-svc",
                    "start_time": float(i),
                    "end_time": float(i) + 1.0,
                }
            )
        for i in range(20, 23):
            engine.add_segment(
                {
                    "trace_id": f"t{i}",
                    "id": f"s{i}",
                    "name": "anomaly-svc",
                    "start_time": float(i),
                    "end_time": float(i) + 20.0,
                }
            )

        req = _make_request("POST", "/InsightSummaries", {"StartTime": 0, "EndTime": 100})
        resp = await handle_xray_request(req, "us-east-1", "123456789012")
        body = json.loads(resp.body)
        assert len(body["InsightSummaries"]) >= 1
