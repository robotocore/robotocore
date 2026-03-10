"""Tests for X-Ray GetServiceGraph with real trace data.

Covers graph structure, time filtering, edge statistics, and
multi-service dependency chains.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.xray import provider as xray_provider
from robotocore.services.xray.provider import handle_xray_request
from robotocore.services.xray.trace_correlation import (
    TraceCorrelationEngine,
    reset_engine,
)


@pytest.fixture(autouse=True)
def _reset_state():
    """Reset provider and engine state between tests."""
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


def _seg_doc(
    trace_id: str,
    seg_id: str,
    name: str,
    start: float,
    end: float,
    parent_id: str | None = None,
    error: bool = False,
    fault: bool = False,
) -> str:
    """Create a JSON segment document string."""
    seg: dict = {
        "trace_id": trace_id,
        "id": seg_id,
        "name": name,
        "start_time": start,
        "end_time": end,
    }
    if parent_id:
        seg["parent_id"] = parent_id
    if error:
        seg["error"] = True
    if fault:
        seg["fault"] = True
    return json.dumps(seg)


@pytest.mark.asyncio
class TestServiceGraphViaProvider:
    async def test_empty_service_graph(self):
        req = _make_request("POST", "/ServiceGraph", {"StartTime": 0, "EndTime": 100})
        resp = await handle_xray_request(req, "us-east-1", "123456789012")
        body = json.loads(resp.body)
        assert body["Services"] == []

    async def test_put_segments_then_get_graph(self):
        """PutTraceSegments followed by GetServiceGraph returns populated graph."""
        # Put segments
        put_req = _make_request(
            "POST",
            "/TraceSegments",
            {
                "TraceSegmentDocuments": [
                    _seg_doc("t1", "root", "api-gateway", 100.0, 102.0),
                    _seg_doc("t1", "child", "lambda", 100.5, 101.5, parent_id="root"),
                ]
            },
        )
        put_resp = await handle_xray_request(put_req, "us-east-1", "123456789012")
        assert json.loads(put_resp.body)["UnprocessedTraceSegments"] == []

        # Get service graph
        graph_req = _make_request("POST", "/ServiceGraph", {"StartTime": 99.0, "EndTime": 103.0})
        resp = await handle_xray_request(graph_req, "us-east-1", "123456789012")
        body = json.loads(resp.body)

        services = body["Services"]
        assert len(services) == 2
        names = {s["Name"] for s in services}
        assert names == {"api-gateway", "lambda"}

    async def test_service_graph_has_edges(self):
        """Service graph edges connect parent to child services."""
        put_req = _make_request(
            "POST",
            "/TraceSegments",
            {
                "TraceSegmentDocuments": [
                    _seg_doc("t1", "gw", "gateway", 1.0, 3.0),
                    _seg_doc("t1", "svc", "service", 1.5, 2.5, parent_id="gw"),
                ]
            },
        )
        await handle_xray_request(put_req, "us-east-1", "123456789012")

        graph_req = _make_request("POST", "/ServiceGraph", {"StartTime": 0.0, "EndTime": 5.0})
        resp = await handle_xray_request(graph_req, "us-east-1", "123456789012")
        services = json.loads(resp.body)["Services"]

        gateway = next(s for s in services if s["Name"] == "gateway")
        assert len(gateway["Edges"]) == 1
        edge = gateway["Edges"][0]
        assert edge["SummaryStatistics"]["TotalCount"] == 1

    async def test_service_graph_root_detection(self):
        """Root services have Root=True, downstream have Root=False."""
        put_req = _make_request(
            "POST",
            "/TraceSegments",
            {
                "TraceSegmentDocuments": [
                    _seg_doc("t1", "a", "frontend", 1.0, 3.0),
                    _seg_doc("t1", "b", "backend", 1.5, 2.5, parent_id="a"),
                    _seg_doc("t1", "c", "database", 1.8, 2.2, parent_id="b"),
                ]
            },
        )
        await handle_xray_request(put_req, "us-east-1", "123456789012")

        graph_req = _make_request("POST", "/ServiceGraph", {"StartTime": 0.0, "EndTime": 5.0})
        resp = await handle_xray_request(graph_req, "us-east-1", "123456789012")
        services = json.loads(resp.body)["Services"]

        frontend = next(s for s in services if s["Name"] == "frontend")
        backend = next(s for s in services if s["Name"] == "backend")
        database = next(s for s in services if s["Name"] == "database")

        assert frontend["Root"] is True
        assert backend["Root"] is False
        assert database["Root"] is False

    async def test_service_graph_time_filtering(self):
        """Only segments within the time range appear in the graph."""
        put_req = _make_request(
            "POST",
            "/TraceSegments",
            {
                "TraceSegmentDocuments": [
                    _seg_doc("t1", "s1", "early-svc", 10.0, 20.0),
                    _seg_doc("t2", "s2", "late-svc", 50.0, 60.0),
                ]
            },
        )
        await handle_xray_request(put_req, "us-east-1", "123456789012")

        # Query only early range
        graph_req = _make_request("POST", "/ServiceGraph", {"StartTime": 5.0, "EndTime": 25.0})
        resp = await handle_xray_request(graph_req, "us-east-1", "123456789012")
        services = json.loads(resp.body)["Services"]
        names = {s["Name"] for s in services}
        assert "early-svc" in names
        assert "late-svc" not in names

    async def test_service_graph_error_statistics(self):
        """Error/fault segments are reflected in summary statistics."""
        put_req = _make_request(
            "POST",
            "/TraceSegments",
            {
                "TraceSegmentDocuments": [
                    _seg_doc("t1", "s1", "flaky-svc", 1.0, 2.0),
                    _seg_doc("t2", "s2", "flaky-svc", 2.0, 3.0, error=True),
                    _seg_doc("t3", "s3", "flaky-svc", 3.0, 4.0, fault=True),
                ]
            },
        )
        await handle_xray_request(put_req, "us-east-1", "123456789012")

        graph_req = _make_request("POST", "/ServiceGraph", {"StartTime": 0.0, "EndTime": 5.0})
        resp = await handle_xray_request(graph_req, "us-east-1", "123456789012")
        services = json.loads(resp.body)["Services"]
        svc = services[0]
        stats = svc["SummaryStatistics"]
        assert stats["TotalCount"] == 3
        assert stats["ErrorStatistics"]["OtherCount"] == 1
        assert stats["FaultStatistics"]["TotalCount"] == 1
        assert stats["OkCount"] == 1

    async def test_put_invalid_segment_returns_unprocessed(self):
        """Invalid JSON in segment doc is reported as unprocessed."""
        put_req = _make_request(
            "POST",
            "/TraceSegments",
            {"TraceSegmentDocuments": ["not-valid-json"]},
        )
        resp = await handle_xray_request(put_req, "us-east-1", "123456789012")
        body = json.loads(resp.body)
        assert len(body["UnprocessedTraceSegments"]) == 1
        assert body["UnprocessedTraceSegments"][0]["ErrorCode"] == "INVALID_DOCUMENT"

    async def test_service_graph_contains_old_group_versions(self):
        """Response includes ContainsOldGroupVersions field."""
        graph_req = _make_request("POST", "/ServiceGraph", {"StartTime": 0.0, "EndTime": 5.0})
        resp = await handle_xray_request(graph_req, "us-east-1", "123456789012")
        body = json.loads(resp.body)
        assert "ContainsOldGroupVersions" in body
        assert body["ContainsOldGroupVersions"] is False


class TestServiceGraphEngine:
    """Direct engine tests for graph building edge cases."""

    def test_same_service_parent_child_no_self_edge(self):
        """When parent and child have same name, no self-edge is created."""
        engine = TraceCorrelationEngine()
        engine.add_segment(
            {"trace_id": "t1", "id": "a", "name": "svc", "start_time": 1, "end_time": 3}
        )
        engine.add_segment(
            {
                "trace_id": "t1",
                "id": "b",
                "name": "svc",
                "start_time": 1.5,
                "end_time": 2.5,
                "parent_id": "a",
            }
        )
        graph = engine.build_service_graph(0.0, 5.0)
        assert len(graph) == 1
        assert graph[0]["Edges"] == []

    def test_multiple_edges_between_services(self):
        """Multiple traces create aggregated edge statistics."""
        engine = TraceCorrelationEngine()
        for i in range(10):
            engine.add_segment(
                {
                    "trace_id": f"t{i}",
                    "id": f"root{i}",
                    "name": "web",
                    "start_time": float(i),
                    "end_time": float(i + 1),
                }
            )
            engine.add_segment(
                {
                    "trace_id": f"t{i}",
                    "id": f"child{i}",
                    "name": "db",
                    "start_time": float(i) + 0.1,
                    "end_time": float(i) + 0.9,
                    "parent_id": f"root{i}",
                }
            )
        graph = engine.build_service_graph(0.0, 20.0)
        web = next(s for s in graph if s["Name"] == "web")
        assert len(web["Edges"]) == 1
        assert web["Edges"][0]["SummaryStatistics"]["TotalCount"] == 10

    def test_graph_reference_ids_are_consistent(self):
        """Edge ReferenceId matches the target service's ReferenceId."""
        engine = TraceCorrelationEngine()
        engine.add_segment(
            {"trace_id": "t1", "id": "a", "name": "alpha", "start_time": 1, "end_time": 2}
        )
        engine.add_segment(
            {
                "trace_id": "t1",
                "id": "b",
                "name": "beta",
                "start_time": 1.1,
                "end_time": 1.9,
                "parent_id": "a",
            }
        )
        graph = engine.build_service_graph(0.0, 5.0)
        alpha = next(s for s in graph if s["Name"] == "alpha")
        beta = next(s for s in graph if s["Name"] == "beta")
        # Edge from alpha should reference beta's ID
        assert alpha["Edges"][0]["ReferenceId"] == beta["ReferenceId"]
