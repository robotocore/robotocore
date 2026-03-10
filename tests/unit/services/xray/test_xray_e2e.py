"""End-to-end tests for X-Ray service map correlation.

Full flow: PutTraceSegments -> GetServiceGraph -> GetTraceSummaries -> verify.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.xray import provider as xray_provider
from robotocore.services.xray.provider import handle_xray_request
from robotocore.services.xray.trace_correlation import reset_engine


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


def _seg(trace_id, seg_id, name, start, end, parent_id=None, **kwargs):
    seg = {
        "trace_id": trace_id,
        "id": seg_id,
        "name": name,
        "start_time": start,
        "end_time": end,
    }
    if parent_id:
        seg["parent_id"] = parent_id
    seg.update(kwargs)
    return json.dumps(seg)


@pytest.mark.asyncio
class TestE2EServiceMapFlow:
    async def test_full_microservice_trace(self):
        """Simulate a microservice architecture trace and verify the service map.

        Architecture: API Gateway -> Lambda -> DynamoDB
        """
        t = "1-67890abc-111111111111111111111111"
        base = 1000.0

        # Put all segments
        put_req = _make_request(
            "POST",
            "/TraceSegments",
            {
                "TraceSegmentDocuments": [
                    _seg(t, "gw", "api-gateway", base, base + 2.0),
                    _seg(t, "fn", "lambda-func", base + 0.1, base + 1.8, parent_id="gw"),
                    _seg(
                        t,
                        "db",
                        "dynamodb",
                        base + 0.3,
                        base + 1.0,
                        parent_id="fn",
                    ),
                ]
            },
        )
        put_resp = await handle_xray_request(put_req, "us-east-1", "123456789012")
        assert json.loads(put_resp.body)["UnprocessedTraceSegments"] == []

        # Get service graph
        graph_req = _make_request(
            "POST",
            "/ServiceGraph",
            {"StartTime": base - 1, "EndTime": base + 5},
        )
        graph_resp = await handle_xray_request(graph_req, "us-east-1", "123456789012")
        graph = json.loads(graph_resp.body)

        services = graph["Services"]
        assert len(services) == 3
        names = {s["Name"] for s in services}
        assert names == {"api-gateway", "lambda-func", "dynamodb"}

        # Verify edges: gw -> lambda, lambda -> dynamodb
        gw = next(s for s in services if s["Name"] == "api-gateway")
        assert gw["Root"] is True
        assert len(gw["Edges"]) == 1

        lam = next(s for s in services if s["Name"] == "lambda-func")
        assert lam["Root"] is False
        assert len(lam["Edges"]) == 1

        ddb = next(s for s in services if s["Name"] == "dynamodb")
        assert ddb["Root"] is False
        assert len(ddb["Edges"]) == 0

        # Get trace summaries
        summary_req = _make_request(
            "POST",
            "/TraceSummaries",
            {"StartTime": base - 1, "EndTime": base + 5},
        )
        summary_resp = await handle_xray_request(summary_req, "us-east-1", "123456789012")
        summaries = json.loads(summary_resp.body)

        assert len(summaries["TraceSummaries"]) == 1
        trace_summary = summaries["TraceSummaries"][0]
        assert trace_summary["Id"] == t
        assert trace_summary["Duration"] == pytest.approx(2.0)
        assert trace_summary["HasFault"] is False

    async def test_multiple_traces_aggregate(self):
        """Multiple traces for the same services aggregate statistics."""
        base = 2000.0

        docs = []
        for i in range(5):
            tid = f"1-67890abc-{i:024d}"
            offset = float(i * 10)
            docs.extend(
                [
                    _seg(tid, f"fe{i}", "frontend", base + offset, base + offset + 2),
                    _seg(
                        tid,
                        f"be{i}",
                        "backend",
                        base + offset + 0.2,
                        base + offset + 1.8,
                        parent_id=f"fe{i}",
                    ),
                ]
            )

        put_req = _make_request("POST", "/TraceSegments", {"TraceSegmentDocuments": docs})
        await handle_xray_request(put_req, "us-east-1", "123456789012")

        graph_req = _make_request(
            "POST",
            "/ServiceGraph",
            {"StartTime": base - 1, "EndTime": base + 100},
        )
        resp = await handle_xray_request(graph_req, "us-east-1", "123456789012")
        services = json.loads(resp.body)["Services"]

        frontend = next(s for s in services if s["Name"] == "frontend")
        assert frontend["SummaryStatistics"]["TotalCount"] == 5
        assert len(frontend["Edges"]) == 1
        assert frontend["Edges"][0]["SummaryStatistics"]["TotalCount"] == 5

    async def test_trace_with_errors_reflected(self):
        """Traces with errors are reflected in summaries and graph."""
        base = 3000.0
        t = "1-67890abc-222222222222222222222222"

        # The http field needs to be a dict not a string in the segment
        seg_dict = {
            "trace_id": t,
            "id": "web",
            "name": "web-app",
            "start_time": base,
            "end_time": base + 1.0,
            "error": True,
            "http": {"response": {"status": 500}},
        }
        put_req = _make_request(
            "POST",
            "/TraceSegments",
            {"TraceSegmentDocuments": [json.dumps(seg_dict)]},
        )
        await handle_xray_request(put_req, "us-east-1", "123456789012")

        # Check trace summary
        summary_req = _make_request(
            "POST",
            "/TraceSummaries",
            {"StartTime": base - 1, "EndTime": base + 5},
        )
        resp = await handle_xray_request(summary_req, "us-east-1", "123456789012")
        summaries = json.loads(resp.body)["TraceSummaries"]
        assert len(summaries) == 1
        assert summaries[0]["HasError"] is True
        assert summaries[0]["Http"]["HttpStatus"] == 500

        # Check service graph
        graph_req = _make_request(
            "POST",
            "/ServiceGraph",
            {"StartTime": base - 1, "EndTime": base + 5},
        )
        resp = await handle_xray_request(graph_req, "us-east-1", "123456789012")
        services = json.loads(resp.body)["Services"]
        web = services[0]
        assert web["SummaryStatistics"]["ErrorStatistics"]["OtherCount"] == 1

    async def test_filter_expression_in_trace_summaries(self):
        """TraceSummaries supports FilterExpression to narrow results."""
        base = 4000.0

        put_req = _make_request(
            "POST",
            "/TraceSegments",
            {
                "TraceSegmentDocuments": [
                    _seg("t-a", "s1", "service-a", base, base + 1),
                    _seg("t-b", "s2", "service-b", base, base + 1),
                ]
            },
        )
        await handle_xray_request(put_req, "us-east-1", "123456789012")

        # Filter for service-a only
        req = _make_request(
            "POST",
            "/TraceSummaries",
            {
                "StartTime": base - 1,
                "EndTime": base + 5,
                "FilterExpression": 'service("service-a")',
            },
        )
        resp = await handle_xray_request(req, "us-east-1", "123456789012")
        summaries = json.loads(resp.body)["TraceSummaries"]
        assert len(summaries) == 1
        assert summaries[0]["Id"] == "t-a"

    async def test_subsegments_appear_in_graph(self):
        """Subsegments create service nodes and edges in the graph."""
        base = 5000.0
        seg_dict = {
            "trace_id": "t-sub",
            "id": "root",
            "name": "main-app",
            "start_time": base,
            "end_time": base + 3.0,
            "subsegments": [
                {
                    "id": "sub1",
                    "name": "cache-client",
                    "start_time": base + 0.5,
                    "end_time": base + 1.0,
                },
                {
                    "id": "sub2",
                    "name": "db-client",
                    "start_time": base + 1.0,
                    "end_time": base + 2.5,
                },
            ],
        }

        put_req = _make_request(
            "POST",
            "/TraceSegments",
            {"TraceSegmentDocuments": [json.dumps(seg_dict)]},
        )
        await handle_xray_request(put_req, "us-east-1", "123456789012")

        graph_req = _make_request(
            "POST",
            "/ServiceGraph",
            {"StartTime": base - 1, "EndTime": base + 5},
        )
        resp = await handle_xray_request(graph_req, "us-east-1", "123456789012")
        services = json.loads(resp.body)["Services"]
        names = {s["Name"] for s in services}
        assert "main-app" in names
        assert "cache-client" in names
        assert "db-client" in names

        # main-app should have edges to both subsegment services
        main = next(s for s in services if s["Name"] == "main-app")
        assert len(main["Edges"]) == 2

    async def test_approximate_time_in_summaries(self):
        """TraceSummaries response always includes ApproximateTime."""
        req = _make_request(
            "POST",
            "/TraceSummaries",
            {"StartTime": 0, "EndTime": 100},
        )
        resp = await handle_xray_request(req, "us-east-1", "123456789012")
        body = json.loads(resp.body)
        assert "ApproximateTime" in body
        assert isinstance(body["ApproximateTime"], float)
