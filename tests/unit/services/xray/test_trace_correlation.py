"""Tests for X-Ray trace correlation engine.

Covers segment parsing, graph building, statistics computation,
and filter expression matching.
"""

import pytest

from robotocore.services.xray.trace_correlation import (
    EdgeStats,
    ServiceStats,
    TraceCorrelationEngine,
    _matches_filter,
)


@pytest.fixture
def engine():
    e = TraceCorrelationEngine()
    yield e
    e.clear()


def _seg(
    trace_id: str,
    seg_id: str,
    name: str,
    start: float,
    end: float,
    parent_id: str | None = None,
    error: bool = False,
    fault: bool = False,
    throttle: bool = False,
    http: dict | None = None,
    aws: dict | None = None,
    subsegments: list | None = None,
    origin: str = "",
) -> dict:
    """Build a minimal segment dict."""
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
    if throttle:
        seg["throttle"] = True
    if http:
        seg["http"] = http
    if aws:
        seg["aws"] = aws
    if subsegments:
        seg["subsegments"] = subsegments
    if origin:
        seg["origin"] = origin
    return seg


class TestServiceStats:
    def test_empty_stats(self):
        stats = ServiceStats()
        assert stats.request_count == 0
        assert stats.avg_latency == 0.0
        assert stats.percentile(50) == 0.0
        assert stats.error_rate == 0.0
        assert stats.fault_rate == 0.0
        assert stats.throttle_rate == 0.0

    def test_add_segment_accumulates(self):
        stats = ServiceStats()
        stats.add_segment({"start_time": 1.0, "end_time": 2.0})
        stats.add_segment({"start_time": 3.0, "end_time": 5.0})
        assert stats.request_count == 2
        assert stats.avg_latency == 1.5  # (1.0 + 2.0) / 2
        assert len(stats.latencies) == 2

    def test_error_fault_throttle_rates(self):
        stats = ServiceStats()
        stats.add_segment({"start_time": 0, "end_time": 1, "error": True})
        stats.add_segment({"start_time": 0, "end_time": 1, "fault": True})
        stats.add_segment({"start_time": 0, "end_time": 1, "throttle": True})
        stats.add_segment({"start_time": 0, "end_time": 1})
        assert stats.error_rate == 0.25
        assert stats.fault_rate == 0.25
        assert stats.throttle_rate == 0.25

    def test_percentile_single_value(self):
        stats = ServiceStats()
        stats.add_segment({"start_time": 0, "end_time": 0.5})
        assert stats.percentile(50) == 0.5
        assert stats.percentile(99) == 0.5

    def test_percentile_multiple_values(self):
        stats = ServiceStats()
        for i in range(100):
            stats.add_segment({"start_time": 0, "end_time": float(i + 1) / 100})
        p50 = stats.percentile(50)
        p99 = stats.percentile(99)
        assert 0.49 < p50 < 0.52
        assert 0.98 < p99 < 1.01

    def test_summary_statistics(self):
        stats = ServiceStats()
        stats.add_segment({"start_time": 0, "end_time": 1})
        stats.add_segment({"start_time": 0, "end_time": 2, "error": True})
        stats.add_segment({"start_time": 0, "end_time": 3, "fault": True})
        summary = stats.to_summary_statistics()
        assert summary["TotalCount"] == 3
        assert summary["OkCount"] == 1
        assert summary["ErrorStatistics"]["OtherCount"] == 1
        assert summary["FaultStatistics"]["TotalCount"] == 1
        assert summary["TotalResponseTime"] == 6.0

    def test_response_time_histogram_empty(self):
        stats = ServiceStats()
        assert stats.to_response_time_histogram() == []

    def test_response_time_histogram_single(self):
        stats = ServiceStats()
        stats.add_segment({"start_time": 0, "end_time": 0.5})
        hist = stats.to_response_time_histogram()
        assert len(hist) == 1
        assert hist[0]["Count"] == 1


class TestEdgeStats:
    def test_to_dict(self):
        edge = EdgeStats(source="svc-a", target="svc-b")
        edge.stats.add_segment({"start_time": 0, "end_time": 1})
        d = edge.to_dict()
        assert "SummaryStatistics" in d
        assert d["SummaryStatistics"]["TotalCount"] == 1


class TestTraceCorrelationEngine:
    def test_add_single_segment(self, engine):
        seg = _seg("trace-1", "seg-1", "frontend", 1.0, 2.0)
        engine.add_segment(seg)
        traces = engine.get_traces_in_range(0.0, 3.0)
        assert "trace-1" in traces
        assert len(traces["trace-1"]) == 1

    def test_add_ignores_missing_trace_id(self, engine):
        engine.add_segment({"id": "x", "name": "x", "start_time": 0, "end_time": 1})
        assert len(engine._traces) == 0

    def test_time_range_filtering(self, engine):
        engine.add_segment(_seg("t1", "s1", "svc", 10.0, 20.0))
        engine.add_segment(_seg("t2", "s2", "svc", 30.0, 40.0))
        # Only t1 overlaps [5, 25]
        traces = engine.get_traces_in_range(5.0, 25.0)
        assert "t1" in traces
        assert "t2" not in traces

    def test_subsegment_processing(self, engine):
        seg = _seg(
            "t1",
            "root",
            "frontend",
            1.0,
            3.0,
            subsegments=[
                {
                    "id": "sub1",
                    "name": "backend",
                    "start_time": 1.5,
                    "end_time": 2.5,
                }
            ],
        )
        engine.add_segment(seg)
        traces = engine.get_traces_in_range(0.0, 5.0)
        # Should have both root and subsegment
        assert len(traces["t1"]) == 2

    def test_build_service_graph_single_service(self, engine):
        engine.add_segment(_seg("t1", "s1", "api-gw", 1.0, 2.0))
        graph = engine.build_service_graph(0.0, 5.0)
        assert len(graph) == 1
        assert graph[0]["Name"] == "api-gw"
        assert graph[0]["Root"] is True
        assert graph[0]["SummaryStatistics"]["TotalCount"] == 1

    def test_build_service_graph_with_edges(self, engine):
        engine.add_segment(_seg("t1", "root", "frontend", 1.0, 3.0))
        engine.add_segment(_seg("t1", "child", "backend", 1.5, 2.5, parent_id="root"))
        graph = engine.build_service_graph(0.0, 5.0)
        assert len(graph) == 2

        frontend = next(n for n in graph if n["Name"] == "frontend")
        backend = next(n for n in graph if n["Name"] == "backend")
        assert frontend["Root"] is True
        assert backend["Root"] is False
        assert len(frontend["Edges"]) == 1
        assert frontend["Edges"][0]["SummaryStatistics"]["TotalCount"] == 1

    def test_build_service_graph_empty(self, engine):
        graph = engine.build_service_graph(0.0, 5.0)
        assert graph == []

    def test_build_service_graph_multiple_traces(self, engine):
        for i in range(5):
            engine.add_segment(_seg(f"t{i}", f"s{i}", "web", float(i), float(i + 1)))
        graph = engine.build_service_graph(0.0, 10.0)
        assert len(graph) == 1
        assert graph[0]["SummaryStatistics"]["TotalCount"] == 5

    def test_clear(self, engine):
        engine.add_segment(_seg("t1", "s1", "svc", 1.0, 2.0))
        engine.clear()
        assert len(engine._traces) == 0


class TestTraceSummaries:
    def test_basic_summary(self, engine):
        engine.add_segment(
            _seg(
                "t1",
                "s1",
                "web",
                100.0,
                101.5,
                http={"response": {"status": 200}},
            )
        )
        summaries = engine.get_trace_summaries(99.0, 102.0)
        assert len(summaries) == 1
        s = summaries[0]
        assert s["Id"] == "t1"
        assert s["Duration"] == pytest.approx(1.5)
        assert s["HasFault"] is False
        assert s["HasError"] is False
        assert s["Http"]["HttpStatus"] == 200

    def test_summary_with_fault(self, engine):
        engine.add_segment(_seg("t1", "s1", "web", 100.0, 101.0, fault=True))
        summaries = engine.get_trace_summaries(99.0, 102.0)
        assert summaries[0]["HasFault"] is True

    def test_summary_service_ids(self, engine):
        engine.add_segment(_seg("t1", "s1", "frontend", 100.0, 102.0))
        engine.add_segment(_seg("t1", "s2", "backend", 100.5, 101.5, parent_id="s1"))
        summaries = engine.get_trace_summaries(99.0, 103.0)
        svc_names = [s["Name"] for s in summaries[0]["ServiceIds"]]
        assert "frontend" in svc_names
        assert "backend" in svc_names

    def test_summary_entry_point(self, engine):
        engine.add_segment(_seg("t1", "root", "api", 1.0, 2.0))
        engine.add_segment(_seg("t1", "child", "db", 1.1, 1.9, parent_id="root"))
        summaries = engine.get_trace_summaries(0.0, 5.0)
        assert summaries[0]["EntryPoint"]["Name"] == "api"

    def test_empty_summaries(self, engine):
        summaries = engine.get_trace_summaries(0.0, 5.0)
        assert summaries == []


class TestFilterExpression:
    def test_service_filter_match(self):
        segments = [{"name": "my-svc"}]
        assert _matches_filter(segments, 'service("my-svc")') is True

    def test_service_filter_no_match(self):
        segments = [{"name": "other-svc"}]
        assert _matches_filter(segments, 'service("my-svc")') is False

    def test_responsetime_gt(self):
        segments = [{"start_time": 0, "end_time": 5.0}]
        assert _matches_filter(segments, "responsetime > 3") is True
        assert _matches_filter(segments, "responsetime > 10") is False

    def test_responsetime_lt(self):
        segments = [{"start_time": 0, "end_time": 2.0}]
        assert _matches_filter(segments, "responsetime < 3") is True

    def test_http_status_filter(self):
        segments = [{"http": {"response": {"status": 500}}}]
        assert _matches_filter(segments, "http.status = 500") is True
        assert _matches_filter(segments, "http.status = 200") is False

    def test_not_ok_filter(self):
        assert _matches_filter([{"error": True}], "!ok") is True
        assert _matches_filter([{"fault": True}], "!ok") is True
        assert _matches_filter([{"name": "ok-svc"}], "!ok") is False

    def test_unknown_filter_passes(self):
        assert _matches_filter([{"name": "x"}], "unknown_filter") is True

    def test_filter_with_summaries(self, engine):
        engine.add_segment(_seg("t1", "s1", "web", 1.0, 2.0))
        engine.add_segment(_seg("t2", "s2", "api", 1.0, 2.0))
        summaries = engine.get_trace_summaries(0.0, 5.0, 'service("web")')
        assert len(summaries) == 1
        assert summaries[0]["Id"] == "t1"
