"""Advanced tests for X-Ray trace correlation engine."""

from robotocore.services.xray.trace_correlation import (
    ServiceStats,
    TraceCorrelationEngine,
    _matches_filter,
)


def _seg(
    trace_id="1-abc-def",
    seg_id="seg1",
    name="ServiceA",
    start_time=1000.0,
    end_time=1001.0,
    parent_id=None,
    error=False,
    fault=False,
    throttle=False,
    http_status=None,
    subsegments=None,
    origin="",
    aws=None,
):
    """Build a trace segment dict."""
    s = {
        "trace_id": trace_id,
        "id": seg_id,
        "name": name,
        "start_time": start_time,
        "end_time": end_time,
        "error": error,
        "fault": fault,
        "throttle": throttle,
    }
    if parent_id:
        s["parent_id"] = parent_id
    if http_status is not None:
        s["http"] = {"response": {"status": http_status}}
    if subsegments:
        s["subsegments"] = subsegments
    if origin:
        s["origin"] = origin
    if aws:
        s["aws"] = aws
    return s


class TestComplexServiceGraph:
    """Complex service graph: A -> B -> C -> D with fan-out."""

    def test_linear_chain_a_b_c_d(self):
        engine = TraceCorrelationEngine()
        t = "1-trace-001"
        engine.add_segment(_seg(t, "s1", "A", 1000, 1004))
        engine.add_segment(_seg(t, "s2", "B", 1000.5, 1003.5, parent_id="s1"))
        engine.add_segment(_seg(t, "s3", "C", 1001, 1003, parent_id="s2"))
        engine.add_segment(_seg(t, "s4", "D", 1001.5, 1002.5, parent_id="s3"))

        graph = engine.build_service_graph(999, 1005)
        names = {n["Name"] for n in graph}
        assert names == {"A", "B", "C", "D"}

        # A should be root (not a target of any edge)
        a_node = next(n for n in graph if n["Name"] == "A")
        assert a_node["Root"] is True

        # D should not be root
        d_node = next(n for n in graph if n["Name"] == "D")
        assert d_node["Root"] is False

        # A should have edge to B
        a_edge_targets = {
            next(n["Name"] for n in graph if n["ReferenceId"] == e["ReferenceId"])
            for e in a_node["Edges"]
        }
        assert "B" in a_edge_targets

    def test_fan_out_a_to_b_and_c(self):
        engine = TraceCorrelationEngine()
        t = "1-trace-002"
        engine.add_segment(_seg(t, "s1", "Gateway", 1000, 1005))
        engine.add_segment(_seg(t, "s2", "ServiceB", 1001, 1003, parent_id="s1"))
        engine.add_segment(_seg(t, "s3", "ServiceC", 1001, 1004, parent_id="s1"))
        engine.add_segment(_seg(t, "s4", "ServiceD", 1002, 1003, parent_id="s3"))

        graph = engine.build_service_graph(999, 1006)
        gateway = next(n for n in graph if n["Name"] == "Gateway")
        assert gateway["Root"] is True
        # Gateway should have edges to both B and C
        assert len(gateway["Edges"]) == 2


class TestLatencyPercentileAccuracy:
    """Latency percentile accuracy (p50, p99)."""

    def test_p50_of_sorted_latencies(self):
        stats = ServiceStats()
        # Add segments with known latencies: 1.0, 2.0, 3.0, 4.0, 5.0
        for i in range(1, 6):
            stats.add_segment({"start_time": 0, "end_time": float(i)})
        assert stats.percentile(50) == 3.0

    def test_p99_skews_high(self):
        stats = ServiceStats()
        # 10 segments: 9 fast (0.1s), 1 slow (10s)
        for _ in range(9):
            stats.add_segment({"start_time": 0, "end_time": 0.1})
        stats.add_segment({"start_time": 0, "end_time": 10.0})
        p99 = stats.percentile(99)
        # With 10 values, p99 index = 0.99 * 9 = 8.91, interpolating between
        # index 8 (0.1) and index 9 (10.0), so p99 should be close to 10.0
        assert p99 > 9.0

    def test_p0_is_minimum(self):
        stats = ServiceStats()
        for v in [3.0, 1.0, 5.0, 2.0, 4.0]:
            stats.add_segment({"start_time": 0, "end_time": v})
        assert stats.percentile(0) == 1.0

    def test_p100_is_maximum(self):
        stats = ServiceStats()
        for v in [3.0, 1.0, 5.0, 2.0, 4.0]:
            stats.add_segment({"start_time": 0, "end_time": v})
        assert stats.percentile(100) == 5.0

    def test_percentile_empty_returns_zero(self):
        stats = ServiceStats()
        assert stats.percentile(50) == 0.0

    def test_single_latency_all_percentiles_equal(self):
        stats = ServiceStats()
        stats.add_segment({"start_time": 0, "end_time": 2.5})
        assert stats.percentile(0) == 2.5
        assert stats.percentile(50) == 2.5
        assert stats.percentile(100) == 2.5


class TestErrorRateCalculation:
    """Error rate calculation across many traces."""

    def test_error_rate_50_percent(self):
        stats = ServiceStats()
        for i in range(10):
            stats.add_segment({"start_time": 0, "end_time": 1, "error": i % 2 == 0})
        assert stats.error_rate == 0.5

    def test_fault_rate(self):
        stats = ServiceStats()
        for i in range(4):
            stats.add_segment({"start_time": 0, "end_time": 1, "fault": i == 0})
        assert stats.fault_rate == 0.25

    def test_throttle_rate(self):
        stats = ServiceStats()
        for _ in range(10):
            stats.add_segment({"start_time": 0, "end_time": 1, "throttle": False})
        stats.add_segment({"start_time": 0, "end_time": 1, "throttle": True})
        assert abs(stats.throttle_rate - 1 / 11) < 0.001

    def test_zero_requests_zero_rates(self):
        stats = ServiceStats()
        assert stats.error_rate == 0.0
        assert stats.fault_rate == 0.0
        assert stats.throttle_rate == 0.0

    def test_summary_statistics_counts(self):
        stats = ServiceStats()
        stats.add_segment({"start_time": 0, "end_time": 1, "error": True})
        stats.add_segment({"start_time": 0, "end_time": 1, "fault": True})
        stats.add_segment({"start_time": 0, "end_time": 1})
        summary = stats.to_summary_statistics()
        assert summary["TotalCount"] == 3
        assert summary["ErrorStatistics"]["OtherCount"] == 1
        assert summary["FaultStatistics"]["TotalCount"] == 1
        assert summary["OkCount"] == 1


class TestTimeRangeFiltering:
    """Time range filtering: only segments within window."""

    def test_only_segments_in_range_returned(self):
        engine = TraceCorrelationEngine()
        engine.add_segment(_seg("t1", "s1", "A", 1000, 1001))
        engine.add_segment(_seg("t2", "s2", "B", 2000, 2001))
        engine.add_segment(_seg("t3", "s3", "C", 3000, 3001))

        traces = engine.get_traces_in_range(1500, 2500)
        assert "t2" in traces
        assert "t1" not in traces
        assert "t3" not in traces

    def test_overlapping_segments_included(self):
        engine = TraceCorrelationEngine()
        engine.add_segment(_seg("t1", "s1", "A", 900, 1100))  # overlaps [1000, 1200]
        traces = engine.get_traces_in_range(1000, 1200)
        assert "t1" in traces

    def test_empty_range_returns_empty(self):
        engine = TraceCorrelationEngine()
        engine.add_segment(_seg("t1", "s1", "A", 1000, 1001))
        traces = engine.get_traces_in_range(5000, 6000)
        assert len(traces) == 0

    def test_service_graph_respects_time_range(self):
        engine = TraceCorrelationEngine()
        engine.add_segment(_seg("t1", "s1", "InRange", 1000, 1001))
        engine.add_segment(_seg("t2", "s2", "OutOfRange", 5000, 5001))
        graph = engine.build_service_graph(999, 1002)
        names = {n["Name"] for n in graph}
        assert "InRange" in names
        assert "OutOfRange" not in names


class TestFilterExpressions:
    """Filter expressions: service(), responsetime, http.status."""

    def test_service_filter_matches(self):
        segs = [_seg(name="MyService")]
        assert _matches_filter(segs, 'service("MyService")') is True

    def test_service_filter_no_match(self):
        segs = [_seg(name="OtherService")]
        assert _matches_filter(segs, 'service("MyService")') is False

    def test_responsetime_greater_than(self):
        segs = [_seg(start_time=0, end_time=5.0)]
        assert _matches_filter(segs, "responsetime > 3") is True
        assert _matches_filter(segs, "responsetime > 10") is False

    def test_responsetime_less_than(self):
        segs = [_seg(start_time=0, end_time=2.0)]
        assert _matches_filter(segs, "responsetime < 5") is True
        assert _matches_filter(segs, "responsetime < 1") is False

    def test_http_status_filter(self):
        segs = [_seg(http_status=500)]
        assert _matches_filter(segs, "http.status = 500") is True
        assert _matches_filter(segs, "http.status = 200") is False

    def test_not_ok_filter(self):
        error_segs = [_seg(error=True)]
        ok_segs = [_seg()]
        assert _matches_filter(error_segs, "!ok") is True
        assert _matches_filter(ok_segs, "!ok") is False

    def test_filter_with_trace_summaries(self):
        engine = TraceCorrelationEngine()
        engine.add_segment(_seg("t1", "s1", "Fast", 0, 1.0))
        engine.add_segment(_seg("t2", "s2", "Slow", 0, 10.0))

        summaries = engine.get_trace_summaries(0, 11, "responsetime > 5")
        trace_ids = {s["Id"] for s in summaries}
        assert "t2" in trace_ids
        assert "t1" not in trace_ids


class TestLargeTraceHandling:
    """Large trace handling (100+ segments)."""

    def test_100_segments_in_single_trace(self):
        engine = TraceCorrelationEngine()
        trace_id = "1-large-trace"
        for i in range(100):
            engine.add_segment(
                _seg(
                    trace_id=trace_id,
                    seg_id=f"s{i}",
                    name=f"svc-{i % 10}",
                    start_time=1000 + i * 0.1,
                    end_time=1000 + i * 0.1 + 0.05,
                    parent_id=f"s{i - 1}" if i > 0 else None,
                )
            )

        graph = engine.build_service_graph(999, 1100)
        # Should have 10 distinct service names (svc-0 through svc-9)
        names = {n["Name"] for n in graph}
        assert len(names) == 10

    def test_many_traces_graph_aggregation(self):
        engine = TraceCorrelationEngine()
        for t in range(50):
            tid = f"1-trace-{t:03d}"
            engine.add_segment(_seg(tid, f"root-{t}", "Frontend", 1000 + t, 1001 + t))
            engine.add_segment(
                _seg(
                    tid,
                    f"back-{t}",
                    "Backend",
                    1000.2 + t,
                    1000.8 + t,
                    parent_id=f"root-{t}",
                )
            )

        graph = engine.build_service_graph(999, 1100)
        fe = next(n for n in graph if n["Name"] == "Frontend")
        be = next(n for n in graph if n["Name"] == "Backend")
        assert fe["SummaryStatistics"]["TotalCount"] == 50
        assert be["SummaryStatistics"]["TotalCount"] == 50
        assert fe["Root"] is True
        assert be["Root"] is False

    def test_subsegments_auto_inherit_trace_id(self):
        engine = TraceCorrelationEngine()
        engine.add_segment(
            _seg(
                "1-parent",
                "root",
                "Gateway",
                1000,
                1002,
                subsegments=[
                    {
                        "id": "sub1",
                        "name": "DB",
                        "start_time": 1000.1,
                        "end_time": 1001.9,
                    }
                ],
            )
        )
        traces = engine.get_traces_in_range(999, 1003)
        assert "1-parent" in traces
        # Both root segment and subsegment should be stored
        assert len(traces["1-parent"]) == 2

    def test_histogram_with_varied_latencies(self):
        stats = ServiceStats()
        for i in range(100):
            stats.add_segment({"start_time": 0, "end_time": (i + 1) * 0.01})
        histogram = stats.to_response_time_histogram()
        assert len(histogram) > 0
        total_count = sum(b["Count"] for b in histogram)
        assert total_count == 100


class TestTraceSummaries:
    """Trace summary generation."""

    def test_summary_has_fault_error_flags(self):
        engine = TraceCorrelationEngine()
        engine.add_segment(_seg("t1", "s1", "A", 1000, 1001, fault=True))
        engine.add_segment(_seg("t2", "s2", "B", 1000, 1001, error=True))
        engine.add_segment(_seg("t3", "s3", "C", 1000, 1001))

        summaries = engine.get_trace_summaries(999, 1002)
        by_id = {s["Id"]: s for s in summaries}
        assert by_id["t1"]["HasFault"] is True
        assert by_id["t2"]["HasError"] is True
        assert by_id["t3"]["HasFault"] is False
        assert by_id["t3"]["HasError"] is False

    def test_summary_http_status(self):
        engine = TraceCorrelationEngine()
        engine.add_segment(_seg("t1", "s1", "A", 1000, 1001, http_status=200))
        summaries = engine.get_trace_summaries(999, 1002)
        assert summaries[0]["Http"]["HttpStatus"] == 200

    def test_summary_duration(self):
        engine = TraceCorrelationEngine()
        engine.add_segment(_seg("t1", "s1", "A", 1000, 1005))
        summaries = engine.get_trace_summaries(999, 1006)
        assert summaries[0]["Duration"] == 5.0
