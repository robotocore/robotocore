"""X-Ray Trace Correlation Engine.

Parses trace segments, builds service dependency graphs, and computes
per-service statistics (latency percentiles, error/fault/throttle rates).
"""

import math
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ServiceStats:
    """Accumulated statistics for a service."""

    request_count: int = 0
    error_count: int = 0
    fault_count: int = 0
    throttle_count: int = 0
    latencies: list[float] = field(default_factory=list)

    def add_segment(self, segment: dict[str, Any]) -> None:
        """Accumulate stats from a parsed segment."""
        self.request_count += 1
        duration = segment.get("end_time", 0) - segment.get("start_time", 0)
        if duration > 0:
            self.latencies.append(duration)
        if segment.get("error"):
            self.error_count += 1
        if segment.get("fault"):
            self.fault_count += 1
        if segment.get("throttle"):
            self.throttle_count += 1

    @property
    def avg_latency(self) -> float:
        if not self.latencies:
            return 0.0
        return statistics.mean(self.latencies)

    def percentile(self, p: float) -> float:
        """Compute pth percentile of latencies (0-100 scale)."""
        if not self.latencies:
            return 0.0
        sorted_lats = sorted(self.latencies)
        k = (p / 100.0) * (len(sorted_lats) - 1)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_lats[int(k)]
        return sorted_lats[f] * (c - k) + sorted_lats[c] * (k - f)

    @property
    def error_rate(self) -> float:
        return self.error_count / self.request_count if self.request_count else 0.0

    @property
    def fault_rate(self) -> float:
        return self.fault_count / self.request_count if self.request_count else 0.0

    @property
    def throttle_rate(self) -> float:
        return self.throttle_count / self.request_count if self.request_count else 0.0

    def to_summary_statistics(self) -> dict[str, Any]:
        """Return AWS-compatible SummaryStatistics dict."""
        return {
            "OkCount": self.request_count - self.error_count - self.fault_count,
            "ErrorStatistics": {
                "ThrottleCount": self.throttle_count,
                "OtherCount": self.error_count,
                "TotalCount": self.error_count + self.throttle_count,
            },
            "FaultStatistics": {
                "OtherCount": self.fault_count,
                "TotalCount": self.fault_count,
            },
            "TotalCount": self.request_count,
            "TotalResponseTime": sum(self.latencies),
        }

    def to_response_time_histogram(self) -> list[dict[str, Any]]:
        """Return AWS-compatible ResponseTimeRootCauseIndex histogram."""
        if not self.latencies:
            return []
        # Build 10-bucket histogram
        sorted_lats = sorted(self.latencies)
        min_lat = sorted_lats[0]
        max_lat = sorted_lats[-1]
        if min_lat == max_lat:
            return [{"Value": min_lat, "Count": len(sorted_lats)}]
        bucket_size = (max_lat - min_lat) / 10
        buckets: list[dict[str, Any]] = []
        for i in range(10):
            lo = min_lat + i * bucket_size
            hi = lo + bucket_size
            count = sum(1 for lat in sorted_lats if lo <= lat < hi or (i == 9 and lat == hi))
            if count > 0:
                buckets.append({"Value": lo + bucket_size / 2, "Count": count})
        return buckets


@dataclass
class EdgeStats:
    """Statistics for an edge between two services."""

    source: str
    target: str
    stats: ServiceStats = field(default_factory=ServiceStats)

    def to_dict(self) -> dict[str, Any]:
        """Convert to AWS-compatible edge dict."""
        return {
            "ReferenceId": 0,  # Set externally
            "StartTime": 0.0,
            "EndTime": 0.0,
            "SummaryStatistics": self.stats.to_summary_statistics(),
            "ResponseTimeHistogram": self.stats.to_response_time_histogram(),
            "Aliases": [],
        }


def _detect_service_type(segment: dict[str, Any]) -> str:
    """Detect service type from segment metadata."""
    aws_info = segment.get("aws", {})
    origin = segment.get("origin", "")

    if origin:
        return origin
    if "operation" in aws_info:
        # AWS SDK call
        service_name = aws_info.get("resource", {}).get("service", "")
        if service_name:
            return f"AWS::{service_name}"
    if segment.get("http", {}).get("request", {}).get("url"):
        return "client"
    return "AWS::Service"


class TraceCorrelationEngine:
    """Builds service dependency graphs and computes statistics from trace segments."""

    def __init__(self) -> None:
        # trace_id -> list of segments
        self._traces: dict[str, list[dict[str, Any]]] = defaultdict(list)
        # Rolling stats per service for anomaly detection
        self._rolling_stats: dict[str, list[float]] = defaultdict(list)
        self._rolling_error_rates: dict[str, list[float]] = defaultdict(list)
        self._max_rolling_window = 100

    def add_segment(self, segment: dict[str, Any]) -> None:
        """Add a parsed trace segment."""
        trace_id = segment.get("trace_id", "")
        if not trace_id:
            return
        self._traces[trace_id].append(segment)

        # Update rolling stats for anomaly detection
        name = segment.get("name", "unknown")
        duration = segment.get("end_time", 0) - segment.get("start_time", 0)
        if duration > 0:
            rolling = self._rolling_stats[name]
            rolling.append(duration)
            if len(rolling) > self._max_rolling_window:
                rolling.pop(0)

        has_error = 1.0 if (segment.get("error") or segment.get("fault")) else 0.0
        error_rolling = self._rolling_error_rates[name]
        error_rolling.append(has_error)
        if len(error_rolling) > self._max_rolling_window:
            error_rolling.pop(0)

        # Process subsegments recursively
        for subseg in segment.get("subsegments", []):
            subseg.setdefault("trace_id", trace_id)
            subseg.setdefault("parent_id", segment.get("id"))
            self.add_segment(subseg)

    def add_segments(self, segments: list[dict[str, Any]]) -> None:
        """Add multiple segments."""
        for seg in segments:
            self.add_segment(seg)

    def get_traces_in_range(
        self, start_time: float, end_time: float
    ) -> dict[str, list[dict[str, Any]]]:
        """Return traces that overlap the given time range."""
        result: dict[str, list[dict[str, Any]]] = {}
        for trace_id, segments in self._traces.items():
            matching = [
                seg
                for seg in segments
                if seg.get("start_time", 0) <= end_time
                and seg.get("end_time", float("inf")) >= start_time
            ]
            if matching:
                result[trace_id] = matching
        return result

    def build_service_graph(self, start_time: float, end_time: float) -> list[dict[str, Any]]:
        """Build AWS-compatible service graph for a time range.

        Returns a list of service nodes, each with edges to downstream services.
        """
        traces = self.get_traces_in_range(start_time, end_time)

        # Collect per-service stats and edges
        service_stats: dict[str, ServiceStats] = defaultdict(ServiceStats)
        # (source, target) -> EdgeStats
        edges: dict[tuple[str, str], EdgeStats] = {}
        # service -> type
        service_types: dict[str, str] = {}
        # segment_id -> service name (for parent lookup)
        segment_service_map: dict[str, str] = {}

        for segments in traces.values():
            # Build segment ID -> name map first
            for seg in segments:
                seg_id = seg.get("id", "")
                seg_name = seg.get("name", "unknown")
                if seg_id:
                    segment_service_map[seg_id] = seg_name

            for seg in segments:
                seg_name = seg.get("name", "unknown")
                service_stats[seg_name].add_segment(seg)
                service_types.setdefault(seg_name, _detect_service_type(seg))

                # If this segment has a parent, create an edge
                parent_id = seg.get("parent_id")
                if parent_id and parent_id in segment_service_map:
                    parent_name = segment_service_map[parent_id]
                    if parent_name != seg_name:
                        edge_key = (parent_name, seg_name)
                        if edge_key not in edges:
                            edges[edge_key] = EdgeStats(source=parent_name, target=seg_name)
                        edges[edge_key].stats.add_segment(seg)

        # Build service nodes
        services: list[dict[str, Any]] = []
        name_to_id: dict[str, int] = {}
        for i, name in enumerate(service_stats):
            name_to_id[name] = i

        for name, stats in service_stats.items():
            ref_id = name_to_id[name]
            svc_edges = []
            for (src, tgt), edge in edges.items():
                if src == name:
                    edge_dict = edge.to_dict()
                    edge_dict["ReferenceId"] = name_to_id.get(tgt, 0)
                    edge_dict["StartTime"] = start_time
                    edge_dict["EndTime"] = end_time
                    svc_edges.append(edge_dict)

            node: dict[str, Any] = {
                "ReferenceId": ref_id,
                "Name": name,
                "Names": [name],
                "Root": not any(tgt == name for (_, tgt) in edges),
                "AccountId": None,
                "Type": service_types.get(name, "AWS::Service"),
                "State": "active",
                "StartTime": start_time,
                "EndTime": end_time,
                "Edges": svc_edges,
                "SummaryStatistics": stats.to_summary_statistics(),
                "DurationHistogram": stats.to_response_time_histogram(),
                "ResponseTimeHistogram": stats.to_response_time_histogram(),
            }
            services.append(node)

        return services

    def get_trace_summaries(
        self,
        start_time: float,
        end_time: float,
        filter_expression: str = "",
    ) -> list[dict[str, Any]]:
        """Build AWS-compatible trace summaries for a time range."""
        traces = self.get_traces_in_range(start_time, end_time)
        summaries: list[dict[str, Any]] = []

        for trace_id, segments in traces.items():
            if not segments:
                continue

            # Filter by expression (basic support)
            if filter_expression and not _matches_filter(segments, filter_expression):
                continue

            # Compute trace-level stats
            min_start = min(s.get("start_time", float("inf")) for s in segments)
            max_end = max(s.get("end_time", 0) for s in segments)
            duration = max_end - min_start

            has_error = any(s.get("error") for s in segments)
            has_fault = any(s.get("fault") for s in segments)
            has_throttle = any(s.get("throttle") for s in segments)

            # Find root segment for response time
            root_segments = [s for s in segments if not s.get("parent_id")]
            response_time = duration
            if root_segments:
                root = root_segments[0]
                response_time = root.get("end_time", 0) - root.get("start_time", 0)

            # Get HTTP status from root segment
            http_status = None
            for seg in root_segments or segments[:1]:
                http_resp = seg.get("http", {}).get("response", {})
                if "status" in http_resp:
                    http_status = http_resp["status"]

            summary: dict[str, Any] = {
                "Id": trace_id,
                "Duration": duration,
                "ResponseTime": response_time,
                "HasFault": has_fault,
                "HasError": has_error,
                "HasThrottle": has_throttle,
                "Http": {},
                "Annotations": {},
                "Users": [],
                "ServiceIds": [],
                "EntryPoint": None,
                "FaultRootCauses": [],
                "ErrorRootCauses": [],
                "ResponseTimeRootCauses": [],
                "Revision": 0,
                "MatchedEventTime": min_start,
            }

            if http_status is not None:
                summary["Http"] = {"HttpStatus": http_status}

            # Build ServiceIds
            seen_services: set[str] = set()
            for seg in segments:
                name = seg.get("name", "unknown")
                if name not in seen_services:
                    seen_services.add(name)
                    summary["ServiceIds"].append(
                        {
                            "Name": name,
                            "Names": [name],
                            "AccountId": None,
                            "Type": _detect_service_type(seg),
                        }
                    )

            if root_segments:
                root = root_segments[0]
                summary["EntryPoint"] = {
                    "Name": root.get("name", "unknown"),
                    "Names": [root.get("name", "unknown")],
                    "AccountId": None,
                    "Type": _detect_service_type(root),
                }

            summaries.append(summary)

        return summaries

    def detect_anomalies(self, start_time: float, end_time: float) -> list[dict[str, Any]]:
        """Detect anomalies: latency spikes and error rate increases.

        Returns a list of insight-like dicts for services with anomalous behavior.
        A service is anomalous if its recent values exceed 2 standard deviations
        from its rolling mean.
        """
        insights: list[dict[str, Any]] = []
        now = time.time()

        for service_name in set(self._rolling_stats) | set(self._rolling_error_rates):
            latencies = self._rolling_stats.get(service_name, [])
            error_rates = self._rolling_error_rates.get(service_name, [])

            anomaly_categories: list[str] = []

            # Latency anomaly detection
            if len(latencies) >= 5:
                mean_lat = statistics.mean(latencies)
                stdev_lat = statistics.stdev(latencies) if len(latencies) > 1 else 0
                recent = latencies[-3:] if len(latencies) >= 3 else latencies[-1:]
                recent_mean = statistics.mean(recent)
                if stdev_lat > 0 and (recent_mean - mean_lat) > 2 * stdev_lat:
                    anomaly_categories.append("LATENCY")

            # Error rate anomaly detection
            if len(error_rates) >= 5:
                mean_err = statistics.mean(error_rates)
                stdev_err = statistics.stdev(error_rates) if len(error_rates) > 1 else 0
                recent_err = error_rates[-3:] if len(error_rates) >= 3 else error_rates[-1:]
                recent_err_mean = statistics.mean(recent_err)
                if stdev_err > 0 and (recent_err_mean - mean_err) > 2 * stdev_err:
                    anomaly_categories.append("ERROR_RATE")

            if anomaly_categories:
                insight_id = f"insight-{service_name}-{int(now)}"
                insight = {
                    "InsightId": insight_id,
                    "GroupARN": None,
                    "GroupName": "Default",
                    "RootCauseServiceId": {
                        "Name": service_name,
                        "Names": [service_name],
                        "AccountId": None,
                        "Type": "AWS::Service",
                    },
                    "Categories": anomaly_categories,
                    "State": "ACTIVE",
                    "StartTime": start_time,
                    "EndTime": end_time,
                    "Summary": (
                        f"Anomaly detected in {service_name}: {', '.join(anomaly_categories)}"
                    ),
                    "ClientRequestImpactStatistics": {
                        "FaultCount": 0,
                        "OkCount": 0,
                        "TotalCount": 0,
                    },
                    "RootCauseServiceRequestImpactStatistics": {
                        "FaultCount": 0,
                        "OkCount": 0,
                        "TotalCount": 0,
                    },
                    "TopAnomalousServices": [
                        {
                            "ServiceId": {
                                "Name": service_name,
                                "Names": [service_name],
                                "AccountId": None,
                                "Type": "AWS::Service",
                            }
                        }
                    ],
                    "LastUpdateTime": now,
                }
                insights.append(insight)

        return insights

    def clear(self) -> None:
        """Clear all stored data."""
        self._traces.clear()
        self._rolling_stats.clear()
        self._rolling_error_rates.clear()


def _matches_filter(segments: list[dict[str, Any]], expression: str) -> bool:
    """Basic filter expression matching.

    Supports:
      - service("name") — match segments with a given name
      - responsetime > N — match traces with response time exceeding N seconds
      - http.status = N — match traces with HTTP status code N
      - !ok — match traces with errors or faults
    """
    expr = expression.strip()

    # service("name") filter
    if expr.startswith("service("):
        name = expr.split('"')[1] if '"' in expr else ""
        return any(seg.get("name") == name for seg in segments)

    # responsetime filter
    if expr.startswith("responsetime"):
        parts = expr.split()
        if len(parts) >= 3:
            op = parts[1]
            try:
                threshold = float(parts[2])
            except ValueError:
                return True
            root_segs = [s for s in segments if not s.get("parent_id")]
            if not root_segs:
                return True
            root = root_segs[0]
            resp_time = root.get("end_time", 0) - root.get("start_time", 0)
            if op == ">":
                return resp_time > threshold
            if op == ">=":
                return resp_time >= threshold
            if op == "<":
                return resp_time < threshold
            if op == "<=":
                return resp_time <= threshold
            if op == "=":
                return abs(resp_time - threshold) < 0.001
        return True

    # http.status filter
    if expr.startswith("http.status"):
        parts = expr.split()
        if len(parts) >= 3:
            try:
                target_status = int(parts[2])
            except ValueError:
                return True
            for seg in segments:
                status = seg.get("http", {}).get("response", {}).get("status")
                if status == target_status:
                    return True
            return False
        return True

    # !ok — match traces with errors/faults
    if expr == "!ok":
        return any(seg.get("error") or seg.get("fault") for seg in segments)

    # Default: no filter
    return True


# Singleton engine instance
_engine: TraceCorrelationEngine | None = None


def get_engine() -> TraceCorrelationEngine:
    """Get or create the global trace correlation engine."""
    global _engine
    if _engine is None:
        _engine = TraceCorrelationEngine()
    return _engine


def reset_engine() -> None:
    """Reset the global engine (for testing)."""
    global _engine
    _engine = None
