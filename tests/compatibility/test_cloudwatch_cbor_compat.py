"""CloudWatch over Smithy RPC v2 CBOR (aws-sdk-go-v2, AWS SDK for .NET v4).

botocore prefers AWS JSON 1.0 for CloudWatch, but it ships an rpc-v2-cbor
serializer and parser. The ``cw_cbor`` client loads a copy of the cloudwatch
model that lists only ``smithy-rpc-v2-cbor``, so its calls go out as
``POST /service/GraniteServiceVersion20100801/operation/<Op>`` with a CBOR
body and the responses are parsed as CBOR.

botocore's parser also accepts text timestamps, so the tests that check wire
types decode the raw body with botocore's CBOR decoder: a tag-1 timestamp
decodes to ``datetime`` and a text one stays ``str``.
"""

import copy
import json
import uuid
from datetime import UTC, datetime, timedelta

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.parsers import RpcV2CBORParser

from tests.compatibility.conftest import ENDPOINT_URL, make_client

CBOR_PATH_PREFIX = "/service/GraniteServiceVersion20100801/operation/"


@pytest.fixture(scope="module")
def cbor_data_path(tmp_path_factory):
    session = boto3.Session()
    loader = session._session.get_component("data_loader")
    model = copy.deepcopy(loader.load_service_model("cloudwatch", "service-2"))
    model["metadata"]["protocol"] = "smithy-rpc-v2-cbor"
    model["metadata"]["protocols"] = ["smithy-rpc-v2-cbor"]
    root = tmp_path_factory.mktemp("cbor-models")
    version_dir = root / "cloudwatch" / model["metadata"]["apiVersion"]
    version_dir.mkdir(parents=True)
    (version_dir / "service-2.json").write_text(json.dumps(model))
    return str(root)


@pytest.fixture
def wire():
    """Requests sent and raw response bodies received by ``cw_cbor``."""
    return {"requests": [], "bodies": []}


@pytest.fixture
def cw_cbor(cbor_data_path, wire):
    session = boto3.Session(
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        region_name="us-east-1",
    )
    session._session.get_component("data_loader").search_paths.insert(0, cbor_data_path)
    client = session.client(
        "cloudwatch",
        endpoint_url=ENDPOINT_URL,
        config=Config(retries={"max_attempts": 1}),
    )
    assert client.meta.service_model.resolved_protocol == "smithy-rpc-v2-cbor"

    def record_request(request, **_):
        wire["requests"].append(request)

    def record_body(response_dict, **_):
        wire["bodies"].append(response_dict["body"])

    client.meta.events.register("before-send.cloudwatch", record_request)
    client.meta.events.register("before-parse.cloudwatch", record_body)
    return client


@pytest.fixture
def cw_json():
    return make_client("cloudwatch")


def raw(wire):
    """The last response body, decoded without the model."""
    return RpcV2CBORParser()._initial_body_parse(wire["bodies"][-1])


def _ns():
    return f"Cbor/{uuid.uuid4().hex[:8]}"


class TestCloudWatchCborWire:
    def test_request_and_response_use_rpcv2_cbor(self, cw_cbor, wire):
        resp = cw_cbor.list_metrics(Namespace=_ns())
        request = wire["requests"][-1]
        assert request.url.endswith(CBOR_PATH_PREFIX + "ListMetrics")
        assert request.headers["smithy-protocol"] == b"rpc-v2-cbor"
        assert "X-Amz-Target" not in request.headers
        headers = resp["ResponseMetadata"]["HTTPHeaders"]
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert headers["content-type"] == "application/cbor"
        assert headers["smithy-protocol"] == "rpc-v2-cbor"
        assert resp["Metrics"] == []
        assert isinstance(raw(wire), dict)

    def test_error_is_cbor_encoded(self, cw_cbor, wire):
        with pytest.raises(ClientError) as exc:
            cw_cbor.get_dashboard(DashboardName=f"missing-{uuid.uuid4().hex[:8]}")
        err = exc.value.response
        assert err["Error"]["Code"] == "ResourceNotFound"
        assert "does not exist" in err["Error"]["Message"]
        headers = err["ResponseMetadata"]["HTTPHeaders"]
        assert headers["content-type"] == "application/cbor"
        # awsQueryCompatible: the shape id in __type, the query code in the header.
        assert headers["x-amzn-query-error"] == "ResourceNotFound;Sender"
        assert raw(wire)["__type"] == "com.amazonaws.cloudwatch#DashboardNotFoundError"


class TestCloudWatchCborMetrics:
    def test_put_metric_data_then_list_metrics(self, cw_cbor):
        ns = _ns()
        cw_cbor.put_metric_data(
            Namespace=ns,
            MetricData=[
                {
                    "MetricName": "Latency",
                    "Dimensions": [{"Name": "Service", "Value": "api"}],
                    "Value": 12.5,
                    "Unit": "Milliseconds",
                }
            ],
        )
        metrics = cw_cbor.list_metrics(Namespace=ns)["Metrics"]
        assert metrics == [
            {
                "Namespace": ns,
                "MetricName": "Latency",
                "Dimensions": [{"Name": "Service", "Value": "api"}],
            }
        ]

    def test_get_metric_statistics_types(self, cw_cbor, wire):
        ns = _ns()
        now = datetime.now(UTC).replace(second=0, microsecond=0)
        cw_cbor.put_metric_data(
            Namespace=ns,
            MetricData=[
                {"MetricName": "Hits", "Value": 3, "Unit": "Count", "Timestamp": now},
                {"MetricName": "Hits", "Value": 4, "Unit": "Count", "Timestamp": now},
            ],
        )
        resp = cw_cbor.get_metric_statistics(
            Namespace=ns,
            MetricName="Hits",
            StartTime=now - timedelta(minutes=5),
            EndTime=now + timedelta(minutes=5),
            Period=60,
            Statistics=["Sum", "SampleCount"],
        )
        assert len(resp["Datapoints"]) == 1
        point = resp["Datapoints"][0]
        assert point["Sum"] == 7.0
        assert point["SampleCount"] == 2.0
        assert point["Timestamp"] == now

        wire_point = raw(wire)["Datapoints"][0]
        assert isinstance(wire_point["Timestamp"], datetime)
        assert isinstance(wire_point["Sum"], float)

    def test_gzipped_put_metric_data_is_stored(self, cw_cbor, wire):
        # PutMetricData carries `requestcompression`; aws-sdk-go-v2 gzips every
        # body, botocore from 10 KiB.
        ns = _ns()
        data = [
            {
                "MetricName": f"m{i:03d}",
                "Value": float(i),
                "Dimensions": [{"Name": "pad", "Value": "x" * 200}],
            }
            for i in range(60)
        ]
        cw_cbor.put_metric_data(Namespace=ns, MetricData=data)
        assert wire["requests"][-1].headers["Content-Encoding"] == b"gzip"
        names = {m["MetricName"] for m in cw_cbor.list_metrics(Namespace=ns)["Metrics"]}
        assert names == {f"m{i:03d}" for i in range(60)}

    def test_cbor_writes_are_visible_over_json(self, cw_cbor, cw_json):
        ns = _ns()
        cw_cbor.put_metric_data(Namespace=ns, MetricData=[{"MetricName": "Shared", "Value": 1.0}])
        names = [m["MetricName"] for m in cw_json.list_metrics(Namespace=ns)["Metrics"]]
        assert names == ["Shared"]


class TestCloudWatchCborAlarms:
    def test_put_describe_delete_alarm(self, cw_cbor, wire):
        name = f"cbor-alarm-{uuid.uuid4().hex[:8]}"
        cw_cbor.put_metric_alarm(
            AlarmName=name,
            Namespace=_ns(),
            MetricName="Errors",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=2,
            Period=60,
            Statistic="Sum",
            Threshold=90.0,
        )
        try:
            alarms = cw_cbor.describe_alarms(AlarmNames=[name])["MetricAlarms"]
            assert len(alarms) == 1
            alarm = alarms[0]
            assert alarm["AlarmName"] == name
            assert alarm["Threshold"] == 90.0
            assert alarm["EvaluationPeriods"] == 2
            assert alarm["Period"] == 60
            assert isinstance(alarm["StateUpdatedTimestamp"], datetime)

            wire_alarm = raw(wire)["MetricAlarms"][0]
            assert isinstance(wire_alarm["StateUpdatedTimestamp"], datetime)
            assert isinstance(wire_alarm["AlarmConfigurationUpdatedTimestamp"], datetime)
            assert isinstance(wire_alarm["Threshold"], float)
            assert isinstance(wire_alarm["Period"], int)
        finally:
            cw_cbor.delete_alarms(AlarmNames=[name])
        assert cw_cbor.describe_alarms(AlarmNames=[name])["MetricAlarms"] == []

    def test_tag_and_list_tags(self, cw_cbor):
        name = f"cbor-tags-{uuid.uuid4().hex[:8]}"
        cw_cbor.put_metric_alarm(
            AlarmName=name,
            Namespace=_ns(),
            MetricName="Errors",
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            Period=60,
            Statistic="Sum",
            Threshold=1.0,
        )
        try:
            arn = cw_cbor.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]["AlarmArn"]
            cw_cbor.tag_resource(ResourceARN=arn, Tags=[{"Key": "team", "Value": "obs"}])
            tags = cw_cbor.list_tags_for_resource(ResourceARN=arn)["Tags"]
            assert {"Key": "team", "Value": "obs"} in tags
        finally:
            cw_cbor.delete_alarms(AlarmNames=[name])


class TestCloudWatchCborDashboards:
    def test_put_get_list_dashboard(self, cw_cbor, wire):
        name = f"cbor-dash-{uuid.uuid4().hex[:8]}"
        widget = {"type": "text", "x": 0, "y": 0, "width": 6, "height": 3}
        body = json.dumps({"widgets": [{**widget, "properties": {"markdown": "cbor"}}]})
        put = cw_cbor.put_dashboard(DashboardName=name, DashboardBody=body)
        assert put["ResponseMetadata"]["HTTPStatusCode"] == 200
        try:
            got = cw_cbor.get_dashboard(DashboardName=name)
            assert got["DashboardName"] == name
            assert got["DashboardBody"] == body

            entries = cw_cbor.list_dashboards(DashboardNamePrefix=name)["DashboardEntries"]
            assert [e["DashboardName"] for e in entries] == [name]
            wire_entry = raw(wire)["DashboardEntries"][0]
            assert isinstance(wire_entry["LastModified"], datetime)
            assert isinstance(wire_entry["Size"], int)
        finally:
            cw_cbor.delete_dashboards(DashboardNames=[name])
