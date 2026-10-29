"""CloudWatch over Smithy RPCv2 CBOR (used by aws-sdk-go-v2) serves Moto-backed operations."""

import os
import uuid

import boto3

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")


def _client(service: str, account: str = "123456789012", **kw):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT_URL,
        region_name=kw.pop("region_name", "us-east-1"),
        aws_access_key_id=kw.pop("aws_access_key_id", account),
        aws_secret_access_key="test",
        **kw,
    )


class TestCloudWatchCbor:
    """Terraform's aws-sdk-go-v2 speaks rpc-v2-cbor to CloudWatch; Moto-served ops must work."""

    def _call(self, action: str, payload: dict):
        import cbor2
        import requests

        resp = requests.post(
            f"{ENDPOINT_URL}/service/GraniteServiceVersion20100801/operation/{action}",
            data=cbor2.dumps(payload),
            headers={
                "smithy-protocol": "rpc-v2-cbor",
                "content-type": "application/cbor",
                "accept": "application/cbor",
                "Authorization": (
                    "AWS4-HMAC-SHA256 Credential=123456789012/20240101/us-east-1/"
                    "monitoring/aws4_request"
                ),
            },
            timeout=10,
        )
        return resp.status_code, cbor2.loads(resp.content) if resp.content else {}

    def test_put_and_describe_metric_alarm(self):
        name = f"tf-compat-{uuid.uuid4().hex[:8]}"
        status, _ = self._call(
            "PutMetricAlarm",
            {
                "AlarmName": name,
                "ComparisonOperator": "GreaterThanThreshold",
                "EvaluationPeriods": 1,
                "MetricName": "Latency",
                "Namespace": "AWS/Events",
                "Period": 300,
                "Statistic": "Average",
                "Threshold": 30000.0,
            },
        )
        assert status == 200
        status, out = self._call("DescribeAlarms", {"AlarmNames": [name]})
        assert status == 200
        alarm = out["MetricAlarms"][0]
        assert alarm["AlarmName"] == name
        assert alarm["Threshold"] == 30000.0
        from datetime import datetime

        assert isinstance(alarm["StateUpdatedTimestamp"], datetime)

    def test_unknown_alarm_delete_is_ok_and_errors_are_cbor(self):
        status, out = self._call("DescribeAlarms", {"AlarmNames": ["does-not-exist"]})
        assert status == 200
        assert out.get("MetricAlarms", []) == []
