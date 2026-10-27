"""Lambda layers published in one account are readable by ARN from another."""

import os

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


class TestCrossAccountLayers:
    def test_lambda_layer_from_other_account(self):
        owner = "300000000033"
        lv = _client("lambda", owner).publish_layer_version(
            LayerName="Vendor-Extension", Content={"ZipFile": b"PK\x05\x06" + b"\0" * 18}
        )
        caller = _client("lambda", "300000000034")
        assert caller.get_layer_version_by_arn(Arn=lv["LayerVersionArn"])["Version"] == 1
        layer_arn = f"arn:aws:lambda:us-east-1:{owner}:layer:Vendor-Extension"
        assert caller.get_layer_version(LayerName=layer_arn, VersionNumber=1)["Version"] == 1
