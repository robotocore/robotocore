"""SSM service settings and parameter reads by another account's ARN."""

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


class TestSsmServiceSettings:
    def test_ssm_service_setting_round_trip_by_path_and_arn(self):
        ssm = _client("ssm", "300000000020")
        path = "/ssm/parameter-store/high-throughput-enabled"
        assert ssm.get_service_setting(SettingId=path)["ServiceSetting"]["SettingValue"] == "false"
        ssm.update_service_setting(SettingId=path, SettingValue="true")
        got = ssm.get_service_setting(SettingId=path)["ServiceSetting"]
        assert (got["SettingId"], got["SettingValue"], got["Status"]) == (
            path,
            "true",
            "Customized",
        )
        by_arn = ssm.get_service_setting(SettingId=got["ARN"])["ServiceSetting"]
        assert by_arn["SettingId"] == path
        assert by_arn["SettingValue"] == "true"
        reset = ssm.reset_service_setting(SettingId=path)["ServiceSetting"]
        assert reset["SettingValue"] == "false"


class TestSsmCrossAccountParameter:
    def test_ssm_parameter_by_other_account_arn(self):
        owner, caller = "300000000031", "300000000032"
        _client("ssm", owner).put_parameter(Name="/shared/x", Value="v", Type="String")
        arn = f"arn:aws:ssm:us-east-1:{owner}:parameter/shared/x"
        assert _client("ssm", caller).get_parameter(Name=arn)["Parameter"]["Value"] == "v"
