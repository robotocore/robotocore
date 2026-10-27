"""Forecast compatibility tests."""

import time
import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client

ACCOUNT = "123456789012"
DOMAIN_ENUM_REPR = (
    "['INVENTORY_PLANNING', 'METRICS', 'RETAIL', 'EC2_CAPACITY', "
    "'CUSTOM', 'WEB_TRAFFIC', 'WORK_FORCE']"
)


def _unique_name():
    return f"dsg-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def forecast():
    return make_client("forecast")


@pytest.fixture
def region_op():
    """Second region client for cross-region isolation checks."""
    return make_client("forecast", region_name="eu-west-1")


def _arn(name, region="us-east-1", account=ACCOUNT):
    return f"arn:aws:forecast:{region}:{account}:dataset-group/{name}"


def err_code(exc):
    return exc.value.response["Error"]["Code"]


def err_message(exc):
    return exc.value.response["Error"]["Message"]


class TestForecastOperations:
    def test_list_dataset_groups(self, forecast):
        resp = forecast.list_dataset_groups()
        assert "DatasetGroups" in resp
        assert isinstance(resp["DatasetGroups"], list)

    def test_describe_nonexistent_dataset_group(self, forecast):
        with pytest.raises(ClientError) as exc:
            forecast.describe_dataset_group(
                DatasetGroupArn="arn:aws:forecast:us-east-1:123456789012:dataset-group/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestDatasetGroupLifecycle:
    def test_create_describe_delete_round_trip(self, forecast):
        name = _unique_name()
        created = forecast.create_dataset_group(DatasetGroupName=name, Domain="CUSTOM")
        assert created["DatasetGroupArn"] == _arn(name)

        described = forecast.describe_dataset_group(DatasetGroupArn=created["DatasetGroupArn"])
        assert described["DatasetGroupArn"] == _arn(name)
        assert described["DatasetGroupName"] == name
        assert described["Domain"] == "CUSTOM"
        assert described["Status"] == "ACTIVE"
        assert described["DatasetArns"] == []
        assert described["CreationTime"] == described["LastModificationTime"]

        forecast.delete_dataset_group(DatasetGroupArn=created["DatasetGroupArn"])
        with pytest.raises(ClientError) as exc:
            forecast.describe_dataset_group(DatasetGroupArn=created["DatasetGroupArn"])
        assert err_code(exc) == "ResourceNotFoundException"

    def test_create_returns_canonical_arn_shape(self, forecast):
        name = _unique_name()
        created = forecast.create_dataset_group(DatasetGroupName=name, Domain="RETAIL")
        arn = created["DatasetGroupArn"]
        assert arn.startswith("arn:aws:forecast:")
        assert f":{ACCOUNT}:" in arn
        assert arn.endswith(f"dataset-group/{name}")
        # Describe must agree with the arn minted at create time.
        described = forecast.describe_dataset_group(DatasetGroupArn=arn)
        assert described["DatasetGroupArn"] == arn

    def test_delete_makes_resource_gone(self, forecast):
        name = _unique_name()
        created = forecast.create_dataset_group(DatasetGroupName=name, Domain="CUSTOM")
        arn = created["DatasetGroupArn"]
        forecast.delete_dataset_group(DatasetGroupArn=arn)
        with pytest.raises(ClientError) as exc:
            forecast.describe_dataset_group(DatasetGroupArn=arn)
        assert err_code(exc) == "ResourceNotFoundException"
        assert err_message(exc) == f"No resource found {arn}"

    def test_delete_nonexistent_raises(self, forecast):
        arn = _arn(_unique_name())
        with pytest.raises(ClientError) as exc:
            forecast.delete_dataset_group(DatasetGroupArn=arn)
        assert err_code(exc) == "ResourceNotFoundException"
        assert err_message(exc) == f"No resource found {arn}"

    def test_update_with_unknown_dataset_arn_raises(self, forecast):
        name = _unique_name()
        created = forecast.create_dataset_group(DatasetGroupName=name, Domain="CUSTOM")
        dataset_arn = f"arn:aws:forecast:us-east-1:{ACCOUNT}:dataset/no-such"
        with pytest.raises(ClientError) as exc:
            forecast.update_dataset_group(
                DatasetGroupArn=created["DatasetGroupArn"], DatasetArns=[dataset_arn]
            )
        assert err_code(exc) == "InvalidInputException"
        assert err_message(exc) == f"Dataset arns: [{dataset_arn}] are not found"

    def test_update_with_empty_list_clears_dataset_arns(self, forecast):
        name = _unique_name()
        created = forecast.create_dataset_group(DatasetGroupName=name, Domain="CUSTOM")
        arn = created["DatasetGroupArn"]
        forecast.update_dataset_group(DatasetGroupArn=arn, DatasetArns=[])
        described = forecast.describe_dataset_group(DatasetGroupArn=arn)
        assert described["DatasetArns"] == []
        # moto never bumps the wire LastModificationTime on update.
        assert described["LastModificationTime"] == described["CreationTime"]

    def test_update_nonexistent_group_raises(self, forecast):
        arn = _arn(_unique_name())
        with pytest.raises(ClientError) as exc:
            forecast.update_dataset_group(DatasetGroupArn=arn, DatasetArns=[])
        assert err_code(exc) == "ResourceNotFoundException"
        assert err_message(exc) == f"No resource found {arn}"

    def test_created_group_appears_in_list(self, forecast):
        name = _unique_name()
        created = forecast.create_dataset_group(DatasetGroupName=name, Domain="WORK_FORCE")
        entries = forecast.list_dataset_groups()["DatasetGroups"]
        mine = [g for g in entries if g["DatasetGroupArn"] == created["DatasetGroupArn"]]
        assert len(mine) == 1
        assert mine[0]["DatasetGroupName"] == name
        assert mine[0]["CreationTime"] is not None
        assert mine[0]["LastModificationTime"] == mine[0]["CreationTime"]
        # list summaries carry no domain/status members
        assert set(mine[0].keys()) == {
            "DatasetGroupArn",
            "DatasetGroupName",
            "CreationTime",
            "LastModificationTime",
        }

    def test_list_orders_newest_first(self, forecast):
        first = forecast.create_dataset_group(
            DatasetGroupName=_unique_name(), Domain="INVENTORY_PLANNING"
        )["DatasetGroupArn"]
        time.sleep(1.1)
        second = forecast.create_dataset_group(
            DatasetGroupName=_unique_name(), Domain="METRICS"
        )["DatasetGroupArn"]
        entries = forecast.list_dataset_groups()["DatasetGroups"]
        positions = [g["DatasetGroupArn"] for g in entries]
        assert positions.index(second) < positions.index(first)


class TestCreateValidation:
    def test_duplicate_name_raises(self, forecast):
        name = _unique_name()
        first = forecast.create_dataset_group(DatasetGroupName=name, Domain="CUSTOM")
        with pytest.raises(ClientError) as exc:
            forecast.create_dataset_group(DatasetGroupName=name, Domain="CUSTOM")
        assert err_code(exc) == "ResourceAlreadyExistsException"
        assert err_message(exc) == (
            f"A dataset group already exists with the arn: {first['DatasetGroupArn']}"
        )

    def test_name_must_start_with_letter(self, forecast):
        with pytest.raises(ClientError) as exc:
            forecast.create_dataset_group(DatasetGroupName="9bad-name", Domain="CUSTOM")
        assert err_code(exc) == "ValidationException"
        assert "at 'datasetGroupName'" in err_message(exc)
        assert (
            "Member must satisfy regular expression pattern ^[a-zA-Z][a-z-A-Z0-9_]*"
            in err_message(exc)
        )

    def test_name_length_capped_at_63(self, forecast):
        with pytest.raises(ClientError) as exc:
            forecast.create_dataset_group(DatasetGroupName="a" * 65, Domain="CUSTOM")
        assert err_code(exc) == "ValidationException"
        assert "Member must have length less than or equal to 63" in err_message(exc)

    def test_domain_must_be_in_enum(self, forecast):
        with pytest.raises(ClientError) as exc:
            forecast.create_dataset_group(DatasetGroupName=_unique_name(), Domain="BOGUS")
        assert err_code(exc) == "ValidationException"
        assert "at 'domain' failed to satisfy constraint" in err_message(exc)
        assert DOMAIN_ENUM_REPR in err_message(exc)

    def test_two_violations_join_into_one_error(self, forecast):
        with pytest.raises(ClientError) as exc:
            forecast.create_dataset_group(DatasetGroupName="9bad", Domain="BOGUS")
        assert err_code(exc) == "ValidationException"
        msg = err_message(exc)
        assert msg.startswith("2 validation errors detected: ")
        pattern_pos = msg.find("datasetGroupName")
        domain_pos = msg.find("at 'domain'")
        assert pattern_pos != -1 and domain_pos != -1
        assert pattern_pos < domain_pos


class TestNotImplemented:
    """Wave-11 (6c973a2e) models these on the twin: unknown members are denied
    with ResourceNotFoundException and list/create paths return modeled 200
    envelopes; nothing here is a 501 stub anymore."""

    def test_list_datasets_not_implemented(self, forecast):
        # wave-11 (6c973a2e): list_datasets now answers with a modeled list
        # envelope (account-scoped, populated by other tests on a hot twin)
        # instead of the old moto-parity 501.
        resp = forecast.list_datasets()
        assert isinstance(resp["Datasets"], list)
        for dataset in resp["Datasets"]:
            assert dataset["DatasetArn"]
            assert dataset["DatasetName"]

    def test_create_dataset_not_implemented(self, forecast):
        # wave-11 (6c973a2e): create_dataset now mints a dataset ARN on the
        # twin instead of the old moto-parity 501.
        resp = forecast.create_dataset(
            DatasetName=_unique_name(),
            Domain="CUSTOM",
            DatasetType="TARGET_TIME_SERIES",
            Schema={"Attributes": [{"AttributeName": "ts", "AttributeType": "timestamp"}]},
        )
        assert "DatasetArn" in resp

    def test_list_predictors_not_implemented(self, forecast):
        # wave-11 (6c973a2e): list_predictors now answers with a modeled list
        # envelope (account-scoped) instead of the old moto-parity 501.
        resp = forecast.list_predictors()
        assert isinstance(resp["Predictors"], list)
        for predictor in resp["Predictors"]:
            assert predictor["PredictorArn"]

    def test_list_tag_resources_not_implemented(self, forecast):
        # wave-11 (6c973a2e): tag_resource now resolves its target ARN before
        # writing, so an unknown resource is denied with
        # ResourceNotFoundException instead of the old moto-parity 501.
        with pytest.raises(ClientError) as exc:
            forecast.tag_resource(
                ResourceArn=_arn(_unique_name()), Tags=[{"Key": "env", "Value": "test"}]
            )
        assert err_code(exc) == "ResourceNotFoundException"

    def test_list_dataset_import_jobs_not_implemented(self, forecast):
        # wave-11 (6c973a2e): list_dataset_import_jobs now answers with a
        # modeled list envelope (account-scoped) instead of the old
        # moto-parity 501.
        resp = forecast.list_dataset_import_jobs()
        assert isinstance(resp["DatasetImportJobs"], list)
        for job in resp["DatasetImportJobs"]:
            assert job["DatasetImportJobArn"]

    def test_describe_predictor_not_implemented(self, forecast):
        # wave-11 (6c973a2e): describe_predictor now resolves its target ARN,
        # so an unknown predictor is denied with ResourceNotFoundException
        # instead of the old moto-parity 501.
        with pytest.raises(ClientError) as exc:
            forecast.describe_predictor(
                PredictorArn=f"arn:aws:forecast:us-east-1:{ACCOUNT}:predictor/nope"
            )
        assert err_code(exc) == "ResourceNotFoundException"


class TestRegionIsolation:
    def test_region_scoped_state_and_arn(self):
        regional = make_client("forecast", region_name="eu-west-1")
        name = _unique_name()
        created = regional.create_dataset_group(DatasetGroupName=name, Domain="EC2_CAPACITY")
        arn = f"arn:aws:forecast:eu-west-1:{ACCOUNT}:dataset-group/{name}"
        assert created["DatasetGroupArn"] == arn

        described = regional.describe_dataset_group(DatasetGroupArn=arn)
        assert described["Domain"] == "EC2_CAPACITY"

        entries = regional.list_dataset_groups()["DatasetGroups"]
        assert any(g["DatasetGroupArn"] == arn for g in entries)

        home = make_client("forecast")
        home_entries = home.list_dataset_groups()["DatasetGroups"]
        assert not any(g["DatasetGroupArn"] == arn for g in home_entries)

        regional.delete_dataset_group(DatasetGroupArn=arn)
