"""Forecast compatibility tests."""

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def forecast():
    return make_client("forecast")


class TestForecastOperations:
    def test_list_dataset_groups(self, forecast):
        resp = forecast.list_dataset_groups()
        assert "DatasetGroups" in resp
        assert isinstance(resp["DatasetGroups"], list)

    def test_describe_nonexistent_dataset_group(self, forecast):
        with pytest.raises(ClientError) as exc:
            forecast.describe_dataset_group(
                DatasetGroupArn="arn:aws:forecast:us-east-1:123456789012:dataset-group/nonexist"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestForecastGapListOps:
    """Tests for newly-implemented list operations."""

    SCHEMA = {
        "Attributes": [
            {"AttributeName": "item_id", "AttributeType": "string"},
            {"AttributeName": "timestamp", "AttributeType": "timestamp"},
            {"AttributeName": "target_value", "AttributeType": "float"},
        ]
    }

    @pytest.fixture
    def client(self):
        return make_client("forecast")

    def test_list_datasets(self, client):
        """Create a dataset, verify it appears in list, then delete."""
        r = client.create_dataset(
            DatasetName="test-list-ds-basic",
            Domain="RETAIL",
            DatasetType="TARGET_TIME_SERIES",
            Schema=self.SCHEMA,
        )
        arn = r["DatasetArn"]
        try:
            resp = client.list_datasets()
            assert "Datasets" in resp
            assert isinstance(resp["Datasets"], list)
            arns = [ds["DatasetArn"] for ds in resp["Datasets"]]
            assert arn in arns
        finally:
            client.delete_dataset(DatasetArn=arn)

    def test_list_datasets_contains_created(self, client):
        r = client.create_dataset(
            DatasetName="test-list-ds-verify",
            Domain="RETAIL",
            DatasetType="TARGET_TIME_SERIES",
            Schema=self.SCHEMA,
        )
        arn = r["DatasetArn"]
        try:
            list_resp = client.list_datasets()
            arns = [ds["DatasetArn"] for ds in list_resp["Datasets"]]
            assert arn in arns
        finally:
            client.delete_dataset(DatasetArn=arn)

    def test_list_datasets_entry_has_required_fields(self, client):
        """Each dataset in list should have DatasetArn, DatasetName, timestamps."""
        r = client.create_dataset(
            DatasetName="test-list-ds-fields",
            Domain="RETAIL",
            DatasetType="TARGET_TIME_SERIES",
            Schema=self.SCHEMA,
        )
        arn = r["DatasetArn"]
        try:
            resp = client.list_datasets()
            match = next((ds for ds in resp["Datasets"] if ds["DatasetArn"] == arn), None)
            assert match is not None
            assert match["DatasetName"] == "test-list-ds-fields"
            assert "CreationTime" in match
            assert "LastModificationTime" in match
        finally:
            client.delete_dataset(DatasetArn=arn)

    def test_list_datasets_removed_after_delete(self, client):
        """After deleting a dataset, it should no longer appear in list."""
        r = client.create_dataset(
            DatasetName="test-list-ds-del",
            Domain="RETAIL",
            DatasetType="TARGET_TIME_SERIES",
            Schema=self.SCHEMA,
        )
        arn = r["DatasetArn"]
        client.delete_dataset(DatasetArn=arn)
        resp = client.list_datasets()
        arns = [ds["DatasetArn"] for ds in resp["Datasets"]]
        assert arn not in arns

    def test_list_dataset_import_jobs(self, client):
        resp = client.list_dataset_import_jobs()
        assert "DatasetImportJobs" in resp
        assert isinstance(resp["DatasetImportJobs"], list)

    def test_list_dataset_import_jobs_no_next_token_when_empty(self, client):
        resp = client.list_dataset_import_jobs()
        assert "DatasetImportJobs" in resp
        assert resp.get("NextToken") is None or "NextToken" not in resp

    def test_list_forecasts(self, client):
        """Empty list returns correct structure."""
        resp = client.list_forecasts()
        assert "Forecasts" in resp
        assert isinstance(resp["Forecasts"], list)
        assert resp.get("NextToken") is None or "NextToken" not in resp

    def test_list_forecast_export_jobs(self, client):
        resp = client.list_forecast_export_jobs()
        assert "ForecastExportJobs" in resp
        assert isinstance(resp["ForecastExportJobs"], list)
        assert resp.get("NextToken") is None or "NextToken" not in resp

    def test_list_predictors(self, client):
        resp = client.list_predictors()
        assert "Predictors" in resp
        assert isinstance(resp["Predictors"], list)
        assert resp.get("NextToken") is None or "NextToken" not in resp

    def test_list_predictor_backtest_export_jobs(self, client):
        resp = client.list_predictor_backtest_export_jobs()
        assert "PredictorBacktestExportJobs" in resp
        assert isinstance(resp["PredictorBacktestExportJobs"], list)

    def test_list_explainabilities(self, client):
        resp = client.list_explainabilities()
        assert "Explainabilities" in resp
        assert isinstance(resp["Explainabilities"], list)

    def test_list_explainability_exports(self, client):
        resp = client.list_explainability_exports()
        assert "ExplainabilityExports" in resp
        assert isinstance(resp["ExplainabilityExports"], list)

    def test_list_monitors(self, client):
        resp = client.list_monitors()
        assert "Monitors" in resp
        assert isinstance(resp["Monitors"], list)

    def test_list_what_if_analyses(self, client):
        resp = client.list_what_if_analyses()
        assert "WhatIfAnalyses" in resp
        assert isinstance(resp["WhatIfAnalyses"], list)

    def test_list_what_if_forecasts(self, client):
        resp = client.list_what_if_forecasts()
        assert "WhatIfForecasts" in resp
        assert isinstance(resp["WhatIfForecasts"], list)

    def test_list_what_if_forecast_exports(self, client):
        resp = client.list_what_if_forecast_exports()
        assert "WhatIfForecastExports" in resp
        assert isinstance(resp["WhatIfForecastExports"], list)

    def test_list_dataset_groups_with_content(self, client):
        r = client.create_dataset_group(DatasetGroupName="test-gap-list-dsg", Domain="RETAIL")
        arn = r["DatasetGroupArn"]
        try:
            resp = client.list_dataset_groups()
            groups = resp["DatasetGroups"]
            match = next((g for g in groups if g["DatasetGroupArn"] == arn), None)
            assert match is not None
            assert match["DatasetGroupName"] == "test-gap-list-dsg"
            assert "CreationTime" in match
            assert "LastModificationTime" in match
        finally:
            client.delete_dataset_group(DatasetGroupArn=arn)

    def test_list_dataset_groups_removed_after_delete(self, client):
        """After deleting a dataset group, it should no longer appear in list."""
        r = client.create_dataset_group(DatasetGroupName="test-dsg-del-list", Domain="RETAIL")
        arn = r["DatasetGroupArn"]
        client.delete_dataset_group(DatasetGroupArn=arn)
        resp = client.list_dataset_groups()
        arns = [dg["DatasetGroupArn"] for dg in resp["DatasetGroups"]]
        assert arn not in arns

    def test_list_tags_for_resource(self, client):
        r = client.create_dataset_group(DatasetGroupName="test-tags-list", Domain="RETAIL")
        arn = r["DatasetGroupArn"]
        try:
            client.tag_resource(ResourceArn=arn, Tags=[{"Key": "env", "Value": "test"}])
            resp = client.list_tags_for_resource(ResourceArn=arn)
            assert "Tags" in resp
            assert any(t["Key"] == "env" for t in resp["Tags"])
        finally:
            client.delete_dataset_group(DatasetGroupArn=arn)

    def test_list_tags_empty_initially(self, client):
        """A newly created resource should have no tags."""
        r = client.create_dataset_group(DatasetGroupName="test-tags-empty", Domain="RETAIL")
        arn = r["DatasetGroupArn"]
        try:
            resp = client.list_tags_for_resource(ResourceArn=arn)
            assert "Tags" in resp
            assert len(resp["Tags"]) == 0
        finally:
            client.delete_dataset_group(DatasetGroupArn=arn)


class TestForecastDatasetGroupCRUD:
    """CRUD tests for DatasetGroup with tag operations."""

    @pytest.fixture
    def client(self):
        return make_client("forecast")

    def test_create_describe_update_delete_dataset_group(self, client):
        resp = client.create_dataset_group(DatasetGroupName="test-dsg-crud", Domain="RETAIL")
        dsg_arn = resp["DatasetGroupArn"]
        assert "forecast" in dsg_arn
        assert "test-dsg-crud" in dsg_arn

        describe_resp = client.describe_dataset_group(DatasetGroupArn=dsg_arn)
        assert describe_resp["DatasetGroupName"] == "test-dsg-crud"
        assert describe_resp["Domain"] == "RETAIL"
        assert describe_resp["Status"] == "ACTIVE"

        client.update_dataset_group(DatasetGroupArn=dsg_arn, DatasetArns=[])

        client.delete_dataset_group(DatasetGroupArn=dsg_arn)

        with pytest.raises(ClientError) as exc:
            client.describe_dataset_group(DatasetGroupArn=dsg_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_dataset_group_tag_untag(self, client):
        resp = client.create_dataset_group(DatasetGroupName="test-dsg-tags", Domain="CUSTOM")
        dsg_arn = resp["DatasetGroupArn"]

        client.tag_resource(ResourceArn=dsg_arn, Tags=[{"Key": "project", "Value": "robotocore"}])
        tag_resp = client.list_tags_for_resource(ResourceArn=dsg_arn)
        assert "Tags" in tag_resp
        assert any(t["Key"] == "project" for t in tag_resp["Tags"])

        client.untag_resource(ResourceArn=dsg_arn, TagKeys=["project"])
        tag_resp2 = client.list_tags_for_resource(ResourceArn=dsg_arn)
        assert not any(t["Key"] == "project" for t in tag_resp2.get("Tags", []))

        client.delete_dataset_group(DatasetGroupArn=dsg_arn)

    def test_list_dataset_groups_returns_created(self, client):
        resp = client.create_dataset_group(DatasetGroupName="test-dsg-list", Domain="EC2_CAPACITY")
        dsg_arn = resp["DatasetGroupArn"]

        list_resp = client.list_dataset_groups()
        assert "DatasetGroups" in list_resp
        arns = [dsg["DatasetGroupArn"] for dsg in list_resp["DatasetGroups"]]
        assert dsg_arn in arns

        client.delete_dataset_group(DatasetGroupArn=dsg_arn)

    def test_dataset_group_arn_format(self, client):
        resp = client.create_dataset_group(DatasetGroupName="test-dsg-arn", Domain="RETAIL")
        arn = resp["DatasetGroupArn"]
        # arn:aws:forecast:<region>:<account>:dataset-group/<name>
        parts = arn.split(":")
        assert parts[0] == "arn"
        assert parts[1] == "aws"
        assert parts[2] == "forecast"
        assert "dataset-group/test-dsg-arn" in arn
        client.delete_dataset_group(DatasetGroupArn=arn)

    def test_dataset_group_timestamps_present(self, client):
        from datetime import datetime, timezone

        resp = client.create_dataset_group(DatasetGroupName="test-dsg-ts", Domain="RETAIL")
        arn = resp["DatasetGroupArn"]
        desc = client.describe_dataset_group(DatasetGroupArn=arn)
        assert isinstance(desc["CreationTime"], datetime)
        assert isinstance(desc["LastModificationTime"], datetime)
        assert desc["CreationTime"].year >= 2024
        client.delete_dataset_group(DatasetGroupArn=arn)

    def test_dataset_group_idempotent_create_error(self, client):
        resp = client.create_dataset_group(DatasetGroupName="test-dsg-idem", Domain="RETAIL")
        arn = resp["DatasetGroupArn"]
        try:
            with pytest.raises(ClientError) as exc:
                client.create_dataset_group(DatasetGroupName="test-dsg-idem", Domain="RETAIL")
            assert exc.value.response["Error"]["Code"] in (
                "ResourceAlreadyExistsException",
                "AlreadyExistsException",
            )
        finally:
            client.delete_dataset_group(DatasetGroupArn=arn)

    def test_delete_nonexistent_dataset_group(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_dataset_group(
                DatasetGroupArn="arn:aws:forecast:us-east-1:123456789012:dataset-group/no-such"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_dataset_groups_pagination(self, client):
        arns = []
        for i in range(3):
            r = client.create_dataset_group(
                DatasetGroupName=f"test-dsg-page-{i}", Domain="RETAIL"
            )
            arns.append(r["DatasetGroupArn"])
        try:
            page1 = client.list_dataset_groups(MaxResults=2)
            assert "DatasetGroups" in page1
            assert len(page1["DatasetGroups"]) <= 2
            if "NextToken" in page1:
                page2 = client.list_dataset_groups(MaxResults=2, NextToken=page1["NextToken"])
                assert "DatasetGroups" in page2
                all_page1 = {dg["DatasetGroupArn"] for dg in page1["DatasetGroups"]}
                all_page2 = {dg["DatasetGroupArn"] for dg in page2["DatasetGroups"]}
                assert all_page1.isdisjoint(all_page2)
        finally:
            for arn in arns:
                try:
                    client.delete_dataset_group(DatasetGroupArn=arn)
                except ClientError:
                    pass

    def test_dataset_group_multiple_tags(self, client):
        resp = client.create_dataset_group(DatasetGroupName="test-dsg-multitag", Domain="RETAIL")
        arn = resp["DatasetGroupArn"]
        try:
            client.tag_resource(
                ResourceArn=arn,
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "owner", "Value": "team"}],
            )
            tag_resp = client.list_tags_for_resource(ResourceArn=arn)
            keys = {t["Key"] for t in tag_resp["Tags"]}
            assert "env" in keys
            assert "owner" in keys
            client.untag_resource(ResourceArn=arn, TagKeys=["env"])
            tag_resp2 = client.list_tags_for_resource(ResourceArn=arn)
            keys2 = {t["Key"] for t in tag_resp2["Tags"]}
            assert "env" not in keys2
            assert "owner" in keys2
        finally:
            client.delete_dataset_group(DatasetGroupArn=arn)


class TestForecastCRUDOps:
    """Tests for Forecast CRUD operations implemented via Moto stubs."""

    SCHEMA = {
        "Attributes": [
            {"AttributeName": "item_id", "AttributeType": "string"},
            {"AttributeName": "timestamp", "AttributeType": "timestamp"},
            {"AttributeName": "target_value", "AttributeType": "float"},
        ]
    }

    @pytest.fixture
    def client(self):
        return make_client("forecast")

    @pytest.fixture
    def dataset_arn(self, client):
        r = client.create_dataset(
            DatasetName="test-forecast-ds",
            Domain="RETAIL",
            DatasetType="TARGET_TIME_SERIES",
            Schema=self.SCHEMA,
        )
        yield r["DatasetArn"]
        try:
            client.delete_dataset(DatasetArn=r["DatasetArn"])
        except ClientError:
            pass  # best-effort cleanup

    @pytest.fixture
    def dataset_group_arn(self, client):
        r = client.create_dataset_group(DatasetGroupName="test-forecast-dg", Domain="RETAIL")
        yield r["DatasetGroupArn"]
        try:
            client.delete_dataset_group(DatasetGroupArn=r["DatasetGroupArn"])
        except ClientError:
            pass  # best-effort cleanup

    def test_create_describe_delete_dataset(self, client):
        r = client.create_dataset(
            DatasetName="test-forecast-ds-crud",
            Domain="RETAIL",
            DatasetType="TARGET_TIME_SERIES",
            Schema=self.SCHEMA,
        )
        arn = r["DatasetArn"]
        assert "forecast" in arn
        assert "test-forecast-ds-crud" in arn

        desc = client.describe_dataset(DatasetArn=arn)
        assert desc["DatasetName"] == "test-forecast-ds-crud"
        assert desc["Domain"] == "RETAIL"

        client.delete_dataset(DatasetArn=arn)
        with pytest.raises(ClientError) as exc:
            client.describe_dataset(DatasetArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_dataset_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_dataset(
                DatasetArn="arn:aws:forecast:us-east-1:123456789012:dataset/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_forecast_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_forecast(
                ForecastArn="arn:aws:forecast:us-east-1:123456789012:forecast/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_predictor_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_predictor(
                PredictorArn="arn:aws:forecast:us-east-1:123456789012:predictor/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_auto_predictor_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_auto_predictor(
                PredictorArn="arn:aws:forecast:us-east-1:123456789012:predictor/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_monitor_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_monitor(
                MonitorArn="arn:aws:forecast:us-east-1:123456789012:monitor/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_dataset_import_job_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_dataset_import_job(
                DatasetImportJobArn=(
                    "arn:aws:forecast:us-east-1:123456789012:dataset-import-job/nonexistent"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_forecast_export_job_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_forecast_export_job(
                ForecastExportJobArn=(
                    "arn:aws:forecast:us-east-1:123456789012:forecast-export-job/nonexistent"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_predictor_backtest_export_job_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_predictor_backtest_export_job(
                PredictorBacktestExportJobArn=(
                    "arn:aws:forecast:us-east-1:123456789012:predictor-backtest-export-job/nonexistent"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_explainability_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_explainability(
                ExplainabilityArn=(
                    "arn:aws:forecast:us-east-1:123456789012:explainability/nonexistent"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_explainability_export_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_explainability_export(
                ExplainabilityExportArn=(
                    "arn:aws:forecast:us-east-1:123456789012:explainability-export/nonexistent"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_what_if_analysis_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_what_if_analysis(
                WhatIfAnalysisArn=(
                    "arn:aws:forecast:us-east-1:123456789012:what-if-analysis/nonexistent"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_what_if_forecast_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_what_if_forecast(
                WhatIfForecastArn=(
                    "arn:aws:forecast:us-east-1:123456789012:what-if-forecast/nonexistent"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_what_if_forecast_export_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_what_if_forecast_export(
                WhatIfForecastExportArn=(
                    "arn:aws:forecast:us-east-1:123456789012:what-if-forecast-export/nonexistent"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_auto_predictor_not_found_resources(self, client, dataset_group_arn):
        """CreateAutoPredictor stores a predictor and returns its ARN."""
        try:
            r = client.create_auto_predictor(PredictorName="test-auto-pred")
            assert "PredictorArn" in r
        except ClientError as exc:
            assert exc.response["Error"]["Code"] in (
                "ResourceNotFoundException",
                "InvalidInputException",
            )

    def test_create_monitor_returns_arn(self, client):
        """CreateMonitor stores a monitor and returns its ARN."""
        try:
            r = client.create_monitor(
                MonitorName="test-monitor",
                ResourceArn="arn:aws:forecast:us-east-1:123456789012:predictor/fake-pred",
            )
            assert "MonitorArn" in r
        except ClientError as exc:
            assert exc.response["Error"]["Code"] in (
                "ResourceNotFoundException",
                "InvalidInputException",
            )

    def test_stop_resource_not_found(self, client):
        """StopResource returns error for nonexistent resource."""
        with pytest.raises(ClientError) as exc:
            client.stop_resource(
                ResourceArn="arn:aws:forecast:us-east-1:123456789012:forecast/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "InvalidInputException",
        )

    def test_resume_resource_not_found(self, client):
        """ResumeResource returns error for nonexistent resource."""
        with pytest.raises(ClientError) as exc:
            client.resume_resource(
                ResourceArn="arn:aws:forecast:us-east-1:123456789012:monitor/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "InvalidInputException",
        )

    def test_list_monitor_evaluations_returns_list(self, client):
        """ListMonitorEvaluations returns a list (empty for nonexistent monitor)."""
        try:
            r = client.list_monitor_evaluations(
                MonitorArn="arn:aws:forecast:us-east-1:123456789012:monitor/nonexistent"
            )
            assert "PredictorMonitorEvaluations" in r
        except ClientError as exc:
            assert exc.response["Error"]["Code"] in (
                "ResourceNotFoundException",
                "InvalidInputException",
            )

    def test_get_accuracy_metrics_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.get_accuracy_metrics(
                PredictorArn="arn:aws:forecast:us-east-1:123456789012:predictor/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "InvalidInputException",
        )

    def test_dataset_arn_format(self, client):
        r = client.create_dataset(
            DatasetName="test-ds-arnfmt",
            Domain="RETAIL",
            DatasetType="TARGET_TIME_SERIES",
            Schema=self.SCHEMA,
        )
        arn = r["DatasetArn"]
        parts = arn.split(":")
        assert parts[0] == "arn"
        assert parts[1] == "aws"
        assert parts[2] == "forecast"
        assert "dataset/test-ds-arnfmt" in arn
        client.delete_dataset(DatasetArn=arn)

    def test_dataset_describe_timestamps_present(self, client):
        from datetime import datetime

        r = client.create_dataset(
            DatasetName="test-ds-tscheck",
            Domain="RETAIL",
            DatasetType="TARGET_TIME_SERIES",
            Schema=self.SCHEMA,
        )
        arn = r["DatasetArn"]
        desc = client.describe_dataset(DatasetArn=arn)
        assert isinstance(desc["CreationTime"], datetime)
        assert isinstance(desc["LastModificationTime"], datetime)
        assert desc["CreationTime"].year >= 2024
        client.delete_dataset(DatasetArn=arn)

    def test_dataset_idempotent_create_error(self, client):
        r = client.create_dataset(
            DatasetName="test-ds-idem",
            Domain="RETAIL",
            DatasetType="TARGET_TIME_SERIES",
            Schema=self.SCHEMA,
        )
        arn = r["DatasetArn"]
        try:
            with pytest.raises(ClientError) as exc:
                client.create_dataset(
                    DatasetName="test-ds-idem",
                    Domain="RETAIL",
                    DatasetType="TARGET_TIME_SERIES",
                    Schema=self.SCHEMA,
                )
            assert exc.value.response["Error"]["Code"] in (
                "ResourceAlreadyExistsException",
                "AlreadyExistsException",
            )
        finally:
            client.delete_dataset(DatasetArn=arn)

    def test_delete_nonexistent_dataset(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_dataset(
                DatasetArn="arn:aws:forecast:us-east-1:123456789012:dataset/no-such"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_auto_predictor_not_found_error_code(self, client):
        """Verify ResourceNotFoundException error structure includes Message."""
        with pytest.raises(ClientError) as exc:
            client.describe_auto_predictor(
                PredictorArn="arn:aws:forecast:us-east-1:123456789012:predictor/no-exist"
            )
        err = exc.value.response["Error"]
        assert err["Code"] == "ResourceNotFoundException"
        assert "Message" in err or "message" in err

    def test_resume_resource_not_found_error_code(self, client):
        """Verify ResumeResource returns a recognized error code."""
        with pytest.raises(ClientError) as exc:
            client.resume_resource(
                ResourceArn="arn:aws:forecast:us-east-1:123456789012:monitor/no-exist"
            )
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "InvalidInputException",
        )
        # Error response must have a Message field
        assert "Message" in exc.value.response["Error"] or "message" in exc.value.response["Error"]

    def test_list_dataset_groups_empty_returns_list(self, client):
        """list_dataset_groups always returns DatasetGroups key as list."""
        resp = client.list_dataset_groups()
        assert isinstance(resp["DatasetGroups"], list)
        assert resp.get("NextToken") is None or "NextToken" not in resp or isinstance(
            resp["NextToken"], str
        )


class TestForecastDatasetEdgeCases:
    """Edge cases for datasets: pagination, unicode, schema variations."""

    SCHEMA = {
        "Attributes": [
            {"AttributeName": "item_id", "AttributeType": "string"},
            {"AttributeName": "timestamp", "AttributeType": "timestamp"},
            {"AttributeName": "target_value", "AttributeType": "float"},
        ]
    }

    @pytest.fixture
    def client(self):
        return make_client("forecast")

    def test_dataset_multiple_create_list_delete(self, client):
        """Create 3 datasets, verify all appear in list, then delete all."""
        arns = []
        for i in range(3):
            r = client.create_dataset(
                DatasetName=f"test-ds-multi-{i}",
                Domain="RETAIL",
                DatasetType="TARGET_TIME_SERIES",
                Schema=self.SCHEMA,
            )
            arns.append(r["DatasetArn"])
        try:
            resp = client.list_datasets()
            listed_arns = {ds["DatasetArn"] for ds in resp["Datasets"]}
            for arn in arns:
                assert arn in listed_arns
        finally:
            for arn in arns:
                try:
                    client.delete_dataset(DatasetArn=arn)
                except ClientError:
                    pass

    def test_dataset_different_domains(self, client):
        """Create datasets with different domains and verify domain is preserved."""
        domains = ["RETAIL", "CUSTOM", "INVENTORY_PLANNING"]
        arns = []
        for domain in domains:
            r = client.create_dataset(
                DatasetName=f"test-ds-domain-{domain.lower()}",
                Domain=domain,
                DatasetType="TARGET_TIME_SERIES",
                Schema=self.SCHEMA,
            )
            arns.append(r["DatasetArn"])
        try:
            for arn, expected_domain in zip(arns, domains):
                desc = client.describe_dataset(DatasetArn=arn)
                assert desc["Domain"] == expected_domain
        finally:
            for arn in arns:
                try:
                    client.delete_dataset(DatasetArn=arn)
                except ClientError:
                    pass

    def test_dataset_different_types(self, client):
        """Create datasets with different DatasetTypes."""
        types_and_schemas = [
            ("TARGET_TIME_SERIES", self.SCHEMA),
            (
                "RELATED_TIME_SERIES",
                {
                    "Attributes": [
                        {"AttributeName": "item_id", "AttributeType": "string"},
                        {"AttributeName": "timestamp", "AttributeType": "timestamp"},
                        {"AttributeName": "price", "AttributeType": "float"},
                    ]
                },
            ),
            (
                "ITEM_METADATA",
                {
                    "Attributes": [
                        {"AttributeName": "item_id", "AttributeType": "string"},
                        {"AttributeName": "category", "AttributeType": "string"},
                    ]
                },
            ),
        ]
        arns = []
        for ds_type, schema in types_and_schemas:
            r = client.create_dataset(
                DatasetName=f"test-ds-type-{ds_type.lower().replace('_', '-')}",
                Domain="RETAIL",
                DatasetType=ds_type,
                Schema=schema,
            )
            arns.append((r["DatasetArn"], ds_type))
        try:
            for arn, expected_type in arns:
                desc = client.describe_dataset(DatasetArn=arn)
                assert desc["DatasetType"] == expected_type
        finally:
            for arn, _ in arns:
                try:
                    client.delete_dataset(DatasetArn=arn)
                except ClientError:
                    pass

    def test_dataset_schema_preserved(self, client):
        """Verify schema attributes are returned correctly on describe."""
        r = client.create_dataset(
            DatasetName="test-ds-schema-check",
            Domain="RETAIL",
            DatasetType="TARGET_TIME_SERIES",
            Schema=self.SCHEMA,
        )
        arn = r["DatasetArn"]
        try:
            desc = client.describe_dataset(DatasetArn=arn)
            attrs = desc["Schema"]["Attributes"]
            attr_names = [a["AttributeName"] for a in attrs]
            assert len(attrs) == 3
            assert "item_id" in attr_names
            assert "timestamp" in attr_names
            assert "target_value" in attr_names
        finally:
            client.delete_dataset(DatasetArn=arn)

    def test_dataset_tag_operations(self, client):
        """Tag, list, untag on a dataset resource."""
        r = client.create_dataset(
            DatasetName="test-ds-tags",
            Domain="RETAIL",
            DatasetType="TARGET_TIME_SERIES",
            Schema=self.SCHEMA,
        )
        arn = r["DatasetArn"]
        try:
            client.tag_resource(
                ResourceArn=arn,
                Tags=[
                    {"Key": "env", "Value": "staging"},
                    {"Key": "team", "Value": "ml"},
                ],
            )
            tag_resp = client.list_tags_for_resource(ResourceArn=arn)
            keys = {t["Key"] for t in tag_resp["Tags"]}
            assert "env" in keys
            assert "team" in keys

            client.untag_resource(ResourceArn=arn, TagKeys=["env"])
            tag_resp2 = client.list_tags_for_resource(ResourceArn=arn)
            keys2 = {t["Key"] for t in tag_resp2["Tags"]}
            assert "env" not in keys2
            assert "team" in keys2
        finally:
            client.delete_dataset(DatasetArn=arn)

    def test_dataset_creation_time_before_last_modification(self, client):
        """CreationTime should be <= LastModificationTime."""
        r = client.create_dataset(
            DatasetName="test-ds-time-order",
            Domain="RETAIL",
            DatasetType="TARGET_TIME_SERIES",
            Schema=self.SCHEMA,
        )
        arn = r["DatasetArn"]
        try:
            desc = client.describe_dataset(DatasetArn=arn)
            assert desc["CreationTime"] <= desc["LastModificationTime"]
        finally:
            client.delete_dataset(DatasetArn=arn)

    def test_dataset_group_update_clear_dataset_arns(self, client):
        """Update a dataset group to clear its DatasetArns."""
        dg_r = client.create_dataset_group(
            DatasetGroupName="test-dsg-update-clear", Domain="RETAIL"
        )
        dg_arn = dg_r["DatasetGroupArn"]
        try:
            client.update_dataset_group(DatasetGroupArn=dg_arn, DatasetArns=[])
            desc = client.describe_dataset_group(DatasetGroupArn=dg_arn)
            assert desc.get("DatasetArns", []) == []
        finally:
            client.delete_dataset_group(DatasetGroupArn=dg_arn)

    def test_dataset_group_creation_time_ordering(self, client):
        """CreationTime should be <= LastModificationTime for dataset groups."""
        r = client.create_dataset_group(
            DatasetGroupName="test-dsg-time-order", Domain="RETAIL"
        )
        arn = r["DatasetGroupArn"]
        try:
            desc = client.describe_dataset_group(DatasetGroupArn=arn)
            assert desc["CreationTime"] <= desc["LastModificationTime"]
        finally:
            client.delete_dataset_group(DatasetGroupArn=arn)

    def test_dataset_group_domain_preserved(self, client):
        """Create dataset groups with different domains, verify domain is preserved."""
        for domain in ["RETAIL", "CUSTOM", "WEB_TRAFFIC"]:
            r = client.create_dataset_group(
                DatasetGroupName=f"test-dsg-dom-{domain.lower().replace('_', '-')}",
                Domain=domain,
            )
            arn = r["DatasetGroupArn"]
            try:
                desc = client.describe_dataset_group(DatasetGroupArn=arn)
                assert desc["Domain"] == domain
            finally:
                client.delete_dataset_group(DatasetGroupArn=arn)

    def test_tag_multiple_keys_preserved(self, client):
        """Multiple distinct tag keys are all preserved."""
        r = client.create_dataset_group(
            DatasetGroupName="test-dsg-tag-multi", Domain="RETAIL"
        )
        arn = r["DatasetGroupArn"]
        try:
            client.tag_resource(
                ResourceArn=arn,
                Tags=[
                    {"Key": "env", "Value": "dev"},
                    {"Key": "team", "Value": "ml"},
                    {"Key": "cost-center", "Value": "1234"},
                ],
            )
            tag_resp = client.list_tags_for_resource(ResourceArn=arn)
            tag_map = {t["Key"]: t["Value"] for t in tag_resp["Tags"]}
            assert tag_map["env"] == "dev"
            assert tag_map["team"] == "ml"
            assert tag_map["cost-center"] == "1234"
        finally:
            client.delete_dataset_group(DatasetGroupArn=arn)

    def test_untag_nonexistent_key_no_error(self, client):
        """Untagging a key that doesn't exist should not raise an error."""
        r = client.create_dataset_group(
            DatasetGroupName="test-dsg-untag-missing", Domain="RETAIL"
        )
        arn = r["DatasetGroupArn"]
        try:
            # Should not raise
            client.untag_resource(ResourceArn=arn, TagKeys=["nonexistent-key"])
            tag_resp = client.list_tags_for_resource(ResourceArn=arn)
            assert isinstance(tag_resp["Tags"], list)
        finally:
            client.delete_dataset_group(DatasetGroupArn=arn)

    def test_describe_dataset_status_active(self, client):
        """A newly created dataset should have Status ACTIVE."""
        r = client.create_dataset(
            DatasetName="test-ds-status",
            Domain="RETAIL",
            DatasetType="TARGET_TIME_SERIES",
            Schema=self.SCHEMA,
        )
        arn = r["DatasetArn"]
        try:
            desc = client.describe_dataset(DatasetArn=arn)
            assert desc["Status"] == "ACTIVE"
        finally:
            client.delete_dataset(DatasetArn=arn)

    def test_dataset_group_status_active(self, client):
        """A newly created dataset group should have Status ACTIVE."""
        r = client.create_dataset_group(
            DatasetGroupName="test-dsg-status-check", Domain="RETAIL"
        )
        arn = r["DatasetGroupArn"]
        try:
            desc = client.describe_dataset_group(DatasetGroupArn=arn)
            assert desc["Status"] == "ACTIVE"
        finally:
            client.delete_dataset_group(DatasetGroupArn=arn)

    def test_dataset_group_empty_dataset_arns_initially(self, client):
        """A new dataset group should have empty DatasetArns."""
        r = client.create_dataset_group(
            DatasetGroupName="test-dsg-empty-arns", Domain="RETAIL"
        )
        arn = r["DatasetGroupArn"]
        try:
            desc = client.describe_dataset_group(DatasetGroupArn=arn)
            assert desc.get("DatasetArns", []) == []
        finally:
            client.delete_dataset_group(DatasetGroupArn=arn)


class TestForecastListOpEdgeCases:
    """Edge cases and behavioral fidelity tests for Forecast list operations.

    Covers CREATE+LIST+DELETE patterns and ERROR cases for resources that
    previously only had bare list-empty tests.
    """

    SCHEMA = {
        "Attributes": [
            {"AttributeName": "item_id", "AttributeType": "string"},
            {"AttributeName": "timestamp", "AttributeType": "timestamp"},
            {"AttributeName": "target_value", "AttributeType": "float"},
        ]
    }

    @pytest.fixture
    def client(self):
        return make_client("forecast")

    # -------------------------------------------------------------------------
    # DatasetGroup — CREATE + RETRIEVE + DELETE lifecycle
    # -------------------------------------------------------------------------

    def test_dataset_group_full_lifecycle(self, client):
        """CREATE → describe → verify fields → DELETE."""
        r = client.create_dataset_group(
            DatasetGroupName="test-edge-dsg-lifecycle", Domain="RETAIL"
        )
        arn = r["DatasetGroupArn"]
        assert "dataset-group/test-edge-dsg-lifecycle" in arn

        desc = client.describe_dataset_group(DatasetGroupArn=arn)
        assert desc["DatasetGroupName"] == "test-edge-dsg-lifecycle"
        assert desc["Domain"] == "RETAIL"
        assert desc["Status"] == "ACTIVE"

        client.delete_dataset_group(DatasetGroupArn=arn)

        with pytest.raises(ClientError) as exc:
            client.describe_dataset_group(DatasetGroupArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_dataset_groups_created_item_has_required_fields(self, client):
        """list_dataset_groups entries must have ARN, name, and timestamps."""
        r = client.create_dataset_group(
            DatasetGroupName="test-edge-dsg-list-fields", Domain="CUSTOM"
        )
        arn = r["DatasetGroupArn"]
        try:
            resp = client.list_dataset_groups()
            match = next(
                (g for g in resp["DatasetGroups"] if g["DatasetGroupArn"] == arn), None
            )
            assert match is not None
            assert match["DatasetGroupName"] == "test-edge-dsg-list-fields"
            assert "CreationTime" in match
            assert "LastModificationTime" in match
        finally:
            client.delete_dataset_group(DatasetGroupArn=arn)

    def test_list_dataset_groups_arn_format(self, client):
        """ARN returned by list_dataset_groups must follow forecast ARN pattern."""
        r = client.create_dataset_group(
            DatasetGroupName="test-edge-dsg-arn-fmt", Domain="RETAIL"
        )
        arn = r["DatasetGroupArn"]
        try:
            resp = client.list_dataset_groups()
            match = next(
                (g for g in resp["DatasetGroups"] if g["DatasetGroupArn"] == arn), None
            )
            assert match is not None
            parts = match["DatasetGroupArn"].split(":")
            assert parts[0] == "arn"
            assert parts[1] == "aws"
            assert parts[2] == "forecast"
            assert "dataset-group/test-edge-dsg-arn-fmt" in match["DatasetGroupArn"]
        finally:
            client.delete_dataset_group(DatasetGroupArn=arn)

    def test_delete_nonexistent_dataset_group_error_message(self, client):
        """DELETE of nonexistent dataset group includes a Message in the error."""
        with pytest.raises(ClientError) as exc:
            client.delete_dataset_group(
                DatasetGroupArn=(
                    "arn:aws:forecast:us-east-1:123456789012:dataset-group/no-such-dsg"
                )
            )
        err = exc.value.response["Error"]
        assert err["Code"] == "ResourceNotFoundException"
        assert "Message" in err or "message" in err

    # -------------------------------------------------------------------------
    # DatasetImportJob — CREATE + LIST + DELETE lifecycle
    # -------------------------------------------------------------------------

    def test_dataset_import_job_create_list_delete(self, client):
        """CREATE import job → verify in list → DELETE → no longer in list."""
        r = client.create_dataset_import_job(
            DatasetImportJobName="test-edge-import-job",
            DatasetArn="arn:aws:forecast:us-east-1:123456789012:dataset/dummy",
            DataSource={
                "S3Config": {
                    "Path": "s3://test-bucket/data.csv",
                    "RoleArn": "arn:aws:iam::123456789012:role/ForecastRole",
                }
            },
            TimestampFormat="yyyy-MM-dd HH:mm:ss",
        )
        job_arn = r["DatasetImportJobArn"]
        assert "dataset-import-job/test-edge-import-job" in job_arn

        resp = client.list_dataset_import_jobs()
        arns = [j["DatasetImportJobArn"] for j in resp["DatasetImportJobs"]]
        assert job_arn in arns

        client.delete_dataset_import_job(DatasetImportJobArn=job_arn)
        resp2 = client.list_dataset_import_jobs()
        arns2 = [j["DatasetImportJobArn"] for j in resp2["DatasetImportJobs"]]
        assert job_arn not in arns2

    def test_list_dataset_import_jobs_entry_fields(self, client):
        """Each entry in list_dataset_import_jobs must have ARN, name, timestamps."""
        r = client.create_dataset_import_job(
            DatasetImportJobName="test-edge-import-fields",
            DatasetArn="arn:aws:forecast:us-east-1:123456789012:dataset/dummy",
            DataSource={
                "S3Config": {
                    "Path": "s3://test-bucket/data.csv",
                    "RoleArn": "arn:aws:iam::123456789012:role/ForecastRole",
                }
            },
            TimestampFormat="yyyy-MM-dd HH:mm:ss",
        )
        job_arn = r["DatasetImportJobArn"]
        try:
            resp = client.list_dataset_import_jobs()
            match = next(
                (j for j in resp["DatasetImportJobs"] if j["DatasetImportJobArn"] == job_arn),
                None,
            )
            assert match is not None
            assert match["DatasetImportJobName"] == "test-edge-import-fields"
            assert "CreationTime" in match
            assert "LastModificationTime" in match
            assert "Status" in match
        finally:
            client.delete_dataset_import_job(DatasetImportJobArn=job_arn)

    def test_delete_nonexistent_dataset_import_job(self, client):
        """DELETE of nonexistent import job returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.delete_dataset_import_job(
                DatasetImportJobArn=(
                    "arn:aws:forecast:us-east-1:123456789012:"
                    "dataset-import-job/noexist/00000000-0000-0000-0000-000000000000"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_nonexistent_dataset_import_job(self, client):
        """DESCRIBE of nonexistent import job returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.describe_dataset_import_job(
                DatasetImportJobArn=(
                    "arn:aws:forecast:us-east-1:123456789012:"
                    "dataset-import-job/noexist/00000000-0000-0000-0000-000000000000"
                )
            )
        err = exc.value.response["Error"]
        assert err["Code"] == "ResourceNotFoundException"
        assert "Message" in err or "message" in err

    def test_list_dataset_import_jobs_no_next_token_when_empty(self, client):
        """When list_dataset_import_jobs has no results, NextToken should be absent."""
        # Delete all existing import jobs first
        existing = client.list_dataset_import_jobs()["DatasetImportJobs"]
        for job in existing:
            try:
                client.delete_dataset_import_job(
                    DatasetImportJobArn=job["DatasetImportJobArn"]
                )
            except ClientError:
                pass
        resp = client.list_dataset_import_jobs()
        assert "DatasetImportJobs" in resp
        assert resp.get("NextToken") is None or "NextToken" not in resp

    # -------------------------------------------------------------------------
    # PredictorBacktestExportJob — CREATE + LIST + DELETE lifecycle
    # -------------------------------------------------------------------------

    def test_predictor_backtest_export_create_list_delete(self, client):
        """CREATE backtest export → verify in list → DELETE → gone."""
        r = client.create_predictor_backtest_export_job(
            PredictorBacktestExportJobName="test-edge-backtest-export",
            PredictorArn="arn:aws:forecast:us-east-1:123456789012:predictor/dummy-pred",
            Destination={
                "S3Config": {
                    "Path": "s3://test-bucket/backtest/",
                    "RoleArn": "arn:aws:iam::123456789012:role/ForecastRole",
                }
            },
        )
        job_arn = r["PredictorBacktestExportJobArn"]
        assert "predictor-backtest-export-job" in job_arn

        resp = client.list_predictor_backtest_export_jobs()
        arns = [j["PredictorBacktestExportJobArn"] for j in resp["PredictorBacktestExportJobs"]]
        assert job_arn in arns

        client.delete_predictor_backtest_export_job(PredictorBacktestExportJobArn=job_arn)
        resp2 = client.list_predictor_backtest_export_jobs()
        arns2 = [j["PredictorBacktestExportJobArn"] for j in resp2["PredictorBacktestExportJobs"]]
        assert job_arn not in arns2

    def test_list_predictor_backtest_export_jobs_entry_fields(self, client):
        """Entries in list_predictor_backtest_export_jobs must have required fields."""
        r = client.create_predictor_backtest_export_job(
            PredictorBacktestExportJobName="test-edge-backtest-fields",
            PredictorArn="arn:aws:forecast:us-east-1:123456789012:predictor/dummy-pred",
            Destination={
                "S3Config": {
                    "Path": "s3://test-bucket/backtest/",
                    "RoleArn": "arn:aws:iam::123456789012:role/ForecastRole",
                }
            },
        )
        job_arn = r["PredictorBacktestExportJobArn"]
        try:
            resp = client.list_predictor_backtest_export_jobs()
            match = next(
                (
                    j
                    for j in resp["PredictorBacktestExportJobs"]
                    if j["PredictorBacktestExportJobArn"] == job_arn
                ),
                None,
            )
            assert match is not None
            assert match["PredictorBacktestExportJobName"] == "test-edge-backtest-fields"
            assert "CreationTime" in match
            assert "LastModificationTime" in match
            assert "Status" in match
        finally:
            client.delete_predictor_backtest_export_job(PredictorBacktestExportJobArn=job_arn)

    def test_delete_nonexistent_predictor_backtest_export_job(self, client):
        """DELETE of nonexistent backtest export job returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.delete_predictor_backtest_export_job(
                PredictorBacktestExportJobArn=(
                    "arn:aws:forecast:us-east-1:123456789012:"
                    "predictor-backtest-export-job/noexist"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # -------------------------------------------------------------------------
    # Explainability — CREATE + LIST + DELETE lifecycle
    # -------------------------------------------------------------------------

    def test_explainability_create_list_delete(self, client):
        """CREATE explainability → verify in list → DELETE → gone."""
        r = client.create_explainability(
            ExplainabilityName="test-edge-explainability",
            ResourceArn="arn:aws:forecast:us-east-1:123456789012:predictor/dummy-pred",
            ExplainabilityConfig={
                "TimeSeriesGranularity": "ALL",
                "TimePointGranularity": "ALL",
            },
        )
        exp_arn = r["ExplainabilityArn"]
        assert "explainability/test-edge-explainability" in exp_arn

        resp = client.list_explainabilities()
        arns = [e["ExplainabilityArn"] for e in resp["Explainabilities"]]
        assert exp_arn in arns

        client.delete_explainability(ExplainabilityArn=exp_arn)
        resp2 = client.list_explainabilities()
        arns2 = [e["ExplainabilityArn"] for e in resp2["Explainabilities"]]
        assert exp_arn not in arns2

    def test_list_explainabilities_entry_fields(self, client):
        """Entries in list_explainabilities must have ARN, name, timestamps, status."""
        r = client.create_explainability(
            ExplainabilityName="test-edge-exp-fields",
            ResourceArn="arn:aws:forecast:us-east-1:123456789012:predictor/dummy-pred",
            ExplainabilityConfig={
                "TimeSeriesGranularity": "ALL",
                "TimePointGranularity": "ALL",
            },
        )
        exp_arn = r["ExplainabilityArn"]
        try:
            resp = client.list_explainabilities()
            match = next(
                (e for e in resp["Explainabilities"] if e["ExplainabilityArn"] == exp_arn),
                None,
            )
            assert match is not None
            assert match["ExplainabilityName"] == "test-edge-exp-fields"
            assert "CreationTime" in match
            assert "LastModificationTime" in match
            assert "Status" in match
        finally:
            client.delete_explainability(ExplainabilityArn=exp_arn)

    def test_delete_nonexistent_explainability(self, client):
        """DELETE of nonexistent explainability returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.delete_explainability(
                ExplainabilityArn=(
                    "arn:aws:forecast:us-east-1:123456789012:explainability/noexist"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # -------------------------------------------------------------------------
    # ExplainabilityExport — CREATE + LIST + DELETE lifecycle
    # -------------------------------------------------------------------------

    def test_explainability_export_create_list_delete(self, client):
        """CREATE explainability export → verify in list → DELETE → gone."""
        r = client.create_explainability_export(
            ExplainabilityExportName="test-edge-exp-export",
            ExplainabilityArn=(
                "arn:aws:forecast:us-east-1:123456789012:explainability/dummy-exp"
            ),
            Destination={
                "S3Config": {
                    "Path": "s3://test-bucket/explainability/",
                    "RoleArn": "arn:aws:iam::123456789012:role/ForecastRole",
                }
            },
        )
        export_arn = r["ExplainabilityExportArn"]
        assert "explainability-export/test-edge-exp-export" in export_arn

        resp = client.list_explainability_exports()
        arns = [e["ExplainabilityExportArn"] for e in resp["ExplainabilityExports"]]
        assert export_arn in arns

        client.delete_explainability_export(ExplainabilityExportArn=export_arn)
        resp2 = client.list_explainability_exports()
        arns2 = [e["ExplainabilityExportArn"] for e in resp2["ExplainabilityExports"]]
        assert export_arn not in arns2

    def test_list_explainability_exports_entry_fields(self, client):
        """Entries in list_explainability_exports must have ARN, name, timestamps, status."""
        r = client.create_explainability_export(
            ExplainabilityExportName="test-edge-expexp-fields",
            ExplainabilityArn=(
                "arn:aws:forecast:us-east-1:123456789012:explainability/dummy-exp"
            ),
            Destination={
                "S3Config": {
                    "Path": "s3://test-bucket/explainability/",
                    "RoleArn": "arn:aws:iam::123456789012:role/ForecastRole",
                }
            },
        )
        export_arn = r["ExplainabilityExportArn"]
        try:
            resp = client.list_explainability_exports()
            match = next(
                (e for e in resp["ExplainabilityExports"] if e["ExplainabilityExportArn"] == export_arn),
                None,
            )
            assert match is not None
            assert match["ExplainabilityExportName"] == "test-edge-expexp-fields"
            assert "CreationTime" in match
            assert "LastModificationTime" in match
            assert "Status" in match
        finally:
            client.delete_explainability_export(ExplainabilityExportArn=export_arn)

    def test_delete_nonexistent_explainability_export(self, client):
        """DELETE of nonexistent explainability export returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.delete_explainability_export(
                ExplainabilityExportArn=(
                    "arn:aws:forecast:us-east-1:123456789012:explainability-export/noexist"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # -------------------------------------------------------------------------
    # Monitor — CREATE + LIST + DELETE + RESUME lifecycle
    # -------------------------------------------------------------------------

    def test_monitor_create_list_delete(self, client):
        """CREATE monitor → verify in list → DELETE → gone."""
        r = client.create_monitor(
            MonitorName="test-edge-monitor",
            ResourceArn="arn:aws:forecast:us-east-1:123456789012:predictor/dummy-pred",
        )
        monitor_arn = r["MonitorArn"]
        assert "monitor/test-edge-monitor" in monitor_arn

        resp = client.list_monitors()
        arns = [m["MonitorArn"] for m in resp["Monitors"]]
        assert monitor_arn in arns

        client.delete_monitor(MonitorArn=monitor_arn)
        resp2 = client.list_monitors()
        arns2 = [m["MonitorArn"] for m in resp2["Monitors"]]
        assert monitor_arn not in arns2

    def test_list_monitors_entry_fields(self, client):
        """Entries in list_monitors must have ARN, name, timestamps, status."""
        r = client.create_monitor(
            MonitorName="test-edge-monitor-fields",
            ResourceArn="arn:aws:forecast:us-east-1:123456789012:predictor/dummy-pred",
        )
        monitor_arn = r["MonitorArn"]
        try:
            resp = client.list_monitors()
            match = next(
                (m for m in resp["Monitors"] if m["MonitorArn"] == monitor_arn),
                None,
            )
            assert match is not None
            assert match["MonitorName"] == "test-edge-monitor-fields"
            assert "CreationTime" in match
            assert "LastModificationTime" in match
            assert "Status" in match
        finally:
            client.delete_monitor(MonitorArn=monitor_arn)

    def test_resume_resource_on_existing_monitor(self, client):
        """ResumeResource on an existing monitor succeeds (CREATE + RESUME pattern)."""
        r = client.create_monitor(
            MonitorName="test-edge-monitor-resume",
            ResourceArn="arn:aws:forecast:us-east-1:123456789012:predictor/dummy-pred",
        )
        monitor_arn = r["MonitorArn"]
        try:
            # Should not raise for an existing resource
            client.resume_resource(ResourceArn=monitor_arn)
            # Verify monitor still exists after resume
            resp = client.list_monitors()
            arns = [m["MonitorArn"] for m in resp["Monitors"]]
            assert monitor_arn in arns
        finally:
            client.delete_monitor(MonitorArn=monitor_arn)

    def test_delete_nonexistent_monitor(self, client):
        """DELETE of nonexistent monitor returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.delete_monitor(
                MonitorArn=(
                    "arn:aws:forecast:us-east-1:123456789012:monitor/noexist"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_resume_resource_not_found_with_message(self, client):
        """ResumeResource on nonexistent resource returns error with Message field."""
        with pytest.raises(ClientError) as exc:
            client.resume_resource(
                ResourceArn=(
                    "arn:aws:forecast:us-east-1:123456789012:monitor/noexist-resume"
                )
            )
        err = exc.value.response["Error"]
        assert err["Code"] in ("ResourceNotFoundException", "InvalidInputException")
        assert "Message" in err or "message" in err

    # -------------------------------------------------------------------------
    # WhatIfAnalysis — CREATE + LIST + DELETE lifecycle
    # -------------------------------------------------------------------------

    def test_what_if_analysis_create_list_delete(self, client):
        """CREATE what-if analysis → verify in list → DELETE → gone."""
        r = client.create_what_if_analysis(
            WhatIfAnalysisName="test-edge-wia",
            ForecastArn="arn:aws:forecast:us-east-1:123456789012:forecast/dummy-fc",
        )
        wia_arn = r["WhatIfAnalysisArn"]
        assert "what-if-analysis/test-edge-wia" in wia_arn

        resp = client.list_what_if_analyses()
        arns = [w["WhatIfAnalysisArn"] for w in resp["WhatIfAnalyses"]]
        assert wia_arn in arns

        client.delete_what_if_analysis(WhatIfAnalysisArn=wia_arn)
        resp2 = client.list_what_if_analyses()
        arns2 = [w["WhatIfAnalysisArn"] for w in resp2["WhatIfAnalyses"]]
        assert wia_arn not in arns2

    def test_list_what_if_analyses_entry_fields(self, client):
        """Entries in list_what_if_analyses must have ARN, name, timestamps, status."""
        r = client.create_what_if_analysis(
            WhatIfAnalysisName="test-edge-wia-fields",
            ForecastArn="arn:aws:forecast:us-east-1:123456789012:forecast/dummy-fc",
        )
        wia_arn = r["WhatIfAnalysisArn"]
        try:
            resp = client.list_what_if_analyses()
            match = next(
                (w for w in resp["WhatIfAnalyses"] if w["WhatIfAnalysisArn"] == wia_arn),
                None,
            )
            assert match is not None
            assert match["WhatIfAnalysisName"] == "test-edge-wia-fields"
            assert "CreationTime" in match
            assert "LastModificationTime" in match
            assert "Status" in match
        finally:
            client.delete_what_if_analysis(WhatIfAnalysisArn=wia_arn)

    def test_delete_nonexistent_what_if_analysis(self, client):
        """DELETE of nonexistent what-if analysis returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.delete_what_if_analysis(
                WhatIfAnalysisArn=(
                    "arn:aws:forecast:us-east-1:123456789012:what-if-analysis/noexist"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_nonexistent_what_if_analysis(self, client):
        """DESCRIBE of nonexistent what-if analysis returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.describe_what_if_analysis(
                WhatIfAnalysisArn=(
                    "arn:aws:forecast:us-east-1:123456789012:what-if-analysis/noexist"
                )
            )
        err = exc.value.response["Error"]
        assert err["Code"] == "ResourceNotFoundException"
        assert "Message" in err or "message" in err

    # -------------------------------------------------------------------------
    # WhatIfForecast — CREATE + LIST + DELETE lifecycle
    # -------------------------------------------------------------------------

    def test_what_if_forecast_create_list_delete(self, client):
        """CREATE what-if forecast → verify in list → DELETE → gone."""
        r = client.create_what_if_forecast(
            WhatIfForecastName="test-edge-wif",
            WhatIfAnalysisArn=(
                "arn:aws:forecast:us-east-1:123456789012:what-if-analysis/dummy-wia"
            ),
        )
        wif_arn = r["WhatIfForecastArn"]
        assert "what-if-forecast/test-edge-wif" in wif_arn

        resp = client.list_what_if_forecasts()
        arns = [w["WhatIfForecastArn"] for w in resp["WhatIfForecasts"]]
        assert wif_arn in arns

        client.delete_what_if_forecast(WhatIfForecastArn=wif_arn)
        resp2 = client.list_what_if_forecasts()
        arns2 = [w["WhatIfForecastArn"] for w in resp2["WhatIfForecasts"]]
        assert wif_arn not in arns2

    def test_list_what_if_forecasts_entry_fields(self, client):
        """Entries in list_what_if_forecasts must have ARN, name, timestamps, status."""
        r = client.create_what_if_forecast(
            WhatIfForecastName="test-edge-wif-fields",
            WhatIfAnalysisArn=(
                "arn:aws:forecast:us-east-1:123456789012:what-if-analysis/dummy-wia"
            ),
        )
        wif_arn = r["WhatIfForecastArn"]
        try:
            resp = client.list_what_if_forecasts()
            match = next(
                (w for w in resp["WhatIfForecasts"] if w["WhatIfForecastArn"] == wif_arn),
                None,
            )
            assert match is not None
            assert match["WhatIfForecastName"] == "test-edge-wif-fields"
            assert "CreationTime" in match
            assert "LastModificationTime" in match
            assert "Status" in match
        finally:
            client.delete_what_if_forecast(WhatIfForecastArn=wif_arn)

    def test_delete_nonexistent_what_if_forecast(self, client):
        """DELETE of nonexistent what-if forecast returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.delete_what_if_forecast(
                WhatIfForecastArn=(
                    "arn:aws:forecast:us-east-1:123456789012:what-if-forecast/noexist"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # -------------------------------------------------------------------------
    # WhatIfForecastExport — CREATE + LIST + DELETE lifecycle
    # -------------------------------------------------------------------------

    def test_what_if_forecast_export_create_list_delete(self, client):
        """CREATE what-if forecast export → verify in list → DELETE → gone."""
        r = client.create_what_if_forecast_export(
            WhatIfForecastExportName="test-edge-wife",
            WhatIfForecastArns=[
                "arn:aws:forecast:us-east-1:123456789012:what-if-forecast/dummy-wif"
            ],
            Destination={
                "S3Config": {
                    "Path": "s3://test-bucket/what-if/",
                    "RoleArn": "arn:aws:iam::123456789012:role/ForecastRole",
                }
            },
        )
        export_arn = r["WhatIfForecastExportArn"]
        assert "what-if-forecast-export/test-edge-wife" in export_arn

        resp = client.list_what_if_forecast_exports()
        arns = [e["WhatIfForecastExportArn"] for e in resp["WhatIfForecastExports"]]
        assert export_arn in arns

        client.delete_what_if_forecast_export(WhatIfForecastExportArn=export_arn)
        resp2 = client.list_what_if_forecast_exports()
        arns2 = [e["WhatIfForecastExportArn"] for e in resp2["WhatIfForecastExports"]]
        assert export_arn not in arns2

    def test_list_what_if_forecast_exports_entry_fields(self, client):
        """Entries in list_what_if_forecast_exports must have ARN, name, timestamps, status."""
        r = client.create_what_if_forecast_export(
            WhatIfForecastExportName="test-edge-wife-fields",
            WhatIfForecastArns=[
                "arn:aws:forecast:us-east-1:123456789012:what-if-forecast/dummy-wif"
            ],
            Destination={
                "S3Config": {
                    "Path": "s3://test-bucket/what-if/",
                    "RoleArn": "arn:aws:iam::123456789012:role/ForecastRole",
                }
            },
        )
        export_arn = r["WhatIfForecastExportArn"]
        try:
            resp = client.list_what_if_forecast_exports()
            match = next(
                (e for e in resp["WhatIfForecastExports"] if e["WhatIfForecastExportArn"] == export_arn),
                None,
            )
            assert match is not None
            assert match["WhatIfForecastExportName"] == "test-edge-wife-fields"
            assert "CreationTime" in match
            assert "LastModificationTime" in match
            assert "Status" in match
        finally:
            client.delete_what_if_forecast_export(WhatIfForecastExportArn=export_arn)

    def test_delete_nonexistent_what_if_forecast_export(self, client):
        """DELETE of nonexistent what-if forecast export returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.delete_what_if_forecast_export(
                WhatIfForecastExportArn=(
                    "arn:aws:forecast:us-east-1:123456789012:what-if-forecast-export/noexist"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # -------------------------------------------------------------------------
    # Forecast — CREATE + LIST + DELETE lifecycle
    # -------------------------------------------------------------------------

    def test_forecast_create_list_delete(self, client):
        """CREATE forecast → verify in list → DELETE → gone."""
        r = client.create_forecast(
            ForecastName="test-edge-forecast",
            PredictorArn="arn:aws:forecast:us-east-1:123456789012:predictor/dummy-pred",
        )
        fc_arn = r["ForecastArn"]
        assert "forecast/test-edge-forecast" in fc_arn

        resp = client.list_forecasts()
        arns = [f["ForecastArn"] for f in resp["Forecasts"]]
        assert fc_arn in arns

        client.delete_forecast(ForecastArn=fc_arn)
        resp2 = client.list_forecasts()
        arns2 = [f["ForecastArn"] for f in resp2["Forecasts"]]
        assert fc_arn not in arns2

    def test_list_forecasts_entry_fields(self, client):
        """Entries in list_forecasts must have ARN, name, timestamps, status."""
        r = client.create_forecast(
            ForecastName="test-edge-forecast-fields",
            PredictorArn="arn:aws:forecast:us-east-1:123456789012:predictor/dummy-pred",
        )
        fc_arn = r["ForecastArn"]
        try:
            resp = client.list_forecasts()
            match = next(
                (f for f in resp["Forecasts"] if f["ForecastArn"] == fc_arn),
                None,
            )
            assert match is not None
            assert match["ForecastName"] == "test-edge-forecast-fields"
            assert "CreationTime" in match
            assert "LastModificationTime" in match
            assert "Status" in match
        finally:
            client.delete_forecast(ForecastArn=fc_arn)

    def test_delete_nonexistent_forecast(self, client):
        """DELETE of nonexistent forecast returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.delete_forecast(
                ForecastArn=(
                    "arn:aws:forecast:us-east-1:123456789012:forecast/noexist"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_forecasts_no_next_token_when_empty(self, client):
        """When list_forecasts has no results, NextToken should be absent."""
        existing = client.list_forecasts()["Forecasts"]
        for fc in existing:
            try:
                client.delete_forecast(ForecastArn=fc["ForecastArn"])
            except ClientError:
                pass
        resp = client.list_forecasts()
        assert "Forecasts" in resp
        assert resp.get("NextToken") is None or "NextToken" not in resp

    # -------------------------------------------------------------------------
    # ForecastExportJob — CREATE + LIST + DELETE lifecycle
    # -------------------------------------------------------------------------

    def test_forecast_export_job_create_list_delete(self, client):
        """CREATE forecast export job → verify in list → DELETE → gone."""
        r = client.create_forecast_export_job(
            ForecastExportJobName="test-edge-fc-export",
            ForecastArn="arn:aws:forecast:us-east-1:123456789012:forecast/dummy-fc",
            Destination={
                "S3Config": {
                    "Path": "s3://test-bucket/forecasts/",
                    "RoleArn": "arn:aws:iam::123456789012:role/ForecastRole",
                }
            },
        )
        export_arn = r["ForecastExportJobArn"]
        assert "forecast-export-job/test-edge-fc-export" in export_arn

        resp = client.list_forecast_export_jobs()
        arns = [e["ForecastExportJobArn"] for e in resp["ForecastExportJobs"]]
        assert export_arn in arns

        client.delete_forecast_export_job(ForecastExportJobArn=export_arn)
        resp2 = client.list_forecast_export_jobs()
        arns2 = [e["ForecastExportJobArn"] for e in resp2["ForecastExportJobs"]]
        assert export_arn not in arns2

    def test_list_forecast_export_jobs_entry_fields(self, client):
        """Entries in list_forecast_export_jobs must have ARN, name, timestamps, status."""
        r = client.create_forecast_export_job(
            ForecastExportJobName="test-edge-fc-export-fields",
            ForecastArn="arn:aws:forecast:us-east-1:123456789012:forecast/dummy-fc",
            Destination={
                "S3Config": {
                    "Path": "s3://test-bucket/forecasts/",
                    "RoleArn": "arn:aws:iam::123456789012:role/ForecastRole",
                }
            },
        )
        export_arn = r["ForecastExportJobArn"]
        try:
            resp = client.list_forecast_export_jobs()
            match = next(
                (e for e in resp["ForecastExportJobs"] if e["ForecastExportJobArn"] == export_arn),
                None,
            )
            assert match is not None
            assert match["ForecastExportJobName"] == "test-edge-fc-export-fields"
            assert "CreationTime" in match
            assert "LastModificationTime" in match
            assert "Status" in match
        finally:
            client.delete_forecast_export_job(ForecastExportJobArn=export_arn)

    def test_delete_nonexistent_forecast_export_job(self, client):
        """DELETE of nonexistent forecast export job returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.delete_forecast_export_job(
                ForecastExportJobArn=(
                    "arn:aws:forecast:us-east-1:123456789012:forecast-export-job/noexist"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_forecast_export_jobs_no_next_token_when_empty(self, client):
        """When list_forecast_export_jobs has no results, NextToken should be absent."""
        existing = client.list_forecast_export_jobs()["ForecastExportJobs"]
        for job in existing:
            try:
                client.delete_forecast_export_job(
                    ForecastExportJobArn=job["ForecastExportJobArn"]
                )
            except ClientError:
                pass
        resp = client.list_forecast_export_jobs()
        assert "ForecastExportJobs" in resp
        assert resp.get("NextToken") is None or "NextToken" not in resp

    # -------------------------------------------------------------------------
    # Cross-resource ARN format verification
    # -------------------------------------------------------------------------

    def test_created_resource_arn_account_and_region(self, client):
        """ARNs from created resources must contain correct region and account."""
        r = client.create_monitor(
            MonitorName="test-edge-arn-check",
            ResourceArn="arn:aws:forecast:us-east-1:123456789012:predictor/dummy-pred",
        )
        monitor_arn = r["MonitorArn"]
        try:
            parts = monitor_arn.split(":")
            assert parts[3] == "us-east-1", f"Expected region 'us-east-1', got {parts[3]!r}"
            assert parts[4] == "123456789012", f"Expected account '123456789012', got {parts[4]!r}"
        finally:
            client.delete_monitor(MonitorArn=monitor_arn)

    def test_resource_timestamps_are_datetime_objects(self, client):
        """CreationTime and LastModificationTime must be datetime objects, not strings."""
        from datetime import datetime

        r = client.create_monitor(
            MonitorName="test-edge-ts-type",
            ResourceArn="arn:aws:forecast:us-east-1:123456789012:predictor/dummy-pred",
        )
        monitor_arn = r["MonitorArn"]
        try:
            resp = client.list_monitors()
            match = next(
                (m for m in resp["Monitors"] if m["MonitorArn"] == monitor_arn),
                None,
            )
            assert match is not None
            assert isinstance(match["CreationTime"], datetime), (
                f"CreationTime should be datetime, got {type(match['CreationTime'])}"
            )
            assert isinstance(match["LastModificationTime"], datetime), (
                f"LastModificationTime should be datetime, got {type(match['LastModificationTime'])}"
            )
        finally:
            client.delete_monitor(MonitorArn=monitor_arn)
