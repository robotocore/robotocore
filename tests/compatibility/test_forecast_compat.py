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

    @pytest.fixture
    def client(self):
        return make_client("forecast")

    def test_list_datasets(self, client):
        resp = client.list_datasets()
        assert "Datasets" in resp
        assert isinstance(resp["Datasets"], list)

    def test_list_dataset_import_jobs(self, client):
        resp = client.list_dataset_import_jobs()
        assert "DatasetImportJobs" in resp
        assert isinstance(resp["DatasetImportJobs"], list)

    def test_list_forecasts(self, client):
        resp = client.list_forecasts()
        assert "Forecasts" in resp
        assert isinstance(resp["Forecasts"], list)

    def test_list_forecast_export_jobs(self, client):
        resp = client.list_forecast_export_jobs()
        assert "ForecastExportJobs" in resp
        assert isinstance(resp["ForecastExportJobs"], list)

    def test_list_predictors(self, client):
        resp = client.list_predictors()
        assert "Predictors" in resp
        assert isinstance(resp["Predictors"], list)

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

    def test_list_tags_for_resource(self, client):
        arn = "arn:aws:forecast:us-east-1:123456789012:dataset-group/test"
        client.tag_resource(ResourceArn=arn, Tags=[{"Key": "env", "Value": "test"}])
        resp = client.list_tags_for_resource(ResourceArn=arn)
        assert "Tags" in resp
        assert any(t["Key"] == "env" for t in resp["Tags"])


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


class TestForecastCreateOps:
    """Tests for Create operations that return ARNs."""

    @pytest.fixture
    def client(self):
        return make_client("forecast")

    def test_create_dataset_import_job_returns_arn(self, client):
        r = client.create_dataset_import_job(
            DatasetImportJobName="test-import-job",
            DatasetArn="arn:aws:forecast:us-east-1:123456789012:dataset/nonexist",
            DataSource={
                "S3Config": {
                    "Path": "s3://test-bucket/data/",
                    "RoleArn": "arn:aws:iam::123456789012:role/test-role",
                }
            },
        )
        assert "DatasetImportJobArn" in r
        assert r["DatasetImportJobArn"].startswith("arn:aws:forecast:")

    def test_create_predictor_returns_arn(self, client):
        r = client.create_predictor(
            PredictorName="test-predictor-ops",
            ForecastHorizon=10,
            InputDataConfig={
                "DatasetGroupArn": "arn:aws:forecast:us-east-1:123456789012:dataset-group/nonexist"
            },
            FeaturizationConfig={"ForecastFrequency": "D"},
        )
        assert "PredictorArn" in r
        assert r["PredictorArn"].startswith("arn:aws:forecast:")

    def test_create_forecast_returns_arn(self, client):
        r = client.create_forecast(
            ForecastName="test-forecast-ops",
            PredictorArn="arn:aws:forecast:us-east-1:123456789012:predictor/nonexist",
        )
        assert "ForecastArn" in r
        assert r["ForecastArn"].startswith("arn:aws:forecast:")

    def test_create_forecast_export_job_returns_arn(self, client):
        r = client.create_forecast_export_job(
            ForecastExportJobName="test-forecast-export",
            ForecastArn="arn:aws:forecast:us-east-1:123456789012:forecast/nonexist",
            Destination={
                "S3Config": {
                    "Path": "s3://test-bucket/output/",
                    "RoleArn": "arn:aws:iam::123456789012:role/test-role",
                }
            },
        )
        assert "ForecastExportJobArn" in r
        assert r["ForecastExportJobArn"].startswith("arn:aws:forecast:")

    def test_create_explainability_returns_arn(self, client):
        r = client.create_explainability(
            ExplainabilityName="test-explainability-ops",
            ResourceArn="arn:aws:forecast:us-east-1:123456789012:predictor/nonexist",
            ExplainabilityConfig={
                "TimeSeriesGranularity": "ALL",
                "TimePointGranularity": "ALL",
            },
        )
        assert "ExplainabilityArn" in r
        assert r["ExplainabilityArn"].startswith("arn:aws:forecast:")

    def test_create_explainability_export_returns_arn(self, client):
        r = client.create_explainability_export(
            ExplainabilityExportName="test-explainability-export",
            ExplainabilityArn="arn:aws:forecast:us-east-1:123456789012:explainability/nonexist",
            Destination={
                "S3Config": {
                    "Path": "s3://test-bucket/output/",
                    "RoleArn": "arn:aws:iam::123456789012:role/test-role",
                }
            },
        )
        assert "ExplainabilityExportArn" in r
        assert r["ExplainabilityExportArn"].startswith("arn:aws:forecast:")

    def test_create_predictor_backtest_export_job_returns_arn(self, client):
        r = client.create_predictor_backtest_export_job(
            PredictorBacktestExportJobName="test-backtest-export",
            PredictorArn="arn:aws:forecast:us-east-1:123456789012:predictor/nonexist",
            Destination={
                "S3Config": {
                    "Path": "s3://test-bucket/output/",
                    "RoleArn": "arn:aws:iam::123456789012:role/test-role",
                }
            },
        )
        assert "PredictorBacktestExportJobArn" in r
        assert r["PredictorBacktestExportJobArn"].startswith("arn:aws:forecast:")

    def test_create_what_if_analysis_returns_arn(self, client):
        r = client.create_what_if_analysis(
            WhatIfAnalysisName="test-what-if-analysis",
            ForecastArn="arn:aws:forecast:us-east-1:123456789012:forecast/nonexist",
        )
        assert "WhatIfAnalysisArn" in r
        assert r["WhatIfAnalysisArn"].startswith("arn:aws:forecast:")

    def test_create_what_if_forecast_returns_arn(self, client):
        r = client.create_what_if_forecast(
            WhatIfForecastName="test-what-if-forecast",
            WhatIfAnalysisArn="arn:aws:forecast:us-east-1:123456789012:what-if-analysis/nonexist",
        )
        assert "WhatIfForecastArn" in r
        assert r["WhatIfForecastArn"].startswith("arn:aws:forecast:")


class TestForecastDeleteOps:
    """Tests for Delete operations returning ResourceNotFoundException for nonexistent resources."""

    @pytest.fixture
    def client(self):
        return make_client("forecast")

    def test_delete_dataset_import_job_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_dataset_import_job(
                DatasetImportJobArn=(
                    "arn:aws:forecast:us-east-1:123456789012:dataset-import-job/nonexistent"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_explainability_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_explainability(
                ExplainabilityArn=(
                    "arn:aws:forecast:us-east-1:123456789012:explainability/nonexistent"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_explainability_export_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_explainability_export(
                ExplainabilityExportArn=(
                    "arn:aws:forecast:us-east-1:123456789012:explainability-export/nonexistent"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_forecast_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_forecast(
                ForecastArn="arn:aws:forecast:us-east-1:123456789012:forecast/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_forecast_export_job_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_forecast_export_job(
                ForecastExportJobArn=(
                    "arn:aws:forecast:us-east-1:123456789012:forecast-export-job/nonexistent"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_monitor_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_monitor(
                MonitorArn="arn:aws:forecast:us-east-1:123456789012:monitor/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_predictor_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_predictor(
                PredictorArn="arn:aws:forecast:us-east-1:123456789012:predictor/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_predictor_backtest_export_job_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_predictor_backtest_export_job(
                PredictorBacktestExportJobArn=(
                    "arn:aws:forecast:us-east-1:123456789012:predictor-backtest-export-job/nonexistent"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_resource_tree_nonexistent_is_ok(self, client):
        """DeleteResourceTree on a nonexistent ARN succeeds silently."""
        resp = client.delete_resource_tree(
            ResourceArn="arn:aws:forecast:us-east-1:123456789012:dataset-group/nonexistent"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_what_if_analysis_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_what_if_analysis(
                WhatIfAnalysisArn=(
                    "arn:aws:forecast:us-east-1:123456789012:what-if-analysis/nonexistent"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_what_if_forecast_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_what_if_forecast(
                WhatIfForecastArn=(
                    "arn:aws:forecast:us-east-1:123456789012:what-if-forecast/nonexistent"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_what_if_forecast_export_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_what_if_forecast_export(
                WhatIfForecastExportArn=(
                    "arn:aws:forecast:us-east-1:123456789012:what-if-forecast-export/nonexistent"
                )
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestCreateWhatIfForecastExport:
    """Tests for CreateWhatIfForecastExport operation."""

    @pytest.fixture
    def client(self):
        return make_client("forecast")

    def test_create_what_if_forecast_export(self, client):
        """CreateWhatIfForecastExport returns a WhatIfForecastExportArn."""
        resp = client.create_what_if_forecast_export(
            WhatIfForecastExportName="test-export",
            WhatIfForecastArns=["arn:aws:forecast:us-east-1:123456789012:whatIfForecast/test"],
            Destination={
                "S3Config": {
                    "Path": "s3://test-bucket/prefix",
                    "RoleArn": "arn:aws:iam::123456789012:role/forecast-role",
                }
            },
        )
        assert "WhatIfForecastExportArn" in resp
        assert resp["WhatIfForecastExportArn"].startswith("arn:aws:forecast:")
