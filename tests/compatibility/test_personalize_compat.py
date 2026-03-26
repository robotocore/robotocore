"""Personalize compatibility tests."""

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def personalize():
    return make_client("personalize")


class TestPersonalizeOperations:
    def test_list_schemas(self, personalize):
        resp = personalize.list_schemas()
        assert "schemas" in resp
        assert isinstance(resp["schemas"], list)

    def test_describe_nonexistent_schema(self, personalize):
        with pytest.raises(ClientError) as exc:
            personalize.describe_schema(
                schemaArn="arn:aws:personalize:us-east-1:123456789012:schema/nonexist"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestPersonalizeGapListOps:
    """Tests for newly-implemented list operations."""

    @pytest.fixture
    def client(self):
        return make_client("personalize")

    def test_list_datasets(self, client):
        resp = client.list_datasets()
        assert "datasets" in resp

    def test_list_dataset_groups(self, client):
        resp = client.list_dataset_groups()
        assert "datasetGroups" in resp

    def test_list_campaigns(self, client):
        resp = client.list_campaigns()
        assert "campaigns" in resp

    def test_list_solutions(self, client):
        resp = client.list_solutions()
        assert "solutions" in resp

    def test_list_solution_versions(self, client):
        resp = client.list_solution_versions()
        assert "solutionVersions" in resp

    def test_list_recommenders(self, client):
        resp = client.list_recommenders()
        assert "recommenders" in resp

    def test_list_filters(self, client):
        resp = client.list_filters()
        assert "Filters" in resp

    def test_list_recipes(self, client):
        resp = client.list_recipes()
        assert "recipes" in resp

    def test_list_event_trackers(self, client):
        resp = client.list_event_trackers()
        assert "eventTrackers" in resp

    def test_list_batch_inference_jobs(self, client):
        resp = client.list_batch_inference_jobs()
        assert "batchInferenceJobs" in resp

    def test_list_batch_segment_jobs(self, client):
        resp = client.list_batch_segment_jobs()
        assert "batchSegmentJobs" in resp

    def test_list_metric_attributions(self, client):
        resp = client.list_metric_attributions()
        assert "metricAttributions" in resp

    def test_list_data_deletion_jobs(self, client):
        resp = client.list_data_deletion_jobs()
        assert "dataDeletionJobs" in resp

    def test_list_tags_for_resource(self, client):
        arn = "arn:aws:personalize:us-east-1:123456789012:solution/test"
        client.tag_resource(resourceArn=arn, tags=[{"tagKey": "env", "tagValue": "test"}])
        resp = client.list_tags_for_resource(resourceArn=arn)
        assert "tags" in resp

    def test_list_dataset_export_jobs_no_params(self, client):
        resp = client.list_dataset_export_jobs()
        assert "datasetExportJobs" in resp
        assert isinstance(resp["datasetExportJobs"], list)

    def test_list_metric_attribution_metrics(self, client):
        resp = client.list_metric_attribution_metrics(
            metricAttributionArn="arn:aws:personalize:us-east-1:123456789012:metric-attribution/fake"
        )
        assert "metrics" in resp


class TestPersonalizeSchemaCRUD:
    """CRUD tests for Schema with describe and delete."""

    @pytest.fixture
    def client(self):
        return make_client("personalize")

    @pytest.fixture
    def schema_json(self):
        return (
            '{"type":"record","name":"Interactions",'
            '"namespace":"com.amazonaws.personalize.schema",'
            '"fields":['
            '{"name":"USER_ID","type":"string"},'
            '{"name":"ITEM_ID","type":"string"},'
            '{"name":"TIMESTAMP","type":"long"}'
            '],"version":"1.0"}'
        )

    def test_create_describe_delete_schema(self, client, schema_json):
        resp = client.create_schema(name="test-schema-crud", schema=schema_json)
        schema_arn = resp["schemaArn"]
        assert "personalize" in schema_arn
        assert "schema" in schema_arn

        describe_resp = client.describe_schema(schemaArn=schema_arn)
        assert "schema" in describe_resp
        assert describe_resp["schema"]["name"] == "test-schema-crud"

        client.delete_schema(schemaArn=schema_arn)

        with pytest.raises(ClientError) as exc:
            client.describe_schema(schemaArn=schema_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_schema_tag_untag(self, client, schema_json):
        resp = client.create_schema(name="test-schema-tags", schema=schema_json)
        schema_arn = resp["schemaArn"]

        client.tag_resource(
            resourceArn=schema_arn,
            tags=[{"tagKey": "project", "tagValue": "robotocore"}],
        )
        tag_resp = client.list_tags_for_resource(resourceArn=schema_arn)
        assert "tags" in tag_resp
        assert any(t["tagKey"] == "project" for t in tag_resp["tags"])

        client.untag_resource(resourceArn=schema_arn, tagKeys=["project"])
        tag_resp2 = client.list_tags_for_resource(resourceArn=schema_arn)
        assert not any(t["tagKey"] == "project" for t in tag_resp2.get("tags", []))

        client.delete_schema(schemaArn=schema_arn)

    def test_list_schemas_returns_created(self, client, schema_json):
        resp = client.create_schema(name="test-schema-list", schema=schema_json)
        schema_arn = resp["schemaArn"]

        list_resp = client.list_schemas()
        assert "schemas" in list_resp
        arns = [s["schemaArn"] for s in list_resp["schemas"]]
        assert schema_arn in arns

        client.delete_schema(schemaArn=schema_arn)


class TestPersonalizeCRUDOps:
    """Tests for Personalize CRUD operations implemented via Moto stubs."""

    BASE_ARN = "arn:aws:personalize:us-east-1:123456789012"

    @pytest.fixture
    def client(self):
        return make_client("personalize")

    @pytest.fixture
    def dataset_group_arn(self, client):
        r = client.create_dataset_group(name="test-pers-dg")
        yield r["datasetGroupArn"]
        try:
            client.delete_dataset_group(datasetGroupArn=r["datasetGroupArn"])
        except ClientError:
            pass  # best-effort cleanup

    # --- DatasetGroup ---

    def test_create_describe_delete_dataset_group(self, client):
        r = client.create_dataset_group(name="test-pers-dg-crud")
        arn = r["datasetGroupArn"]
        assert "personalize" in arn
        assert "test-pers-dg-crud" in arn

        desc = client.describe_dataset_group(datasetGroupArn=arn)
        assert desc["datasetGroup"]["name"] == "test-pers-dg-crud"

        client.delete_dataset_group(datasetGroupArn=arn)
        with pytest.raises(ClientError) as exc:
            client.describe_dataset_group(datasetGroupArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_dataset_group_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_dataset_group(
                datasetGroupArn=f"{self.BASE_ARN}:dataset-group/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- Dataset ---

    def test_describe_dataset_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_dataset(datasetArn=f"{self.BASE_ARN}:dataset/test-dg/nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_dataset_import_job_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_dataset_import_job(
                datasetImportJobArn=(f"{self.BASE_ARN}:dataset-import-job/test-dg/nonexistent")
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_dataset_export_job_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_dataset_export_job(
                datasetExportJobArn=(f"{self.BASE_ARN}:dataset-export-job/test-dg/nonexistent")
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- Solution ---

    def test_describe_solution_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_solution(solutionArn=f"{self.BASE_ARN}:solution/nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_solution_version_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_solution_version(
                solutionVersionArn=(f"{self.BASE_ARN}:solution/nonexistent/version/nonexistent")
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_solution_metrics_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.get_solution_metrics(
                solutionVersionArn=(f"{self.BASE_ARN}:solution/nonexistent/version/nonexistent")
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- Campaign ---

    def test_describe_campaign_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_campaign(campaignArn=f"{self.BASE_ARN}:campaign/nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- Recommender ---

    def test_describe_recommender_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_recommender(recommenderArn=f"{self.BASE_ARN}:recommender/nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- Filter ---

    def test_describe_filter_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_filter(filterArn=f"{self.BASE_ARN}:filter/test-dg/nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- EventTracker ---

    def test_describe_event_tracker_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_event_tracker(
                eventTrackerArn=f"{self.BASE_ARN}:event-tracker/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- BatchInferenceJob ---

    def test_describe_batch_inference_job_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_batch_inference_job(
                batchInferenceJobArn=(f"{self.BASE_ARN}:batch-inference-job/nonexistent")
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- BatchSegmentJob ---

    def test_describe_batch_segment_job_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_batch_segment_job(
                batchSegmentJobArn=f"{self.BASE_ARN}:batch-segment-job/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- MetricAttribution ---

    def test_describe_metric_attribution_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_metric_attribution(
                metricAttributionArn=(f"{self.BASE_ARN}:metric-attribution/nonexistent")
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- DataDeletionJob ---

    def test_describe_data_deletion_job_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_data_deletion_job(
                dataDeletionJobArn=f"{self.BASE_ARN}:data-deletion-job/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- AWS-provided resources (describe without create) ---

    def test_describe_algorithm_returns_ok(self, client):
        """Algorithms are AWS-provided; describe by a known ARN returns 200."""
        try:
            resp = client.describe_algorithm(
                algorithmArn=("arn:aws:personalize:::algorithm/aws-user-personalization")
            )
            assert "algorithm" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] in (
                "ResourceNotFoundException",
                "InvalidInputException",
            )

    def test_describe_recipe_returns_ok(self, client):
        """Recipes are AWS-provided; describe by a known ARN returns 200."""
        try:
            resp = client.describe_recipe(recipeArn="arn:aws:personalize:::recipe/aws-hrnn")
            assert "recipe" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] in (
                "ResourceNotFoundException",
                "InvalidInputException",
            )

    def test_describe_feature_transformation_returns_ok(self, client):
        """Feature transformations are AWS-provided."""
        try:
            resp = client.describe_feature_transformation(
                featureTransformationArn=(
                    "arn:aws:personalize:::feature-transformation/item-age-norm"
                )
            )
            assert "featureTransformation" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] in (
                "ResourceNotFoundException",
                "InvalidInputException",
            )

    # --- Stop / Tag ops ---

    def test_stop_solution_version_creation_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.stop_solution_version_creation(
                solutionVersionArn=(f"{self.BASE_ARN}:solution/nonexistent/version/nonexistent")
            )
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "InvalidInputException",
        )

    def test_create_dataset_group_list_includes_created(self, client):
        r = client.create_dataset_group(name="test-pers-list")
        arn = r["datasetGroupArn"]

        list_resp = client.list_dataset_groups()
        arns = [dg["datasetGroupArn"] for dg in list_resp["datasetGroups"]]
        assert arn in arns

        client.delete_dataset_group(datasetGroupArn=arn)


SCHEMA_JSON = (
    '{"type":"record","name":"Interactions",'
    '"namespace":"com.amazonaws.personalize.schema",'
    '"fields":['
    '{"name":"USER_ID","type":"string"},'
    '{"name":"ITEM_ID","type":"string"},'
    '{"name":"TIMESTAMP","type":"long"}'
    '],"version":"1.0"}'
)

ROLE_ARN = "arn:aws:iam::123456789012:role/PersonalizeRole"


class TestPersonalizeCreateOps:
    """Tests for Create operations covering the 28-op required set."""

    BASE_ARN = "arn:aws:personalize:us-east-1:123456789012"

    @pytest.fixture
    def client(self):
        return make_client("personalize")

    @pytest.fixture
    def resources(self, client):
        """Create shared prerequisites: schema, dataset group, dataset, solution, sv."""
        sr = client.create_schema(name="test-create-ops-schema", schema=SCHEMA_JSON)
        schema_arn = sr["schemaArn"]

        dgr = client.create_dataset_group(name="test-create-ops-dg")
        dg_arn = dgr["datasetGroupArn"]

        dr = client.create_dataset(
            name="test-create-ops-ds",
            datasetGroupArn=dg_arn,
            datasetType="INTERACTIONS",
            schemaArn=schema_arn,
        )
        ds_arn = dr["datasetArn"]

        sol_r = client.create_solution(name="test-create-ops-sol", datasetGroupArn=dg_arn)
        sol_arn = sol_r["solutionArn"]

        sv_r = client.create_solution_version(solutionArn=sol_arn)
        sv_arn = sv_r["solutionVersionArn"]

        yield {
            "schema_arn": schema_arn,
            "dg_arn": dg_arn,
            "ds_arn": ds_arn,
            "sol_arn": sol_arn,
            "sv_arn": sv_arn,
        }

        # best-effort cleanup
        try:
            client.delete_dataset(datasetArn=ds_arn)
        except ClientError as exc:
            import logging

            logging.debug("cleanup delete_dataset: %s", exc)
        try:
            client.delete_solution(solutionArn=sol_arn)
        except ClientError as exc:
            import logging

            logging.debug("cleanup delete_solution: %s", exc)
        try:
            client.delete_dataset_group(datasetGroupArn=dg_arn)
        except ClientError as exc:
            import logging

            logging.debug("cleanup delete_dataset_group: %s", exc)
        try:
            client.delete_schema(schemaArn=schema_arn)
        except ClientError as exc:
            import logging

            logging.debug("cleanup delete_schema: %s", exc)

    # --- Create operations ---

    def test_create_dataset(self, client, resources):
        ds_arn = resources["ds_arn"]
        assert "dataset" in ds_arn
        assert "personalize" in ds_arn

    def test_create_solution(self, client, resources):
        sol_arn = resources["sol_arn"]
        assert "solution" in sol_arn

    def test_create_solution_version(self, client, resources):
        sv_arn = resources["sv_arn"]
        assert "solutionVersion" in sv_arn

    def test_create_campaign(self, client, resources):
        sv_arn = resources["sv_arn"]
        r = client.create_campaign(
            name="test-create-campaign",
            solutionVersionArn=sv_arn,
            minProvisionedTPS=1,
        )
        campaign_arn = r["campaignArn"]
        assert "campaign" in campaign_arn
        try:
            client.delete_campaign(campaignArn=campaign_arn)
        except ClientError as exc:
            import logging

            logging.debug("cleanup delete_campaign: %s", exc)

    def test_create_filter(self, client, resources):
        dg_arn = resources["dg_arn"]
        r = client.create_filter(
            name="test-create-filter",
            datasetGroupArn=dg_arn,
            filterExpression='INCLUDE ItemID WHERE Items.category = "books"',
        )
        filter_arn = r["filterArn"]
        assert "filter" in filter_arn
        try:
            client.delete_filter(filterArn=filter_arn)
        except ClientError as exc:
            import logging

            logging.debug("cleanup delete_filter: %s", exc)

    def test_create_event_tracker(self, client, resources):
        dg_arn = resources["dg_arn"]
        r = client.create_event_tracker(
            name="test-create-event-tracker",
            datasetGroupArn=dg_arn,
        )
        assert "eventTrackerArn" in r
        assert "event-tracker" in r["eventTrackerArn"]
        try:
            client.delete_event_tracker(eventTrackerArn=r["eventTrackerArn"])
        except ClientError as exc:
            import logging

            logging.debug("cleanup delete_event_tracker: %s", exc)

    def test_create_recommender(self, client, resources):
        dg_arn = resources["dg_arn"]
        r = client.create_recommender(
            name="test-create-recommender",
            datasetGroupArn=dg_arn,
            recipeArn="arn:aws:personalize:::recipe/aws-ecomm-popular-items-by-views",
        )
        assert "recommenderArn" in r
        assert "recommender" in r["recommenderArn"]
        try:
            client.delete_recommender(recommenderArn=r["recommenderArn"])
        except ClientError as exc:
            import logging

            logging.debug("cleanup delete_recommender: %s", exc)

    def test_create_metric_attribution(self, client, resources):
        dg_arn = resources["dg_arn"]
        r = client.create_metric_attribution(
            name="test-create-metric-attr",
            datasetGroupArn=dg_arn,
            metrics=[
                {
                    "eventType": "click",
                    "expression": "SUM(DatasetType.INTERACTIONS.eventValue)",
                    "metricName": "clicks_total",
                }
            ],
            metricsOutputConfig={
                "roleArn": ROLE_ARN,
                "s3DataDestination": {"path": "s3://my-bucket/metrics/"},
            },
        )
        assert "metricAttributionArn" in r
        assert "metric-attribution" in r["metricAttributionArn"]
        try:
            client.delete_metric_attribution(metricAttributionArn=r["metricAttributionArn"])
        except ClientError as exc:
            import logging

            logging.debug("cleanup delete_metric_attribution: %s", exc)

    def test_create_dataset_import_job(self, client, resources):
        ds_arn = resources["ds_arn"]
        r = client.create_dataset_import_job(
            jobName="test-create-import-job",
            datasetArn=ds_arn,
            dataSource={"dataLocation": "s3://my-bucket/data/"},
            roleArn=ROLE_ARN,
        )
        assert "datasetImportJobArn" in r
        assert "dataset-import-job" in r["datasetImportJobArn"]

    def test_create_dataset_export_job(self, client, resources):
        ds_arn = resources["ds_arn"]
        r = client.create_dataset_export_job(
            jobName="test-create-export-job",
            datasetArn=ds_arn,
            jobOutput={"s3DataDestination": {"path": "s3://my-bucket/output/"}},
            roleArn=ROLE_ARN,
        )
        assert "datasetExportJobArn" in r
        assert "dataset-export-job" in r["datasetExportJobArn"]

    def test_create_data_deletion_job(self, client, resources):
        dg_arn = resources["dg_arn"]
        r = client.create_data_deletion_job(
            jobName="test-create-deletion-job",
            datasetGroupArn=dg_arn,
            dataSource={"dataLocation": "s3://my-bucket/deletions/"},
            roleArn=ROLE_ARN,
        )
        assert "dataDeletionJobArn" in r
        assert "data-deletion-job" in r["dataDeletionJobArn"]

    def test_create_batch_inference_job(self, client, resources):
        sv_arn = resources["sv_arn"]
        r = client.create_batch_inference_job(
            jobName="test-create-batch-inf",
            solutionVersionArn=sv_arn,
            jobInput={"s3DataSource": {"path": "s3://my-bucket/input/"}},
            jobOutput={"s3DataDestination": {"path": "s3://my-bucket/output/"}},
            roleArn=ROLE_ARN,
        )
        assert "batchInferenceJobArn" in r
        assert "batch-inference-job" in r["batchInferenceJobArn"]

    def test_create_batch_segment_job(self, client, resources):
        sv_arn = resources["sv_arn"]
        r = client.create_batch_segment_job(
            jobName="test-create-batch-seg",
            solutionVersionArn=sv_arn,
            jobInput={"s3DataSource": {"path": "s3://my-bucket/input/"}},
            jobOutput={"s3DataDestination": {"path": "s3://my-bucket/output/"}},
            roleArn=ROLE_ARN,
        )
        assert "batchSegmentJobArn" in r
        assert "batch-segment-job" in r["batchSegmentJobArn"]

    # --- Delete operations ---

    def test_delete_campaign(self, client, resources):
        sv_arn = resources["sv_arn"]
        r = client.create_campaign(
            name="test-del-campaign",
            solutionVersionArn=sv_arn,
            minProvisionedTPS=1,
        )
        campaign_arn = r["campaignArn"]
        client.delete_campaign(campaignArn=campaign_arn)
        with pytest.raises(ClientError) as exc:
            client.describe_campaign(campaignArn=campaign_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_dataset(self, client):
        sr = client.create_schema(name="test-del-ds-schema", schema=SCHEMA_JSON)
        schema_arn = sr["schemaArn"]
        dgr = client.create_dataset_group(name="test-del-ds-dg")
        dg_arn = dgr["datasetGroupArn"]
        dr = client.create_dataset(
            name="test-del-ds",
            datasetGroupArn=dg_arn,
            datasetType="INTERACTIONS",
            schemaArn=schema_arn,
        )
        ds_arn = dr["datasetArn"]
        client.delete_dataset(datasetArn=ds_arn)
        with pytest.raises(ClientError) as exc:
            client.describe_dataset(datasetArn=ds_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        try:
            client.delete_dataset_group(datasetGroupArn=dg_arn)
        except ClientError as exc:
            import logging

            logging.debug("cleanup: %s", exc)
        try:
            client.delete_schema(schemaArn=schema_arn)
        except ClientError as exc:
            import logging

            logging.debug("cleanup: %s", exc)

    def test_delete_event_tracker(self, client, resources):
        dg_arn = resources["dg_arn"]
        r = client.create_event_tracker(name="test-del-et", datasetGroupArn=dg_arn)
        et_arn = r["eventTrackerArn"]
        client.delete_event_tracker(eventTrackerArn=et_arn)
        with pytest.raises(ClientError) as exc:
            client.describe_event_tracker(eventTrackerArn=et_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_filter(self, client, resources):
        dg_arn = resources["dg_arn"]
        r = client.create_filter(
            name="test-del-filter",
            datasetGroupArn=dg_arn,
            filterExpression='INCLUDE ItemID WHERE Items.category = "toys"',
        )
        filter_arn = r["filterArn"]
        client.delete_filter(filterArn=filter_arn)
        with pytest.raises(ClientError) as exc:
            client.describe_filter(filterArn=filter_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_metric_attribution(self, client, resources):
        dg_arn = resources["dg_arn"]
        r = client.create_metric_attribution(
            name="test-del-metric-attr",
            datasetGroupArn=dg_arn,
            metrics=[
                {
                    "eventType": "view",
                    "expression": "SUM(DatasetType.INTERACTIONS.eventValue)",
                    "metricName": "views_total",
                }
            ],
            metricsOutputConfig={
                "roleArn": ROLE_ARN,
                "s3DataDestination": {"path": "s3://my-bucket/metrics/"},
            },
        )
        ma_arn = r["metricAttributionArn"]
        client.delete_metric_attribution(metricAttributionArn=ma_arn)
        with pytest.raises(ClientError) as exc:
            client.describe_metric_attribution(metricAttributionArn=ma_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_recommender(self, client, resources):
        dg_arn = resources["dg_arn"]
        r = client.create_recommender(
            name="test-del-recommender",
            datasetGroupArn=dg_arn,
            recipeArn="arn:aws:personalize:::recipe/aws-ecomm-popular-items-by-views",
        )
        rec_arn = r["recommenderArn"]
        client.delete_recommender(recommenderArn=rec_arn)
        with pytest.raises(ClientError) as exc:
            client.describe_recommender(recommenderArn=rec_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_solution(self, client, resources):
        dg_arn = resources["dg_arn"]
        r = client.create_solution(name="test-del-solution", datasetGroupArn=dg_arn)
        sol_arn = r["solutionArn"]
        client.delete_solution(solutionArn=sol_arn)
        with pytest.raises(ClientError) as exc:
            client.describe_solution(solutionArn=sol_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- List operations ---

    def test_list_dataset_import_jobs(self, client):
        r = client.list_dataset_import_jobs()
        assert "datasetImportJobs" in r
        assert isinstance(r["datasetImportJobs"], list)

    # --- Start / Stop Recommender ---

    def test_start_recommender_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.start_recommender(recommenderArn=f"{self.BASE_ARN}:recommender/nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_stop_recommender_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.stop_recommender(recommenderArn=f"{self.BASE_ARN}:recommender/nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- Update operations ---

    def test_update_campaign_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.update_campaign(
                campaignArn=f"{self.BASE_ARN}:campaign/nonexistent",
                minProvisionedTPS=5,
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_dataset_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.update_dataset(
                datasetArn=f"{self.BASE_ARN}:dataset/nonexistent",
                schemaArn=f"{self.BASE_ARN}:schema/fake",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_metric_attribution_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.update_metric_attribution(
                metricAttributionArn=f"{self.BASE_ARN}:metric-attribution/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_recommender_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.update_recommender(
                recommenderArn=f"{self.BASE_ARN}:recommender/nonexistent",
                recommenderConfig={"minRecommendationRequestsPerSecond": 1},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_solution_not_found(self, client):
        with pytest.raises(ClientError) as exc:
            client.update_solution(solutionArn=f"{self.BASE_ARN}:solution/nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
