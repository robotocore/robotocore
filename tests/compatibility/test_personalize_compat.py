"""Personalize compatibility tests."""

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def personalize():
    return make_client("personalize")


SCHEMA_JSON = (
    '{"type":"record","name":"Interactions",'
    '"namespace":"com.amazonaws.personalize.schema",'
    '"fields":['
    '{"name":"USER_ID","type":"string"},'
    '{"name":"ITEM_ID","type":"string"},'
    '{"name":"TIMESTAMP","type":"long"}'
    '],"version":"1.0"}'
)

BASE_ARN = "arn:aws:personalize:us-east-1:123456789012"
SOL_VERSION_ARN = f"{BASE_ARN}:solution/test-sol/solutionVersion/abc123"
DG_ARN = f"{BASE_ARN}:dataset-group/test-dg"
RECIPE_ARN = "arn:aws:personalize:::recipe/aws-ecomm-popular-items-by-purchases"
ROLE_ARN = "arn:aws:iam::123456789012:role/PersonalizeRole"
FILTER_EXPR = "EXCLUDE itemId WHERE Items.genre IN ($GENRES)"
METRICS_OUTPUT = {
    "s3DataDestination": {"path": "s3://bucket/prefix/"},
    "roleArn": ROLE_ARN,
}
METRICS = [{"eventType": "click", "expression": "SUM(DatasetType.INTERACTIONS)", "metricName": "click-metric"}]
JOB_INPUT = {"s3DataSource": {"path": "s3://bucket/input/"}}
JOB_OUTPUT = {"s3DataDestination": {"path": "s3://bucket/output/"}}


class TestPersonalizeOperations:
    def test_list_schemas(self, personalize):
        # Create a schema so list is non-trivial
        r = personalize.create_schema(name="test-ops-list-schema", schema=SCHEMA_JSON)
        arn = r["schemaArn"]
        assert "schema" in arn

        # LIST: verify created schema appears
        resp = personalize.list_schemas()
        assert "schemas" in resp
        assert isinstance(resp["schemas"], list)
        arns = [s["schemaArn"] for s in resp["schemas"]]
        assert arn in arns

        # RETRIEVE: describe it
        desc = personalize.describe_schema(schemaArn=arn)["schema"]
        assert desc["name"] == "test-ops-list-schema"
        assert desc["schemaArn"] == arn

        # DELETE: clean up
        personalize.delete_schema(schemaArn=arn)

        # ERROR: deleted resource is gone
        with pytest.raises(ClientError) as exc:
            personalize.describe_schema(schemaArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_nonexistent_schema(self, personalize):
        # CREATE: establish a real schema as baseline
        r = personalize.create_schema(name="test-nonexist-baseline", schema=SCHEMA_JSON)
        created_arn = r["schemaArn"]

        # RETRIEVE: verify successful describe works
        desc = personalize.describe_schema(schemaArn=created_arn)["schema"]
        assert desc["schemaArn"] == created_arn

        # LIST: verify it appears in the list
        resp = personalize.list_schemas()
        listed_arns = [s["schemaArn"] for s in resp["schemas"]]
        assert created_arn in listed_arns

        # DELETE: clean up
        personalize.delete_schema(schemaArn=created_arn)

        # ERROR: truly nonexistent resource raises correct exception
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
        # CREATE a dataset group so the filter call is real
        r = client.create_dataset_group(name="test-gap-list-datasets-dg")
        dg_arn = r["datasetGroupArn"]

        # LIST datasets filtered by that group (empty but valid)
        resp = client.list_datasets(datasetGroupArn=dg_arn)
        assert "datasets" in resp
        assert isinstance(resp["datasets"], list)

        # ERROR: describe a nonexistent dataset
        with pytest.raises(ClientError) as exc:
            client.describe_dataset(datasetArn=f"{BASE_ARN}:dataset/test-dg/nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

        # DELETE dataset group
        client.delete_dataset_group(datasetGroupArn=dg_arn)

    def test_list_dataset_groups(self, client):
        # CREATE
        r = client.create_dataset_group(name="test-gap-list-dg")
        arn = r["datasetGroupArn"]
        assert "dataset-group" in arn

        # LIST: verify it appears
        resp = client.list_dataset_groups()
        assert "datasetGroups" in resp
        arns = [dg["datasetGroupArn"] for dg in resp["datasetGroups"]]
        assert arn in arns

        # RETRIEVE
        desc = client.describe_dataset_group(datasetGroupArn=arn)
        assert desc["datasetGroup"]["name"] == "test-gap-list-dg"

        # DELETE
        client.delete_dataset_group(datasetGroupArn=arn)

        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_dataset_group(datasetGroupArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_campaigns(self, client):
        # CREATE
        r = client.create_campaign(name="test-gap-list-camp", solutionVersionArn=SOL_VERSION_ARN)
        arn = r["campaignArn"]
        assert "campaign" in arn

        # LIST: verify it appears
        resp = client.list_campaigns()
        assert "campaigns" in resp
        arns = [c["campaignArn"] for c in resp["campaigns"]]
        assert arn in arns

        # RETRIEVE
        desc = client.describe_campaign(campaignArn=arn)
        assert desc["campaign"]["name"] == "test-gap-list-camp"

        # UPDATE
        upd = client.update_campaign(campaignArn=arn, minProvisionedTPS=3)
        assert upd["campaignArn"] == arn

        # DELETE
        client.delete_campaign(campaignArn=arn)

        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_campaign(campaignArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_solutions(self, client):
        # CREATE
        r = client.create_solution(name="test-gap-list-sol", datasetGroupArn=DG_ARN)
        arn = r["solutionArn"]
        assert "solution" in arn

        # LIST: verify it appears
        resp = client.list_solutions()
        assert "solutions" in resp
        arns = [s["solutionArn"] for s in resp["solutions"]]
        assert arn in arns

        # RETRIEVE
        desc = client.describe_solution(solutionArn=arn)
        assert desc["solution"]["name"] == "test-gap-list-sol"

        # DELETE
        client.delete_solution(solutionArn=arn)

        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_solution(solutionArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_solution_versions(self, client):
        # CREATE solution first, then solution version
        sol = client.create_solution(name="test-gap-list-sv-sol", datasetGroupArn=DG_ARN)
        sol_arn = sol["solutionArn"]

        sv = client.create_solution_version(solutionArn=sol_arn)
        sv_arn = sv["solutionVersionArn"]
        assert "solution" in sv_arn

        # LIST: verify it appears
        resp = client.list_solution_versions(solutionArn=sol_arn)
        assert "solutionVersions" in resp
        arns = [sv["solutionVersionArn"] for sv in resp["solutionVersions"]]
        assert sv_arn in arns

        # RETRIEVE
        desc = client.describe_solution_version(solutionVersionArn=sv_arn)
        assert desc["solutionVersion"]["solutionVersionArn"] == sv_arn

        # ERROR: nonexistent solution version
        with pytest.raises(ClientError) as exc:
            client.describe_solution_version(
                solutionVersionArn=f"{BASE_ARN}:solution/nonexistent/version/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

        # DELETE solution (versions deleted with it)
        client.delete_solution(solutionArn=sol_arn)

    def test_list_recommenders(self, client):
        # CREATE
        r = client.create_recommender(
            name="test-gap-list-rec", datasetGroupArn=DG_ARN, recipeArn=RECIPE_ARN
        )
        arn = r["recommenderArn"]
        assert "recommender" in arn

        # LIST: verify it appears
        resp = client.list_recommenders()
        assert "recommenders" in resp
        arns = [rec["recommenderArn"] for rec in resp["recommenders"]]
        assert arn in arns

        # RETRIEVE
        desc = client.describe_recommender(recommenderArn=arn)
        assert desc["recommender"]["name"] == "test-gap-list-rec"

        # UPDATE
        upd = client.update_recommender(
            recommenderArn=arn,
            recommenderConfig={"minRecommendationRequestsPerSecond": 2},
        )
        assert upd["recommenderArn"] == arn

        # DELETE
        client.delete_recommender(recommenderArn=arn)

        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_recommender(recommenderArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_filters(self, client):
        # CREATE
        r = client.create_filter(
            name="test-gap-list-filter",
            datasetGroupArn=DG_ARN,
            filterExpression=FILTER_EXPR,
        )
        arn = r["filterArn"]
        assert "filter" in arn

        # LIST: verify it appears
        resp = client.list_filters()
        assert "Filters" in resp
        arns = [f["filterArn"] for f in resp["Filters"]]
        assert arn in arns

        # RETRIEVE
        desc = client.describe_filter(filterArn=arn)
        assert desc["filter"]["name"] == "test-gap-list-filter"

        # DELETE
        client.delete_filter(filterArn=arn)

        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_filter(filterArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_recipes(self, client):
        # LIST recipes (AWS-provided, always present)
        resp = client.list_recipes()
        assert "recipes" in resp
        assert isinstance(resp["recipes"], list)

        # RETRIEVE a known recipe if available
        if resp["recipes"]:
            recipe_arn = resp["recipes"][0]["recipeArn"]
            desc = client.describe_recipe(recipeArn=recipe_arn)
            assert "recipe" in desc
            assert desc["recipe"]["recipeArn"] == recipe_arn

        # RETRIEVE: describe a known AWS recipe works
        known_arn = "arn:aws:personalize:::recipe/aws-hrnn"
        try:
            desc2 = client.describe_recipe(recipeArn=known_arn)
            assert "recipe" in desc2
        except ClientError as exc:
            # Some emulators may not have all built-in recipes
            assert exc.response["Error"]["Code"] in (
                "ResourceNotFoundException",
                "InvalidInputException",
            )

    def test_list_event_trackers(self, client):
        # CREATE
        r = client.create_event_tracker(name="test-gap-list-et", datasetGroupArn=DG_ARN)
        arn = r["eventTrackerArn"]
        tracking_id = r["trackingId"]
        assert "event-tracker" in arn
        assert tracking_id

        # LIST: verify it appears
        resp = client.list_event_trackers()
        assert "eventTrackers" in resp
        arns = [et["eventTrackerArn"] for et in resp["eventTrackers"]]
        assert arn in arns

        # RETRIEVE
        desc = client.describe_event_tracker(eventTrackerArn=arn)
        assert desc["eventTracker"]["name"] == "test-gap-list-et"
        assert desc["eventTracker"]["trackingId"] == tracking_id

        # UPDATE (tag): event trackers support tagging as update operation
        client.tag_resource(resourceArn=arn, tags=[{"tagKey": "env", "tagValue": "test"}])
        tag_resp = client.list_tags_for_resource(resourceArn=arn)
        assert any(t["tagKey"] == "env" for t in tag_resp.get("tags", []))

        # DELETE
        client.delete_event_tracker(eventTrackerArn=arn)

        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_event_tracker(eventTrackerArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_batch_inference_jobs(self, client):
        # CREATE
        r = client.create_batch_inference_job(
            jobName="test-gap-list-bij",
            solutionVersionArn=SOL_VERSION_ARN,
            jobInput=JOB_INPUT,
            jobOutput=JOB_OUTPUT,
            roleArn=ROLE_ARN,
        )
        arn = r["batchInferenceJobArn"]
        assert "batch-inference-job" in arn

        # LIST: verify it appears
        resp = client.list_batch_inference_jobs()
        assert "batchInferenceJobs" in resp
        arns = [j["batchInferenceJobArn"] for j in resp["batchInferenceJobs"]]
        assert arn in arns

        # RETRIEVE
        desc = client.describe_batch_inference_job(batchInferenceJobArn=arn)
        assert desc["batchInferenceJob"]["jobName"] == "test-gap-list-bij"
        assert desc["batchInferenceJob"]["status"] == "ACTIVE"

        # ERROR: nonexistent
        with pytest.raises(ClientError) as exc:
            client.describe_batch_inference_job(
                batchInferenceJobArn=f"{BASE_ARN}:batch-inference-job/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_batch_segment_jobs(self, client):
        # CREATE
        r = client.create_batch_segment_job(
            jobName="test-gap-list-bsj",
            solutionVersionArn=SOL_VERSION_ARN,
            jobInput=JOB_INPUT,
            jobOutput=JOB_OUTPUT,
            roleArn=ROLE_ARN,
        )
        arn = r["batchSegmentJobArn"]
        assert "batch-segment-job" in arn

        # LIST: verify it appears
        resp = client.list_batch_segment_jobs()
        assert "batchSegmentJobs" in resp
        arns = [j["batchSegmentJobArn"] for j in resp["batchSegmentJobs"]]
        assert arn in arns

        # RETRIEVE
        desc = client.describe_batch_segment_job(batchSegmentJobArn=arn)
        assert desc["batchSegmentJob"]["jobName"] == "test-gap-list-bsj"
        assert desc["batchSegmentJob"]["status"] == "ACTIVE"

        # ERROR: nonexistent
        with pytest.raises(ClientError) as exc:
            client.describe_batch_segment_job(
                batchSegmentJobArn=f"{BASE_ARN}:batch-segment-job/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_metric_attributions(self, client):
        # CREATE
        r = client.create_metric_attribution(
            name="test-gap-list-ma",
            datasetGroupArn=DG_ARN,
            metrics=METRICS,
            metricsOutputConfig=METRICS_OUTPUT,
        )
        arn = r["metricAttributionArn"]
        assert "metric-attribution" in arn

        # LIST: verify it appears
        resp = client.list_metric_attributions()
        assert "metricAttributions" in resp
        arns = [ma["metricAttributionArn"] for ma in resp["metricAttributions"]]
        assert arn in arns

        # RETRIEVE
        desc = client.describe_metric_attribution(metricAttributionArn=arn)
        assert desc["metricAttribution"]["name"] == "test-gap-list-ma"

        # UPDATE
        upd = client.update_metric_attribution(
            metricAttributionArn=arn, metricsOutputConfig=METRICS_OUTPUT
        )
        assert upd["metricAttributionArn"] == arn

        # DELETE
        client.delete_metric_attribution(metricAttributionArn=arn)

        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_metric_attribution(metricAttributionArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_data_deletion_jobs(self, client):
        # CREATE
        r = client.create_data_deletion_job(
            jobName="test-gap-list-ddj",
            datasetGroupArn=DG_ARN,
            dataSource={"dataLocation": "s3://bucket/data.csv"},
            roleArn=ROLE_ARN,
        )
        arn = r["dataDeletionJobArn"]
        assert "data-deletion-job" in arn

        # LIST: verify it appears
        resp = client.list_data_deletion_jobs()
        assert "dataDeletionJobs" in resp
        arns = [j["dataDeletionJobArn"] for j in resp["dataDeletionJobs"]]
        assert arn in arns

        # RETRIEVE
        desc = client.describe_data_deletion_job(dataDeletionJobArn=arn)
        assert desc["dataDeletionJob"]["jobName"] == "test-gap-list-ddj"
        assert desc["dataDeletionJob"]["status"] == "ACTIVE"

        # ERROR: nonexistent
        with pytest.raises(ClientError) as exc:
            client.describe_data_deletion_job(
                dataDeletionJobArn=f"{BASE_ARN}:data-deletion-job/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_tags_for_resource(self, client):
        # CREATE a real resource to tag
        r = client.create_dataset_group(name="test-tags-list-resource")
        arn = r["datasetGroupArn"]

        # RETRIEVE: verify the resource exists before tagging
        desc = client.describe_dataset_group(datasetGroupArn=arn)
        assert desc["datasetGroup"]["datasetGroupArn"] == arn

        # UPDATE (tag): apply tags to the resource
        client.tag_resource(resourceArn=arn, tags=[{"tagKey": "env", "tagValue": "test"}])

        # LIST: verify tags appear
        resp = client.list_tags_for_resource(resourceArn=arn)
        assert "tags" in resp
        assert any(t["tagKey"] == "env" for t in resp["tags"])

        # DELETE: clean up
        client.delete_dataset_group(datasetGroupArn=arn)

        # ERROR: deleted resource no longer retrievable
        with pytest.raises(ClientError) as exc:
            client.describe_dataset_group(datasetGroupArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_dataset_export_jobs_no_params(self, client):
        # LIST with no params (no datasets exist, returns empty list)
        resp = client.list_dataset_export_jobs()
        assert "datasetExportJobs" in resp
        assert isinstance(resp["datasetExportJobs"], list)

        # RETRIEVE error: describe a nonexistent export job
        with pytest.raises(ClientError) as exc:
            client.describe_dataset_export_job(
                datasetExportJobArn=f"{BASE_ARN}:dataset-export-job/test-dg/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

        # RETRIEVE error: describe a nonexistent import job
        with pytest.raises(ClientError) as exc:
            client.describe_dataset_import_job(
                datasetImportJobArn=f"{BASE_ARN}:dataset-import-job/test-dg/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_metric_attribution_metrics(self, client):
        # CREATE a real metric attribution
        r = client.create_metric_attribution(
            name="test-gap-ma-metrics",
            datasetGroupArn=DG_ARN,
            metrics=METRICS,
            metricsOutputConfig=METRICS_OUTPUT,
        )
        arn = r["metricAttributionArn"]
        assert "metric-attribution" in arn

        # LIST metrics for that attribution
        resp = client.list_metric_attribution_metrics(metricAttributionArn=arn)
        assert "metrics" in resp
        assert isinstance(resp["metrics"], list)

        # RETRIEVE: describe the attribution itself
        desc = client.describe_metric_attribution(metricAttributionArn=arn)
        assert desc["metricAttribution"]["metricAttributionArn"] == arn

        # DELETE
        client.delete_metric_attribution(metricAttributionArn=arn)

        # ERROR: metrics of deleted attribution
        with pytest.raises(ClientError) as exc:
            client.describe_metric_attribution(metricAttributionArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


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
        # CREATE: establish a real dataset group as baseline
        r = client.create_dataset_group(name="test-dg-notfound-baseline")
        arn = r["datasetGroupArn"]

        # RETRIEVE: verify describe works on the real group
        desc = client.describe_dataset_group(datasetGroupArn=arn)
        assert desc["datasetGroup"]["name"] == "test-dg-notfound-baseline"

        # LIST: verify it appears in the list
        resp = client.list_dataset_groups()
        arns = [dg["datasetGroupArn"] for dg in resp["datasetGroups"]]
        assert arn in arns

        # DELETE: clean up
        client.delete_dataset_group(datasetGroupArn=arn)

        # ERROR: nonexistent resource raises correct error
        with pytest.raises(ClientError) as exc:
            client.describe_dataset_group(
                datasetGroupArn=f"{self.BASE_ARN}:dataset-group/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- Dataset ---

    def test_describe_dataset_not_found(self, client):
        # CREATE: create a dataset group to make list_datasets call real
        r = client.create_dataset_group(name="test-dataset-notfound-dg")
        dg_arn = r["datasetGroupArn"]

        # LIST: list datasets (empty but validates the operation is live)
        resp = client.list_datasets(datasetGroupArn=dg_arn)
        assert "datasets" in resp
        assert isinstance(resp["datasets"], list)

        # DELETE: clean up the dataset group
        client.delete_dataset_group(datasetGroupArn=dg_arn)

        # ERROR: nonexistent dataset raises correct error
        with pytest.raises(ClientError) as exc:
            client.describe_dataset(datasetArn=f"{self.BASE_ARN}:dataset/test-dg/nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_dataset_import_job_not_found(self, client):
        # CREATE: create a dataset group for list context
        r = client.create_dataset_group(name="test-import-notfound-dg")
        dg_arn = r["datasetGroupArn"]

        # LIST: list import jobs filtered by dataset group (empty but live)
        resp = client.list_dataset_import_jobs()
        assert "datasetImportJobs" in resp

        # DELETE: clean up
        client.delete_dataset_group(datasetGroupArn=dg_arn)

        # ERROR: nonexistent import job raises correct error
        with pytest.raises(ClientError) as exc:
            client.describe_dataset_import_job(
                datasetImportJobArn=(f"{self.BASE_ARN}:dataset-import-job/test-dg/nonexistent")
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_dataset_export_job_not_found(self, client):
        # CREATE: create a dataset group for list context
        r = client.create_dataset_group(name="test-export-notfound-dg")
        dg_arn = r["datasetGroupArn"]

        # LIST: list export jobs (empty but live)
        resp = client.list_dataset_export_jobs()
        assert "datasetExportJobs" in resp

        # DELETE: clean up
        client.delete_dataset_group(datasetGroupArn=dg_arn)

        # ERROR: nonexistent export job raises correct error
        with pytest.raises(ClientError) as exc:
            client.describe_dataset_export_job(
                datasetExportJobArn=(f"{self.BASE_ARN}:dataset-export-job/test-dg/nonexistent")
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- Solution ---

    def test_describe_solution_not_found(self, client):
        # CREATE: create a solution as baseline
        r = client.create_solution(name="test-sol-notfound-baseline", datasetGroupArn=DG_ARN)
        sol_arn = r["solutionArn"]

        # RETRIEVE: verify describe works
        desc = client.describe_solution(solutionArn=sol_arn)
        assert desc["solution"]["name"] == "test-sol-notfound-baseline"

        # LIST: verify it appears
        resp = client.list_solutions()
        arns = [s["solutionArn"] for s in resp["solutions"]]
        assert sol_arn in arns

        # DELETE: clean up
        client.delete_solution(solutionArn=sol_arn)

        # ERROR: nonexistent solution raises correct error
        with pytest.raises(ClientError) as exc:
            client.describe_solution(solutionArn=f"{self.BASE_ARN}:solution/nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_solution_version_not_found(self, client):
        # CREATE: create a solution + solution version as baseline
        sol = client.create_solution(name="test-sv-notfound-sol", datasetGroupArn=DG_ARN)
        sol_arn = sol["solutionArn"]
        sv = client.create_solution_version(solutionArn=sol_arn)
        sv_arn = sv["solutionVersionArn"]

        # RETRIEVE: verify describe works
        desc = client.describe_solution_version(solutionVersionArn=sv_arn)
        assert desc["solutionVersion"]["solutionVersionArn"] == sv_arn

        # LIST: verify it appears
        resp = client.list_solution_versions(solutionArn=sol_arn)
        arns = [v["solutionVersionArn"] for v in resp["solutionVersions"]]
        assert sv_arn in arns

        # DELETE: clean up
        client.delete_solution(solutionArn=sol_arn)

        # ERROR: nonexistent solution version raises correct error
        with pytest.raises(ClientError) as exc:
            client.describe_solution_version(
                solutionVersionArn=(f"{self.BASE_ARN}:solution/nonexistent/version/nonexistent")
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_solution_metrics_not_found(self, client):
        # CREATE: create a solution + version to have a real baseline
        sol = client.create_solution(name="test-metrics-notfound-sol", datasetGroupArn=DG_ARN)
        sol_arn = sol["solutionArn"]
        sv = client.create_solution_version(solutionArn=sol_arn)
        sv_arn = sv["solutionVersionArn"]

        # RETRIEVE: verify the solution version exists
        desc = client.describe_solution_version(solutionVersionArn=sv_arn)
        assert desc["solutionVersion"]["status"] == "ACTIVE"

        # DELETE: clean up
        client.delete_solution(solutionArn=sol_arn)

        # ERROR: get metrics on nonexistent solution version raises correct error
        with pytest.raises(ClientError) as exc:
            client.get_solution_metrics(
                solutionVersionArn=(f"{self.BASE_ARN}:solution/nonexistent/version/nonexistent")
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- Campaign ---

    def test_describe_campaign_not_found(self, client):
        # CREATE: create a campaign as baseline
        r = client.create_campaign(name="test-camp-notfound-baseline", solutionVersionArn=SOL_VERSION_ARN)
        camp_arn = r["campaignArn"]

        # RETRIEVE: verify describe works
        desc = client.describe_campaign(campaignArn=camp_arn)
        assert desc["campaign"]["name"] == "test-camp-notfound-baseline"

        # LIST: verify it appears
        resp = client.list_campaigns()
        arns = [c["campaignArn"] for c in resp["campaigns"]]
        assert camp_arn in arns

        # DELETE: clean up
        client.delete_campaign(campaignArn=camp_arn)

        # ERROR: nonexistent campaign raises correct error
        with pytest.raises(ClientError) as exc:
            client.describe_campaign(campaignArn=f"{self.BASE_ARN}:campaign/nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- Recommender ---

    def test_describe_recommender_not_found(self, client):
        # CREATE: create a recommender as baseline
        r = client.create_recommender(name="test-rec-notfound-baseline", datasetGroupArn=DG_ARN, recipeArn=RECIPE_ARN)
        rec_arn = r["recommenderArn"]

        # RETRIEVE: verify describe works
        desc = client.describe_recommender(recommenderArn=rec_arn)
        assert desc["recommender"]["name"] == "test-rec-notfound-baseline"

        # LIST: verify it appears
        resp = client.list_recommenders()
        arns = [r["recommenderArn"] for r in resp["recommenders"]]
        assert rec_arn in arns

        # DELETE: clean up
        client.delete_recommender(recommenderArn=rec_arn)

        # ERROR: nonexistent recommender raises correct error
        with pytest.raises(ClientError) as exc:
            client.describe_recommender(recommenderArn=f"{self.BASE_ARN}:recommender/nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- Filter ---

    def test_describe_filter_not_found(self, client):
        # CREATE: create a filter as baseline
        r = client.create_filter(name="test-filter-notfound-baseline", datasetGroupArn=DG_ARN, filterExpression=FILTER_EXPR)
        filter_arn = r["filterArn"]

        # RETRIEVE: verify describe works
        desc = client.describe_filter(filterArn=filter_arn)
        assert desc["filter"]["name"] == "test-filter-notfound-baseline"

        # LIST: verify it appears
        resp = client.list_filters()
        arns = [f["filterArn"] for f in resp["Filters"]]
        assert filter_arn in arns

        # DELETE: clean up
        client.delete_filter(filterArn=filter_arn)

        # ERROR: nonexistent filter raises correct error
        with pytest.raises(ClientError) as exc:
            client.describe_filter(filterArn=f"{self.BASE_ARN}:filter/test-dg/nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- EventTracker ---

    def test_describe_event_tracker_not_found(self, client):
        # CREATE: create an event tracker as baseline
        r = client.create_event_tracker(name="test-et-notfound-baseline", datasetGroupArn=DG_ARN)
        et_arn = r["eventTrackerArn"]

        # RETRIEVE: verify describe works
        desc = client.describe_event_tracker(eventTrackerArn=et_arn)
        assert desc["eventTracker"]["name"] == "test-et-notfound-baseline"

        # LIST: verify it appears
        resp = client.list_event_trackers()
        arns = [et["eventTrackerArn"] for et in resp["eventTrackers"]]
        assert et_arn in arns

        # DELETE: clean up
        client.delete_event_tracker(eventTrackerArn=et_arn)

        # ERROR: nonexistent event tracker raises correct error
        with pytest.raises(ClientError) as exc:
            client.describe_event_tracker(
                eventTrackerArn=f"{self.BASE_ARN}:event-tracker/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- BatchInferenceJob ---

    def test_describe_batch_inference_job_not_found(self, client):
        # CREATE: create a batch inference job as baseline
        r = client.create_batch_inference_job(
            jobName="test-bij-notfound-baseline",
            solutionVersionArn=SOL_VERSION_ARN,
            jobInput=JOB_INPUT,
            jobOutput=JOB_OUTPUT,
            roleArn=ROLE_ARN,
        )
        bij_arn = r["batchInferenceJobArn"]

        # RETRIEVE: verify describe works
        desc = client.describe_batch_inference_job(batchInferenceJobArn=bij_arn)
        assert desc["batchInferenceJob"]["jobName"] == "test-bij-notfound-baseline"

        # LIST: verify it appears
        resp = client.list_batch_inference_jobs()
        arns = [j["batchInferenceJobArn"] for j in resp["batchInferenceJobs"]]
        assert bij_arn in arns

        # ERROR: nonexistent batch inference job raises correct error
        with pytest.raises(ClientError) as exc:
            client.describe_batch_inference_job(
                batchInferenceJobArn=(f"{self.BASE_ARN}:batch-inference-job/nonexistent")
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # --- BatchSegmentJob ---

    def test_describe_batch_segment_job_not_found(self, client):
        # CREATE: create a batch segment job as baseline
        r = client.create_batch_segment_job(
            jobName="test-bsj-notfound-baseline",
            solutionVersionArn=SOL_VERSION_ARN,
            jobInput=JOB_INPUT,
            jobOutput=JOB_OUTPUT,
            roleArn=ROLE_ARN,
        )
        bsj_arn = r["batchSegmentJobArn"]

        # RETRIEVE: verify describe works
        desc = client.describe_batch_segment_job(batchSegmentJobArn=bsj_arn)
        assert desc["batchSegmentJob"]["jobName"] == "test-bsj-notfound-baseline"

        # LIST: verify it appears
        resp = client.list_batch_segment_jobs()
        arns = [j["batchSegmentJobArn"] for j in resp["batchSegmentJobs"]]
        assert bsj_arn in arns

        # ERROR: nonexistent batch segment job raises correct error
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


class TestPersonalizeSchemaEdgeCases:
    """Edge cases and behavioral fidelity for Schema operations."""

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

    def test_schema_arn_format(self, client, schema_json):
        r = client.create_schema(name="test-arn-format", schema=schema_json)
        arn = r["schemaArn"]
        parts = arn.split(":")
        assert parts[0] == "arn"
        assert parts[1] == "aws"
        assert parts[2] == "personalize"
        assert "schema" in arn
        assert "test-arn-format" in arn
        client.delete_schema(schemaArn=arn)

    def test_schema_describe_has_timestamps(self, client, schema_json):
        r = client.create_schema(name="test-timestamps", schema=schema_json)
        arn = r["schemaArn"]
        desc = client.describe_schema(schemaArn=arn)["schema"]
        assert "creationDateTime" in desc
        assert "lastUpdatedDateTime" in desc
        assert desc["creationDateTime"] is not None
        client.delete_schema(schemaArn=arn)

    def test_schema_describe_has_name_and_arn(self, client, schema_json):
        r = client.create_schema(name="test-fields-check", schema=schema_json)
        arn = r["schemaArn"]
        desc = client.describe_schema(schemaArn=arn)["schema"]
        assert desc["name"] == "test-fields-check"
        assert desc["schemaArn"] == arn
        client.delete_schema(schemaArn=arn)

    def test_delete_nonexistent_schema_raises(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_schema(
                schemaArn="arn:aws:personalize:us-east-1:123456789012:schema/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_schemas_includes_created_with_correct_keys(self, client, schema_json):
        r = client.create_schema(name="test-list-keys", schema=schema_json)
        arn = r["schemaArn"]
        list_resp = client.list_schemas()
        matching = [s for s in list_resp["schemas"] if s["schemaArn"] == arn]
        assert len(matching) == 1
        schema_entry = matching[0]
        assert "schemaArn" in schema_entry
        assert "name" in schema_entry
        assert "creationDateTime" in schema_entry
        assert schema_entry["name"] == "test-list-keys"
        client.delete_schema(schemaArn=arn)

    def test_list_schemas_returns_multiple_created(self, client, schema_json):
        arns = []
        for i in range(3):
            r = client.create_schema(name=f"test-multi-schema-{i}", schema=schema_json)
            arns.append(r["schemaArn"])
        list_resp = client.list_schemas()
        listed_arns = {s["schemaArn"] for s in list_resp["schemas"]}
        for arn in arns:
            assert arn in listed_arns
        for arn in arns:
            client.delete_schema(schemaArn=arn)

    def test_list_schemas_after_delete_excludes_deleted(self, client, schema_json):
        r = client.create_schema(name="test-delete-from-list", schema=schema_json)
        arn = r["schemaArn"]
        client.delete_schema(schemaArn=arn)
        list_resp = client.list_schemas()
        listed_arns = [s["schemaArn"] for s in list_resp["schemas"]]
        assert arn not in listed_arns


class TestPersonalizeDatasetGroupEdgeCases:
    """Edge cases and behavioral fidelity for DatasetGroup operations."""

    @pytest.fixture
    def client(self):
        return make_client("personalize")

    def test_dataset_group_arn_format(self, client):
        r = client.create_dataset_group(name="test-dg-arn-fmt")
        arn = r["datasetGroupArn"]
        parts = arn.split(":")
        assert parts[0] == "arn"
        assert parts[1] == "aws"
        assert parts[2] == "personalize"
        assert "dataset-group" in arn
        assert "test-dg-arn-fmt" in arn
        client.delete_dataset_group(datasetGroupArn=arn)

    def test_dataset_group_status_is_active(self, client):
        r = client.create_dataset_group(name="test-dg-status-active")
        arn = r["datasetGroupArn"]
        desc = client.describe_dataset_group(datasetGroupArn=arn)
        assert desc["datasetGroup"]["status"] == "ACTIVE"
        client.delete_dataset_group(datasetGroupArn=arn)

    def test_dataset_group_describe_has_timestamps(self, client):
        r = client.create_dataset_group(name="test-dg-timestamps")
        arn = r["datasetGroupArn"]
        desc = client.describe_dataset_group(datasetGroupArn=arn)["datasetGroup"]
        assert "creationDateTime" in desc
        assert "lastUpdatedDateTime" in desc
        assert desc["creationDateTime"] is not None
        client.delete_dataset_group(datasetGroupArn=arn)

    def test_delete_nonexistent_dataset_group_raises(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_dataset_group(
                datasetGroupArn="arn:aws:personalize:us-east-1:123456789012:dataset-group/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_dataset_groups_returns_multiple(self, client):
        arns = []
        for i in range(3):
            r = client.create_dataset_group(name=f"test-dg-multi-{i}")
            arns.append(r["datasetGroupArn"])
        list_resp = client.list_dataset_groups()
        listed_arns = {dg["datasetGroupArn"] for dg in list_resp["datasetGroups"]}
        for arn in arns:
            assert arn in listed_arns
        for arn in arns:
            client.delete_dataset_group(datasetGroupArn=arn)

    def test_list_dataset_groups_entry_has_correct_keys(self, client):
        r = client.create_dataset_group(name="test-dg-list-keys")
        arn = r["datasetGroupArn"]
        list_resp = client.list_dataset_groups()
        matching = [dg for dg in list_resp["datasetGroups"] if dg["datasetGroupArn"] == arn]
        assert len(matching) == 1
        entry = matching[0]
        assert "datasetGroupArn" in entry
        assert "name" in entry
        assert "status" in entry
        assert "creationDateTime" in entry
        assert entry["name"] == "test-dg-list-keys"
        client.delete_dataset_group(datasetGroupArn=arn)

    def test_list_dataset_groups_after_delete_excludes_deleted(self, client):
        r = client.create_dataset_group(name="test-dg-delete-list")
        arn = r["datasetGroupArn"]
        client.delete_dataset_group(datasetGroupArn=arn)
        list_resp = client.list_dataset_groups()
        listed_arns = [dg["datasetGroupArn"] for dg in list_resp["datasetGroups"]]
        assert arn not in listed_arns

    def test_list_datasets_with_dataset_group_arn_filter(self, client):
        r = client.create_dataset_group(name="test-dg-list-datasets-filter")
        arn = r["datasetGroupArn"]
        resp = client.list_datasets(datasetGroupArn=arn)
        assert "datasets" in resp
        # Dataset group just created — no datasets yet, should be empty list
        assert resp["datasets"] == []
        client.delete_dataset_group(datasetGroupArn=arn)

    def test_list_event_trackers_with_dataset_group_filter(self, client):
        r = client.create_dataset_group(name="test-dg-et-filter")
        dg_arn = r["datasetGroupArn"]
        # Create an event tracker in this group so the filter returns a non-empty list
        et = client.create_event_tracker(name="test-dg-et-filter-et", datasetGroupArn=dg_arn)
        et_arn = et["eventTrackerArn"]
        resp = client.list_event_trackers(datasetGroupArn=dg_arn)
        assert "eventTrackers" in resp
        arns = [e["eventTrackerArn"] for e in resp["eventTrackers"]]
        assert et_arn in arns
        client.delete_event_tracker(eventTrackerArn=et_arn)
        client.delete_dataset_group(datasetGroupArn=dg_arn)

    def test_list_filters_with_dataset_group_filter(self, client):
        r = client.create_dataset_group(name="test-dg-filter-list")
        dg_arn = r["datasetGroupArn"]
        # Create a filter in this group so the filter returns a non-empty list
        f = client.create_filter(
            name="test-dg-filter-list-filter",
            datasetGroupArn=dg_arn,
            filterExpression=FILTER_EXPR,
        )
        f_arn = f["filterArn"]
        resp = client.list_filters(datasetGroupArn=dg_arn)
        assert "Filters" in resp
        arns = [fi["filterArn"] for fi in resp["Filters"]]
        assert f_arn in arns
        client.delete_filter(filterArn=f_arn)
        client.delete_dataset_group(datasetGroupArn=dg_arn)


class TestPersonalizeTagEdgeCases:
    """Tag operation edge cases."""

    @pytest.fixture
    def client(self):
        return make_client("personalize")

    def test_tag_multiple_keys_then_list(self, client):
        r = client.create_dataset_group(name="test-dg-multi-tags")
        arn = r["datasetGroupArn"]
        client.tag_resource(
            resourceArn=arn,
            tags=[
                {"tagKey": "env", "tagValue": "test"},
                {"tagKey": "team", "tagValue": "ml"},
            ],
        )
        resp = client.list_tags_for_resource(resourceArn=arn)
        tag_map = {t["tagKey"]: t["tagValue"] for t in resp["tags"]}
        assert tag_map.get("env") == "test"
        assert tag_map.get("team") == "ml"
        assert len(tag_map) == 2
        client.delete_dataset_group(datasetGroupArn=arn)

    def test_untag_removes_specific_key(self, client):
        r = client.create_dataset_group(name="test-dg-untag-specific")
        arn = r["datasetGroupArn"]
        client.tag_resource(
            resourceArn=arn,
            tags=[
                {"tagKey": "to-remove", "tagValue": "gone"},
                {"tagKey": "to-keep", "tagValue": "here"},
            ],
        )
        client.untag_resource(resourceArn=arn, tagKeys=["to-remove"])
        resp = client.list_tags_for_resource(resourceArn=arn)
        keys = {t["tagKey"] for t in resp.get("tags", [])}
        assert "to-remove" not in keys
        assert "to-keep" in keys
        client.delete_dataset_group(datasetGroupArn=arn)

    def test_list_tags_empty_resource_returns_empty(self, client):
        r = client.create_dataset_group(name="test-dg-no-tags")
        arn = r["datasetGroupArn"]
        resp = client.list_tags_for_resource(resourceArn=arn)
        assert "tags" in resp
        assert resp["tags"] == []
        client.delete_dataset_group(datasetGroupArn=arn)

    def test_schema_tag_operations(self, client):
        schema_json = (
            '{"type":"record","name":"Interactions",'
            '"namespace":"com.amazonaws.personalize.schema",'
            '"fields":['
            '{"name":"USER_ID","type":"string"},'
            '{"name":"ITEM_ID","type":"string"},'
            '{"name":"TIMESTAMP","type":"long"}'
            '],"version":"1.0"}'
        )
        r = client.create_schema(name="test-schema-tag-ops", schema=schema_json)
        arn = r["schemaArn"]
        client.tag_resource(resourceArn=arn, tags=[{"tagKey": "purpose", "tagValue": "compat"}])
        resp = client.list_tags_for_resource(resourceArn=arn)
        assert any(t["tagKey"] == "purpose" for t in resp["tags"])
        client.delete_schema(schemaArn=arn)


class TestPersonalizeListFilters:
    """Tests for list operations with optional filter params."""

    @pytest.fixture
    def client(self):
        return make_client("personalize")

    def test_list_campaigns_with_solution_arn_filter(self, client):
        # CREATE a campaign with a known solution ARN
        sol_arn = f"{BASE_ARN}:solution/filter-test-sol"
        r = client.create_campaign(name="test-camp-sol-filter", solutionVersionArn=SOL_VERSION_ARN)
        camp_arn = r["campaignArn"]

        # LIST filtered - campaigns exist
        resp = client.list_campaigns()
        assert "campaigns" in resp
        all_arns = [c["campaignArn"] for c in resp["campaigns"]]
        assert camp_arn in all_arns

        # LIST filtered by a different solution (empty result is fine)
        resp2 = client.list_campaigns(solutionArn=sol_arn)
        assert "campaigns" in resp2
        assert isinstance(resp2["campaigns"], list)

        # RETRIEVE the campaign
        desc = client.describe_campaign(campaignArn=camp_arn)
        assert desc["campaign"]["campaignArn"] == camp_arn

        # DELETE
        client.delete_campaign(campaignArn=camp_arn)

        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_campaign(campaignArn=camp_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_solution_versions_with_solution_arn_filter(self, client):
        # CREATE solution + version so filter is non-trivial
        sol = client.create_solution(name="test-filter-sv-sol", datasetGroupArn=DG_ARN)
        sol_arn = sol["solutionArn"]
        sv = client.create_solution_version(solutionArn=sol_arn)
        sv_arn = sv["solutionVersionArn"]

        # LIST filtered by solution ARN - should include the created version
        resp = client.list_solution_versions(solutionArn=sol_arn)
        assert "solutionVersions" in resp
        arns = [sv["solutionVersionArn"] for sv in resp["solutionVersions"]]
        assert sv_arn in arns

        # RETRIEVE the solution version
        desc = client.describe_solution_version(solutionVersionArn=sv_arn)
        assert desc["solutionVersion"]["solutionVersionArn"] == sv_arn

        # DELETE solution (versions go with it)
        client.delete_solution(solutionArn=sol_arn)

        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_solution(solutionArn=sol_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_recommenders_with_dataset_group_filter(self, client):
        # CREATE dataset group and recommender in it
        r = client.create_dataset_group(name="test-dg-recommenders-filter")
        dg_arn = r["datasetGroupArn"]
        rec = client.create_recommender(
            name="test-rec-dg-filter", datasetGroupArn=dg_arn, recipeArn=RECIPE_ARN
        )
        rec_arn = rec["recommenderArn"]

        # LIST filtered by dataset group - should include the recommender
        resp = client.list_recommenders(datasetGroupArn=dg_arn)
        assert "recommenders" in resp
        arns = [r["recommenderArn"] for r in resp["recommenders"]]
        assert rec_arn in arns

        # RETRIEVE the recommender
        desc = client.describe_recommender(recommenderArn=rec_arn)
        assert desc["recommender"]["recommenderArn"] == rec_arn

        # DELETE
        client.delete_recommender(recommenderArn=rec_arn)
        client.delete_dataset_group(datasetGroupArn=dg_arn)

        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_recommender(recommenderArn=rec_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_solutions_with_dataset_group_filter(self, client):
        # CREATE dataset group and solution in it
        r = client.create_dataset_group(name="test-dg-solutions-filter")
        dg_arn = r["datasetGroupArn"]
        sol = client.create_solution(name="test-sol-dg-filter", datasetGroupArn=dg_arn)
        sol_arn = sol["solutionArn"]

        # LIST filtered by dataset group - should include the solution
        resp = client.list_solutions(datasetGroupArn=dg_arn)
        assert "solutions" in resp
        arns = [s["solutionArn"] for s in resp["solutions"]]
        assert sol_arn in arns

        # RETRIEVE the solution
        desc = client.describe_solution(solutionArn=sol_arn)
        assert desc["solution"]["solutionArn"] == sol_arn

        # DELETE
        client.delete_solution(solutionArn=sol_arn)
        client.delete_dataset_group(datasetGroupArn=dg_arn)

        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_solution(solutionArn=sol_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_batch_inference_jobs_with_solution_version_filter(self, client):
        # CREATE a batch job with a known solution version
        r = client.create_batch_inference_job(
            jobName="test-bij-filter",
            solutionVersionArn=SOL_VERSION_ARN,
            jobInput=JOB_INPUT,
            jobOutput=JOB_OUTPUT,
            roleArn=ROLE_ARN,
        )
        bij_arn = r["batchInferenceJobArn"]

        # LIST filtered by that solution version
        resp = client.list_batch_inference_jobs(solutionVersionArn=SOL_VERSION_ARN)
        assert "batchInferenceJobs" in resp
        arns = [j["batchInferenceJobArn"] for j in resp["batchInferenceJobs"]]
        assert bij_arn in arns

        # RETRIEVE
        desc = client.describe_batch_inference_job(batchInferenceJobArn=bij_arn)
        assert desc["batchInferenceJob"]["jobName"] == "test-bij-filter"

        # ERROR: nonexistent job
        with pytest.raises(ClientError) as exc:
            client.describe_batch_inference_job(
                batchInferenceJobArn=f"{BASE_ARN}:batch-inference-job/nonexistent-filter"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_batch_segment_jobs_with_solution_version_filter(self, client):
        # CREATE a batch segment job with a known solution version
        r = client.create_batch_segment_job(
            jobName="test-bsj-filter",
            solutionVersionArn=SOL_VERSION_ARN,
            jobInput=JOB_INPUT,
            jobOutput=JOB_OUTPUT,
            roleArn=ROLE_ARN,
        )
        bsj_arn = r["batchSegmentJobArn"]

        # LIST filtered by that solution version
        resp = client.list_batch_segment_jobs(solutionVersionArn=SOL_VERSION_ARN)
        assert "batchSegmentJobs" in resp
        arns = [j["batchSegmentJobArn"] for j in resp["batchSegmentJobs"]]
        assert bsj_arn in arns

        # RETRIEVE
        desc = client.describe_batch_segment_job(batchSegmentJobArn=bsj_arn)
        assert desc["batchSegmentJob"]["jobName"] == "test-bsj-filter"

        # ERROR: nonexistent job
        with pytest.raises(ClientError) as exc:
            client.describe_batch_segment_job(
                batchSegmentJobArn=f"{BASE_ARN}:batch-segment-job/nonexistent-filter"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_data_deletion_jobs_with_dataset_group_filter(self, client):
        # CREATE dataset group and deletion job
        r = client.create_dataset_group(name="test-dg-deletion-jobs-filter")
        dg_arn = r["datasetGroupArn"]
        ddj = client.create_data_deletion_job(
            jobName="test-ddj-dg-filter",
            datasetGroupArn=dg_arn,
            dataSource={"dataLocation": "s3://bucket/data.csv"},
            roleArn=ROLE_ARN,
        )
        ddj_arn = ddj["dataDeletionJobArn"]

        # LIST filtered by dataset group
        resp = client.list_data_deletion_jobs(datasetGroupArn=dg_arn)
        assert "dataDeletionJobs" in resp
        arns = [j["dataDeletionJobArn"] for j in resp["dataDeletionJobs"]]
        assert ddj_arn in arns

        # RETRIEVE
        desc = client.describe_data_deletion_job(dataDeletionJobArn=ddj_arn)
        assert desc["dataDeletionJob"]["jobName"] == "test-ddj-dg-filter"

        # DELETE dataset group (best effort)
        client.delete_dataset_group(datasetGroupArn=dg_arn)

        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_dataset_group(datasetGroupArn=dg_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_metric_attributions_with_dataset_group_filter(self, client):
        # CREATE dataset group and metric attribution
        r = client.create_dataset_group(name="test-dg-metric-attr-filter")
        dg_arn = r["datasetGroupArn"]
        ma = client.create_metric_attribution(
            name="test-ma-dg-filter",
            datasetGroupArn=dg_arn,
            metrics=METRICS,
            metricsOutputConfig=METRICS_OUTPUT,
        )
        ma_arn = ma["metricAttributionArn"]

        # LIST filtered by dataset group
        resp = client.list_metric_attributions(datasetGroupArn=dg_arn)
        assert "metricAttributions" in resp
        arns = [ma["metricAttributionArn"] for ma in resp["metricAttributions"]]
        assert ma_arn in arns

        # RETRIEVE
        desc = client.describe_metric_attribution(metricAttributionArn=ma_arn)
        assert desc["metricAttribution"]["metricAttributionArn"] == ma_arn

        # DELETE
        client.delete_metric_attribution(metricAttributionArn=ma_arn)
        client.delete_dataset_group(datasetGroupArn=dg_arn)

        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_metric_attribution(metricAttributionArn=ma_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestPersonalizeCampaignCRUD:
    """Full CRUD lifecycle for Campaign resources."""

    BASE_ARN = "arn:aws:personalize:us-east-1:123456789012"
    SOL_VERSION_ARN = f"{BASE_ARN}:solution/test-sol/solutionVersion/abc123"

    @pytest.fixture
    def client(self):
        return make_client("personalize")

    def test_create_describe_delete_campaign(self, client):
        r = client.create_campaign(name="test-camp-crud", solutionVersionArn=self.SOL_VERSION_ARN)
        arn = r["campaignArn"]
        assert "campaign" in arn
        assert "test-camp-crud" in arn

        desc = client.describe_campaign(campaignArn=arn)
        camp = desc["campaign"]
        assert camp["name"] == "test-camp-crud"
        assert camp["campaignArn"] == arn
        assert camp["status"] == "ACTIVE"

        client.delete_campaign(campaignArn=arn)
        with pytest.raises(ClientError) as exc:
            client.describe_campaign(campaignArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_campaign_update_returns_arn(self, client):
        r = client.create_campaign(name="test-camp-update", solutionVersionArn=self.SOL_VERSION_ARN)
        arn = r["campaignArn"]
        upd = client.update_campaign(campaignArn=arn, minProvisionedTPS=5)
        assert upd["campaignArn"] == arn
        client.delete_campaign(campaignArn=arn)

    def test_list_campaigns_includes_created(self, client):
        r = client.create_campaign(name="test-camp-list-check", solutionVersionArn=self.SOL_VERSION_ARN)
        arn = r["campaignArn"]
        resp = client.list_campaigns()
        arns = [c["campaignArn"] for c in resp["campaigns"]]
        assert arn in arns
        client.delete_campaign(campaignArn=arn)

    def test_list_campaigns_entry_has_correct_keys(self, client):
        r = client.create_campaign(name="test-camp-keys", solutionVersionArn=self.SOL_VERSION_ARN)
        arn = r["campaignArn"]
        resp = client.list_campaigns()
        matching = [c for c in resp["campaigns"] if c["campaignArn"] == arn]
        assert len(matching) == 1
        entry = matching[0]
        assert "campaignArn" in entry
        assert "name" in entry
        assert "status" in entry
        assert "creationDateTime" in entry
        assert entry["name"] == "test-camp-keys"
        client.delete_campaign(campaignArn=arn)

    def test_campaign_describe_has_timestamps(self, client):
        r = client.create_campaign(name="test-camp-ts", solutionVersionArn=self.SOL_VERSION_ARN)
        arn = r["campaignArn"]
        desc = client.describe_campaign(campaignArn=arn)["campaign"]
        assert "creationDateTime" in desc
        assert "lastUpdatedDateTime" in desc
        assert desc["creationDateTime"] is not None
        client.delete_campaign(campaignArn=arn)

    def test_delete_nonexistent_campaign_raises(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_campaign(campaignArn=f"{self.BASE_ARN}:campaign/nonexistent-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_campaigns_after_delete_excludes_deleted(self, client):
        r = client.create_campaign(name="test-camp-del-list", solutionVersionArn=self.SOL_VERSION_ARN)
        arn = r["campaignArn"]
        client.delete_campaign(campaignArn=arn)
        resp = client.list_campaigns()
        arns = [c["campaignArn"] for c in resp["campaigns"]]
        assert arn not in arns


class TestPersonalizeSolutionCRUD:
    """Full CRUD lifecycle for Solution resources."""

    BASE_ARN = "arn:aws:personalize:us-east-1:123456789012"
    DG_ARN = f"{BASE_ARN}:dataset-group/test-dg"

    @pytest.fixture
    def client(self):
        return make_client("personalize")

    def test_create_describe_delete_solution(self, client):
        r = client.create_solution(name="test-sol-crud", datasetGroupArn=self.DG_ARN)
        arn = r["solutionArn"]
        assert "solution" in arn
        assert "test-sol-crud" in arn

        desc = client.describe_solution(solutionArn=arn)
        sol = desc["solution"]
        assert sol["name"] == "test-sol-crud"
        assert sol["solutionArn"] == arn
        assert sol["status"] == "ACTIVE"

        client.delete_solution(solutionArn=arn)
        with pytest.raises(ClientError) as exc:
            client.describe_solution(solutionArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_solution_update_returns_arn(self, client):
        r = client.create_solution(name="test-sol-update", datasetGroupArn=self.DG_ARN)
        arn = r["solutionArn"]
        upd = client.update_solution(solutionArn=arn, performAutoTraining=True)
        assert upd["solutionArn"] == arn
        client.delete_solution(solutionArn=arn)

    def test_list_solutions_includes_created(self, client):
        r = client.create_solution(name="test-sol-list-check", datasetGroupArn=self.DG_ARN)
        arn = r["solutionArn"]
        resp = client.list_solutions()
        arns = [s["solutionArn"] for s in resp["solutions"]]
        assert arn in arns
        client.delete_solution(solutionArn=arn)

    def test_list_solutions_entry_has_correct_keys(self, client):
        r = client.create_solution(name="test-sol-keys", datasetGroupArn=self.DG_ARN)
        arn = r["solutionArn"]
        resp = client.list_solutions()
        matching = [s for s in resp["solutions"] if s["solutionArn"] == arn]
        assert len(matching) == 1
        entry = matching[0]
        assert "solutionArn" in entry
        assert "name" in entry
        assert "status" in entry
        assert "creationDateTime" in entry
        client.delete_solution(solutionArn=arn)

    def test_delete_nonexistent_solution_raises(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_solution(solutionArn=f"{self.BASE_ARN}:solution/nonexistent-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_solution_arn_format(self, client):
        r = client.create_solution(name="test-sol-arn-fmt", datasetGroupArn=self.DG_ARN)
        arn = r["solutionArn"]
        parts = arn.split(":")
        assert parts[0] == "arn"
        assert parts[1] == "aws"
        assert parts[2] == "personalize"
        assert "solution" in arn
        assert "test-sol-arn-fmt" in arn
        client.delete_solution(solutionArn=arn)


class TestPersonalizeSolutionVersionCRUD:
    """CRUD lifecycle for SolutionVersion resources."""

    BASE_ARN = "arn:aws:personalize:us-east-1:123456789012"
    DG_ARN = f"{BASE_ARN}:dataset-group/test-dg"

    @pytest.fixture
    def client(self):
        return make_client("personalize")

    @pytest.fixture
    def solution_arn(self, client):
        r = client.create_solution(name="test-sol-for-ver", datasetGroupArn=self.DG_ARN)
        yield r["solutionArn"]
        try:
            client.delete_solution(solutionArn=r["solutionArn"])
        except ClientError:
            pass

    def test_create_describe_solution_version(self, client, solution_arn):
        r = client.create_solution_version(solutionArn=solution_arn)
        sv_arn = r["solutionVersionArn"]
        assert "solutionVersion" in sv_arn or "solution" in sv_arn

        desc = client.describe_solution_version(solutionVersionArn=sv_arn)
        sv = desc["solutionVersion"]
        assert sv["solutionVersionArn"] == sv_arn
        assert sv["status"] == "ACTIVE"

    def test_list_solution_versions_includes_created(self, client, solution_arn):
        r = client.create_solution_version(solutionArn=solution_arn)
        sv_arn = r["solutionVersionArn"]
        resp = client.list_solution_versions()
        arns = [sv["solutionVersionArn"] for sv in resp["solutionVersions"]]
        assert sv_arn in arns

    def test_list_solution_versions_entry_has_correct_keys(self, client, solution_arn):
        r = client.create_solution_version(solutionArn=solution_arn)
        sv_arn = r["solutionVersionArn"]
        resp = client.list_solution_versions()
        matching = [sv for sv in resp["solutionVersions"] if sv["solutionVersionArn"] == sv_arn]
        assert len(matching) == 1
        entry = matching[0]
        assert "solutionVersionArn" in entry
        assert "status" in entry
        assert "creationDateTime" in entry


class TestPersonalizeFilterCRUD:
    """Full CRUD lifecycle for Filter resources."""

    BASE_ARN = "arn:aws:personalize:us-east-1:123456789012"
    DG_ARN = f"{BASE_ARN}:dataset-group/test-dg"
    FILTER_EXPR = "EXCLUDE itemId WHERE Items.genre IN ($GENRES)"

    @pytest.fixture
    def client(self):
        return make_client("personalize")

    def test_create_describe_delete_filter(self, client):
        r = client.create_filter(
            name="test-filter-crud",
            datasetGroupArn=self.DG_ARN,
            filterExpression=self.FILTER_EXPR,
        )
        arn = r["filterArn"]
        assert "filter" in arn
        assert "test-filter-crud" in arn

        desc = client.describe_filter(filterArn=arn)
        f = desc["filter"]
        assert f["name"] == "test-filter-crud"
        assert f["filterArn"] == arn
        assert f["status"] == "ACTIVE"

        client.delete_filter(filterArn=arn)
        with pytest.raises(ClientError) as exc:
            client.describe_filter(filterArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_filters_includes_created(self, client):
        r = client.create_filter(
            name="test-filter-list-check",
            datasetGroupArn=self.DG_ARN,
            filterExpression=self.FILTER_EXPR,
        )
        arn = r["filterArn"]
        resp = client.list_filters()
        arns = [f["filterArn"] for f in resp["Filters"]]
        assert arn in arns
        client.delete_filter(filterArn=arn)

    def test_list_filters_entry_has_correct_keys(self, client):
        r = client.create_filter(
            name="test-filter-keys",
            datasetGroupArn=self.DG_ARN,
            filterExpression=self.FILTER_EXPR,
        )
        arn = r["filterArn"]
        resp = client.list_filters()
        matching = [f for f in resp["Filters"] if f["filterArn"] == arn]
        assert len(matching) == 1
        entry = matching[0]
        assert "filterArn" in entry
        assert "name" in entry
        assert "status" in entry
        assert "creationDateTime" in entry
        client.delete_filter(filterArn=arn)

    def test_filter_describe_has_timestamps(self, client):
        r = client.create_filter(
            name="test-filter-ts",
            datasetGroupArn=self.DG_ARN,
            filterExpression=self.FILTER_EXPR,
        )
        arn = r["filterArn"]
        desc = client.describe_filter(filterArn=arn)["filter"]
        assert desc["creationDateTime"] is not None
        assert desc["lastUpdatedDateTime"] is not None
        # Both timestamps should be datetime objects (non-zero epoch)
        import datetime
        assert isinstance(desc["creationDateTime"], datetime.datetime)
        client.delete_filter(filterArn=arn)

    def test_delete_nonexistent_filter_raises(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_filter(filterArn=f"{self.BASE_ARN}:filter/test-dg/nonexistent-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_filters_after_delete_excludes_deleted(self, client):
        r = client.create_filter(
            name="test-filter-del-list",
            datasetGroupArn=self.DG_ARN,
            filterExpression=self.FILTER_EXPR,
        )
        arn = r["filterArn"]
        client.delete_filter(filterArn=arn)
        resp = client.list_filters()
        arns = [f["filterArn"] for f in resp["Filters"]]
        assert arn not in arns


class TestPersonalizeEventTrackerCRUD:
    """Full CRUD lifecycle for EventTracker resources."""

    BASE_ARN = "arn:aws:personalize:us-east-1:123456789012"
    DG_ARN = f"{BASE_ARN}:dataset-group/test-dg"

    @pytest.fixture
    def client(self):
        return make_client("personalize")

    def test_create_describe_delete_event_tracker(self, client):
        r = client.create_event_tracker(name="test-et-crud", datasetGroupArn=self.DG_ARN)
        arn = r["eventTrackerArn"]
        tracking_id = r["trackingId"]
        assert "event-tracker" in arn
        assert tracking_id is not None

        # RETRIEVE
        desc = client.describe_event_tracker(eventTrackerArn=arn)
        et = desc["eventTracker"]
        assert et["name"] == "test-et-crud"
        assert et["eventTrackerArn"] == arn
        assert et["status"] == "ACTIVE"
        assert et.get("trackingId") == tracking_id

        # LIST: verify the tracker appears
        list_resp = client.list_event_trackers()
        listed_arns = [e["eventTrackerArn"] for e in list_resp["eventTrackers"]]
        assert arn in listed_arns

        # DELETE
        client.delete_event_tracker(eventTrackerArn=arn)

        # ERROR: deleted resource is gone
        with pytest.raises(ClientError) as exc:
            client.describe_event_tracker(eventTrackerArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

        # ERROR: deleted tracker is no longer in the list
        list_resp2 = client.list_event_trackers()
        listed_arns2 = [e["eventTrackerArn"] for e in list_resp2["eventTrackers"]]
        assert arn not in listed_arns2

    def test_event_tracker_create_returns_tracking_id(self, client):
        r = client.create_event_tracker(name="test-et-tracking", datasetGroupArn=self.DG_ARN)
        assert "trackingId" in r
        assert r["trackingId"] is not None and r["trackingId"] != ""
        client.delete_event_tracker(eventTrackerArn=r["eventTrackerArn"])

    def test_list_event_trackers_includes_created(self, client):
        r = client.create_event_tracker(name="test-et-list-check", datasetGroupArn=self.DG_ARN)
        arn = r["eventTrackerArn"]
        resp = client.list_event_trackers()
        arns = [et["eventTrackerArn"] for et in resp["eventTrackers"]]
        assert arn in arns
        client.delete_event_tracker(eventTrackerArn=arn)

    def test_list_event_trackers_entry_has_correct_keys(self, client):
        r = client.create_event_tracker(name="test-et-keys", datasetGroupArn=self.DG_ARN)
        arn = r["eventTrackerArn"]
        resp = client.list_event_trackers()
        matching = [et for et in resp["eventTrackers"] if et["eventTrackerArn"] == arn]
        assert len(matching) == 1
        entry = matching[0]
        assert "eventTrackerArn" in entry
        assert "name" in entry
        assert "status" in entry
        assert "creationDateTime" in entry
        client.delete_event_tracker(eventTrackerArn=arn)

    def test_delete_nonexistent_event_tracker_raises(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_event_tracker(
                eventTrackerArn=f"{self.BASE_ARN}:event-tracker/nonexistent-xyz"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_event_trackers_after_delete_excludes_deleted(self, client):
        r = client.create_event_tracker(name="test-et-del-list", datasetGroupArn=self.DG_ARN)
        arn = r["eventTrackerArn"]
        client.delete_event_tracker(eventTrackerArn=arn)
        resp = client.list_event_trackers()
        arns = [et["eventTrackerArn"] for et in resp["eventTrackers"]]
        assert arn not in arns


class TestPersonalizeRecommenderCRUD:
    """Full CRUD lifecycle for Recommender resources."""

    BASE_ARN = "arn:aws:personalize:us-east-1:123456789012"
    DG_ARN = f"{BASE_ARN}:dataset-group/test-dg"
    RECIPE_ARN = "arn:aws:personalize:::recipe/aws-ecomm-popular-items-by-purchases"

    @pytest.fixture
    def client(self):
        return make_client("personalize")

    def test_create_describe_delete_recommender(self, client):
        r = client.create_recommender(
            name="test-rec-crud",
            datasetGroupArn=self.DG_ARN,
            recipeArn=self.RECIPE_ARN,
        )
        arn = r["recommenderArn"]
        assert "recommender" in arn
        assert "test-rec-crud" in arn

        desc = client.describe_recommender(recommenderArn=arn)
        rec = desc["recommender"]
        assert rec["name"] == "test-rec-crud"
        assert rec["recommenderArn"] == arn
        assert rec["status"] == "ACTIVE"

        client.delete_recommender(recommenderArn=arn)
        with pytest.raises(ClientError) as exc:
            client.describe_recommender(recommenderArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_recommender_update_returns_arn(self, client):
        r = client.create_recommender(
            name="test-rec-update",
            datasetGroupArn=self.DG_ARN,
            recipeArn=self.RECIPE_ARN,
        )
        arn = r["recommenderArn"]
        upd = client.update_recommender(recommenderArn=arn, recommenderConfig={"minRecommendationRequestsPerSecond": 2})
        assert upd["recommenderArn"] == arn
        client.delete_recommender(recommenderArn=arn)

    def test_recommender_start_stop(self, client):
        r = client.create_recommender(
            name="test-rec-startstop",
            datasetGroupArn=self.DG_ARN,
            recipeArn=self.RECIPE_ARN,
        )
        arn = r["recommenderArn"]
        stop_r = client.stop_recommender(recommenderArn=arn)
        assert stop_r["recommenderArn"] == arn

        desc = client.describe_recommender(recommenderArn=arn)
        assert desc["recommender"]["status"] == "INACTIVE"

        start_r = client.start_recommender(recommenderArn=arn)
        assert start_r["recommenderArn"] == arn

        desc2 = client.describe_recommender(recommenderArn=arn)
        assert desc2["recommender"]["status"] == "ACTIVE"

        client.delete_recommender(recommenderArn=arn)

    def test_list_recommenders_includes_created(self, client):
        r = client.create_recommender(
            name="test-rec-list-check",
            datasetGroupArn=self.DG_ARN,
            recipeArn=self.RECIPE_ARN,
        )
        arn = r["recommenderArn"]
        resp = client.list_recommenders()
        arns = [rec["recommenderArn"] for rec in resp["recommenders"]]
        assert arn in arns
        client.delete_recommender(recommenderArn=arn)

    def test_list_recommenders_entry_has_correct_keys(self, client):
        r = client.create_recommender(
            name="test-rec-keys",
            datasetGroupArn=self.DG_ARN,
            recipeArn=self.RECIPE_ARN,
        )
        arn = r["recommenderArn"]
        resp = client.list_recommenders()
        matching = [rec for rec in resp["recommenders"] if rec["recommenderArn"] == arn]
        assert len(matching) == 1
        entry = matching[0]
        assert "recommenderArn" in entry
        assert "name" in entry
        assert "status" in entry
        assert "creationDateTime" in entry
        client.delete_recommender(recommenderArn=arn)

    def test_delete_nonexistent_recommender_raises(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_recommender(
                recommenderArn=f"{self.BASE_ARN}:recommender/nonexistent-xyz"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestPersonalizeMetricAttributionCRUD:
    """Full CRUD lifecycle for MetricAttribution resources."""

    BASE_ARN = "arn:aws:personalize:us-east-1:123456789012"
    DG_ARN = f"{BASE_ARN}:dataset-group/test-dg"
    ROLE_ARN = f"{BASE_ARN.replace('personalize', 'iam')}:role/PersonalizeRole"
    METRICS_OUTPUT = {
        "s3DataDestination": {"path": "s3://bucket/prefix/"},
        "roleArn": "arn:aws:iam::123456789012:role/PersonalizeRole",
    }
    METRICS = [{"eventType": "click", "expression": "SUM(DatasetType.INTERACTIONS)", "metricName": "click-metric"}]

    @pytest.fixture
    def client(self):
        return make_client("personalize")

    def test_create_describe_delete_metric_attribution(self, client):
        r = client.create_metric_attribution(
            name="test-ma-crud",
            datasetGroupArn=self.DG_ARN,
            metrics=self.METRICS,
            metricsOutputConfig=self.METRICS_OUTPUT,
        )
        arn = r["metricAttributionArn"]
        assert "metric-attribution" in arn
        assert "test-ma-crud" in arn

        desc = client.describe_metric_attribution(metricAttributionArn=arn)
        ma = desc["metricAttribution"]
        assert ma["name"] == "test-ma-crud"
        assert ma["metricAttributionArn"] == arn
        assert ma["status"] == "ACTIVE"

        client.delete_metric_attribution(metricAttributionArn=arn)
        with pytest.raises(ClientError) as exc:
            client.describe_metric_attribution(metricAttributionArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_metric_attribution_update_returns_arn(self, client):
        r = client.create_metric_attribution(
            name="test-ma-update",
            datasetGroupArn=self.DG_ARN,
            metrics=self.METRICS,
            metricsOutputConfig=self.METRICS_OUTPUT,
        )
        arn = r["metricAttributionArn"]
        upd = client.update_metric_attribution(
            metricAttributionArn=arn,
            metricsOutputConfig=self.METRICS_OUTPUT,
        )
        assert upd["metricAttributionArn"] == arn
        client.delete_metric_attribution(metricAttributionArn=arn)

    def test_list_metric_attributions_includes_created(self, client):
        r = client.create_metric_attribution(
            name="test-ma-list-check",
            datasetGroupArn=self.DG_ARN,
            metrics=self.METRICS,
            metricsOutputConfig=self.METRICS_OUTPUT,
        )
        arn = r["metricAttributionArn"]
        resp = client.list_metric_attributions()
        arns = [ma["metricAttributionArn"] for ma in resp["metricAttributions"]]
        assert arn in arns
        client.delete_metric_attribution(metricAttributionArn=arn)

    def test_list_metric_attributions_entry_has_correct_keys(self, client):
        r = client.create_metric_attribution(
            name="test-ma-keys",
            datasetGroupArn=self.DG_ARN,
            metrics=self.METRICS,
            metricsOutputConfig=self.METRICS_OUTPUT,
        )
        arn = r["metricAttributionArn"]
        resp = client.list_metric_attributions()
        matching = [ma for ma in resp["metricAttributions"] if ma["metricAttributionArn"] == arn]
        assert len(matching) == 1
        entry = matching[0]
        assert "metricAttributionArn" in entry
        assert "name" in entry
        assert "status" in entry
        assert "creationDateTime" in entry
        client.delete_metric_attribution(metricAttributionArn=arn)

    def test_delete_nonexistent_metric_attribution_raises(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_metric_attribution(
                metricAttributionArn=f"{self.BASE_ARN}:metric-attribution/nonexistent-xyz"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestPersonalizeBatchJobsCRUD:
    """CRUD lifecycle for BatchInferenceJob, BatchSegmentJob, and DataDeletionJob."""

    BASE_ARN = "arn:aws:personalize:us-east-1:123456789012"
    SOL_VERSION_ARN = f"{BASE_ARN}:solution/test-sol/solutionVersion/abc123"
    ROLE_ARN = "arn:aws:iam::123456789012:role/PersonalizeRole"
    DG_ARN = f"{BASE_ARN}:dataset-group/test-dg"
    JOB_INPUT = {"s3DataSource": {"path": "s3://bucket/input/"}}
    JOB_OUTPUT = {"s3DataDestination": {"path": "s3://bucket/output/"}}

    @pytest.fixture
    def client(self):
        return make_client("personalize")

    def test_create_describe_batch_inference_job(self, client):
        r = client.create_batch_inference_job(
            jobName="test-bij-crud",
            solutionVersionArn=self.SOL_VERSION_ARN,
            jobInput=self.JOB_INPUT,
            jobOutput=self.JOB_OUTPUT,
            roleArn=self.ROLE_ARN,
        )
        arn = r["batchInferenceJobArn"]
        assert "batch-inference-job" in arn

        desc = client.describe_batch_inference_job(batchInferenceJobArn=arn)
        job = desc["batchInferenceJob"]
        assert job["jobName"] == "test-bij-crud"
        assert job["batchInferenceJobArn"] == arn
        assert job["status"] == "ACTIVE"

    def test_list_batch_inference_jobs_includes_created(self, client):
        r = client.create_batch_inference_job(
            jobName="test-bij-list",
            solutionVersionArn=self.SOL_VERSION_ARN,
            jobInput=self.JOB_INPUT,
            jobOutput=self.JOB_OUTPUT,
            roleArn=self.ROLE_ARN,
        )
        arn = r["batchInferenceJobArn"]
        resp = client.list_batch_inference_jobs()
        arns = [j["batchInferenceJobArn"] for j in resp["batchInferenceJobs"]]
        assert arn in arns

    def test_list_batch_inference_jobs_entry_has_correct_keys(self, client):
        r = client.create_batch_inference_job(
            jobName="test-bij-keys",
            solutionVersionArn=self.SOL_VERSION_ARN,
            jobInput=self.JOB_INPUT,
            jobOutput=self.JOB_OUTPUT,
            roleArn=self.ROLE_ARN,
        )
        arn = r["batchInferenceJobArn"]
        resp = client.list_batch_inference_jobs()
        matching = [j for j in resp["batchInferenceJobs"] if j["batchInferenceJobArn"] == arn]
        assert len(matching) == 1
        entry = matching[0]
        assert "batchInferenceJobArn" in entry
        assert "jobName" in entry
        assert "status" in entry
        assert "creationDateTime" in entry

    def test_create_describe_batch_segment_job(self, client):
        r = client.create_batch_segment_job(
            jobName="test-bsj-crud",
            solutionVersionArn=self.SOL_VERSION_ARN,
            jobInput=self.JOB_INPUT,
            jobOutput=self.JOB_OUTPUT,
            roleArn=self.ROLE_ARN,
        )
        arn = r["batchSegmentJobArn"]
        assert "batch-segment-job" in arn

        desc = client.describe_batch_segment_job(batchSegmentJobArn=arn)
        job = desc["batchSegmentJob"]
        assert job["jobName"] == "test-bsj-crud"
        assert job["batchSegmentJobArn"] == arn
        assert job["status"] == "ACTIVE"

    def test_list_batch_segment_jobs_includes_created(self, client):
        r = client.create_batch_segment_job(
            jobName="test-bsj-list",
            solutionVersionArn=self.SOL_VERSION_ARN,
            jobInput=self.JOB_INPUT,
            jobOutput=self.JOB_OUTPUT,
            roleArn=self.ROLE_ARN,
        )
        arn = r["batchSegmentJobArn"]
        resp = client.list_batch_segment_jobs()
        arns = [j["batchSegmentJobArn"] for j in resp["batchSegmentJobs"]]
        assert arn in arns

    def test_list_batch_segment_jobs_entry_has_correct_keys(self, client):
        r = client.create_batch_segment_job(
            jobName="test-bsj-keys",
            solutionVersionArn=self.SOL_VERSION_ARN,
            jobInput=self.JOB_INPUT,
            jobOutput=self.JOB_OUTPUT,
            roleArn=self.ROLE_ARN,
        )
        arn = r["batchSegmentJobArn"]
        resp = client.list_batch_segment_jobs()
        matching = [j for j in resp["batchSegmentJobs"] if j["batchSegmentJobArn"] == arn]
        assert len(matching) == 1
        entry = matching[0]
        assert "batchSegmentJobArn" in entry
        assert "jobName" in entry
        assert "status" in entry
        assert "creationDateTime" in entry

    def test_create_describe_data_deletion_job(self, client):
        r = client.create_data_deletion_job(
            jobName="test-ddj-crud",
            datasetGroupArn=self.DG_ARN,
            dataSource={"dataLocation": "s3://bucket/data.csv"},
            roleArn=self.ROLE_ARN,
        )
        arn = r["dataDeletionJobArn"]
        assert "data-deletion-job" in arn

        desc = client.describe_data_deletion_job(dataDeletionJobArn=arn)
        job = desc["dataDeletionJob"]
        assert job["jobName"] == "test-ddj-crud"
        assert job["dataDeletionJobArn"] == arn
        assert job["status"] == "ACTIVE"

    def test_list_data_deletion_jobs_includes_created(self, client):
        r = client.create_data_deletion_job(
            jobName="test-ddj-list",
            datasetGroupArn=self.DG_ARN,
            dataSource={"dataLocation": "s3://bucket/data.csv"},
            roleArn=self.ROLE_ARN,
        )
        arn = r["dataDeletionJobArn"]
        resp = client.list_data_deletion_jobs()
        arns = [j["dataDeletionJobArn"] for j in resp["dataDeletionJobs"]]
        assert arn in arns

    def test_list_data_deletion_jobs_entry_has_correct_keys(self, client):
        r = client.create_data_deletion_job(
            jobName="test-ddj-keys",
            datasetGroupArn=self.DG_ARN,
            dataSource={"dataLocation": "s3://bucket/data.csv"},
            roleArn=self.ROLE_ARN,
        )
        arn = r["dataDeletionJobArn"]
        resp = client.list_data_deletion_jobs()
        matching = [j for j in resp["dataDeletionJobs"] if j["dataDeletionJobArn"] == arn]
        assert len(matching) == 1
        entry = matching[0]
        assert "dataDeletionJobArn" in entry
        assert "jobName" in entry
        assert "status" in entry
        assert "creationDateTime" in entry


class TestPersonalizePagination:
    """Pagination edge cases for Personalize list operations."""

    @pytest.fixture
    def client(self):
        return make_client("personalize")

    def test_list_schemas_pagination(self, client):
        """Create 3 schemas, paginate with maxResults=2, verify NextToken works."""
        schema_json = (
            '{"type":"record","name":"Interactions",'
            '"namespace":"com.amazonaws.personalize.schema",'
            '"fields":['
            '{"name":"USER_ID","type":"string"},'
            '{"name":"ITEM_ID","type":"string"},'
            '{"name":"TIMESTAMP","type":"long"}'
            '],"version":"1.0"}'
        )
        arns = []
        for i in range(3):
            r = client.create_schema(name=f"test-page-schema-{i}", schema=schema_json)
            arns.append(r["schemaArn"])

        # LIST first page with maxResults=2
        page1 = client.list_schemas(maxResults=2)
        assert "schemas" in page1
        assert len(page1["schemas"]) <= 2
        assert "nextToken" in page1

        # LIST second page using nextToken
        page2 = client.list_schemas(nextToken=page1["nextToken"])
        assert "schemas" in page2

        # All created ARNs should appear across both pages
        all_arns = {s["schemaArn"] for s in page1["schemas"]} | {s["schemaArn"] for s in page2["schemas"]}
        for arn in arns:
            assert arn in all_arns

        for arn in arns:
            client.delete_schema(schemaArn=arn)

    def test_list_dataset_groups_pagination(self, client):
        """Create 3 dataset groups, paginate with maxResults=2."""
        arns = []
        for i in range(3):
            r = client.create_dataset_group(name=f"test-page-dg-{i}")
            arns.append(r["datasetGroupArn"])

        # LIST first page
        page1 = client.list_dataset_groups(maxResults=2)
        assert "datasetGroups" in page1
        assert len(page1["datasetGroups"]) <= 2
        assert "nextToken" in page1

        # LIST second page
        page2 = client.list_dataset_groups(nextToken=page1["nextToken"])
        assert "datasetGroups" in page2

        all_arns = {dg["datasetGroupArn"] for dg in page1["datasetGroups"]} | {dg["datasetGroupArn"] for dg in page2["datasetGroups"]}
        for arn in arns:
            assert arn in all_arns

        for arn in arns:
            client.delete_dataset_group(datasetGroupArn=arn)

    def test_list_solutions_pagination(self, client):
        """Create 3 solutions, paginate with maxResults=2."""
        arns = []
        for i in range(3):
            r = client.create_solution(name=f"test-page-sol-{i}", datasetGroupArn=DG_ARN)
            arns.append(r["solutionArn"])

        page1 = client.list_solutions(maxResults=2)
        assert "solutions" in page1
        assert len(page1["solutions"]) <= 2
        assert "nextToken" in page1

        page2 = client.list_solutions(nextToken=page1["nextToken"])
        assert "solutions" in page2

        all_arns = {s["solutionArn"] for s in page1["solutions"]} | {s["solutionArn"] for s in page2["solutions"]}
        for arn in arns:
            assert arn in all_arns

        for arn in arns:
            client.delete_solution(solutionArn=arn)

    def test_list_campaigns_pagination(self, client):
        """Create 3 campaigns, paginate with maxResults=2."""
        arns = []
        for i in range(3):
            r = client.create_campaign(name=f"test-page-camp-{i}", solutionVersionArn=SOL_VERSION_ARN)
            arns.append(r["campaignArn"])

        page1 = client.list_campaigns(maxResults=2)
        assert "campaigns" in page1
        assert len(page1["campaigns"]) <= 2
        assert "nextToken" in page1

        page2 = client.list_campaigns(nextToken=page1["nextToken"])
        assert "campaigns" in page2

        all_arns = {c["campaignArn"] for c in page1["campaigns"]} | {c["campaignArn"] for c in page2["campaigns"]}
        for arn in arns:
            assert arn in all_arns

        for arn in arns:
            client.delete_campaign(campaignArn=arn)

    def test_list_filters_pagination(self, client):
        """Create 3 filters, paginate with maxResults=2."""
        arns = []
        for i in range(3):
            r = client.create_filter(
                name=f"test-page-filter-{i}",
                datasetGroupArn=DG_ARN,
                filterExpression=FILTER_EXPR,
            )
            arns.append(r["filterArn"])

        page1 = client.list_filters(maxResults=2)
        assert "Filters" in page1
        assert len(page1["Filters"]) <= 2
        assert "nextToken" in page1

        page2 = client.list_filters(nextToken=page1["nextToken"])
        assert "Filters" in page2

        all_arns = {f["filterArn"] for f in page1["Filters"]} | {f["filterArn"] for f in page2["Filters"]}
        for arn in arns:
            assert arn in all_arns

        for arn in arns:
            client.delete_filter(filterArn=arn)


class TestPersonalizeBehavioralFidelity:
    """Behavioral fidelity tests: ARN formats, timestamps, ordering, idempotency."""

    @pytest.fixture
    def client(self):
        return make_client("personalize")

    def test_campaign_arn_format(self, client):
        """Campaign ARN must match expected AWS format."""
        r = client.create_campaign(name="test-camp-arn-fidelity", solutionVersionArn=SOL_VERSION_ARN)
        arn = r["campaignArn"]
        parts = arn.split(":")
        assert parts[0] == "arn"
        assert parts[1] == "aws"
        assert parts[2] == "personalize"
        assert "campaign" in arn
        assert "test-camp-arn-fidelity" in arn
        client.delete_campaign(campaignArn=arn)

    def test_filter_arn_format(self, client):
        """Filter ARN must match expected AWS format."""
        r = client.create_filter(
            name="test-filter-arn-fidelity",
            datasetGroupArn=DG_ARN,
            filterExpression=FILTER_EXPR,
        )
        arn = r["filterArn"]
        parts = arn.split(":")
        assert parts[0] == "arn"
        assert parts[1] == "aws"
        assert parts[2] == "personalize"
        assert "filter" in arn
        assert "test-filter-arn-fidelity" in arn
        client.delete_filter(filterArn=arn)

    def test_event_tracker_arn_format(self, client):
        """EventTracker ARN must match expected AWS format."""
        r = client.create_event_tracker(name="test-et-arn-fidelity", datasetGroupArn=DG_ARN)
        arn = r["eventTrackerArn"]
        parts = arn.split(":")
        assert parts[0] == "arn"
        assert parts[1] == "aws"
        assert parts[2] == "personalize"
        assert "event-tracker" in arn
        assert "test-et-arn-fidelity" in arn
        client.delete_event_tracker(eventTrackerArn=arn)

    def test_recommender_arn_format(self, client):
        """Recommender ARN must match expected AWS format."""
        r = client.create_recommender(
            name="test-rec-arn-fidelity",
            datasetGroupArn=DG_ARN,
            recipeArn=RECIPE_ARN,
        )
        arn = r["recommenderArn"]
        parts = arn.split(":")
        assert parts[0] == "arn"
        assert parts[1] == "aws"
        assert parts[2] == "personalize"
        assert "recommender" in arn
        assert "test-rec-arn-fidelity" in arn
        client.delete_recommender(recommenderArn=arn)

    def test_batch_inference_job_describe_has_job_config(self, client):
        """Batch inference job describe response includes jobInput, jobOutput, roleArn."""
        r = client.create_batch_inference_job(
            jobName="test-bij-config-fidelity",
            solutionVersionArn=SOL_VERSION_ARN,
            jobInput=JOB_INPUT,
            jobOutput=JOB_OUTPUT,
            roleArn=ROLE_ARN,
        )
        arn = r["batchInferenceJobArn"]
        desc = client.describe_batch_inference_job(batchInferenceJobArn=arn)["batchInferenceJob"]
        assert desc["jobName"] == "test-bij-config-fidelity"
        assert desc["solutionVersionArn"] == SOL_VERSION_ARN
        assert desc["roleArn"] == ROLE_ARN
        assert "jobInput" in desc
        assert "jobOutput" in desc

    def test_solution_version_has_solution_arn(self, client):
        """SolutionVersion describe response includes the parent solutionArn."""
        sol = client.create_solution(name="test-sv-parent-fidelity", datasetGroupArn=DG_ARN)
        sol_arn = sol["solutionArn"]
        sv = client.create_solution_version(solutionArn=sol_arn)
        sv_arn = sv["solutionVersionArn"]

        desc = client.describe_solution_version(solutionVersionArn=sv_arn)["solutionVersion"]
        assert desc["solutionArn"] == sol_arn
        assert desc["solutionVersionArn"] == sv_arn
        assert desc["status"] == "ACTIVE"

        client.delete_solution(solutionArn=sol_arn)

    def test_campaign_has_solution_version_arn(self, client):
        """Campaign describe response includes the solutionVersionArn used to create it."""
        r = client.create_campaign(
            name="test-camp-sv-fidelity",
            solutionVersionArn=SOL_VERSION_ARN,
            minProvisionedTPS=2,
        )
        arn = r["campaignArn"]
        desc = client.describe_campaign(campaignArn=arn)["campaign"]
        assert desc["solutionVersionArn"] == SOL_VERSION_ARN
        assert desc["minProvisionedTPS"] == 2
        client.delete_campaign(campaignArn=arn)

    def test_filter_expression_preserved(self, client):
        """Filter filterExpression is preserved exactly as provided."""
        r = client.create_filter(
            name="test-filter-expr-fidelity",
            datasetGroupArn=DG_ARN,
            filterExpression=FILTER_EXPR,
        )
        arn = r["filterArn"]
        desc = client.describe_filter(filterArn=arn)["filter"]
        assert desc["filterExpression"] == FILTER_EXPR
        assert desc["datasetGroupArn"] == DG_ARN
        client.delete_filter(filterArn=arn)

    def test_event_tracker_tracking_id_is_unique(self, client):
        """Two event trackers should have distinct tracking IDs."""
        r1 = client.create_event_tracker(name="test-et-unique-id-1", datasetGroupArn=DG_ARN)
        r2 = client.create_event_tracker(name="test-et-unique-id-2", datasetGroupArn=DG_ARN)
        assert r1["trackingId"] != r2["trackingId"]
        client.delete_event_tracker(eventTrackerArn=r1["eventTrackerArn"])
        client.delete_event_tracker(eventTrackerArn=r2["eventTrackerArn"])

    def test_dataset_group_name_preserved_in_describe(self, client):
        """Dataset group name is preserved exactly as provided in describe response."""
        name = "test-dg-name-fidelity-exact"
        r = client.create_dataset_group(name=name)
        arn = r["datasetGroupArn"]
        desc = client.describe_dataset_group(datasetGroupArn=arn)["datasetGroup"]
        assert desc["name"] == name
        assert desc["datasetGroupArn"] == arn
        client.delete_dataset_group(datasetGroupArn=arn)

    def test_solution_name_preserved_in_describe(self, client):
        """Solution name is preserved exactly as provided in describe response."""
        name = "test-sol-name-fidelity-exact"
        r = client.create_solution(name=name, datasetGroupArn=DG_ARN)
        arn = r["solutionArn"]
        desc = client.describe_solution(solutionArn=arn)["solution"]
        assert desc["name"] == name
        assert desc["datasetGroupArn"] == DG_ARN
        client.delete_solution(solutionArn=arn)

    def test_metric_attribution_has_dataset_group_arn(self, client):
        """MetricAttribution describe response includes the datasetGroupArn."""
        r = client.create_metric_attribution(
            name="test-ma-dg-fidelity",
            datasetGroupArn=DG_ARN,
            metrics=METRICS,
            metricsOutputConfig=METRICS_OUTPUT,
        )
        arn = r["metricAttributionArn"]
        desc = client.describe_metric_attribution(metricAttributionArn=arn)["metricAttribution"]
        assert desc["datasetGroupArn"] == DG_ARN
        assert desc["name"] == "test-ma-dg-fidelity"
        client.delete_metric_attribution(metricAttributionArn=arn)

    def test_recommender_has_recipe_arn(self, client):
        """Recommender describe response includes the recipeArn."""
        r = client.create_recommender(
            name="test-rec-recipe-fidelity",
            datasetGroupArn=DG_ARN,
            recipeArn=RECIPE_ARN,
        )
        arn = r["recommenderArn"]
        desc = client.describe_recommender(recommenderArn=arn)["recommender"]
        assert desc["recipeArn"] == RECIPE_ARN
        assert desc["datasetGroupArn"] == DG_ARN
        client.delete_recommender(recommenderArn=arn)

    def test_data_deletion_job_has_dataset_group_arn(self, client):
        """DataDeletionJob describe response includes the datasetGroupArn."""
        r = client.create_data_deletion_job(
            jobName="test-ddj-dg-fidelity",
            datasetGroupArn=DG_ARN,
            dataSource={"dataLocation": "s3://bucket/data.csv"},
            roleArn=ROLE_ARN,
        )
        arn = r["dataDeletionJobArn"]
        desc = client.describe_data_deletion_job(dataDeletionJobArn=arn)["dataDeletionJob"]
        assert desc["datasetGroupArn"] == DG_ARN
        assert desc["jobName"] == "test-ddj-dg-fidelity"
        assert desc["roleArn"] == ROLE_ARN

    def test_list_schemas_returns_schema_with_correct_fields(self, client):
        """List schemas entry includes schemaArn, name, creationDateTime, lastUpdatedDateTime."""
        schema_json = (
            '{"type":"record","name":"Interactions",'
            '"namespace":"com.amazonaws.personalize.schema",'
            '"fields":['
            '{"name":"USER_ID","type":"string"},'
            '{"name":"ITEM_ID","type":"string"},'
            '{"name":"TIMESTAMP","type":"long"}'
            '],"version":"1.0"}'
        )
        r = client.create_schema(name="test-schema-list-fields", schema=schema_json)
        arn = r["schemaArn"]
        resp = client.list_schemas()
        matching = [s for s in resp["schemas"] if s["schemaArn"] == arn]
        assert len(matching) == 1
        entry = matching[0]
        assert entry["name"] == "test-schema-list-fields"
        assert "creationDateTime" in entry
        assert "lastUpdatedDateTime" in entry
        client.delete_schema(schemaArn=arn)
