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
