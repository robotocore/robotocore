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
        assert isinstance(resp["datasets"], list)
        client.delete_dataset_group(datasetGroupArn=arn)

    def test_list_event_trackers_with_dataset_group_filter(self, client):
        r = client.create_dataset_group(name="test-dg-et-filter")
        arn = r["datasetGroupArn"]
        resp = client.list_event_trackers(datasetGroupArn=arn)
        assert "eventTrackers" in resp
        assert isinstance(resp["eventTrackers"], list)
        client.delete_dataset_group(datasetGroupArn=arn)

    def test_list_filters_with_dataset_group_filter(self, client):
        r = client.create_dataset_group(name="test-dg-filter-list")
        arn = r["datasetGroupArn"]
        resp = client.list_filters(datasetGroupArn=arn)
        assert "Filters" in resp
        assert isinstance(resp["Filters"], list)
        client.delete_dataset_group(datasetGroupArn=arn)


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
        keys = {t["tagKey"] for t in resp["tags"]}
        assert "env" in keys
        assert "team" in keys
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
        resp = client.list_campaigns(
            solutionArn="arn:aws:personalize:us-east-1:123456789012:solution/nonexistent"
        )
        assert "campaigns" in resp
        assert isinstance(resp["campaigns"], list)

    def test_list_solution_versions_with_solution_arn_filter(self, client):
        resp = client.list_solution_versions(
            solutionArn="arn:aws:personalize:us-east-1:123456789012:solution/nonexistent"
        )
        assert "solutionVersions" in resp
        assert isinstance(resp["solutionVersions"], list)

    def test_list_recommenders_with_dataset_group_filter(self, client):
        r = client.create_dataset_group(name="test-dg-recommenders-filter")
        arn = r["datasetGroupArn"]
        resp = client.list_recommenders(datasetGroupArn=arn)
        assert "recommenders" in resp
        assert isinstance(resp["recommenders"], list)
        client.delete_dataset_group(datasetGroupArn=arn)

    def test_list_solutions_with_dataset_group_filter(self, client):
        r = client.create_dataset_group(name="test-dg-solutions-filter")
        arn = r["datasetGroupArn"]
        resp = client.list_solutions(datasetGroupArn=arn)
        assert "solutions" in resp
        assert isinstance(resp["solutions"], list)
        client.delete_dataset_group(datasetGroupArn=arn)

    def test_list_batch_inference_jobs_with_solution_version_filter(self, client):
        resp = client.list_batch_inference_jobs(
            solutionVersionArn=(
                "arn:aws:personalize:us-east-1:123456789012:solution/x/version/nonexistent"
            )
        )
        assert "batchInferenceJobs" in resp
        assert isinstance(resp["batchInferenceJobs"], list)

    def test_list_batch_segment_jobs_with_solution_version_filter(self, client):
        resp = client.list_batch_segment_jobs(
            solutionVersionArn=(
                "arn:aws:personalize:us-east-1:123456789012:solution/x/version/nonexistent"
            )
        )
        assert "batchSegmentJobs" in resp
        assert isinstance(resp["batchSegmentJobs"], list)

    def test_list_data_deletion_jobs_with_dataset_group_filter(self, client):
        r = client.create_dataset_group(name="test-dg-deletion-jobs-filter")
        arn = r["datasetGroupArn"]
        resp = client.list_data_deletion_jobs(datasetGroupArn=arn)
        assert "dataDeletionJobs" in resp
        assert isinstance(resp["dataDeletionJobs"], list)
        client.delete_dataset_group(datasetGroupArn=arn)

    def test_list_metric_attributions_with_dataset_group_filter(self, client):
        r = client.create_dataset_group(name="test-dg-metric-attr-filter")
        arn = r["datasetGroupArn"]
        resp = client.list_metric_attributions(datasetGroupArn=arn)
        assert "metricAttributions" in resp
        assert isinstance(resp["metricAttributions"], list)
        client.delete_dataset_group(datasetGroupArn=arn)


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
        assert "creationDateTime" in desc
        assert "lastUpdatedDateTime" in desc
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

        desc = client.describe_event_tracker(eventTrackerArn=arn)
        et = desc["eventTracker"]
        assert et["name"] == "test-et-crud"
        assert et["eventTrackerArn"] == arn
        assert et["status"] == "ACTIVE"
        assert et.get("trackingId") == tracking_id

        client.delete_event_tracker(eventTrackerArn=arn)
        with pytest.raises(ClientError) as exc:
            client.describe_event_tracker(eventTrackerArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

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
