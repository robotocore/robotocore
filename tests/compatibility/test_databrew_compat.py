"""Compatibility tests for AWS Glue DataBrew service."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def databrew_client():
    return make_client("databrew")


@pytest.fixture
def unique_name():
    return f"test-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def created_dataset(databrew_client, unique_name):
    """Create a dataset and clean up after the test."""
    databrew_client.create_dataset(
        Name=unique_name,
        Input={"S3InputDefinition": {"Bucket": "my-bucket", "Key": "data.csv"}},
        FormatOptions={"Csv": {"Delimiter": ","}},
    )
    yield unique_name
    try:
        databrew_client.delete_dataset(Name=unique_name)
    except Exception:
        pass  # best-effort cleanup


@pytest.fixture
def created_recipe(databrew_client):
    """Create a recipe and clean up after the test."""
    name = f"recipe-{uuid.uuid4().hex[:8]}"
    databrew_client.create_recipe(
        Name=name,
        Steps=[
            {
                "Action": {
                    "Operation": "UPPER_CASE",
                    "Parameters": {"sourceColumn": "col1"},
                }
            }
        ],
    )
    yield name
    # No reliable delete for unpublished recipes; leave for server reset


class TestDataBrewDatasets:
    def test_list_datasets(self, databrew_client):
        resp = databrew_client.list_datasets()
        assert "Datasets" in resp
        assert isinstance(resp["Datasets"], list)

    def test_create_dataset(self, databrew_client, unique_name):
        resp = databrew_client.create_dataset(
            Name=unique_name,
            Input={"S3InputDefinition": {"Bucket": "my-bucket", "Key": "data.csv"}},
            FormatOptions={"Csv": {"Delimiter": ","}},
        )
        assert resp["Name"] == unique_name
        # Cleanup
        databrew_client.delete_dataset(Name=unique_name)

    def test_describe_dataset(self, databrew_client, created_dataset):
        resp = databrew_client.describe_dataset(Name=created_dataset)
        assert resp["Name"] == created_dataset
        assert "Input" in resp

    def test_delete_dataset(self, databrew_client):
        name = f"test-{uuid.uuid4().hex[:8]}"
        databrew_client.create_dataset(
            Name=name,
            Input={"S3InputDefinition": {"Bucket": "my-bucket", "Key": "data.csv"}},
            FormatOptions={"Csv": {"Delimiter": ","}},
        )
        resp = databrew_client.delete_dataset(Name=name)
        assert resp["Name"] == name
        # Verify it's gone
        with pytest.raises(Exception):
            databrew_client.describe_dataset(Name=name)

    def test_created_dataset_appears_in_list(self, databrew_client, created_dataset):
        resp = databrew_client.list_datasets()
        names = [d["Name"] for d in resp["Datasets"]]
        assert created_dataset in names


class TestDataBrewRecipes:
    def test_list_recipes(self, databrew_client):
        resp = databrew_client.list_recipes()
        assert "Recipes" in resp
        assert isinstance(resp["Recipes"], list)

    def test_create_recipe(self, databrew_client):
        name = f"recipe-{uuid.uuid4().hex[:8]}"
        resp = databrew_client.create_recipe(
            Name=name,
            Steps=[
                {
                    "Action": {
                        "Operation": "UPPER_CASE",
                        "Parameters": {"sourceColumn": "col1"},
                    }
                }
            ],
        )
        assert resp["Name"] == name

    def test_describe_recipe(self, databrew_client, created_recipe):
        resp = databrew_client.describe_recipe(Name=created_recipe, RecipeVersion="LATEST_WORKING")
        assert resp["Name"] == created_recipe
        assert "Steps" in resp


@pytest.fixture
def created_ruleset(databrew_client):
    """Create a ruleset and clean up after the test."""
    name = f"ruleset-{uuid.uuid4().hex[:8]}"
    databrew_client.create_ruleset(
        Name=name,
        Description="test ruleset",
        TargetArn="arn:aws:databrew:us-east-1:123456789012:dataset/dummy",
        Rules=[
            {
                "Name": "rule1",
                "Disabled": False,
                "CheckExpression": "IS_NOT_NULL(:col)",
                "SubstitutionMap": {":col": "col1"},
            }
        ],
    )
    yield name
    try:
        databrew_client.delete_ruleset(Name=name)
    except Exception:
        pass  # best-effort cleanup


class TestDataBrewJobs:
    def test_list_jobs(self, databrew_client):
        resp = databrew_client.list_jobs()
        assert "Jobs" in resp
        assert isinstance(resp["Jobs"], list)


class TestDataBrewJobOps:
    def test_describe_job(self, databrew_client):
        name = f"job-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe_job(
            Name=name,
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        try:
            resp = databrew_client.describe_job(Name=name)
            assert resp["Name"] == name
        finally:
            databrew_client.delete_job(Name=name)

    def test_delete_job(self, databrew_client):
        name = f"job-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe_job(
            Name=name,
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        resp = databrew_client.delete_job(Name=name)
        assert resp["Name"] == name
        with pytest.raises(Exception):
            databrew_client.describe_job(Name=name)


class TestDataBrewProfileJob:
    def test_create_profile_job(self, databrew_client, created_dataset):
        name = f"pjob-{uuid.uuid4().hex[:8]}"
        resp = databrew_client.create_profile_job(
            DatasetName=created_dataset,
            Name=name,
            OutputLocation={"Bucket": "my-bucket"},
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        assert resp["Name"] == name
        databrew_client.delete_job(Name=name)

    def test_update_profile_job(self, databrew_client, created_dataset):
        name = f"pjob-{uuid.uuid4().hex[:8]}"
        databrew_client.create_profile_job(
            DatasetName=created_dataset,
            Name=name,
            OutputLocation={"Bucket": "my-bucket"},
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        try:
            resp = databrew_client.update_profile_job(
                Name=name,
                OutputLocation={"Bucket": "my-other-bucket"},
                RoleArn="arn:aws:iam::123456789012:role/test-role",
            )
            assert resp["Name"] == name
        finally:
            databrew_client.delete_job(Name=name)


class TestDataBrewRecipeJob:
    def test_create_recipe_job(self, databrew_client):
        name = f"rjob-{uuid.uuid4().hex[:8]}"
        resp = databrew_client.create_recipe_job(
            Name=name,
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        assert resp["Name"] == name
        databrew_client.delete_job(Name=name)

    def test_update_recipe_job(self, databrew_client):
        name = f"rjob-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe_job(
            Name=name,
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        try:
            resp = databrew_client.update_recipe_job(
                Name=name,
                RoleArn="arn:aws:iam::123456789012:role/updated-role",
            )
            assert resp["Name"] == name
        finally:
            databrew_client.delete_job(Name=name)


class TestDataBrewRecipeOps:
    def test_publish_recipe(self, databrew_client, created_recipe):
        resp = databrew_client.publish_recipe(Name=created_recipe)
        assert resp["Name"] == created_recipe

    def test_update_recipe(self, databrew_client, created_recipe):
        resp = databrew_client.update_recipe(
            Name=created_recipe,
            Steps=[
                {
                    "Action": {
                        "Operation": "LOWER_CASE",
                        "Parameters": {"sourceColumn": "col1"},
                    }
                }
            ],
        )
        assert resp["Name"] == created_recipe

    def test_list_recipe_versions(self, databrew_client, created_recipe):
        # Publish to create a version
        databrew_client.publish_recipe(Name=created_recipe)
        resp = databrew_client.list_recipe_versions(Name=created_recipe)
        assert "Recipes" in resp

    def test_delete_recipe_version(self, databrew_client):
        name = f"recipe-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe(
            Name=name,
            Steps=[
                {
                    "Action": {
                        "Operation": "UPPER_CASE",
                        "Parameters": {"sourceColumn": "col1"},
                    }
                }
            ],
        )
        databrew_client.publish_recipe(Name=name)
        resp = databrew_client.delete_recipe_version(Name=name, RecipeVersion="1.0")
        assert resp["Name"] == name

    def test_delete_recipe_latest_working(self, databrew_client):
        name = f"recipe-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe(
            Name=name,
            Steps=[
                {
                    "Action": {
                        "Operation": "UPPER_CASE",
                        "Parameters": {"sourceColumn": "col1"},
                    }
                }
            ],
        )
        resp = databrew_client.delete_recipe_version(Name=name, RecipeVersion="LATEST_WORKING")
        assert resp["Name"] == name
        assert resp["RecipeVersion"] == "LATEST_WORKING"


class TestDataBrewRuleset:
    def test_describe_ruleset(self, databrew_client, created_ruleset):
        resp = databrew_client.describe_ruleset(Name=created_ruleset)
        assert resp["Name"] == created_ruleset
        assert "Rules" in resp

    def test_delete_ruleset(self, databrew_client):
        name = f"ruleset-{uuid.uuid4().hex[:8]}"
        databrew_client.create_ruleset(
            Name=name,
            Description="test",
            TargetArn="arn:aws:databrew:us-east-1:123456789012:dataset/dummy",
            Rules=[
                {
                    "Name": "rule1",
                    "Disabled": False,
                    "CheckExpression": "IS_NOT_NULL(:col)",
                    "SubstitutionMap": {":col": "col1"},
                }
            ],
        )
        resp = databrew_client.delete_ruleset(Name=name)
        assert resp["Name"] == name
        with pytest.raises(Exception):
            databrew_client.describe_ruleset(Name=name)


class TestDataBrewDatasetOps:
    def test_update_dataset(self, databrew_client, created_dataset):
        resp = databrew_client.update_dataset(
            Name=created_dataset,
            Input={"S3InputDefinition": {"Bucket": "other-bucket", "Key": "other.csv"}},
        )
        assert resp["Name"] == created_dataset


class TestDataBrewUpdateRuleset:
    def test_update_ruleset(self, databrew_client, created_ruleset):
        resp = databrew_client.update_ruleset(
            Name=created_ruleset,
            Rules=[
                {
                    "Name": "updated-rule",
                    "Disabled": False,
                    "CheckExpression": "IS_NOT_NULL(:col)",
                    "SubstitutionMap": {":col": "col2"},
                }
            ],
        )
        assert resp["Name"] == created_ruleset

    def test_update_ruleset_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ClientError) as exc_info:
            databrew_client.update_ruleset(
                Name="nonexistent-ruleset",
                Rules=[
                    {
                        "Name": "rule1",
                        "Disabled": False,
                        "CheckExpression": "IS_NOT_NULL(:col)",
                        "SubstitutionMap": {":col": "col1"},
                    }
                ],
            )
        assert exc_info.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestDataBrewCreateRuleset:
    def test_create_ruleset(self, databrew_client):
        name = f"ruleset-{uuid.uuid4().hex[:8]}"
        resp = databrew_client.create_ruleset(
            Name=name,
            Description="test",
            TargetArn="arn:aws:databrew:us-east-1:123456789012:dataset/dummy",
            Rules=[
                {
                    "Name": "rule1",
                    "Disabled": False,
                    "CheckExpression": "IS_NOT_NULL(:col)",
                    "SubstitutionMap": {":col": "col1"},
                }
            ],
        )
        assert resp["Name"] == name
        databrew_client.delete_ruleset(Name=name)


class TestDataBrewNotFound:
    """Tests that operations raise errors for nonexistent resources."""

    def test_describe_dataset_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.describe_dataset(Name="nonexistent-dataset")

    def test_describe_job_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.describe_job(Name="nonexistent-job")

    def test_describe_ruleset_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ClientError) as exc_info:
            databrew_client.describe_ruleset(Name="nonexistent-ruleset")
        assert exc_info.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_delete_dataset_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.delete_dataset(Name="nonexistent-dataset")

    def test_delete_job_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.delete_job(Name="nonexistent-job")

    def test_delete_ruleset_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ClientError) as exc_info:
            databrew_client.delete_ruleset(Name="nonexistent-ruleset")
        assert exc_info.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_update_dataset_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.update_dataset(
                Name="nonexistent-dataset",
                Input={"S3InputDefinition": {"Bucket": "my-bucket", "Key": "data.csv"}},
            )

    def test_update_recipe_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.update_recipe(
                Name="nonexistent-recipe",
                Steps=[
                    {
                        "Action": {
                            "Operation": "UPPER_CASE",
                            "Parameters": {"sourceColumn": "col1"},
                        }
                    }
                ],
            )

    def test_update_recipe_job_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.update_recipe_job(
                Name="nonexistent-recipe-job",
                RoleArn="arn:aws:iam::123456789012:role/test-role",
            )

    def test_update_profile_job_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.update_profile_job(
                Name="nonexistent-profile-job",
                OutputLocation={"Bucket": "my-bucket"},
                RoleArn="arn:aws:iam::123456789012:role/test-role",
            )

    def test_delete_recipe_version_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.delete_recipe_version(Name="nonexistent-recipe", RecipeVersion="1.0")

    def test_publish_recipe_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.publish_recipe(Name="nonexistent-recipe")


class TestDatabrewAutoCoverage:
    """Auto-generated coverage tests for databrew."""

    @pytest.fixture
    def client(self):
        return make_client("databrew")

    def test_list_rulesets(self, client):
        """ListRulesets returns a response."""
        resp = client.list_rulesets()
        assert "Rulesets" in resp
