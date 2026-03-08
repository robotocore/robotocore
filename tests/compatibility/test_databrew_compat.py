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
        pass


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


class TestDataBrewJobs:
    def test_list_jobs(self, databrew_client):
        resp = databrew_client.list_jobs()
        assert "Jobs" in resp
        assert isinstance(resp["Jobs"], list)


class TestDatabrewAutoCoverage:
    """Auto-generated coverage tests for databrew."""

    @pytest.fixture
    def client(self):
        return make_client("databrew")

    def test_list_rulesets(self, client):
        """ListRulesets returns a response."""
        resp = client.list_rulesets()
        assert "Rulesets" in resp
