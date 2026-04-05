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


class TestDataBrewDatasetEdgeCases:
    def test_describe_dataset_has_arn(self, databrew_client, created_dataset):
        resp = databrew_client.describe_dataset(Name=created_dataset)
        assert "ResourceArn" in resp
        assert "databrew" in resp["ResourceArn"]
        assert created_dataset in resp["ResourceArn"]

    def test_describe_dataset_has_timestamps(self, databrew_client, created_dataset):
        import datetime
        resp = databrew_client.describe_dataset(Name=created_dataset)
        assert "CreateDate" in resp
        assert isinstance(resp["CreateDate"], datetime.datetime)

    def test_create_dataset_duplicate_raises(self, databrew_client, created_dataset):
        from botocore.exceptions import ClientError
        with pytest.raises(ClientError) as exc_info:
            databrew_client.create_dataset(
                Name=created_dataset,
                Input={"S3InputDefinition": {"Bucket": "my-bucket", "Key": "data.csv"}},
            )
        assert exc_info.value.response["Error"]["Code"] in ("AlreadyExistsException", "ConflictException")

    def test_dataset_list_pagination(self, databrew_client):
        names = [f"pds-{uuid.uuid4().hex[:8]}" for _ in range(3)]
        for name in names:
            databrew_client.create_dataset(
                Name=name,
                Input={"S3InputDefinition": {"Bucket": "my-bucket", "Key": "data.csv"}},
            )
        try:
            resp = databrew_client.list_datasets(MaxResults=2)
            assert len(resp["Datasets"]) <= 2
            if "NextToken" in resp:
                resp2 = databrew_client.list_datasets(MaxResults=2, NextToken=resp["NextToken"])
                assert "Datasets" in resp2
        finally:
            for name in names:
                try:
                    databrew_client.delete_dataset(Name=name)
                except Exception:
                    pass

    def test_update_dataset_input_persists(self, databrew_client, created_dataset):
        databrew_client.update_dataset(
            Name=created_dataset,
            Input={"S3InputDefinition": {"Bucket": "updated-bucket", "Key": "updated.csv"}},
        )
        resp = databrew_client.describe_dataset(Name=created_dataset)
        assert resp["Input"]["S3InputDefinition"]["Bucket"] == "updated-bucket"

    def test_created_dataset_has_correct_input(self, databrew_client):
        name = f"ds-{uuid.uuid4().hex[:8]}"
        databrew_client.create_dataset(
            Name=name,
            Input={"S3InputDefinition": {"Bucket": "specific-bucket", "Key": "specific.csv"}},
            FormatOptions={"Csv": {"Delimiter": "|"}},
        )
        try:
            resp = databrew_client.describe_dataset(Name=name)
            assert resp["Input"]["S3InputDefinition"]["Bucket"] == "specific-bucket"
            assert resp["Input"]["S3InputDefinition"]["Key"] == "specific.csv"
        finally:
            databrew_client.delete_dataset(Name=name)


class TestDataBrewRecipeEdgeCases:
    def test_create_recipe_duplicate_raises(self, databrew_client, created_recipe):
        from botocore.exceptions import ClientError
        with pytest.raises(ClientError) as exc_info:
            databrew_client.create_recipe(
                Name=created_recipe,
                Steps=[
                    {
                        "Action": {
                            "Operation": "UPPER_CASE",
                            "Parameters": {"sourceColumn": "col1"},
                        }
                    }
                ],
            )
        assert exc_info.value.response["Error"]["Code"] in ("AlreadyExistsException", "ConflictException")

    def test_describe_recipe_steps_content(self, databrew_client, created_recipe):
        resp = databrew_client.describe_recipe(Name=created_recipe, RecipeVersion="LATEST_WORKING")
        assert len(resp["Steps"]) == 1
        assert resp["Steps"][0]["Action"]["Operation"] == "UPPER_CASE"

    def test_update_recipe_steps_persist(self, databrew_client, created_recipe):
        databrew_client.update_recipe(
            Name=created_recipe,
            Steps=[
                {
                    "Action": {
                        "Operation": "LOWER_CASE",
                        "Parameters": {"sourceColumn": "col2"},
                    }
                }
            ],
        )
        resp = databrew_client.describe_recipe(Name=created_recipe, RecipeVersion="LATEST_WORKING")
        assert resp["Steps"][0]["Action"]["Operation"] == "LOWER_CASE"

    def test_list_recipes_working_contains_created(self, databrew_client, created_recipe):
        # LATEST_WORKING includes unpublished recipes
        resp = databrew_client.list_recipes(RecipeVersion="LATEST_WORKING")
        names = [r["Name"] for r in resp["Recipes"]]
        assert created_recipe in names

    def test_list_recipe_versions_has_version_numbers(self, databrew_client, created_recipe):
        databrew_client.publish_recipe(Name=created_recipe)
        resp = databrew_client.list_recipe_versions(Name=created_recipe)
        assert "Recipes" in resp
        assert len(resp["Recipes"]) >= 1
        versions = [r.get("RecipeVersion") for r in resp["Recipes"]]
        assert any(v is not None for v in versions)

    def test_publish_recipe_creates_numbered_version(self, databrew_client, created_recipe):
        databrew_client.publish_recipe(Name=created_recipe)
        resp = databrew_client.list_recipe_versions(Name=created_recipe)
        version_numbers = [r.get("RecipeVersion") for r in resp["Recipes"] if r.get("RecipeVersion") != "LATEST_WORKING"]
        assert len(version_numbers) >= 1
        assert "1.0" in version_numbers


class TestDataBrewRulesetEdgeCases:
    def test_describe_ruleset_has_target_arn(self, databrew_client, created_ruleset):
        resp = databrew_client.describe_ruleset(Name=created_ruleset)
        assert "TargetArn" in resp
        assert "databrew" in resp["TargetArn"]

    def test_describe_ruleset_has_rules_content(self, databrew_client, created_ruleset):
        resp = databrew_client.describe_ruleset(Name=created_ruleset)
        assert len(resp["Rules"]) >= 1
        assert resp["Rules"][0]["Name"] == "rule1"
        assert "CheckExpression" in resp["Rules"][0]

    def test_update_ruleset_rules_persist(self, databrew_client, created_ruleset):
        databrew_client.update_ruleset(
            Name=created_ruleset,
            Rules=[
                {
                    "Name": "new-rule",
                    "Disabled": False,
                    "CheckExpression": "IS_NOT_NULL(:col)",
                    "SubstitutionMap": {":col": "col3"},
                }
            ],
        )
        resp = databrew_client.describe_ruleset(Name=created_ruleset)
        assert resp["Rules"][0]["Name"] == "new-rule"

    def test_list_rulesets_contains_created(self, databrew_client, created_ruleset):
        resp = databrew_client.list_rulesets()
        names = [r["Name"] for r in resp["Rulesets"]]
        assert created_ruleset in names

    def test_list_rulesets_pagination(self, databrew_client):
        names = [f"rs-{uuid.uuid4().hex[:8]}" for _ in range(3)]
        for name in names:
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
        try:
            resp = databrew_client.list_rulesets(MaxResults=2)
            assert len(resp["Rulesets"]) <= 2
            if "NextToken" in resp:
                resp2 = databrew_client.list_rulesets(MaxResults=2, NextToken=resp["NextToken"])
                assert "Rulesets" in resp2
        finally:
            for name in names:
                try:
                    databrew_client.delete_ruleset(Name=name)
                except Exception:
                    pass

    def test_create_ruleset_duplicate_raises(self, databrew_client, created_ruleset):
        with pytest.raises(databrew_client.exceptions.ClientError) as exc_info:
            databrew_client.create_ruleset(
                Name=created_ruleset,
                Description="duplicate",
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
        assert exc_info.value.response["Error"]["Code"] in ("AlreadyExistsException", "ConflictException")


class TestDataBrewJobEdgeCases:
    def test_list_jobs_pagination(self, databrew_client):
        names = [f"job-{uuid.uuid4().hex[:8]}" for _ in range(3)]
        for name in names:
            databrew_client.create_recipe_job(
                Name=name,
                RoleArn="arn:aws:iam::123456789012:role/test-role",
            )
        try:
            resp = databrew_client.list_jobs(MaxResults=2)
            assert len(resp["Jobs"]) <= 2
            if "NextToken" in resp:
                resp2 = databrew_client.list_jobs(MaxResults=2, NextToken=resp["NextToken"])
                assert "Jobs" in resp2
        finally:
            for name in names:
                try:
                    databrew_client.delete_job(Name=name)
                except Exception:
                    pass

    def test_create_profile_job_has_arn(self, databrew_client, created_dataset):
        name = f"pjob-{uuid.uuid4().hex[:8]}"
        databrew_client.create_profile_job(
            DatasetName=created_dataset,
            Name=name,
            OutputLocation={"Bucket": "my-bucket"},
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        try:
            resp = databrew_client.describe_job(Name=name)
            assert "ResourceArn" in resp
            assert "databrew" in resp["ResourceArn"]
        finally:
            databrew_client.delete_job(Name=name)

    def test_create_profile_job_type_is_profile(self, databrew_client, created_dataset):
        name = f"pjob-{uuid.uuid4().hex[:8]}"
        databrew_client.create_profile_job(
            DatasetName=created_dataset,
            Name=name,
            OutputLocation={"Bucket": "my-bucket"},
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        try:
            resp = databrew_client.describe_job(Name=name)
            assert resp["Type"] == "PROFILE"
        finally:
            databrew_client.delete_job(Name=name)

    def test_create_recipe_job_type_is_recipe(self, databrew_client):
        name = f"rjob-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe_job(
            Name=name,
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        try:
            resp = databrew_client.describe_job(Name=name)
            assert resp["Type"] == "RECIPE"
        finally:
            databrew_client.delete_job(Name=name)

    def test_job_appears_in_list(self, databrew_client):
        name = f"rjob-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe_job(
            Name=name,
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        try:
            resp = databrew_client.list_jobs()
            job_names = [j["Name"] for j in resp["Jobs"]]
            assert name in job_names
        finally:
            databrew_client.delete_job(Name=name)


class TestDataBrewTagsOnCreate:
    """Tags specified at creation time persist and are retrievable."""

    def test_create_dataset_with_tags_preserved(self, databrew_client):
        name = f"ds-{uuid.uuid4().hex[:8]}"
        databrew_client.create_dataset(
            Name=name,
            Input={"S3InputDefinition": {"Bucket": "my-bucket", "Key": "data.csv"}},
            Tags={"env": "staging", "team": "data"},
        )
        try:
            resp = databrew_client.describe_dataset(Name=name)
            assert resp["Tags"]["env"] == "staging"
            assert resp["Tags"]["team"] == "data"
        finally:
            databrew_client.delete_dataset(Name=name)

    def test_create_recipe_job_with_tags_preserved(self, databrew_client):
        name = f"rjob-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe_job(
            Name=name,
            RoleArn="arn:aws:iam::123456789012:role/test-role",
            Tags={"env": "test", "version": "1"},
        )
        try:
            resp = databrew_client.describe_job(Name=name)
            assert resp["Tags"]["env"] == "test"
            assert resp["Tags"]["version"] == "1"
        finally:
            databrew_client.delete_job(Name=name)

    def test_create_ruleset_with_tags_preserved(self, databrew_client):
        name = f"rs-{uuid.uuid4().hex[:8]}"
        databrew_client.create_ruleset(
            Name=name,
            Description="tagged ruleset",
            TargetArn="arn:aws:databrew:us-east-1:123456789012:dataset/dummy",
            Rules=[
                {
                    "Name": "rule1",
                    "Disabled": False,
                    "CheckExpression": "IS_NOT_NULL(:col)",
                    "SubstitutionMap": {":col": "col1"},
                }
            ],
            Tags={"env": "prod", "criticality": "high"},
        )
        try:
            resp = databrew_client.describe_ruleset(Name=name)
            assert resp["Tags"]["env"] == "prod"
            assert resp["Tags"]["criticality"] == "high"
        finally:
            databrew_client.delete_ruleset(Name=name)

    def test_create_profile_job_with_tags_preserved(self, databrew_client, created_dataset):
        name = f"pjob-{uuid.uuid4().hex[:8]}"
        databrew_client.create_profile_job(
            DatasetName=created_dataset,
            Name=name,
            OutputLocation={"Bucket": "my-bucket"},
            RoleArn="arn:aws:iam::123456789012:role/test-role",
            Tags={"project": "analytics"},
        )
        try:
            resp = databrew_client.describe_job(Name=name)
            assert resp["Tags"]["project"] == "analytics"
        finally:
            databrew_client.delete_job(Name=name)


class TestDataBrewJobBehavior:
    """Behavioral fidelity tests for DataBrew jobs."""

    def test_recipe_job_has_arn(self, databrew_client):
        name = f"rjob-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe_job(
            Name=name,
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        try:
            resp = databrew_client.describe_job(Name=name)
            assert "ResourceArn" in resp
            assert "databrew" in resp["ResourceArn"]
            assert name in resp["ResourceArn"]
        finally:
            databrew_client.delete_job(Name=name)

    def test_job_has_createdate(self, databrew_client):
        import datetime
        name = f"rjob-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe_job(
            Name=name,
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        try:
            resp = databrew_client.describe_job(Name=name)
            assert "CreateDate" in resp
            assert isinstance(resp["CreateDate"], datetime.datetime)
        finally:
            databrew_client.delete_job(Name=name)

    def test_job_role_arn_persists(self, databrew_client):
        name = f"rjob-{uuid.uuid4().hex[:8]}"
        role = "arn:aws:iam::123456789012:role/my-specific-role"
        databrew_client.create_recipe_job(Name=name, RoleArn=role)
        try:
            resp = databrew_client.describe_job(Name=name)
            assert resp["RoleArn"] == role
        finally:
            databrew_client.delete_job(Name=name)

    def test_update_recipe_job_role_persists(self, databrew_client):
        name = f"rjob-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe_job(
            Name=name,
            RoleArn="arn:aws:iam::123456789012:role/original-role",
        )
        try:
            new_role = "arn:aws:iam::123456789012:role/updated-role"
            databrew_client.update_recipe_job(Name=name, RoleArn=new_role)
            resp = databrew_client.describe_job(Name=name)
            assert resp["RoleArn"] == new_role
        finally:
            databrew_client.delete_job(Name=name)

    def test_create_duplicate_job_raises(self, databrew_client):
        name = f"rjob-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe_job(
            Name=name,
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        try:
            with pytest.raises(databrew_client.exceptions.ClientError) as exc_info:
                databrew_client.create_recipe_job(
                    Name=name,
                    RoleArn="arn:aws:iam::123456789012:role/test-role",
                )
            assert exc_info.value.response["Error"]["Code"] in (
                "ConflictException",
                "AlreadyExistsException",
            )
        finally:
            databrew_client.delete_job(Name=name)

    def test_list_jobs_filter_by_dataset(self, databrew_client, created_dataset):
        name = f"pjob-{uuid.uuid4().hex[:8]}"
        databrew_client.create_profile_job(
            DatasetName=created_dataset,
            Name=name,
            OutputLocation={"Bucket": "my-bucket"},
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        try:
            resp = databrew_client.list_jobs(DatasetName=created_dataset)
            job_names = [j["Name"] for j in resp["Jobs"]]
            assert name in job_names
        finally:
            databrew_client.delete_job(Name=name)


class TestDataBrewDatasetBehavioral:
    """Behavioral fidelity tests for DataBrew datasets."""

    def test_list_datasets_returns_created_order(self, databrew_client):
        names = [f"ord-{uuid.uuid4().hex[:6]}" for _ in range(3)]
        for n in names:
            databrew_client.create_dataset(
                Name=n,
                Input={"S3InputDefinition": {"Bucket": "my-bucket", "Key": "data.csv"}},
            )
        try:
            resp = databrew_client.list_datasets()
            listed = [d["Name"] for d in resp["Datasets"] if d["Name"] in set(names)]
            assert listed == names
        finally:
            for n in names:
                try:
                    databrew_client.delete_dataset(Name=n)
                except Exception:
                    pass

    def test_describe_dataset_has_createdate(self, databrew_client, created_dataset):
        import datetime
        resp = databrew_client.describe_dataset(Name=created_dataset)
        assert "CreateDate" in resp
        assert isinstance(resp["CreateDate"], datetime.datetime)

    def test_dataset_arn_format(self, databrew_client, created_dataset):
        resp = databrew_client.describe_dataset(Name=created_dataset)
        arn = resp["ResourceArn"]
        parts = arn.split(":")
        assert parts[0] == "arn"
        assert parts[2] == "databrew"
        assert created_dataset in arn

    def test_create_dataset_with_pipe_delimiter(self, databrew_client):
        name = f"ds-{uuid.uuid4().hex[:8]}"
        databrew_client.create_dataset(
            Name=name,
            Input={"S3InputDefinition": {"Bucket": "my-bucket", "Key": "data.csv"}},
            FormatOptions={"Csv": {"Delimiter": "|"}},
        )
        try:
            resp = databrew_client.describe_dataset(Name=name)
            assert resp["FormatOptions"]["Csv"]["Delimiter"] == "|"
        finally:
            databrew_client.delete_dataset(Name=name)

    def test_delete_dataset_removes_from_list(self, databrew_client):
        name = f"ds-{uuid.uuid4().hex[:8]}"
        databrew_client.create_dataset(
            Name=name,
            Input={"S3InputDefinition": {"Bucket": "my-bucket", "Key": "data.csv"}},
        )
        databrew_client.delete_dataset(Name=name)
        resp = databrew_client.list_datasets()
        names = [d["Name"] for d in resp["Datasets"]]
        assert name not in names


class TestDataBrewRecipeBehavioral:
    """Behavioral fidelity tests for DataBrew recipes."""

    def test_describe_recipe_has_createdate(self, databrew_client, created_recipe):
        import datetime
        resp = databrew_client.describe_recipe(Name=created_recipe, RecipeVersion="LATEST_WORKING")
        assert "CreateDate" in resp
        assert isinstance(resp["CreateDate"], datetime.datetime)

    def test_list_recipes_shows_version_number(self, databrew_client, created_recipe):
        resp = databrew_client.list_recipes(RecipeVersion="LATEST_WORKING")
        recipes = {r["Name"]: r for r in resp["Recipes"]}
        assert created_recipe in recipes
        assert "RecipeVersion" in recipes[created_recipe]

    def test_publish_recipe_increments_version(self, databrew_client):
        name = f"recipe-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe(
            Name=name,
            Steps=[{"Action": {"Operation": "UPPER_CASE", "Parameters": {"sourceColumn": "col1"}}}],
        )
        databrew_client.publish_recipe(Name=name)
        databrew_client.update_recipe(
            Name=name,
            Steps=[{"Action": {"Operation": "LOWER_CASE", "Parameters": {"sourceColumn": "col1"}}}],
        )
        databrew_client.publish_recipe(Name=name)
        resp = databrew_client.list_recipe_versions(Name=name)
        versions = [r["RecipeVersion"] for r in resp["Recipes"] if r.get("RecipeVersion") not in (None, "LATEST_WORKING")]
        assert "1.0" in versions
        assert "2.0" in versions

    def test_describe_recipe_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.describe_recipe(Name="no-such-recipe", RecipeVersion="LATEST_WORKING")

    def test_list_recipes_reflects_deletion(self, databrew_client):
        name = f"recipe-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe(
            Name=name,
            Steps=[{"Action": {"Operation": "UPPER_CASE", "Parameters": {"sourceColumn": "c"}}}],
        )
        resp = databrew_client.list_recipes(RecipeVersion="LATEST_WORKING")
        assert name in [r["Name"] for r in resp["Recipes"]]
        databrew_client.delete_recipe_version(Name=name, RecipeVersion="LATEST_WORKING")
        resp = databrew_client.list_recipes(RecipeVersion="LATEST_WORKING")
        assert name not in [r["Name"] for r in resp["Recipes"]]


class TestDataBrewRulesetBehavioral:
    """Behavioral fidelity tests for DataBrew rulesets."""

    def test_describe_ruleset_has_createdate(self, databrew_client, created_ruleset):
        import datetime
        resp = databrew_client.describe_ruleset(Name=created_ruleset)
        assert "CreateDate" in resp
        assert isinstance(resp["CreateDate"], datetime.datetime)

    def test_describe_ruleset_shows_description(self, databrew_client, created_ruleset):
        resp = databrew_client.describe_ruleset(Name=created_ruleset)
        assert resp["Description"] == "test ruleset"

    def test_list_rulesets_shows_target_arn(self, databrew_client, created_ruleset):
        resp = databrew_client.list_rulesets()
        rs = next((r for r in resp["Rulesets"] if r["Name"] == created_ruleset), None)
        assert rs is not None
        assert "TargetArn" in rs

    def test_delete_ruleset_removes_from_list(self, databrew_client):
        name = f"rs-{uuid.uuid4().hex[:8]}"
        databrew_client.create_ruleset(
            Name=name,
            Description="temp",
            TargetArn="arn:aws:databrew:us-east-1:123456789012:dataset/dummy",
            Rules=[
                {
                    "Name": "r1",
                    "Disabled": False,
                    "CheckExpression": "IS_NOT_NULL(:col)",
                    "SubstitutionMap": {":col": "col1"},
                }
            ],
        )
        databrew_client.delete_ruleset(Name=name)
        resp = databrew_client.list_rulesets()
        names = [r["Name"] for r in resp["Rulesets"]]
        assert name not in names

    def test_describe_ruleset_rule_substitution_map(self, databrew_client, created_ruleset):
        resp = databrew_client.describe_ruleset(Name=created_ruleset)
        rule = resp["Rules"][0]
        assert "SubstitutionMap" in rule
        assert rule["SubstitutionMap"][":col"] == "col1"


@pytest.fixture
def project_prerequisites(databrew_client):
    """Create a dataset and recipe needed for project tests."""
    ds_name = f"ds-{uuid.uuid4().hex[:8]}"
    recipe_name = f"recipe-{uuid.uuid4().hex[:8]}"
    databrew_client.create_dataset(
        Name=ds_name,
        Input={"S3InputDefinition": {"Bucket": "my-bucket", "Key": "data.csv"}},
    )
    databrew_client.create_recipe(
        Name=recipe_name,
        Steps=[{"Action": {"Operation": "UPPER_CASE", "Parameters": {"sourceColumn": "col1"}}}],
    )
    yield ds_name, recipe_name
    try:
        databrew_client.delete_dataset(Name=ds_name)
    except Exception:
        pass  # best-effort cleanup


@pytest.fixture
def created_project(databrew_client, project_prerequisites):
    """Create a project and clean up after the test."""
    ds_name, recipe_name = project_prerequisites
    name = f"proj-{uuid.uuid4().hex[:8]}"
    databrew_client.create_project(
        DatasetName=ds_name,
        Name=name,
        RecipeName=recipe_name,
        RoleArn="arn:aws:iam::123456789012:role/test-role",
        Sample={"Size": 500, "Type": "HEAD"},
    )
    yield name, ds_name, recipe_name
    try:
        databrew_client.delete_project(Name=name)
    except Exception:
        pass  # best-effort cleanup


@pytest.fixture
def created_schedule(databrew_client):
    """Create a schedule and clean up after the test."""
    name = f"sched-{uuid.uuid4().hex[:8]}"
    databrew_client.create_schedule(
        Name=name,
        CronExpression="cron(0 12 * * ? *)",
        JobNames=["job1"],
    )
    yield name
    try:
        databrew_client.delete_schedule(Name=name)
    except Exception:
        pass  # best-effort cleanup


class TestDataBrewSchedules:
    def test_create_schedule(self, databrew_client):
        name = f"sched-{uuid.uuid4().hex[:8]}"
        resp = databrew_client.create_schedule(
            Name=name,
            CronExpression="cron(0 12 * * ? *)",
            JobNames=["job1"],
        )
        try:
            assert resp["Name"] == name
        finally:
            databrew_client.delete_schedule(Name=name)

    def test_describe_schedule(self, databrew_client, created_schedule):
        resp = databrew_client.describe_schedule(Name=created_schedule)
        assert resp["Name"] == created_schedule
        assert resp["CronExpression"] == "cron(0 12 * * ? *)"

    def test_list_schedules(self, databrew_client, created_schedule):
        resp = databrew_client.list_schedules()
        assert "Schedules" in resp
        names = [s["Name"] for s in resp["Schedules"]]
        assert created_schedule in names

    def test_update_schedule(self, databrew_client, created_schedule):
        resp = databrew_client.update_schedule(
            Name=created_schedule,
            CronExpression="cron(0 8 * * ? *)",
            JobNames=["job2"],
        )
        assert resp["Name"] == created_schedule
        desc = databrew_client.describe_schedule(Name=created_schedule)
        assert desc["CronExpression"] == "cron(0 8 * * ? *)"

    def test_delete_schedule(self, databrew_client):
        name = f"sched-{uuid.uuid4().hex[:8]}"
        databrew_client.create_schedule(
            Name=name,
            CronExpression="cron(0 12 * * ? *)",
            JobNames=["job1"],
        )
        resp = databrew_client.delete_schedule(Name=name)
        assert resp["Name"] == name
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.describe_schedule(Name=name)

    def test_schedule_has_arn(self, databrew_client, created_schedule):
        resp = databrew_client.describe_schedule(Name=created_schedule)
        assert "ResourceArn" in resp
        assert "databrew" in resp["ResourceArn"]
        assert created_schedule in resp["ResourceArn"]

    def test_describe_schedule_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.describe_schedule(Name="nonexistent-schedule")

    def test_update_schedule_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.update_schedule(
                Name="nonexistent-schedule",
                CronExpression="cron(0 12 * * ? *)",
                JobNames=[],
            )

    def test_delete_schedule_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.delete_schedule(Name="nonexistent-schedule")

    def test_create_duplicate_schedule_raises(self, databrew_client, created_schedule):
        with pytest.raises(databrew_client.exceptions.ClientError) as exc_info:
            databrew_client.create_schedule(
                Name=created_schedule,
                CronExpression="cron(0 12 * * ? *)",
                JobNames=["job1"],
            )
        assert exc_info.value.response["Error"]["Code"] in ("ConflictException", "AlreadyExistsException")


class TestDataBrewProjects:
    def test_create_project(self, databrew_client, project_prerequisites):
        ds_name, recipe_name = project_prerequisites
        name = f"proj-{uuid.uuid4().hex[:8]}"
        resp = databrew_client.create_project(
            DatasetName=ds_name,
            Name=name,
            RecipeName=recipe_name,
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        try:
            assert resp["Name"] == name
        finally:
            databrew_client.delete_project(Name=name)

    def test_describe_project(self, databrew_client, created_project):
        name, ds_name, recipe_name = created_project
        resp = databrew_client.describe_project(Name=name)
        assert resp["Name"] == name
        assert resp["DatasetName"] == ds_name
        assert resp["RecipeName"] == recipe_name

    def test_list_projects(self, databrew_client, created_project):
        name, _, _ = created_project
        resp = databrew_client.list_projects()
        assert "Projects" in resp
        names = [p["Name"] for p in resp["Projects"]]
        assert name in names

    def test_update_project(self, databrew_client, created_project):
        name, _, _ = created_project
        new_role = "arn:aws:iam::123456789012:role/updated-role"
        resp = databrew_client.update_project(Name=name, RoleArn=new_role)
        assert resp["Name"] == name
        desc = databrew_client.describe_project(Name=name)
        assert desc["RoleArn"] == new_role

    def test_delete_project(self, databrew_client, project_prerequisites):
        ds_name, recipe_name = project_prerequisites
        name = f"proj-{uuid.uuid4().hex[:8]}"
        databrew_client.create_project(
            DatasetName=ds_name,
            Name=name,
            RecipeName=recipe_name,
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        resp = databrew_client.delete_project(Name=name)
        assert resp["Name"] == name
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.describe_project(Name=name)

    def test_project_has_arn(self, databrew_client, created_project):
        name, _, _ = created_project
        resp = databrew_client.describe_project(Name=name)
        assert "ResourceArn" in resp
        assert "databrew" in resp["ResourceArn"]
        assert name in resp["ResourceArn"]

    def test_describe_project_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.describe_project(Name="nonexistent-project")

    def test_create_duplicate_project_raises(self, databrew_client, created_project, project_prerequisites):
        name, ds_name, recipe_name = created_project
        with pytest.raises(databrew_client.exceptions.ClientError) as exc_info:
            databrew_client.create_project(
                DatasetName=ds_name,
                Name=name,
                RecipeName=recipe_name,
                RoleArn="arn:aws:iam::123456789012:role/test-role",
            )
        assert exc_info.value.response["Error"]["Code"] in ("ConflictException", "AlreadyExistsException")


class TestDataBrewTagResource:
    def test_tag_resource_and_list(self, databrew_client, created_dataset):
        arn = databrew_client.describe_dataset(Name=created_dataset)["ResourceArn"]
        databrew_client.tag_resource(ResourceArn=arn, Tags={"env": "prod", "team": "data"})
        resp = databrew_client.list_tags_for_resource(ResourceArn=arn)
        assert resp["Tags"]["env"] == "prod"
        assert resp["Tags"]["team"] == "data"

    def test_untag_resource(self, databrew_client, created_dataset):
        arn = databrew_client.describe_dataset(Name=created_dataset)["ResourceArn"]
        databrew_client.tag_resource(ResourceArn=arn, Tags={"env": "prod", "team": "data"})
        databrew_client.untag_resource(ResourceArn=arn, TagKeys=["env"])
        resp = databrew_client.list_tags_for_resource(ResourceArn=arn)
        assert "env" not in resp["Tags"]
        assert resp["Tags"]["team"] == "data"

    def test_list_tags_empty(self, databrew_client, created_dataset):
        arn = databrew_client.describe_dataset(Name=created_dataset)["ResourceArn"]
        resp = databrew_client.list_tags_for_resource(ResourceArn=arn)
        assert "Tags" in resp
        assert isinstance(resp["Tags"], dict)

    def test_tag_resource_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.tag_resource(
                ResourceArn="arn:aws:databrew:us-east-1:123456789012:dataset/nonexistent",
                Tags={"key": "val"},
            )

    def test_list_tags_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.list_tags_for_resource(
                ResourceArn="arn:aws:databrew:us-east-1:123456789012:dataset/nonexistent"
            )


class TestDataBrewJobRuns:
    def test_start_job_run(self, databrew_client):
        name = f"job-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe_job(
            Name=name, RoleArn="arn:aws:iam::123456789012:role/test-role"
        )
        try:
            resp = databrew_client.start_job_run(Name=name)
            assert "RunId" in resp
            assert resp["RunId"]
        finally:
            databrew_client.delete_job(Name=name)

    def test_list_job_runs(self, databrew_client):
        name = f"job-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe_job(
            Name=name, RoleArn="arn:aws:iam::123456789012:role/test-role"
        )
        try:
            run_resp = databrew_client.start_job_run(Name=name)
            run_id = run_resp["RunId"]
            resp = databrew_client.list_job_runs(Name=name)
            assert "JobRuns" in resp
            run_ids = [r["RunId"] for r in resp["JobRuns"]]
            assert run_id in run_ids
        finally:
            databrew_client.delete_job(Name=name)

    def test_describe_job_run(self, databrew_client):
        name = f"job-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe_job(
            Name=name, RoleArn="arn:aws:iam::123456789012:role/test-role"
        )
        try:
            run_id = databrew_client.start_job_run(Name=name)["RunId"]
            resp = databrew_client.describe_job_run(Name=name, RunId=run_id)
            assert resp["State"] == "RUNNING"
            assert resp["RunId"] == run_id
        finally:
            databrew_client.delete_job(Name=name)

    def test_stop_job_run(self, databrew_client):
        name = f"job-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe_job(
            Name=name, RoleArn="arn:aws:iam::123456789012:role/test-role"
        )
        try:
            run_id = databrew_client.start_job_run(Name=name)["RunId"]
            resp = databrew_client.stop_job_run(Name=name, RunId=run_id)
            assert resp["RunId"] == run_id
            desc = databrew_client.describe_job_run(Name=name, RunId=run_id)
            assert desc["State"] == "STOPPED"
        finally:
            databrew_client.delete_job(Name=name)

    def test_start_job_run_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.start_job_run(Name="nonexistent-job")


class TestDataBrewBatchDeleteRecipeVersion:
    def test_batch_delete_recipe_version(self, databrew_client):
        name = f"recipe-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe(
            Name=name,
            Steps=[{"Action": {"Operation": "UPPER_CASE", "Parameters": {"sourceColumn": "col1"}}}],
        )
        databrew_client.publish_recipe(Name=name)
        resp = databrew_client.batch_delete_recipe_version(Name=name, RecipeVersions=["1.0"])
        assert resp["Name"] == name
        assert isinstance(resp["Errors"], list)
        assert len(resp["Errors"]) == 0

    def test_batch_delete_nonexistent_version_returns_error(self, databrew_client):
        name = f"recipe-{uuid.uuid4().hex[:8]}"
        databrew_client.create_recipe(
            Name=name,
            Steps=[{"Action": {"Operation": "UPPER_CASE", "Parameters": {"sourceColumn": "col1"}}}],
        )
        resp = databrew_client.batch_delete_recipe_version(Name=name, RecipeVersions=["99.0"])
        assert len(resp["Errors"]) == 1
        assert resp["Errors"][0]["RecipeVersion"] == "99.0"

    def test_batch_delete_recipe_not_found(self, databrew_client):
        with pytest.raises(databrew_client.exceptions.ResourceNotFoundException):
            databrew_client.batch_delete_recipe_version(
                Name="nonexistent-recipe", RecipeVersions=["1.0"]
            )


class TestDataBrewProjectSession:
    def test_start_project_session(self, databrew_client, created_project):
        name, _, _ = created_project
        resp = databrew_client.start_project_session(Name=name)
        assert resp["Name"] == name
        assert "ClientSessionId" in resp

    def test_send_project_session_action(self, databrew_client, created_project):
        name, _, _ = created_project
        resp = databrew_client.send_project_session_action(Name=name, Preview=True)
        assert resp["Name"] == name
        assert "ActionId" in resp
