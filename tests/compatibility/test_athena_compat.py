"""Athena compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def athena():
    return make_client("athena")


class TestAthenaWorkGroupOperations:
    def test_create_work_group(self, athena):
        name = _unique("wg")
        athena.create_work_group(
            Name=name,
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}},
        )
        resp = athena.get_work_group(WorkGroup=name)
        assert resp["WorkGroup"]["Name"] == name
        # cleanup
        athena.delete_work_group(WorkGroup=name)

    def test_get_work_group(self, athena):
        name = _unique("wg")
        athena.create_work_group(
            Name=name,
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}},
            Description="test workgroup",
        )
        resp = athena.get_work_group(WorkGroup=name)
        wg = resp["WorkGroup"]
        assert wg["Name"] == name
        assert wg["Description"] == "test workgroup"
        assert wg["State"] == "ENABLED"
        # cleanup
        athena.delete_work_group(WorkGroup=name)

    def test_list_work_groups(self, athena):
        name = _unique("wg")
        athena.create_work_group(
            Name=name,
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}},
        )
        resp = athena.list_work_groups()
        names = [wg["Name"] for wg in resp["WorkGroups"]]
        assert name in names
        # cleanup
        athena.delete_work_group(WorkGroup=name)

    def test_delete_work_group(self, athena):
        name = _unique("wg")
        athena.create_work_group(
            Name=name,
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}},
        )
        athena.delete_work_group(WorkGroup=name)
        # Verify it's gone from the list
        resp = athena.list_work_groups()
        names = [wg["Name"] for wg in resp["WorkGroups"]]
        assert name not in names


class TestAthenaNamedQueryOperations:
    def test_create_named_query(self, athena):
        name = _unique("nq")
        resp = athena.create_named_query(
            Name=name,
            Database="default",
            QueryString="SELECT 1",
        )
        assert "NamedQueryId" in resp

    def test_get_named_query(self, athena):
        name = _unique("nq")
        create_resp = athena.create_named_query(
            Name=name,
            Database="default",
            QueryString="SELECT 1",
            Description="test query",
        )
        query_id = create_resp["NamedQueryId"]
        resp = athena.get_named_query(NamedQueryId=query_id)
        nq = resp["NamedQuery"]
        assert nq["Name"] == name
        assert nq["QueryString"] == "SELECT 1"
        assert nq["Database"] == "default"


class TestAthenaQueryExecution:
    def test_start_query_execution(self, athena):
        resp = athena.start_query_execution(
            QueryString="SELECT 1",
            WorkGroup="primary",
            ResultConfiguration={"OutputLocation": "s3://test-bucket/results/"},
        )
        assert "QueryExecutionId" in resp

    def test_get_query_execution(self, athena):
        start_resp = athena.start_query_execution(
            QueryString="SELECT 1",
            WorkGroup="primary",
            ResultConfiguration={"OutputLocation": "s3://test-bucket/results/"},
        )
        qe_id = start_resp["QueryExecutionId"]
        resp = athena.get_query_execution(QueryExecutionId=qe_id)
        qe = resp["QueryExecution"]
        assert qe["QueryExecutionId"] == qe_id
        assert qe["Query"] == "SELECT 1"
        assert "Status" in qe

    def test_list_query_executions(self, athena):
        start_resp = athena.start_query_execution(
            QueryString="SELECT 1",
            WorkGroup="primary",
            ResultConfiguration={"OutputLocation": "s3://test-bucket/results/"},
        )
        qe_id = start_resp["QueryExecutionId"]
        resp = athena.list_query_executions()
        assert qe_id in resp["QueryExecutionIds"]


class TestAthenaDataCatalogOperations:
    def test_create_data_catalog(self, athena):
        name = _unique("dc")
        athena.create_data_catalog(
            Name=name,
            Type="HIVE",
            Description="test catalog",
            Parameters={
                "metadata-function": "arn:aws:lambda:us-east-1:123456789012:function:my-func"
            },
        )
        resp = athena.get_data_catalog(Name=name)
        catalog = resp["DataCatalog"]
        assert catalog["Name"] == name
        assert catalog["Type"] == "HIVE"

    def test_get_data_catalog(self, athena):
        name = _unique("dc")
        athena.create_data_catalog(
            Name=name,
            Type="LAMBDA",
            Description="lambda catalog",
            Parameters={
                "metadata-function": "arn:aws:lambda:us-east-1:123456789012:function:my-func"
            },
        )
        resp = athena.get_data_catalog(Name=name)
        catalog = resp["DataCatalog"]
        assert catalog["Name"] == name
        assert catalog["Type"] == "LAMBDA"
        assert catalog["Description"] == "lambda catalog"


class TestAthenaCapacityReservationOperations:
    def test_get_capacity_reservation_nonexistent(self, athena):
        with pytest.raises(ClientError) as exc:
            athena.get_capacity_reservation(Name="does-not-exist")
        assert exc.value.response["Error"]["Code"] in (
            "InvalidRequestException",
            "ResourceNotFoundException",
            "NotFoundException",
        )


class TestAthenaPreparedStatementOperations:
    def test_create_prepared_statement(self, athena):
        name = _unique("ps")
        athena.create_prepared_statement(
            StatementName=name,
            WorkGroup="primary",
            QueryStatement="SELECT ? FROM my_table",
        )
        resp = athena.get_prepared_statement(
            StatementName=name,
            WorkGroup="primary",
        )
        ps = resp["PreparedStatement"]
        assert ps["StatementName"] == name
        assert ps["QueryStatement"] == "SELECT ? FROM my_table"


class TestAthenaQueryLifecycle:
    def test_stop_query_execution(self, athena):
        start_resp = athena.start_query_execution(
            QueryString="SELECT 1",
            WorkGroup="primary",
            ResultConfiguration={"OutputLocation": "s3://test-bucket/results/"},
        )
        qe_id = start_resp["QueryExecutionId"]
        athena.stop_query_execution(QueryExecutionId=qe_id)
        resp = athena.get_query_execution(QueryExecutionId=qe_id)
        assert resp["QueryExecution"]["Status"]["State"] in (
            "CANCELLED",
            "SUCCEEDED",
            "FAILED",
        )


class TestAthenaQueryResultsOperations:
    def test_get_query_results(self, athena):
        start_resp = athena.start_query_execution(
            QueryString="SELECT 1",
            WorkGroup="primary",
            ResultConfiguration={"OutputLocation": "s3://test-bucket/results/"},
        )
        qe_id = start_resp["QueryExecutionId"]
        resp = athena.get_query_results(QueryExecutionId=qe_id)
        assert "ResultSet" in resp

    def test_get_query_runtime_statistics(self, athena):
        start_resp = athena.start_query_execution(
            QueryString="SELECT 1",
            WorkGroup="primary",
            ResultConfiguration={"OutputLocation": "s3://test-bucket/results/"},
        )
        qe_id = start_resp["QueryExecutionId"]
        resp = athena.get_query_runtime_statistics(QueryExecutionId=qe_id)
        assert "QueryRuntimeStatistics" in resp


class TestAthenaTagOperations:
    def test_list_tags_for_resource(self, athena):
        name = _unique("wg")
        athena.create_work_group(
            Name=name,
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}},
            Tags=[{"Key": "env", "Value": "test"}],
        )
        # Get the ARN - work groups don't return ARN directly, construct it
        resp = athena.list_tags_for_resource(
            ResourceARN=f"arn:aws:athena:us-east-1:123456789012:workgroup/{name}",
        )
        assert "Tags" in resp
        athena.delete_work_group(WorkGroup=name)


class TestAthenaListOperations:
    def test_list_data_catalogs(self, athena):
        resp = athena.list_data_catalogs()
        assert "DataCatalogsSummary" in resp

    def test_list_capacity_reservations(self, athena):
        resp = athena.list_capacity_reservations()
        assert "CapacityReservations" in resp


class TestAthenaWorkGroupAdvanced:
    """Advanced workgroup CRUD and edge cases."""

    def test_create_work_group_with_tags(self, athena):
        """CreateWorkGroup with tags preserves them in ListTagsForResource."""
        name = _unique("wg")
        athena.create_work_group(
            Name=name,
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}},
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        try:
            resp = athena.list_tags_for_resource(
                ResourceARN=f"arn:aws:athena:us-east-1:123456789012:workgroup/{name}",
            )
            tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tags["env"] == "test"
            assert tags["team"] == "platform"
        finally:
            athena.delete_work_group(WorkGroup=name)

    def test_create_work_group_with_description(self, athena):
        """CreateWorkGroup with description is returned in GetWorkGroup."""
        name = _unique("wg")
        athena.create_work_group(
            Name=name,
            Description="my test workgroup",
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}},
        )
        try:
            resp = athena.get_work_group(WorkGroup=name)
            assert resp["WorkGroup"]["Description"] == "my test workgroup"
        finally:
            athena.delete_work_group(WorkGroup=name)

    def test_create_duplicate_work_group_raises(self, athena):
        """Creating a workgroup with an existing name raises an error."""
        name = _unique("wg")
        athena.create_work_group(
            Name=name,
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}},
        )
        try:
            with pytest.raises(ClientError) as exc:
                athena.create_work_group(
                    Name=name,
                    Configuration={
                        "ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}
                    },
                )
            assert exc.value.response["Error"]["Code"] in (
                "InvalidRequestException",
                "ConflictException",
            )
        finally:
            athena.delete_work_group(WorkGroup=name)

    def test_work_group_state_enabled_by_default(self, athena):
        """New workgroup has ENABLED state by default."""
        name = _unique("wg")
        athena.create_work_group(
            Name=name,
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}},
        )
        try:
            resp = athena.get_work_group(WorkGroup=name)
            assert resp["WorkGroup"]["State"] == "ENABLED"
        finally:
            athena.delete_work_group(WorkGroup=name)

    def test_work_group_configuration_preserved(self, athena):
        """WorkGroup configuration is preserved in describe response."""
        name = _unique("wg")
        athena.create_work_group(
            Name=name,
            Configuration={
                "ResultConfiguration": {"OutputLocation": "s3://my-bucket/output/"},
                "EnforceWorkGroupConfiguration": True,
            },
        )
        try:
            resp = athena.get_work_group(WorkGroup=name)
            config = resp["WorkGroup"]["Configuration"]
            assert config["ResultConfiguration"]["OutputLocation"] == "s3://my-bucket/output/"
        finally:
            athena.delete_work_group(WorkGroup=name)


class TestAthenaNamedQueryAdvanced:
    """Advanced named query operations."""

    def test_create_named_query_with_description(self, athena):
        """CreateNamedQuery with description preserves it."""
        name = _unique("nq")
        create_resp = athena.create_named_query(
            Name=name,
            Database="default",
            QueryString="SELECT count(*) FROM my_table",
            Description="count all rows",
        )
        query_id = create_resp["NamedQueryId"]
        resp = athena.get_named_query(NamedQueryId=query_id)
        assert resp["NamedQuery"]["Description"] == "count all rows"

    def test_create_named_query_in_workgroup(self, athena):
        """CreateNamedQuery associated with a specific workgroup."""
        wg_name = _unique("wg")
        athena.create_work_group(
            Name=wg_name,
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}},
        )
        try:
            name = _unique("nq")
            create_resp = athena.create_named_query(
                Name=name,
                Database="default",
                QueryString="SELECT 1",
                WorkGroup=wg_name,
            )
            query_id = create_resp["NamedQueryId"]
            resp = athena.get_named_query(NamedQueryId=query_id)
            assert resp["NamedQuery"]["WorkGroup"] == wg_name
        finally:
            athena.delete_work_group(WorkGroup=wg_name)

    def test_create_multiple_named_queries(self, athena):
        """Creating multiple named queries succeeds."""
        ids = []
        for i in range(3):
            name = _unique(f"nq{i}")
            resp = athena.create_named_query(
                Name=name,
                Database="default",
                QueryString=f"SELECT {i}",
            )
            ids.append(resp["NamedQueryId"])
        assert len(ids) == 3
        assert len(set(ids)) == 3  # all unique

    def test_get_named_query_nonexistent_raises(self, athena):
        """GetNamedQuery with a fake ID raises an error."""
        with pytest.raises(ClientError):
            athena.get_named_query(NamedQueryId="00000000-0000-0000-0000-000000000000")


class TestAthenaPreparedStatementAdvanced:
    """Advanced prepared statement operations."""

    def test_get_prepared_statement_details(self, athena):
        """GetPreparedStatement returns statement details."""
        name = _unique("ps")
        athena.create_prepared_statement(
            StatementName=name,
            WorkGroup="primary",
            QueryStatement="SELECT ? AS col1, ? AS col2",
        )
        resp = athena.get_prepared_statement(
            StatementName=name,
            WorkGroup="primary",
        )
        ps = resp["PreparedStatement"]
        assert ps["StatementName"] == name
        assert "?" in ps["QueryStatement"]
        assert ps["WorkGroupName"] == "primary"

    def test_get_prepared_statement_nonexistent_raises(self, athena):
        """GetPreparedStatement with nonexistent name raises error."""
        with pytest.raises(ClientError):
            athena.get_prepared_statement(
                StatementName="does-not-exist",
                WorkGroup="primary",
            )


class TestAthenaQueryExecutionAdvanced:
    """Advanced query execution operations."""

    def test_start_query_execution_in_workgroup(self, athena):
        """StartQueryExecution with custom workgroup."""
        wg_name = _unique("wg")
        athena.create_work_group(
            Name=wg_name,
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}},
        )
        try:
            resp = athena.start_query_execution(
                QueryString="SELECT 1",
                WorkGroup=wg_name,
            )
            qe_id = resp["QueryExecutionId"]
            desc = athena.get_query_execution(QueryExecutionId=qe_id)
            assert desc["QueryExecution"]["WorkGroup"] == wg_name
        finally:
            athena.delete_work_group(WorkGroup=wg_name)

    def test_query_execution_has_status(self, athena):
        """Query execution status includes State field."""
        resp = athena.start_query_execution(
            QueryString="SELECT 42",
            WorkGroup="primary",
            ResultConfiguration={"OutputLocation": "s3://test-bucket/results/"},
        )
        qe_id = resp["QueryExecutionId"]
        desc = athena.get_query_execution(QueryExecutionId=qe_id)
        status = desc["QueryExecution"]["Status"]
        assert "State" in status
        assert status["State"] in ("QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED")

    def test_query_execution_has_query_string(self, athena):
        """Query execution retains the original query string."""
        resp = athena.start_query_execution(
            QueryString="SELECT 'hello'",
            WorkGroup="primary",
            ResultConfiguration={"OutputLocation": "s3://test-bucket/results/"},
        )
        qe_id = resp["QueryExecutionId"]
        desc = athena.get_query_execution(QueryExecutionId=qe_id)
        assert desc["QueryExecution"]["Query"] == "SELECT 'hello'"

    def test_list_query_executions_in_workgroup(self, athena):
        """ListQueryExecutions filtered by workgroup."""
        wg_name = _unique("wg")
        athena.create_work_group(
            Name=wg_name,
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}},
        )
        try:
            resp = athena.start_query_execution(
                QueryString="SELECT 1",
                WorkGroup=wg_name,
            )
            qe_id = resp["QueryExecutionId"]
            list_resp = athena.list_query_executions(WorkGroup=wg_name)
            assert qe_id in list_resp["QueryExecutionIds"]
        finally:
            athena.delete_work_group(WorkGroup=wg_name)


class TestAthenaDataCatalogAdvanced:
    """Advanced data catalog operations."""

    def test_list_data_catalogs_includes_created(self, athena):
        """ListDataCatalogs includes a newly created catalog."""
        name = _unique("dc")
        athena.create_data_catalog(
            Name=name,
            Type="HIVE",
            Parameters={"metadata-function": "arn:aws:lambda:us-east-1:123456789012:function:f"},
        )
        resp = athena.list_data_catalogs()
        names = [c["CatalogName"] for c in resp["DataCatalogsSummary"]]
        assert name in names

    def test_get_data_catalog_with_parameters(self, athena):
        """GetDataCatalog returns parameters set at creation."""
        name = _unique("dc")
        athena.create_data_catalog(
            Name=name,
            Type="LAMBDA",
            Description="test catalog with params",
            Parameters={
                "metadata-function": "arn:aws:lambda:us-east-1:123456789012:function:my-func"
            },
        )
        resp = athena.get_data_catalog(Name=name)
        catalog = resp["DataCatalog"]
        assert catalog["Parameters"]["metadata-function"].endswith("my-func")
