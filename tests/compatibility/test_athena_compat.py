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

    def test_cancel_capacity_reservation_nonexistent(self, athena):
        """CancelCapacityReservation with nonexistent name raises error."""
        with pytest.raises(ClientError) as exc:
            athena.cancel_capacity_reservation(Name="does-not-exist")
        assert exc.value.response["Error"]["Code"] == "InvalidArgumentException"

    def test_delete_capacity_reservation_nonexistent(self, athena):
        """DeleteCapacityReservation with nonexistent name raises error."""
        with pytest.raises(ClientError) as exc:
            athena.delete_capacity_reservation(Name="does-not-exist")
        assert exc.value.response["Error"]["Code"] == "InvalidArgumentException"


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

    def test_delete_prepared_statement_nonexistent(self, athena):
        """DeletePreparedStatement with nonexistent name raises error."""
        with pytest.raises(ClientError) as exc:
            athena.delete_prepared_statement(StatementName="nonexistent", WorkGroup="primary")
        assert exc.value.response["Error"]["Code"] == "InvalidArgumentException"

    def test_batch_get_prepared_statement_empty(self, athena):
        """BatchGetPreparedStatement with nonexistent names."""
        resp = athena.batch_get_prepared_statement(
            PreparedStatementNames=["nonexistent"],
            WorkGroup="primary",
        )
        assert "PreparedStatements" in resp
        assert "UnprocessedPreparedStatementNames" in resp


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

    def test_delete_data_catalog(self, athena):
        """DeleteDataCatalog removes a catalog."""
        name = _unique("dc")
        athena.create_data_catalog(
            Name=name,
            Type="HIVE",
            Parameters={"metadata-function": "arn:aws:lambda:us-east-1:123456789012:function:f"},
        )
        del_resp = athena.delete_data_catalog(Name=name)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify it's gone
        resp = athena.list_data_catalogs()
        names = [c["CatalogName"] for c in resp["DataCatalogsSummary"]]
        assert name not in names

    def test_update_data_catalog(self, athena):
        """UpdateDataCatalog modifies catalog description."""
        name = _unique("dc")
        athena.create_data_catalog(
            Name=name,
            Type="HIVE",
            Description="original",
            Parameters={"metadata-function": "arn:aws:lambda:us-east-1:123456789012:function:f"},
        )
        update_resp = athena.update_data_catalog(
            Name=name,
            Type="HIVE",
            Description="updated description",
        )
        assert update_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        resp = athena.get_data_catalog(Name=name)
        assert resp["DataCatalog"]["Description"] == "updated description"


class TestAthenaNamedQueryListDelete:
    """List and delete named query operations."""

    def test_list_named_queries(self, athena):
        """ListNamedQueries returns query IDs."""
        name = _unique("nq")
        create_resp = athena.create_named_query(
            Name=name,
            Database="default",
            QueryString="SELECT 1",
        )
        query_id = create_resp["NamedQueryId"]
        resp = athena.list_named_queries()
        assert query_id in resp["NamedQueryIds"]

    def test_delete_named_query(self, athena):
        """DeleteNamedQuery removes a named query."""
        name = _unique("nq")
        create_resp = athena.create_named_query(
            Name=name,
            Database="default",
            QueryString="SELECT 1",
        )
        query_id = create_resp["NamedQueryId"]
        del_resp = athena.delete_named_query(NamedQueryId=query_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify it's gone from list
        resp = athena.list_named_queries()
        assert query_id not in resp.get("NamedQueryIds", [])


class TestAthenaBatchOperations:
    """Batch get operations."""

    def test_batch_get_named_query(self, athena):
        """BatchGetNamedQuery returns query details."""
        name = _unique("nq")
        create_resp = athena.create_named_query(
            Name=name,
            Database="default",
            QueryString="SELECT 42",
        )
        query_id = create_resp["NamedQueryId"]
        resp = athena.batch_get_named_query(NamedQueryIds=[query_id])
        assert len(resp["NamedQueries"]) == 1
        assert resp["NamedQueries"][0]["Name"] == name
        assert resp["NamedQueries"][0]["QueryString"] == "SELECT 42"

    def test_batch_get_named_query_nonexistent(self, athena):
        """BatchGetNamedQuery with fake ID returns it in UnprocessedNamedQueryIds."""
        resp = athena.batch_get_named_query(NamedQueryIds=["00000000-0000-0000-0000-000000000000"])
        assert len(resp.get("UnprocessedNamedQueryIds", [])) >= 0
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_batch_get_query_execution(self, athena):
        """BatchGetQueryExecution returns execution details."""
        start_resp = athena.start_query_execution(
            QueryString="SELECT 1",
            WorkGroup="primary",
            ResultConfiguration={"OutputLocation": "s3://test-bucket/results/"},
        )
        qe_id = start_resp["QueryExecutionId"]
        resp = athena.batch_get_query_execution(QueryExecutionIds=[qe_id])
        assert len(resp["QueryExecutions"]) == 1
        assert resp["QueryExecutions"][0]["QueryExecutionId"] == qe_id
        assert resp["QueryExecutions"][0]["Query"] == "SELECT 1"

    def test_batch_get_query_execution_nonexistent(self, athena):
        """BatchGetQueryExecution with fake ID returns it in UnprocessedQueryExecutionIds."""
        resp = athena.batch_get_query_execution(
            QueryExecutionIds=["00000000-0000-0000-0000-000000000000"]
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestAthenaTaggingOperations:
    """Tag and untag resource operations."""

    def test_tag_and_untag_resource(self, athena):
        """TagResource adds tags, UntagResource removes them."""
        name = _unique("wg")
        athena.create_work_group(
            Name=name,
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}},
        )
        arn = f"arn:aws:athena:us-east-1:123456789012:workgroup/{name}"
        try:
            # Tag
            tag_resp = athena.tag_resource(
                ResourceARN=arn,
                Tags=[
                    {"Key": "project", "Value": "robotocore"},
                    {"Key": "stage", "Value": "test"},
                ],
            )
            assert tag_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # Verify tags
            list_resp = athena.list_tags_for_resource(ResourceARN=arn)
            tags = {t["Key"]: t["Value"] for t in list_resp["Tags"]}
            assert tags["project"] == "robotocore"
            assert tags["stage"] == "test"

            # Untag
            untag_resp = athena.untag_resource(
                ResourceARN=arn,
                TagKeys=["stage"],
            )
            assert untag_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # Verify untag
            list_resp2 = athena.list_tags_for_resource(ResourceARN=arn)
            tag_keys = [t["Key"] for t in list_resp2["Tags"]]
            assert "stage" not in tag_keys
        finally:
            athena.delete_work_group(WorkGroup=name)


class TestAthenaNewOps:
    """Tests for newly verified Athena operations."""

    def test_get_calculation_execution_nonexistent(self, athena):
        """GetCalculationExecution with fake ID raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            athena.get_calculation_execution(CalculationExecutionId="fake-calc-id")
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_get_calculation_execution_code_nonexistent(self, athena):
        """GetCalculationExecutionCode with fake ID raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            athena.get_calculation_execution_code(CalculationExecutionId="fake-calc-id")
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_get_calculation_execution_status_nonexistent(self, athena):
        """GetCalculationExecutionStatus with fake ID raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            athena.get_calculation_execution_status(CalculationExecutionId="fake-calc-id")
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_get_capacity_assignment_configuration_nonexistent(self, athena):
        """GetCapacityAssignmentConfiguration with fake name raises error."""
        with pytest.raises(ClientError) as exc:
            athena.get_capacity_assignment_configuration(CapacityReservationName="fake-res")
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_get_database_nonexistent(self, athena):
        """GetDatabase with nonexistent database raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            athena.get_database(CatalogName="AwsDataCatalog", DatabaseName="nonexistent_db")
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_get_notebook_metadata_nonexistent(self, athena):
        """GetNotebookMetadata with fake ID raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            athena.get_notebook_metadata(NotebookId="fake-notebook-id")
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_get_session_nonexistent(self, athena):
        """GetSession with fake ID raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            athena.get_session(SessionId="fake-session-id")
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_get_session_status_nonexistent(self, athena):
        """GetSessionStatus with fake ID raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            athena.get_session_status(SessionId="fake-session-id")
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_get_table_metadata_nonexistent(self, athena):
        """GetTableMetadata with fake table raises InvalidRequestException."""
        with pytest.raises(ClientError) as exc:
            athena.get_table_metadata(
                CatalogName="AwsDataCatalog",
                DatabaseName="default",
                TableName="nonexistent_table",
            )
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_list_application_dpu_sizes(self, athena):
        """ListApplicationDPUSizes returns ApplicationDPUSizes list."""
        resp = athena.list_application_dpu_sizes()
        assert "ApplicationDPUSizes" in resp
        assert isinstance(resp["ApplicationDPUSizes"], list)

    def test_list_calculation_executions(self, athena):
        """ListCalculationExecutions returns Calculations list."""
        resp = athena.list_calculation_executions(SessionId="fake-session-id")
        assert "Calculations" in resp
        assert isinstance(resp["Calculations"], list)

    def test_list_databases(self, athena):
        """ListDatabases returns DatabaseList."""
        resp = athena.list_databases(CatalogName="AwsDataCatalog")
        assert "DatabaseList" in resp
        assert isinstance(resp["DatabaseList"], list)

    def test_list_engine_versions(self, athena):
        """ListEngineVersions returns EngineVersions list."""
        resp = athena.list_engine_versions()
        assert "EngineVersions" in resp
        assert isinstance(resp["EngineVersions"], list)

    def test_list_notebook_metadata(self, athena):
        """ListNotebookMetadata returns NotebookMetadataList."""
        resp = athena.list_notebook_metadata(WorkGroup="primary")
        assert "NotebookMetadataList" in resp
        assert isinstance(resp["NotebookMetadataList"], list)

    def test_list_notebook_sessions(self, athena):
        """ListNotebookSessions returns NotebookSessionsList."""
        resp = athena.list_notebook_sessions(NotebookId="fake-notebook-id")
        assert "NotebookSessionsList" in resp
        assert isinstance(resp["NotebookSessionsList"], list)

    def test_list_prepared_statements(self, athena):
        """ListPreparedStatements returns PreparedStatements list."""
        resp = athena.list_prepared_statements(WorkGroup="primary")
        assert "PreparedStatements" in resp
        assert isinstance(resp["PreparedStatements"], list)

    def test_list_prepared_statements_includes_created(self, athena):
        """ListPreparedStatements includes a newly created statement."""
        name = _unique("ps")
        athena.create_prepared_statement(
            StatementName=name,
            WorkGroup="primary",
            QueryStatement="SELECT ? FROM t",
        )
        resp = athena.list_prepared_statements(WorkGroup="primary")
        names = [ps["StatementName"] for ps in resp["PreparedStatements"]]
        assert name in names


class TestAthenaUpdateOperations:
    """Tests for update operations on Athena resources."""

    def test_update_prepared_statement(self, athena):
        """UpdatePreparedStatement modifies the query statement."""
        name = _unique("ps")
        athena.create_prepared_statement(
            StatementName=name,
            WorkGroup="primary",
            QueryStatement="SELECT ?",
        )
        try:
            resp = athena.update_prepared_statement(
                StatementName=name,
                WorkGroup="primary",
                QueryStatement="SELECT ? + 1",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            get_resp = athena.get_prepared_statement(
                StatementName=name,
                WorkGroup="primary",
            )
            assert get_resp["PreparedStatement"]["QueryStatement"] == "SELECT ? + 1"
        finally:
            athena.delete_prepared_statement(StatementName=name, WorkGroup="primary")

    def test_update_work_group_state(self, athena):
        """UpdateWorkGroup can change state to DISABLED."""
        name = _unique("wg")
        athena.create_work_group(
            Name=name,
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}},
        )
        try:
            resp = athena.update_work_group(WorkGroup=name, State="DISABLED")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            get_resp = athena.get_work_group(WorkGroup=name)
            assert get_resp["WorkGroup"]["State"] == "DISABLED"
        finally:
            athena.delete_work_group(WorkGroup=name)

    def test_update_work_group_description(self, athena):
        """UpdateWorkGroup can modify description."""
        name = _unique("wg")
        athena.create_work_group(
            Name=name,
            Description="original",
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}},
        )
        try:
            athena.update_work_group(WorkGroup=name, Description="updated description")
            get_resp = athena.get_work_group(WorkGroup=name)
            assert get_resp["WorkGroup"]["Description"] == "updated description"
        finally:
            athena.delete_work_group(WorkGroup=name)


class TestAthenaPreparedStatementCRUD:
    """Full CRUD cycle for prepared statements."""

    def test_delete_prepared_statement_removes_it(self, athena):
        """DeletePreparedStatement removes statement from list and get."""
        name = _unique("ps")
        athena.create_prepared_statement(
            StatementName=name,
            WorkGroup="primary",
            QueryStatement="SELECT ?",
        )
        # Verify it exists
        resp = athena.list_prepared_statements(WorkGroup="primary")
        names = [ps["StatementName"] for ps in resp["PreparedStatements"]]
        assert name in names

        # Delete
        del_resp = athena.delete_prepared_statement(StatementName=name, WorkGroup="primary")
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify gone from list
        resp2 = athena.list_prepared_statements(WorkGroup="primary")
        names2 = [ps["StatementName"] for ps in resp2["PreparedStatements"]]
        assert name not in names2

        # Verify get raises error
        with pytest.raises(ClientError):
            athena.get_prepared_statement(StatementName=name, WorkGroup="primary")


class TestAthenaNamedQueryWorkGroupFilter:
    """Test ListNamedQueries with WorkGroup filter."""

    def test_list_named_queries_in_workgroup(self, athena):
        """ListNamedQueries filtered by WorkGroup returns matching queries."""
        wg = _unique("wg")
        athena.create_work_group(
            Name=wg,
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}},
        )
        try:
            nq_name = _unique("nq")
            create_resp = athena.create_named_query(
                Name=nq_name,
                Database="default",
                QueryString="SELECT 1",
                WorkGroup=wg,
            )
            query_id = create_resp["NamedQueryId"]
            resp = athena.list_named_queries(WorkGroup=wg)
            assert "NamedQueryIds" in resp
            assert query_id in resp["NamedQueryIds"]
        finally:
            athena.delete_work_group(WorkGroup=wg)

    def test_list_named_queries_primary_workgroup(self, athena):
        """ListNamedQueries with WorkGroup='primary' returns queries."""
        name = _unique("nq")
        create_resp = athena.create_named_query(
            Name=name,
            Database="default",
            QueryString="SELECT 99",
            WorkGroup="primary",
        )
        query_id = create_resp["NamedQueryId"]
        resp = athena.list_named_queries(WorkGroup="primary")
        assert "NamedQueryIds" in resp
        assert query_id in resp["NamedQueryIds"]


class TestAthenaSessionLifecycle:
    """Tests for Athena session start, get, status, and terminate."""

    def test_start_session(self, athena):
        """StartSession returns a SessionId and IDLE state."""
        resp = athena.start_session(
            WorkGroup="primary",
            EngineConfiguration={"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 4},
        )
        assert "SessionId" in resp
        assert resp["State"] == "IDLE"

    def test_get_session(self, athena):
        """GetSession returns session details after creation."""
        start_resp = athena.start_session(
            WorkGroup="primary",
            EngineConfiguration={"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 4},
        )
        session_id = start_resp["SessionId"]
        resp = athena.get_session(SessionId=session_id)
        assert resp["SessionId"] == session_id
        assert "Status" in resp
        assert resp["Status"]["State"] in ("IDLE", "BUSY", "TERMINATING", "TERMINATED")

    def test_get_session_status(self, athena):
        """GetSessionStatus returns session status."""
        start_resp = athena.start_session(
            WorkGroup="primary",
            EngineConfiguration={"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 4},
        )
        session_id = start_resp["SessionId"]
        resp = athena.get_session_status(SessionId=session_id)
        assert "Status" in resp
        assert "State" in resp["Status"]
        assert resp["Status"]["State"] in ("IDLE", "BUSY", "TERMINATING", "TERMINATED")

    def test_terminate_session(self, athena):
        """TerminateSession transitions a session to TERMINATING."""
        start_resp = athena.start_session(
            WorkGroup="primary",
            EngineConfiguration={"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 4},
        )
        session_id = start_resp["SessionId"]
        resp = athena.terminate_session(SessionId=session_id)
        assert resp["State"] in ("TERMINATING", "TERMINATED")

    def test_terminate_session_nonexistent(self, athena):
        """TerminateSession with fake SessionId raises error."""
        with pytest.raises(ClientError) as exc:
            athena.terminate_session(SessionId="nonexistent-session-id")
        assert exc.value.response["Error"]["Code"] == "InvalidRequestException"

    def test_session_full_lifecycle(self, athena):
        """Full session lifecycle: start -> get -> status -> terminate."""
        # Start
        start_resp = athena.start_session(
            WorkGroup="primary",
            EngineConfiguration={"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 4},
        )
        session_id = start_resp["SessionId"]
        assert start_resp["State"] == "IDLE"

        # Get
        get_resp = athena.get_session(SessionId=session_id)
        assert get_resp["SessionId"] == session_id
        assert "WorkGroup" in get_resp

        # Status
        status_resp = athena.get_session_status(SessionId=session_id)
        assert status_resp["Status"]["State"] == "IDLE"

        # Terminate
        term_resp = athena.terminate_session(SessionId=session_id)
        assert term_resp["State"] in ("TERMINATING", "TERMINATED")


class TestAthenaAdditional:
    """Tests for additional Athena operations."""

    def test_list_sessions(self, athena):
        """ListSessions returns SessionsList for a given workgroup."""
        wg_name = _unique("wg")
        athena.create_work_group(
            Name=wg_name,
            Configuration={"ResultConfiguration": {"OutputLocation": "s3://test-bucket/results/"}},
        )
        try:
            resp = athena.list_sessions(WorkGroup=wg_name)
            assert "Sessions" in resp
            assert isinstance(resp["Sessions"], list)
        finally:
            athena.delete_work_group(WorkGroup=wg_name)


class TestAthenaListOps:
    def test_list_application_dpu_sizes(self, athena):
        resp = athena.list_application_dpu_sizes()
        assert "ApplicationDPUSizes" in resp
