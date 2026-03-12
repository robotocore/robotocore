"""Comprehensive unit tests for the ECS native provider.

Tests call the inner action functions directly with EcsStore instances,
avoiding the HTTP/async layer. This exercises the business logic in isolation.
"""

import json
import time

import pytest

from robotocore.services.ecs.provider import (
    EcsError,
    EcsStore,
    _create_cluster,
    _create_service,
    _create_task_set,
    _delete_attributes,
    _delete_cluster,
    _delete_service,
    _delete_task_definitions,
    _delete_task_set,
    _deregister_container_instance,
    _deregister_task_definition,
    _describe_clusters,
    _describe_container_instances,
    _describe_services,
    _describe_task_definition,
    _describe_task_sets,
    _describe_tasks,
    _error,
    _find_resource_by_arn,
    _get_store,
    _json_response,
    _list_attributes,
    _list_clusters,
    _list_container_instances,
    _list_services,
    _list_tags_for_resource,
    _list_task_definition_families,
    _list_task_definitions,
    _list_tasks,
    _put_attributes,
    _put_cluster_capacity_providers,
    _register_container_instance,
    _register_task_definition,
    _require_cluster,
    _resolve_cluster_name,
    _resolve_task_definition,
    _run_task,
    _stop_task,
    _stores,
    _tag_resource,
    _untag_resource,
    _update_container_instances_state,
    _update_service,
    _update_task_set,
)

REGION = "us-east-1"
ACCOUNT = "123456789012"


@pytest.fixture(autouse=True)
def _clear_stores():
    _stores.clear()
    yield
    _stores.clear()


@pytest.fixture
def store() -> EcsStore:
    return _get_store(REGION, ACCOUNT)


def _make_cluster(store: EcsStore, name: str = "test-cluster") -> dict:
    return _create_cluster(store, {"clusterName": name}, REGION, ACCOUNT)


def _make_task_def(store: EcsStore, family: str = "web", **kwargs) -> dict:
    params = {
        "family": family,
        "containerDefinitions": [{"name": "app", "image": "nginx"}],
        **kwargs,
    }
    return _register_task_definition(store, params, REGION, ACCOUNT)


def _make_service(
    store: EcsStore,
    cluster: str = "test-cluster",
    name: str = "web-svc",
    td: str = "web",
) -> dict:
    return _create_service(
        store,
        {"cluster": cluster, "serviceName": name, "taskDefinition": td},
        REGION,
        ACCOUNT,
    )


# ===========================================================================
# EcsStore and _get_store
# ===========================================================================


class TestEcsStore:
    def test_store_initializes_empty(self, store: EcsStore):
        assert store.clusters == {}
        assert store.task_definitions == {}
        assert store.services == {}
        assert store.tasks == {}
        assert store.region == REGION
        assert store.account_id == ACCOUNT

    def test_get_store_returns_same_instance(self):
        s1 = _get_store(REGION, ACCOUNT)
        s2 = _get_store(REGION, ACCOUNT)
        assert s1 is s2

    def test_get_store_different_region(self):
        s1 = _get_store("us-east-1", ACCOUNT)
        s2 = _get_store("eu-west-1", ACCOUNT)
        assert s1 is not s2

    def test_get_store_different_account(self):
        s1 = _get_store(REGION, "111111111111")
        s2 = _get_store(REGION, "222222222222")
        assert s1 is not s2


# ===========================================================================
# EcsError
# ===========================================================================


class TestEcsError:
    def test_default_status_400(self):
        e = EcsError("SomeCode", "some message")
        assert e.code == "SomeCode"
        assert e.message == "some message"
        assert e.status == 400

    def test_custom_status(self):
        e = EcsError("NotFound", "gone", 404)
        assert e.status == 404


# ===========================================================================
# Response helpers
# ===========================================================================


class TestResponseHelpers:
    def test_json_response_status_and_content_type(self):
        resp = _json_response({"foo": "bar"})
        assert resp.status_code == 200
        assert resp.media_type == "application/x-amz-json-1.1"
        data = json.loads(resp.body)
        assert data["foo"] == "bar"

    def test_error_response(self):
        resp = _error("BadCode", "bad message", 400)
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "BadCode"
        assert data["message"] == "bad message"

    def test_json_response_serializes_timestamps(self):
        """time.time() floats should serialize via default=str."""
        resp = _json_response({"ts": time.time()})
        assert resp.status_code == 200
        # Should not raise
        json.loads(resp.body)


# ===========================================================================
# Helper functions
# ===========================================================================


class TestResolveClusterName:
    def test_plain_name(self):
        assert _resolve_cluster_name("my-cluster") == "my-cluster"

    def test_arn(self):
        arn = "arn:aws:ecs:us-east-1:123456789012:cluster/my-cluster"
        assert _resolve_cluster_name(arn) == "my-cluster"

    def test_partial_path(self):
        assert _resolve_cluster_name("some/path/cluster") == "cluster"


class TestRequireCluster:
    def test_raises_when_cluster_missing(self, store: EcsStore):
        with pytest.raises(EcsError) as exc_info:
            _require_cluster(store, "nonexistent")
        assert exc_info.value.code == "ClusterNotFoundException"
        assert exc_info.value.status == 404

    def test_passes_when_cluster_exists(self, store: EcsStore):
        _make_cluster(store, "c1")
        _require_cluster(store, "c1")  # should not raise


class TestResolveTaskDefinition:
    def test_resolve_by_family_returns_latest_active(self, store: EcsStore):
        _make_task_def(store, "web")
        _make_task_def(store, "web")
        td = _resolve_task_definition(store, "web")
        assert td is not None
        assert td["revision"] == 2

    def test_resolve_by_family_colon_revision(self, store: EcsStore):
        _make_task_def(store, "web")
        _make_task_def(store, "web")
        td = _resolve_task_definition(store, "web:1")
        assert td is not None
        assert td["revision"] == 1

    def test_resolve_by_arn(self, store: EcsStore):
        result = _make_task_def(store, "web")
        arn = result["taskDefinition"]["taskDefinitionArn"]
        td = _resolve_task_definition(store, arn)
        assert td is not None
        assert td["revision"] == 1

    def test_resolve_missing_family(self, store: EcsStore):
        td = _resolve_task_definition(store, "nonexistent")
        assert td is None

    def test_resolve_invalid_revision(self, store: EcsStore):
        _make_task_def(store, "web")
        td = _resolve_task_definition(store, "web:abc")
        assert td is None

    def test_resolve_missing_revision(self, store: EcsStore):
        _make_task_def(store, "web")
        td = _resolve_task_definition(store, "web:99")
        assert td is None

    def test_resolve_by_family_skips_inactive(self, store: EcsStore):
        _make_task_def(store, "web")
        _make_task_def(store, "web")
        # Deregister revision 2
        _deregister_task_definition(store, {"taskDefinition": "web:2"}, REGION, ACCOUNT)
        td = _resolve_task_definition(store, "web")
        assert td is not None
        assert td["revision"] == 1

    def test_resolve_returns_none_when_all_inactive(self, store: EcsStore):
        _make_task_def(store, "web")
        _deregister_task_definition(store, {"taskDefinition": "web:1"}, REGION, ACCOUNT)
        td = _resolve_task_definition(store, "web")
        assert td is None


class TestFindResourceByArn:
    def test_find_cluster(self, store: EcsStore):
        result = _make_cluster(store, "c1")
        arn = result["cluster"]["clusterArn"]
        resource = _find_resource_by_arn(store, arn)
        assert resource is not None
        assert resource["clusterName"] == "c1"

    def test_find_service(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        result = _make_service(store, "c1", "svc1", "web")
        arn = result["service"]["serviceArn"]
        resource = _find_resource_by_arn(store, arn)
        assert resource is not None
        assert resource["serviceName"] == "svc1"

    def test_find_task(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        result = _run_task(store, {"cluster": "c1", "taskDefinition": "web"}, REGION, ACCOUNT)
        arn = result["tasks"][0]["taskArn"]
        resource = _find_resource_by_arn(store, arn)
        assert resource is not None
        assert resource["lastStatus"] == "RUNNING"

    def test_find_task_definition(self, store: EcsStore):
        result = _make_task_def(store, "web")
        arn = result["taskDefinition"]["taskDefinitionArn"]
        resource = _find_resource_by_arn(store, arn)
        assert resource is not None
        assert resource["family"] == "web"

    def test_not_found(self, store: EcsStore):
        assert _find_resource_by_arn(store, "arn:aws:ecs:us-east-1:123:cluster/nope") is None


# ===========================================================================
# Cluster CRUD
# ===========================================================================


class TestCreateCluster:
    def test_basic_create(self, store: EcsStore):
        result = _make_cluster(store, "my-cluster")
        cluster = result["cluster"]
        assert cluster["clusterName"] == "my-cluster"
        assert cluster["status"] == "ACTIVE"
        assert "cluster/my-cluster" in cluster["clusterArn"]

    def test_default_name(self, store: EcsStore):
        result = _create_cluster(store, {}, REGION, ACCOUNT)
        assert result["cluster"]["clusterName"] == "default"

    def test_create_with_tags(self, store: EcsStore):
        tags = [{"key": "env", "value": "prod"}]
        result = _create_cluster(store, {"clusterName": "c1", "tags": tags}, REGION, ACCOUNT)
        assert result["cluster"]["tags"] == tags
        arn = result["cluster"]["clusterArn"]
        assert store.tags[arn] == tags

    def test_create_with_capacity_providers(self, store: EcsStore):
        result = _create_cluster(
            store,
            {"clusterName": "c1", "capacityProviders": ["FARGATE"]},
            REGION,
            ACCOUNT,
        )
        assert result["cluster"]["capacityProviders"] == ["FARGATE"]

    def test_create_initializes_counts(self, store: EcsStore):
        result = _make_cluster(store, "c1")
        c = result["cluster"]
        assert c["registeredContainerInstancesCount"] == 0
        assert c["runningTasksCount"] == 0
        assert c["pendingTasksCount"] == 0
        assert c["activeServicesCount"] == 0

    def test_create_initializes_service_and_task_dicts(self, store: EcsStore):
        _make_cluster(store, "c1")
        assert "c1" in store.services
        assert "c1" in store.tasks

    def test_overwrite_cluster(self, store: EcsStore):
        """Creating a cluster with the same name overwrites it."""
        _make_cluster(store, "c1")
        result = _create_cluster(
            store,
            {"clusterName": "c1", "settings": [{"name": "containerInsights", "value": "enabled"}]},
            REGION,
            ACCOUNT,
        )
        assert result["cluster"]["settings"][0]["name"] == "containerInsights"


class TestDescribeClusters:
    def test_by_name(self, store: EcsStore):
        _make_cluster(store, "c1")
        result = _describe_clusters(store, {"clusters": ["c1"]}, REGION, ACCOUNT)
        assert len(result["clusters"]) == 1
        assert result["clusters"][0]["clusterName"] == "c1"

    def test_by_arn(self, store: EcsStore):
        r = _make_cluster(store, "c1")
        arn = r["cluster"]["clusterArn"]
        result = _describe_clusters(store, {"clusters": [arn]}, REGION, ACCOUNT)
        assert len(result["clusters"]) == 1

    def test_missing_cluster_in_failures(self, store: EcsStore):
        result = _describe_clusters(store, {"clusters": ["nope"]}, REGION, ACCOUNT)
        assert len(result["clusters"]) == 0
        assert len(result["failures"]) == 1
        assert result["failures"][0]["reason"] == "MISSING"

    def test_mixed_found_and_missing(self, store: EcsStore):
        _make_cluster(store, "c1")
        result = _describe_clusters(store, {"clusters": ["c1", "c2"]}, REGION, ACCOUNT)
        assert len(result["clusters"]) == 1
        assert len(result["failures"]) == 1

    def test_dynamic_service_count(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _make_service(store, "c1", "svc1", "web")
        result = _describe_clusters(store, {"clusters": ["c1"]}, REGION, ACCOUNT)
        assert result["clusters"][0]["activeServicesCount"] == 1

    def test_dynamic_running_task_count(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _run_task(store, {"cluster": "c1", "taskDefinition": "web", "count": 3}, REGION, ACCOUNT)
        result = _describe_clusters(store, {"clusters": ["c1"]}, REGION, ACCOUNT)
        assert result["clusters"][0]["runningTasksCount"] == 3


class TestListClusters:
    def test_empty(self, store: EcsStore):
        result = _list_clusters(store, {}, REGION, ACCOUNT)
        assert result["clusterArns"] == []

    def test_multiple(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_cluster(store, "c2")
        result = _list_clusters(store, {}, REGION, ACCOUNT)
        assert len(result["clusterArns"]) == 2


class TestDeleteCluster:
    def test_delete_existing(self, store: EcsStore):
        _make_cluster(store, "c1")
        result = _delete_cluster(store, {"cluster": "c1"}, REGION, ACCOUNT)
        assert result["cluster"]["status"] == "INACTIVE"
        assert "c1" not in store.clusters

    def test_delete_by_arn(self, store: EcsStore):
        r = _make_cluster(store, "c1")
        arn = r["cluster"]["clusterArn"]
        result = _delete_cluster(store, {"cluster": arn}, REGION, ACCOUNT)
        assert result["cluster"]["status"] == "INACTIVE"

    def test_delete_nonexistent(self, store: EcsStore):
        with pytest.raises(EcsError) as exc_info:
            _delete_cluster(store, {"cluster": "nope"}, REGION, ACCOUNT)
        assert exc_info.value.code == "ClusterNotFoundException"

    def test_delete_cascades_services(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _make_service(store, "c1", "svc1", "web")
        _delete_cluster(store, {"cluster": "c1"}, REGION, ACCOUNT)
        assert "c1" not in store.services or len(store.services.get("c1", {})) == 0

    def test_delete_cascades_tasks(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _run_task(store, {"cluster": "c1", "taskDefinition": "web"}, REGION, ACCOUNT)
        _delete_cluster(store, {"cluster": "c1"}, REGION, ACCOUNT)
        assert "c1" not in store.tasks or len(store.tasks.get("c1", {})) == 0

    def test_delete_cascades_tags(self, store: EcsStore):
        r = _make_cluster(store, "c1")
        arn = r["cluster"]["clusterArn"]
        _tag_resource(
            store,
            {"resourceArn": arn, "tags": [{"key": "k", "value": "v"}]},
            REGION,
            ACCOUNT,
        )
        _delete_cluster(store, {"cluster": "c1"}, REGION, ACCOUNT)
        assert arn not in store.tags


# ===========================================================================
# Task Definitions
# ===========================================================================


class TestRegisterTaskDefinition:
    def test_basic_registration(self, store: EcsStore):
        result = _make_task_def(store, "web")
        td = result["taskDefinition"]
        assert td["family"] == "web"
        assert td["revision"] == 1
        assert td["status"] == "ACTIVE"
        assert "task-definition/web:1" in td["taskDefinitionArn"]

    def test_auto_increment_revision(self, store: EcsStore):
        _make_task_def(store, "web")
        result = _make_task_def(store, "web")
        assert result["taskDefinition"]["revision"] == 2

    def test_missing_family(self, store: EcsStore):
        with pytest.raises(EcsError) as exc_info:
            _register_task_definition(
                store,
                {"containerDefinitions": [{"name": "app", "image": "nginx"}]},
                REGION,
                ACCOUNT,
            )
        assert exc_info.value.code == "ClientException"

    def test_empty_family(self, store: EcsStore):
        with pytest.raises(EcsError):
            _register_task_definition(
                store,
                {"family": "", "containerDefinitions": []},
                REGION,
                ACCOUNT,
            )

    def test_defaults(self, store: EcsStore):
        result = _make_task_def(store, "web")
        td = result["taskDefinition"]
        assert td["cpu"] == "256"
        assert td["memory"] == "512"
        assert td["networkMode"] == "awsvpc"
        assert td["requiresCompatibilities"] == ["FARGATE"]

    def test_custom_cpu_memory(self, store: EcsStore):
        result = _make_task_def(store, "web", cpu="1024", memory="2048")
        td = result["taskDefinition"]
        assert td["cpu"] == "1024"
        assert td["memory"] == "2048"

    def test_with_tags(self, store: EcsStore):
        tags = [{"key": "env", "value": "prod"}]
        result = _make_task_def(store, "web", tags=tags)
        assert result["tags"] == tags
        arn = result["taskDefinition"]["taskDefinitionArn"]
        assert store.tags[arn] == tags

    def test_no_tags_means_no_store_entry(self, store: EcsStore):
        result = _make_task_def(store, "web")
        arn = result["taskDefinition"]["taskDefinitionArn"]
        assert arn not in store.tags


class TestDescribeTaskDefinition:
    def test_by_family_colon_revision(self, store: EcsStore):
        _make_task_def(store, "web")
        result = _describe_task_definition(store, {"taskDefinition": "web:1"}, REGION, ACCOUNT)
        assert result["taskDefinition"]["revision"] == 1

    def test_by_family_returns_latest(self, store: EcsStore):
        _make_task_def(store, "web")
        _make_task_def(store, "web")
        result = _describe_task_definition(store, {"taskDefinition": "web"}, REGION, ACCOUNT)
        assert result["taskDefinition"]["revision"] == 2

    def test_not_found(self, store: EcsStore):
        with pytest.raises(EcsError) as exc_info:
            _describe_task_definition(store, {"taskDefinition": "nope"}, REGION, ACCOUNT)
        assert exc_info.value.code == "ClientException"


class TestListTaskDefinitions:
    def test_empty(self, store: EcsStore):
        result = _list_task_definitions(store, {}, REGION, ACCOUNT)
        assert result["taskDefinitionArns"] == []

    def test_all_families(self, store: EcsStore):
        _make_task_def(store, "web")
        _make_task_def(store, "api")
        result = _list_task_definitions(store, {}, REGION, ACCOUNT)
        assert len(result["taskDefinitionArns"]) == 2

    def test_filter_by_family_prefix(self, store: EcsStore):
        _make_task_def(store, "web-frontend")
        _make_task_def(store, "web-backend")
        _make_task_def(store, "api")
        result = _list_task_definitions(store, {"familyPrefix": "web"}, REGION, ACCOUNT)
        assert len(result["taskDefinitionArns"]) == 2

    def test_filter_by_status(self, store: EcsStore):
        _make_task_def(store, "web")
        _deregister_task_definition(store, {"taskDefinition": "web:1"}, REGION, ACCOUNT)
        result = _list_task_definitions(store, {"status": "INACTIVE"}, REGION, ACCOUNT)
        assert len(result["taskDefinitionArns"]) == 1
        result_active = _list_task_definitions(store, {"status": "ACTIVE"}, REGION, ACCOUNT)
        assert len(result_active["taskDefinitionArns"]) == 0


class TestListTaskDefinitionFamilies:
    def test_empty(self, store: EcsStore):
        result = _list_task_definition_families(store, {}, REGION, ACCOUNT)
        assert result["families"] == []

    def test_lists_active_families(self, store: EcsStore):
        _make_task_def(store, "web")
        _make_task_def(store, "api")
        result = _list_task_definition_families(store, {}, REGION, ACCOUNT)
        assert sorted(result["families"]) == ["api", "web"]

    def test_filter_by_prefix(self, store: EcsStore):
        _make_task_def(store, "web-frontend")
        _make_task_def(store, "api")
        result = _list_task_definition_families(store, {"familyPrefix": "web"}, REGION, ACCOUNT)
        assert result["families"] == ["web-frontend"]

    def test_filter_inactive(self, store: EcsStore):
        _make_task_def(store, "web")
        _deregister_task_definition(store, {"taskDefinition": "web:1"}, REGION, ACCOUNT)
        result = _list_task_definition_families(store, {"status": "INACTIVE"}, REGION, ACCOUNT)
        assert result["families"] == ["web"]
        result_active = _list_task_definition_families(store, {"status": "ACTIVE"}, REGION, ACCOUNT)
        assert result_active["families"] == []

    def test_filter_all(self, store: EcsStore):
        _make_task_def(store, "web")
        _make_task_def(store, "api")
        _deregister_task_definition(store, {"taskDefinition": "api:1"}, REGION, ACCOUNT)
        result = _list_task_definition_families(store, {"status": "ALL"}, REGION, ACCOUNT)
        assert sorted(result["families"]) == ["api", "web"]

    def test_results_are_sorted(self, store: EcsStore):
        _make_task_def(store, "zebra")
        _make_task_def(store, "alpha")
        _make_task_def(store, "mid")
        result = _list_task_definition_families(store, {}, REGION, ACCOUNT)
        assert result["families"] == sorted(result["families"])


class TestDeregisterTaskDefinition:
    def test_deregister(self, store: EcsStore):
        _make_task_def(store, "web")
        result = _deregister_task_definition(store, {"taskDefinition": "web:1"}, REGION, ACCOUNT)
        assert result["taskDefinition"]["status"] == "INACTIVE"

    def test_deregister_not_found(self, store: EcsStore):
        with pytest.raises(EcsError) as exc_info:
            _deregister_task_definition(store, {"taskDefinition": "nope:1"}, REGION, ACCOUNT)
        assert exc_info.value.code == "ClientException"


class TestDeleteTaskDefinitions:
    def test_delete_batch(self, store: EcsStore):
        _make_task_def(store, "web")
        _make_task_def(store, "api")
        result = _delete_task_definitions(
            store, {"taskDefinitions": ["web:1", "api:1"]}, REGION, ACCOUNT
        )
        assert len(result["taskDefinitions"]) == 2
        assert all(td["status"] == "DELETE_IN_PROGRESS" for td in result["taskDefinitions"])

    def test_delete_with_failures(self, store: EcsStore):
        _make_task_def(store, "web")
        result = _delete_task_definitions(
            store, {"taskDefinitions": ["web:1", "nope:99"]}, REGION, ACCOUNT
        )
        assert len(result["taskDefinitions"]) == 1
        assert len(result["failures"]) == 1

    def test_delete_all_missing(self, store: EcsStore):
        result = _delete_task_definitions(store, {"taskDefinitions": ["nope:1"]}, REGION, ACCOUNT)
        assert len(result["taskDefinitions"]) == 0
        assert len(result["failures"]) == 1


# ===========================================================================
# Services
# ===========================================================================


class TestCreateService:
    def test_basic_create(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        result = _make_service(store, "c1", "svc1", "web")
        svc = result["service"]
        assert svc["serviceName"] == "svc1"
        assert svc["status"] == "ACTIVE"
        assert svc["desiredCount"] == 1  # default

    def test_missing_cluster(self, store: EcsStore):
        _make_task_def(store, "web")
        with pytest.raises(EcsError) as exc_info:
            _make_service(store, "nonexistent", "svc1", "web")
        assert exc_info.value.code == "ClusterNotFoundException"

    def test_missing_service_name(self, store: EcsStore):
        _make_cluster(store, "c1")
        with pytest.raises(EcsError) as exc_info:
            _create_service(
                store,
                {"cluster": "c1", "serviceName": "", "taskDefinition": "web"},
                REGION,
                ACCOUNT,
            )
        assert exc_info.value.code == "ClientException"

    def test_duplicate_service_name(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _make_service(store, "c1", "svc1", "web")
        with pytest.raises(EcsError) as exc_info:
            _make_service(store, "c1", "svc1", "web")
        assert exc_info.value.code == "ClientException"

    def test_service_with_tags(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        result = _create_service(
            store,
            {
                "cluster": "c1",
                "serviceName": "svc1",
                "taskDefinition": "web",
                "tags": [{"key": "team", "value": "backend"}],
            },
            REGION,
            ACCOUNT,
        )
        svc_arn = result["service"]["serviceArn"]
        assert store.tags[svc_arn] == [{"key": "team", "value": "backend"}]

    def test_service_arn_format(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        result = _make_service(store, "c1", "svc1", "web")
        arn = result["service"]["serviceArn"]
        assert "service/c1/svc1" in arn

    def test_custom_desired_count(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        result = _create_service(
            store,
            {"cluster": "c1", "serviceName": "svc1", "taskDefinition": "web", "desiredCount": 5},
            REGION,
            ACCOUNT,
        )
        assert result["service"]["desiredCount"] == 5


class TestDescribeServices:
    def test_found(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _make_service(store, "c1", "svc1", "web")
        result = _describe_services(store, {"cluster": "c1", "services": ["svc1"]}, REGION, ACCOUNT)
        assert len(result["services"]) == 1
        assert result["services"][0]["serviceName"] == "svc1"

    def test_missing(self, store: EcsStore):
        _make_cluster(store, "c1")
        result = _describe_services(store, {"cluster": "c1", "services": ["nope"]}, REGION, ACCOUNT)
        assert len(result["services"]) == 0
        assert len(result["failures"]) == 1

    def test_by_arn(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        r = _make_service(store, "c1", "svc1", "web")
        arn = r["service"]["serviceArn"]
        result = _describe_services(store, {"cluster": "c1", "services": [arn]}, REGION, ACCOUNT)
        assert len(result["services"]) == 1

    def test_default_cluster(self, store: EcsStore):
        _make_cluster(store, "default")
        _make_task_def(store, "web")
        _create_service(
            store,
            {"serviceName": "svc1", "taskDefinition": "web"},
            REGION,
            ACCOUNT,
        )
        result = _describe_services(store, {"services": ["svc1"]}, REGION, ACCOUNT)
        assert len(result["services"]) == 1


class TestListServices:
    def test_empty_cluster(self, store: EcsStore):
        _make_cluster(store, "c1")
        result = _list_services(store, {"cluster": "c1"}, REGION, ACCOUNT)
        assert result["serviceArns"] == []

    def test_with_services(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _make_service(store, "c1", "svc1", "web")
        _make_service(store, "c1", "svc2", "web")
        result = _list_services(store, {"cluster": "c1"}, REGION, ACCOUNT)
        assert len(result["serviceArns"]) == 2


class TestUpdateService:
    def test_update_desired_count(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _make_service(store, "c1", "svc1", "web")
        result = _update_service(
            store, {"cluster": "c1", "service": "svc1", "desiredCount": 10}, REGION, ACCOUNT
        )
        assert result["service"]["desiredCount"] == 10

    def test_update_task_definition(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _make_task_def(store, "web2")
        _make_service(store, "c1", "svc1", "web")
        result = _update_service(
            store, {"cluster": "c1", "service": "svc1", "taskDefinition": "web2"}, REGION, ACCOUNT
        )
        assert result["service"]["taskDefinition"] == "web2"

    def test_update_nonexistent(self, store: EcsStore):
        _make_cluster(store, "c1")
        with pytest.raises(EcsError) as exc_info:
            _update_service(
                store, {"cluster": "c1", "service": "nope", "desiredCount": 1}, REGION, ACCOUNT
            )
        assert exc_info.value.code == "ServiceNotFoundException"

    def test_update_by_arn(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        r = _make_service(store, "c1", "svc1", "web")
        arn = r["service"]["serviceArn"]
        result = _update_service(
            store, {"cluster": "c1", "service": arn, "desiredCount": 3}, REGION, ACCOUNT
        )
        assert result["service"]["desiredCount"] == 3


class TestDeleteService:
    def test_delete(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _make_service(store, "c1", "svc1", "web")
        result = _delete_service(store, {"cluster": "c1", "service": "svc1"}, REGION, ACCOUNT)
        assert result["service"]["status"] == "INACTIVE"
        assert result["service"]["desiredCount"] == 0
        # Service should be removed from store
        assert "svc1" not in store.services.get("c1", {})

    def test_delete_nonexistent(self, store: EcsStore):
        _make_cluster(store, "c1")
        with pytest.raises(EcsError) as exc_info:
            _delete_service(store, {"cluster": "c1", "service": "nope"}, REGION, ACCOUNT)
        assert exc_info.value.code == "ServiceNotFoundException"

    def test_delete_cleans_up_tags(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        r = _create_service(
            store,
            {
                "cluster": "c1",
                "serviceName": "svc1",
                "taskDefinition": "web",
                "tags": [{"key": "k", "value": "v"}],
            },
            REGION,
            ACCOUNT,
        )
        svc_arn = r["service"]["serviceArn"]
        assert svc_arn in store.tags
        _delete_service(store, {"cluster": "c1", "service": "svc1"}, REGION, ACCOUNT)
        assert svc_arn not in store.tags


# ===========================================================================
# Tasks
# ===========================================================================


class TestRunTask:
    def test_basic_run(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        result = _run_task(store, {"cluster": "c1", "taskDefinition": "web"}, REGION, ACCOUNT)
        assert len(result["tasks"]) == 1
        assert result["failures"] == []
        task = result["tasks"][0]
        assert task["lastStatus"] == "RUNNING"
        assert task["desiredStatus"] == "RUNNING"

    def test_run_count(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        result = _run_task(
            store, {"cluster": "c1", "taskDefinition": "web", "count": 3}, REGION, ACCOUNT
        )
        assert len(result["tasks"]) == 3

    def test_missing_cluster(self, store: EcsStore):
        _make_task_def(store, "web")
        with pytest.raises(EcsError) as exc_info:
            _run_task(store, {"cluster": "nope", "taskDefinition": "web"}, REGION, ACCOUNT)
        assert exc_info.value.code == "ClusterNotFoundException"

    def test_missing_task_definition(self, store: EcsStore):
        _make_cluster(store, "c1")
        with pytest.raises(EcsError) as exc_info:
            _run_task(store, {"cluster": "c1", "taskDefinition": "nope"}, REGION, ACCOUNT)
        assert exc_info.value.code == "ClientException"

    def test_task_has_containers(self, store: EcsStore):
        _make_cluster(store, "c1")
        _register_task_definition(
            store,
            {
                "family": "multi",
                "containerDefinitions": [
                    {"name": "app", "image": "nginx"},
                    {"name": "sidecar", "image": "envoy"},
                ],
            },
            REGION,
            ACCOUNT,
        )
        result = _run_task(store, {"cluster": "c1", "taskDefinition": "multi"}, REGION, ACCOUNT)
        containers = result["tasks"][0]["containers"]
        assert len(containers) == 2
        assert containers[0]["name"] == "app"
        assert containers[1]["name"] == "sidecar"

    def test_task_arn_format(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        result = _run_task(store, {"cluster": "c1", "taskDefinition": "web"}, REGION, ACCOUNT)
        arn = result["tasks"][0]["taskArn"]
        assert "task/c1/" in arn

    def test_task_inherits_cpu_memory(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web", cpu="1024", memory="2048")
        result = _run_task(store, {"cluster": "c1", "taskDefinition": "web"}, REGION, ACCOUNT)
        task = result["tasks"][0]
        assert task["cpu"] == "1024"
        assert task["memory"] == "2048"

    def test_task_with_tags(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        tags = [{"key": "env", "value": "test"}]
        result = _run_task(
            store,
            {"cluster": "c1", "taskDefinition": "web", "tags": tags},
            REGION,
            ACCOUNT,
        )
        task_arn = result["tasks"][0]["taskArn"]
        assert store.tags[task_arn] == tags

    def test_task_with_overrides(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        overrides = {"containerOverrides": [{"name": "app", "command": ["echo", "hi"]}]}
        result = _run_task(
            store,
            {"cluster": "c1", "taskDefinition": "web", "overrides": overrides},
            REGION,
            ACCOUNT,
        )
        assert result["tasks"][0]["overrides"] == overrides


class TestDescribeTasks:
    def test_found(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        run_result = _run_task(store, {"cluster": "c1", "taskDefinition": "web"}, REGION, ACCOUNT)
        task_arn = run_result["tasks"][0]["taskArn"]
        task_id = task_arn.split("/")[-1]
        result = _describe_tasks(store, {"cluster": "c1", "tasks": [task_id]}, REGION, ACCOUNT)
        assert len(result["tasks"]) == 1

    def test_by_arn(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        run_result = _run_task(store, {"cluster": "c1", "taskDefinition": "web"}, REGION, ACCOUNT)
        task_arn = run_result["tasks"][0]["taskArn"]
        result = _describe_tasks(store, {"cluster": "c1", "tasks": [task_arn]}, REGION, ACCOUNT)
        assert len(result["tasks"]) == 1

    def test_missing(self, store: EcsStore):
        _make_cluster(store, "c1")
        result = _describe_tasks(store, {"cluster": "c1", "tasks": ["nope"]}, REGION, ACCOUNT)
        assert len(result["tasks"]) == 0
        assert len(result["failures"]) == 1


class TestListTasks:
    def test_empty(self, store: EcsStore):
        _make_cluster(store, "c1")
        result = _list_tasks(store, {"cluster": "c1"}, REGION, ACCOUNT)
        assert result["taskArns"] == []

    def test_running_only(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        run_result = _run_task(
            store, {"cluster": "c1", "taskDefinition": "web", "count": 2}, REGION, ACCOUNT
        )
        # Stop one task
        task_id = run_result["tasks"][0]["taskArn"].split("/")[-1]
        _stop_task(store, {"cluster": "c1", "task": task_id}, REGION, ACCOUNT)

        result = _list_tasks(store, {"cluster": "c1"}, REGION, ACCOUNT)
        assert len(result["taskArns"]) == 1

    def test_stopped_filter(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        run_result = _run_task(store, {"cluster": "c1", "taskDefinition": "web"}, REGION, ACCOUNT)
        task_id = run_result["tasks"][0]["taskArn"].split("/")[-1]
        _stop_task(store, {"cluster": "c1", "task": task_id}, REGION, ACCOUNT)

        result = _list_tasks(store, {"cluster": "c1", "desiredStatus": "STOPPED"}, REGION, ACCOUNT)
        assert len(result["taskArns"]) == 1


class TestStopTask:
    def test_stop(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        run_result = _run_task(store, {"cluster": "c1", "taskDefinition": "web"}, REGION, ACCOUNT)
        task_id = run_result["tasks"][0]["taskArn"].split("/")[-1]
        result = _stop_task(store, {"cluster": "c1", "task": task_id}, REGION, ACCOUNT)
        assert result["task"]["lastStatus"] == "STOPPED"
        assert result["task"]["desiredStatus"] == "STOPPED"
        assert "stoppedAt" in result["task"]

    def test_stop_with_reason(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        run_result = _run_task(store, {"cluster": "c1", "taskDefinition": "web"}, REGION, ACCOUNT)
        task_id = run_result["tasks"][0]["taskArn"].split("/")[-1]
        result = _stop_task(
            store,
            {"cluster": "c1", "task": task_id, "reason": "OOM killed"},
            REGION,
            ACCOUNT,
        )
        assert result["task"]["stoppedReason"] == "OOM killed"

    def test_stop_default_reason(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        run_result = _run_task(store, {"cluster": "c1", "taskDefinition": "web"}, REGION, ACCOUNT)
        task_id = run_result["tasks"][0]["taskArn"].split("/")[-1]
        result = _stop_task(store, {"cluster": "c1", "task": task_id}, REGION, ACCOUNT)
        assert result["task"]["stoppedReason"] == "Task stopped by user"

    def test_stop_nonexistent(self, store: EcsStore):
        _make_cluster(store, "c1")
        with pytest.raises(EcsError) as exc_info:
            _stop_task(store, {"cluster": "c1", "task": "nope"}, REGION, ACCOUNT)
        assert exc_info.value.code == "InvalidParameterException"

    def test_stop_by_arn(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        run_result = _run_task(store, {"cluster": "c1", "taskDefinition": "web"}, REGION, ACCOUNT)
        task_arn = run_result["tasks"][0]["taskArn"]
        result = _stop_task(store, {"cluster": "c1", "task": task_arn}, REGION, ACCOUNT)
        assert result["task"]["lastStatus"] == "STOPPED"


# ===========================================================================
# Container Instances
# ===========================================================================


class TestRegisterContainerInstance:
    def test_basic_register(self, store: EcsStore):
        _make_cluster(store, "c1")
        result = _register_container_instance(store, {"cluster": "c1"}, REGION, ACCOUNT)
        ci = result["containerInstance"]
        assert ci["status"] == "ACTIVE"
        assert ci["agentConnected"] is True
        assert ci["runningTasksCount"] == 0

    def test_register_increments_count(self, store: EcsStore):
        _make_cluster(store, "c1")
        _register_container_instance(store, {"cluster": "c1"}, REGION, ACCOUNT)
        assert store.clusters["c1"]["registeredContainerInstancesCount"] == 1
        _register_container_instance(store, {"cluster": "c1"}, REGION, ACCOUNT)
        assert store.clusters["c1"]["registeredContainerInstancesCount"] == 2

    def test_register_with_doc(self, store: EcsStore):
        _make_cluster(store, "c1")
        doc = json.dumps({"instanceId": "i-abc123"})
        result = _register_container_instance(
            store, {"cluster": "c1", "instanceIdentityDocument": doc}, REGION, ACCOUNT
        )
        assert result["containerInstance"]["ec2InstanceId"] == "i-abc123"

    def test_missing_cluster(self, store: EcsStore):
        with pytest.raises(EcsError) as exc_info:
            _register_container_instance(store, {"cluster": "nope"}, REGION, ACCOUNT)
        assert exc_info.value.code == "ClusterNotFoundException"

    def test_with_tags(self, store: EcsStore):
        _make_cluster(store, "c1")
        tags = [{"key": "role", "value": "worker"}]
        result = _register_container_instance(
            store, {"cluster": "c1", "tags": tags}, REGION, ACCOUNT
        )
        ci_arn = result["containerInstance"]["containerInstanceArn"]
        assert store.tags[ci_arn] == tags

    def test_default_resources(self, store: EcsStore):
        _make_cluster(store, "c1")
        result = _register_container_instance(store, {"cluster": "c1"}, REGION, ACCOUNT)
        ci = result["containerInstance"]
        cpu = next(r for r in ci["registeredResources"] if r["name"] == "CPU")
        assert cpu["integerValue"] == 2048


class TestDeregisterContainerInstance:
    def test_deregister(self, store: EcsStore):
        _make_cluster(store, "c1")
        reg = _register_container_instance(store, {"cluster": "c1"}, REGION, ACCOUNT)
        ci_arn = reg["containerInstance"]["containerInstanceArn"]
        ci_id = ci_arn.split("/")[-1]

        result = _deregister_container_instance(
            store, {"cluster": "c1", "containerInstance": ci_id}, REGION, ACCOUNT
        )
        assert result["containerInstance"]["status"] == "INACTIVE"
        assert result["containerInstance"]["agentConnected"] is False
        assert store.clusters["c1"]["registeredContainerInstancesCount"] == 0

    def test_deregister_by_arn(self, store: EcsStore):
        _make_cluster(store, "c1")
        reg = _register_container_instance(store, {"cluster": "c1"}, REGION, ACCOUNT)
        ci_arn = reg["containerInstance"]["containerInstanceArn"]

        result = _deregister_container_instance(
            store, {"cluster": "c1", "containerInstance": ci_arn}, REGION, ACCOUNT
        )
        assert result["containerInstance"]["status"] == "INACTIVE"

    def test_deregister_nonexistent(self, store: EcsStore):
        _make_cluster(store, "c1")
        with pytest.raises(EcsError) as exc_info:
            _deregister_container_instance(
                store, {"cluster": "c1", "containerInstance": "nope"}, REGION, ACCOUNT
            )
        assert exc_info.value.code == "InvalidParameterException"

    def test_deregister_cleans_up_tags(self, store: EcsStore):
        _make_cluster(store, "c1")
        reg = _register_container_instance(
            store,
            {"cluster": "c1", "tags": [{"key": "k", "value": "v"}]},
            REGION,
            ACCOUNT,
        )
        ci_arn = reg["containerInstance"]["containerInstanceArn"]
        ci_id = ci_arn.split("/")[-1]
        assert ci_arn in store.tags
        _deregister_container_instance(
            store, {"cluster": "c1", "containerInstance": ci_id}, REGION, ACCOUNT
        )
        assert ci_arn not in store.tags


class TestDescribeContainerInstances:
    def test_found(self, store: EcsStore):
        _make_cluster(store, "c1")
        reg = _register_container_instance(store, {"cluster": "c1"}, REGION, ACCOUNT)
        ci_id = reg["containerInstance"]["containerInstanceArn"].split("/")[-1]
        result = _describe_container_instances(
            store, {"cluster": "c1", "containerInstances": [ci_id]}, REGION, ACCOUNT
        )
        assert len(result["containerInstances"]) == 1

    def test_missing(self, store: EcsStore):
        result = _describe_container_instances(
            store, {"cluster": "c1", "containerInstances": ["nope"]}, REGION, ACCOUNT
        )
        assert len(result["containerInstances"]) == 0
        assert len(result["failures"]) == 1


class TestUpdateContainerInstancesState:
    def test_update_to_draining(self, store: EcsStore):
        _make_cluster(store, "c1")
        reg = _register_container_instance(store, {"cluster": "c1"}, REGION, ACCOUNT)
        ci_id = reg["containerInstance"]["containerInstanceArn"].split("/")[-1]

        result = _update_container_instances_state(
            store,
            {"cluster": "c1", "containerInstances": [ci_id], "status": "DRAINING"},
            REGION,
            ACCOUNT,
        )
        assert len(result["containerInstances"]) == 1
        assert result["containerInstances"][0]["status"] == "DRAINING"

    def test_update_missing(self, store: EcsStore):
        _make_cluster(store, "c1")
        result = _update_container_instances_state(
            store,
            {"cluster": "c1", "containerInstances": ["nope"], "status": "DRAINING"},
            REGION,
            ACCOUNT,
        )
        assert len(result["failures"]) == 1

    def test_missing_cluster(self, store: EcsStore):
        with pytest.raises(EcsError):
            _update_container_instances_state(
                store,
                {"cluster": "nope", "containerInstances": ["ci1"], "status": "DRAINING"},
                REGION,
                ACCOUNT,
            )


class TestListContainerInstances:
    def test_empty(self, store: EcsStore):
        _make_cluster(store, "c1")
        result = _list_container_instances(store, {"cluster": "c1"}, REGION, ACCOUNT)
        assert result["containerInstanceArns"] == []

    def test_with_instances(self, store: EcsStore):
        _make_cluster(store, "c1")
        _register_container_instance(store, {"cluster": "c1"}, REGION, ACCOUNT)
        _register_container_instance(store, {"cluster": "c1"}, REGION, ACCOUNT)
        result = _list_container_instances(store, {"cluster": "c1"}, REGION, ACCOUNT)
        assert len(result["containerInstanceArns"]) == 2


# ===========================================================================
# Task Sets
# ===========================================================================


class TestCreateTaskSet:
    def test_basic_create(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _make_service(store, "c1", "svc1", "web")
        result = _create_task_set(
            store,
            {"cluster": "c1", "service": "svc1", "taskDefinition": "web"},
            REGION,
            ACCOUNT,
        )
        ts = result["taskSet"]
        assert ts["status"] == "ACTIVE"
        assert "taskSetArn" in ts

    def test_missing_service(self, store: EcsStore):
        _make_cluster(store, "c1")
        with pytest.raises(EcsError) as exc_info:
            _create_task_set(
                store,
                {"cluster": "c1", "service": "nope", "taskDefinition": "web"},
                REGION,
                ACCOUNT,
            )
        assert exc_info.value.code == "ServiceNotFoundException"

    def test_with_scale(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _make_service(store, "c1", "svc1", "web")
        scale = {"value": 50.0, "unit": "PERCENT"}
        result = _create_task_set(
            store,
            {"cluster": "c1", "service": "svc1", "taskDefinition": "web", "scale": scale},
            REGION,
            ACCOUNT,
        )
        assert result["taskSet"]["scale"] == scale


class TestDescribeTaskSets:
    def test_describe_all(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _make_service(store, "c1", "svc1", "web")
        _create_task_set(
            store,
            {"cluster": "c1", "service": "svc1", "taskDefinition": "web"},
            REGION,
            ACCOUNT,
        )
        result = _describe_task_sets(store, {"cluster": "c1", "service": "svc1"}, REGION, ACCOUNT)
        assert len(result["taskSets"]) == 1

    def test_describe_by_id(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _make_service(store, "c1", "svc1", "web")
        ts_result = _create_task_set(
            store,
            {"cluster": "c1", "service": "svc1", "taskDefinition": "web"},
            REGION,
            ACCOUNT,
        )
        ts_id = ts_result["taskSet"]["id"]
        result = _describe_task_sets(
            store, {"cluster": "c1", "service": "svc1", "taskSets": [ts_id]}, REGION, ACCOUNT
        )
        assert len(result["taskSets"]) == 1

    def test_missing_service(self, store: EcsStore):
        _make_cluster(store, "c1")
        with pytest.raises(EcsError):
            _describe_task_sets(store, {"cluster": "c1", "service": "nope"}, REGION, ACCOUNT)


class TestUpdateTaskSet:
    def test_update_scale(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _make_service(store, "c1", "svc1", "web")
        ts = _create_task_set(
            store,
            {"cluster": "c1", "service": "svc1", "taskDefinition": "web"},
            REGION,
            ACCOUNT,
        )
        ts_id = ts["taskSet"]["id"]
        new_scale = {"value": 25.0, "unit": "PERCENT"}
        result = _update_task_set(
            store,
            {"cluster": "c1", "service": "svc1", "taskSet": ts_id, "scale": new_scale},
            REGION,
            ACCOUNT,
        )
        assert result["taskSet"]["scale"] == new_scale

    def test_update_nonexistent(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _make_service(store, "c1", "svc1", "web")
        with pytest.raises(EcsError) as exc_info:
            _update_task_set(
                store,
                {"cluster": "c1", "service": "svc1", "taskSet": "nope", "scale": {}},
                REGION,
                ACCOUNT,
            )
        assert exc_info.value.code == "InvalidParameterException"


class TestDeleteTaskSet:
    def test_delete(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _make_service(store, "c1", "svc1", "web")
        ts = _create_task_set(
            store,
            {"cluster": "c1", "service": "svc1", "taskDefinition": "web"},
            REGION,
            ACCOUNT,
        )
        ts_id = ts["taskSet"]["id"]
        result = _delete_task_set(
            store,
            {"cluster": "c1", "service": "svc1", "taskSet": ts_id},
            REGION,
            ACCOUNT,
        )
        assert result["taskSet"]["status"] == "INACTIVE"

    def test_delete_nonexistent(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _make_service(store, "c1", "svc1", "web")
        with pytest.raises(EcsError):
            _delete_task_set(
                store,
                {"cluster": "c1", "service": "svc1", "taskSet": "nope"},
                REGION,
                ACCOUNT,
            )

    def test_delete_cleans_up_tags(self, store: EcsStore):
        _make_cluster(store, "c1")
        _make_task_def(store, "web")
        _make_service(store, "c1", "svc1", "web")
        ts = _create_task_set(
            store,
            {
                "cluster": "c1",
                "service": "svc1",
                "taskDefinition": "web",
                "tags": [{"key": "k", "value": "v"}],
            },
            REGION,
            ACCOUNT,
        )
        ts_arn = ts["taskSet"]["taskSetArn"]
        ts_id = ts["taskSet"]["id"]
        assert ts_arn in store.tags
        _delete_task_set(
            store,
            {"cluster": "c1", "service": "svc1", "taskSet": ts_id},
            REGION,
            ACCOUNT,
        )
        assert ts_arn not in store.tags


# ===========================================================================
# Attributes
# ===========================================================================


class TestPutAttributes:
    def test_basic_put(self, store: EcsStore):
        _make_cluster(store, "c1")
        attrs = [{"name": "custom", "value": "val", "targetType": "container-instance"}]
        result = _put_attributes(store, {"cluster": "c1", "attributes": attrs}, REGION, ACCOUNT)
        assert result["attributes"] == attrs

    def test_replace_existing(self, store: EcsStore):
        _make_cluster(store, "c1")
        attrs1 = [{"name": "custom", "value": "v1", "targetType": "container-instance"}]
        _put_attributes(store, {"cluster": "c1", "attributes": attrs1}, REGION, ACCOUNT)
        attrs2 = [{"name": "custom", "value": "v2", "targetType": "container-instance"}]
        _put_attributes(store, {"cluster": "c1", "attributes": attrs2}, REGION, ACCOUNT)
        assert len(store.attributes["c1"]) == 1
        assert store.attributes["c1"][0]["value"] == "v2"

    def test_missing_cluster(self, store: EcsStore):
        with pytest.raises(EcsError):
            _put_attributes(
                store,
                {"cluster": "nope", "attributes": [{"name": "a"}]},
                REGION,
                ACCOUNT,
            )


class TestDeleteAttributes:
    def test_delete(self, store: EcsStore):
        _make_cluster(store, "c1")
        attrs = [{"name": "custom", "value": "v1", "targetType": "ci"}]
        _put_attributes(store, {"cluster": "c1", "attributes": attrs}, REGION, ACCOUNT)
        _delete_attributes(
            store,
            {"cluster": "c1", "attributes": [{"name": "custom", "targetType": "ci"}]},
            REGION,
            ACCOUNT,
        )
        assert len(store.attributes["c1"]) == 0

    def test_delete_nonexistent(self, store: EcsStore):
        _make_cluster(store, "c1")
        result = _delete_attributes(
            store,
            {"cluster": "c1", "attributes": [{"name": "nope"}]},
            REGION,
            ACCOUNT,
        )
        # Should not error
        assert result["attributes"] == [{"name": "nope"}]


class TestListAttributes:
    def test_list_all(self, store: EcsStore):
        _make_cluster(store, "c1")
        attrs = [
            {"name": "a1", "value": "v1", "targetType": "container-instance"},
            {"name": "a2", "value": "v2", "targetType": "container-instance"},
        ]
        _put_attributes(store, {"cluster": "c1", "attributes": attrs}, REGION, ACCOUNT)
        result = _list_attributes(
            store, {"cluster": "c1", "targetType": "container-instance"}, REGION, ACCOUNT
        )
        assert len(result["attributes"]) == 2

    def test_filter_by_name(self, store: EcsStore):
        _make_cluster(store, "c1")
        attrs = [
            {"name": "a1", "value": "v1", "targetType": "ci"},
            {"name": "a2", "value": "v2", "targetType": "ci"},
        ]
        _put_attributes(store, {"cluster": "c1", "attributes": attrs}, REGION, ACCOUNT)
        result = _list_attributes(
            store, {"cluster": "c1", "targetType": "ci", "attributeName": "a1"}, REGION, ACCOUNT
        )
        assert len(result["attributes"]) == 1
        assert result["attributes"][0]["name"] == "a1"

    def test_filter_by_target_type(self, store: EcsStore):
        _make_cluster(store, "c1")
        attrs = [
            {"name": "a1", "targetType": "container-instance"},
            {"name": "a2", "targetType": "task"},
        ]
        _put_attributes(store, {"cluster": "c1", "attributes": attrs}, REGION, ACCOUNT)
        result = _list_attributes(store, {"cluster": "c1", "targetType": "task"}, REGION, ACCOUNT)
        assert len(result["attributes"]) == 1


# ===========================================================================
# Tagging
# ===========================================================================


class TestTagResource:
    def test_add_tags(self, store: EcsStore):
        r = _make_cluster(store, "c1")
        arn = r["cluster"]["clusterArn"]
        _tag_resource(
            store,
            {"resourceArn": arn, "tags": [{"key": "env", "value": "prod"}]},
            REGION,
            ACCOUNT,
        )
        assert len(store.tags[arn]) == 1

    def test_overwrite_tag(self, store: EcsStore):
        r = _make_cluster(store, "c1")
        arn = r["cluster"]["clusterArn"]
        _tag_resource(
            store,
            {"resourceArn": arn, "tags": [{"key": "env", "value": "dev"}]},
            REGION,
            ACCOUNT,
        )
        _tag_resource(
            store,
            {"resourceArn": arn, "tags": [{"key": "env", "value": "prod"}]},
            REGION,
            ACCOUNT,
        )
        assert len(store.tags[arn]) == 1
        assert store.tags[arn][0]["value"] == "prod"

    def test_syncs_inline_tags(self, store: EcsStore):
        r = _make_cluster(store, "c1")
        arn = r["cluster"]["clusterArn"]
        _tag_resource(
            store,
            {"resourceArn": arn, "tags": [{"key": "env", "value": "prod"}]},
            REGION,
            ACCOUNT,
        )
        assert store.clusters["c1"]["tags"][0]["value"] == "prod"


class TestUntagResource:
    def test_remove_tag(self, store: EcsStore):
        r = _make_cluster(store, "c1")
        arn = r["cluster"]["clusterArn"]
        _tag_resource(
            store,
            {
                "resourceArn": arn,
                "tags": [{"key": "env", "value": "prod"}, {"key": "team", "value": "x"}],
            },
            REGION,
            ACCOUNT,
        )
        _untag_resource(store, {"resourceArn": arn, "tagKeys": ["env"]}, REGION, ACCOUNT)
        assert len(store.tags[arn]) == 1
        assert store.tags[arn][0]["key"] == "team"

    def test_syncs_inline_tags(self, store: EcsStore):
        r = _make_cluster(store, "c1")
        arn = r["cluster"]["clusterArn"]
        _tag_resource(
            store,
            {"resourceArn": arn, "tags": [{"key": "env", "value": "prod"}]},
            REGION,
            ACCOUNT,
        )
        _untag_resource(store, {"resourceArn": arn, "tagKeys": ["env"]}, REGION, ACCOUNT)
        assert store.clusters["c1"]["tags"] == []


class TestListTagsForResource:
    def test_list(self, store: EcsStore):
        r = _make_cluster(store, "c1")
        arn = r["cluster"]["clusterArn"]
        _tag_resource(
            store,
            {"resourceArn": arn, "tags": [{"key": "env", "value": "prod"}]},
            REGION,
            ACCOUNT,
        )
        result = _list_tags_for_resource(store, {"resourceArn": arn}, REGION, ACCOUNT)
        assert len(result["tags"]) == 1

    def test_list_empty(self, store: EcsStore):
        result = _list_tags_for_resource(store, {"resourceArn": "arn:nope"}, REGION, ACCOUNT)
        assert result["tags"] == []


# ===========================================================================
# PutClusterCapacityProviders
# ===========================================================================


class TestPutClusterCapacityProviders:
    def test_set_capacity_providers(self, store: EcsStore):
        _make_cluster(store, "c1")
        result = _put_cluster_capacity_providers(
            store,
            {
                "cluster": "c1",
                "capacityProviders": ["FARGATE", "FARGATE_SPOT"],
                "defaultCapacityProviderStrategy": [{"capacityProvider": "FARGATE", "weight": 1}],
            },
            REGION,
            ACCOUNT,
        )
        assert result["cluster"]["capacityProviders"] == ["FARGATE", "FARGATE_SPOT"]
        assert len(result["cluster"]["defaultCapacityProviderStrategy"]) == 1

    def test_nonexistent_cluster(self, store: EcsStore):
        with pytest.raises(EcsError) as exc_info:
            _put_cluster_capacity_providers(
                store,
                {"cluster": "nope", "capacityProviders": []},
                REGION,
                ACCOUNT,
            )
        assert exc_info.value.code == "ClusterNotFoundException"


# ===========================================================================
# Full lifecycle: register TD -> create service -> run task -> stop task
# ===========================================================================


class TestTaskLifecycle:
    def test_full_lifecycle(self, store: EcsStore):
        # 1. Create cluster
        cluster_result = _make_cluster(store, "prod")
        assert cluster_result["cluster"]["status"] == "ACTIVE"

        # 2. Register task definition
        td_result = _register_task_definition(
            store,
            {
                "family": "webapp",
                "containerDefinitions": [
                    {"name": "web", "image": "nginx:latest"},
                    {"name": "api", "image": "myapi:v1"},
                ],
                "cpu": "512",
                "memory": "1024",
            },
            REGION,
            ACCOUNT,
        )
        assert td_result["taskDefinition"]["revision"] == 1

        # 3. Create service
        svc_result = _create_service(
            store,
            {
                "cluster": "prod",
                "serviceName": "webapp-svc",
                "taskDefinition": "webapp",
                "desiredCount": 2,
            },
            REGION,
            ACCOUNT,
        )
        assert svc_result["service"]["serviceName"] == "webapp-svc"

        # 4. Run tasks
        run_result = _run_task(
            store,
            {"cluster": "prod", "taskDefinition": "webapp", "count": 2},
            REGION,
            ACCOUNT,
        )
        assert len(run_result["tasks"]) == 2
        for task in run_result["tasks"]:
            assert task["lastStatus"] == "RUNNING"
            assert len(task["containers"]) == 2

        # 5. Verify dynamic counts
        desc = _describe_clusters(store, {"clusters": ["prod"]}, REGION, ACCOUNT)
        assert desc["clusters"][0]["runningTasksCount"] == 2
        assert desc["clusters"][0]["activeServicesCount"] == 1

        # 6. Stop one task
        task_id = run_result["tasks"][0]["taskArn"].split("/")[-1]
        stop_result = _stop_task(store, {"cluster": "prod", "task": task_id}, REGION, ACCOUNT)
        assert stop_result["task"]["lastStatus"] == "STOPPED"

        # 7. Verify running count dropped
        desc = _describe_clusters(store, {"clusters": ["prod"]}, REGION, ACCOUNT)
        assert desc["clusters"][0]["runningTasksCount"] == 1

        # 8. Update service to new revision
        _register_task_definition(
            store,
            {
                "family": "webapp",
                "containerDefinitions": [{"name": "web", "image": "nginx:v2"}],
            },
            REGION,
            ACCOUNT,
        )
        update_result = _update_service(
            store,
            {"cluster": "prod", "service": "webapp-svc", "taskDefinition": "webapp:2"},
            REGION,
            ACCOUNT,
        )
        assert update_result["service"]["taskDefinition"] == "webapp:2"

        # 9. Delete service
        del_svc = _delete_service(
            store, {"cluster": "prod", "service": "webapp-svc"}, REGION, ACCOUNT
        )
        assert del_svc["service"]["status"] == "INACTIVE"

        # 10. Delete cluster (cascades)
        del_cluster = _delete_cluster(store, {"cluster": "prod"}, REGION, ACCOUNT)
        assert del_cluster["cluster"]["status"] == "INACTIVE"
        assert "prod" not in store.clusters


class TestMultiRegionIsolation:
    def test_clusters_isolated_by_region(self):
        store_east = _get_store("us-east-1", ACCOUNT)
        store_west = _get_store("us-west-2", ACCOUNT)
        _create_cluster(store_east, {"clusterName": "shared"}, "us-east-1", ACCOUNT)
        result = _list_clusters(store_west, {}, "us-west-2", ACCOUNT)
        assert result["clusterArns"] == []

    def test_task_defs_isolated_by_account(self):
        store_a = _get_store(REGION, "111111111111")
        store_b = _get_store(REGION, "222222222222")
        _register_task_definition(
            store_a,
            {"family": "web", "containerDefinitions": [{"name": "app", "image": "nginx"}]},
            REGION,
            "111111111111",
        )
        result = _list_task_definitions(store_b, {}, REGION, "222222222222")
        assert result["taskDefinitionArns"] == []
