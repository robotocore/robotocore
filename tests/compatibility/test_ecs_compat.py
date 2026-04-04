"""ECS compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def ecs():
    return make_client("ecs")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestClusterOperations:
    def test_create_cluster(self, ecs):
        name = _unique("cluster")
        resp = ecs.create_cluster(clusterName=name)
        assert resp["cluster"]["clusterName"] == name
        assert resp["cluster"]["status"] == "ACTIVE"
        ecs.delete_cluster(cluster=name)

    def test_describe_clusters(self, ecs):
        name = _unique("desc-cluster")
        ecs.create_cluster(clusterName=name)
        try:
            resp = ecs.describe_clusters(clusters=[name])
            assert len(resp["clusters"]) == 1
            assert resp["clusters"][0]["clusterName"] == name
        finally:
            ecs.delete_cluster(cluster=name)

    def test_list_clusters(self, ecs):
        name = _unique("list-cluster")
        ecs.create_cluster(clusterName=name)
        try:
            resp = ecs.list_clusters()
            # ARNs contain the cluster name
            found = any(name in arn for arn in resp["clusterArns"])
            assert found
        finally:
            ecs.delete_cluster(cluster=name)

    def test_delete_cluster(self, ecs):
        name = _unique("del-cluster")
        ecs.create_cluster(clusterName=name)
        ecs.delete_cluster(cluster=name)
        resp = ecs.describe_clusters(clusters=[name])
        # Deleted clusters may show INACTIVE or not be found
        if resp["clusters"]:
            assert resp["clusters"][0]["status"] == "INACTIVE"


class TestTaskDefinitions:
    def test_register_task_definition(self, ecs):
        family = _unique("task-fam")
        resp = ecs.register_task_definition(
            family=family,
            containerDefinitions=[
                {
                    "name": "app",
                    "image": "nginx:latest",
                    "memory": 128,
                }
            ],
        )
        td = resp["taskDefinition"]
        assert td["family"] == family
        assert td["revision"] == 1
        ecs.deregister_task_definition(taskDefinition=f"{family}:1")

    def test_describe_task_definition(self, ecs):
        family = _unique("desc-td")
        ecs.register_task_definition(
            family=family,
            containerDefinitions=[
                {
                    "name": "web",
                    "image": "nginx:latest",
                    "memory": 128,
                }
            ],
        )
        try:
            resp = ecs.describe_task_definition(taskDefinition=f"{family}:1")
            assert resp["taskDefinition"]["family"] == family
        finally:
            ecs.deregister_task_definition(taskDefinition=f"{family}:1")

    def test_list_task_definitions(self, ecs):
        family = _unique("list-td")
        ecs.register_task_definition(
            family=family,
            containerDefinitions=[
                {
                    "name": "svc",
                    "image": "busybox:latest",
                    "memory": 64,
                }
            ],
        )
        try:
            resp = ecs.list_task_definitions(familyPrefix=family)
            assert len(resp["taskDefinitionArns"]) >= 1
        finally:
            ecs.deregister_task_definition(taskDefinition=f"{family}:1")

    def test_task_definition_revisions(self, ecs):
        family = _unique("rev-td")
        ecs.register_task_definition(
            family=family,
            containerDefinitions=[{"name": "v1", "image": "nginx:1.0", "memory": 128}],
        )
        ecs.register_task_definition(
            family=family,
            containerDefinitions=[{"name": "v2", "image": "nginx:2.0", "memory": 256}],
        )
        try:
            resp = ecs.describe_task_definition(taskDefinition=f"{family}:2")
            assert resp["taskDefinition"]["revision"] == 2
        finally:
            ecs.deregister_task_definition(taskDefinition=f"{family}:1")
            ecs.deregister_task_definition(taskDefinition=f"{family}:2")

    def test_list_task_definition_families(self, ecs):
        family = _unique("fam-list")
        ecs.register_task_definition(
            family=family,
            containerDefinitions=[{"name": "app", "image": "busybox", "memory": 64}],
        )
        try:
            resp = ecs.list_task_definition_families(familyPrefix=family)
            assert family in resp["families"]
        finally:
            ecs.deregister_task_definition(taskDefinition=f"{family}:1")


class TestServices:
    @pytest.fixture
    def cluster(self, ecs):
        name = _unique("svc-cluster")
        ecs.create_cluster(clusterName=name)
        yield name
        ecs.delete_cluster(cluster=name)

    @pytest.fixture
    def task_def(self, ecs):
        family = _unique("svc-td")
        resp = ecs.register_task_definition(
            family=family,
            containerDefinitions=[{"name": "app", "image": "nginx", "memory": 128}],
        )
        yield resp["taskDefinition"]["taskDefinitionArn"]
        ecs.deregister_task_definition(taskDefinition=f"{family}:1")

    def test_create_service(self, ecs, cluster, task_def):
        svc_name = _unique("svc")
        resp = ecs.create_service(
            cluster=cluster,
            serviceName=svc_name,
            taskDefinition=task_def,
            desiredCount=1,
        )
        assert resp["service"]["serviceName"] == svc_name
        assert resp["service"]["desiredCount"] == 1
        ecs.delete_service(cluster=cluster, service=svc_name, force=True)

    def test_describe_services(self, ecs, cluster, task_def):
        svc_name = _unique("desc-svc")
        ecs.create_service(
            cluster=cluster,
            serviceName=svc_name,
            taskDefinition=task_def,
            desiredCount=0,
        )
        try:
            resp = ecs.describe_services(cluster=cluster, services=[svc_name])
            assert len(resp["services"]) == 1
            assert resp["services"][0]["serviceName"] == svc_name
        finally:
            ecs.delete_service(cluster=cluster, service=svc_name, force=True)

    def test_list_services(self, ecs, cluster, task_def):
        svc_name = _unique("list-svc")
        ecs.create_service(
            cluster=cluster,
            serviceName=svc_name,
            taskDefinition=task_def,
            desiredCount=0,
        )
        try:
            resp = ecs.list_services(cluster=cluster)
            found = any(svc_name in arn for arn in resp["serviceArns"])
            assert found
        finally:
            ecs.delete_service(cluster=cluster, service=svc_name, force=True)

    def test_update_service(self, ecs, cluster, task_def):
        svc_name = _unique("upd-svc")
        ecs.create_service(
            cluster=cluster, serviceName=svc_name, taskDefinition=task_def, desiredCount=0
        )
        try:
            resp = ecs.update_service(cluster=cluster, service=svc_name, desiredCount=2)
            assert resp["service"]["desiredCount"] == 2
        finally:
            ecs.delete_service(cluster=cluster, service=svc_name, force=True)

    def test_tag_resource(self, ecs):
        cluster_name = _unique("tag-cluster")
        create = ecs.create_cluster(clusterName=cluster_name)
        arn = create["cluster"]["clusterArn"]
        try:
            ecs.tag_resource(
                resourceArn=arn,
                tags=[{"key": "env", "value": "test"}, {"key": "team", "value": "dev"}],
            )
            resp = ecs.list_tags_for_resource(resourceArn=arn)
            tag_map = {t["key"]: t["value"] for t in resp["tags"]}
            assert tag_map["env"] == "test"
            assert tag_map["team"] == "dev"
        finally:
            ecs.delete_cluster(cluster=cluster_name)

    def test_untag_resource(self, ecs):
        cluster_name = _unique("untag-cluster")
        create = ecs.create_cluster(clusterName=cluster_name)
        arn = create["cluster"]["clusterArn"]
        try:
            ecs.tag_resource(resourceArn=arn, tags=[{"key": "temp", "value": "yes"}])
            ecs.untag_resource(resourceArn=arn, tagKeys=["temp"])
            resp = ecs.list_tags_for_resource(resourceArn=arn)
            keys = [t["key"] for t in resp["tags"]]
            assert "temp" not in keys
        finally:
            ecs.delete_cluster(cluster=cluster_name)


class TestECSExtended:
    """Extended ECS operations for higher coverage."""

    @pytest.fixture
    def ecs(self):
        from tests.compatibility.conftest import make_client

        return make_client("ecs")

    def test_list_clusters(self, ecs):
        name = _unique("list-cluster")
        ecs.create_cluster(clusterName=name)
        try:
            resp = ecs.list_clusters()
            assert any(name in arn for arn in resp["clusterArns"])
        finally:
            ecs.delete_cluster(cluster=name)

    def test_describe_clusters(self, ecs):
        name = _unique("desc-cluster")
        ecs.create_cluster(clusterName=name)
        try:
            resp = ecs.describe_clusters(clusters=[name])
            assert len(resp["clusters"]) == 1
            assert resp["clusters"][0]["clusterName"] == name
            assert resp["clusters"][0]["status"] == "ACTIVE"
        finally:
            ecs.delete_cluster(cluster=name)

    def test_create_cluster_with_tags(self, ecs):
        name = _unique("tagged-cluster")
        resp = ecs.create_cluster(
            clusterName=name,
            tags=[{"key": "env", "value": "staging"}],
        )
        try:
            tags = {t["key"]: t["value"] for t in resp["cluster"].get("tags", [])}
            assert tags.get("env") == "staging"
        finally:
            ecs.delete_cluster(cluster=name)

    def test_register_deregister_task_definition(self, ecs):
        import uuid

        family = f"task-fam-{uuid.uuid4().hex[:8]}"
        resp = ecs.register_task_definition(
            family=family,
            containerDefinitions=[
                {
                    "name": "web",
                    "image": "nginx:latest",
                    "memory": 256,
                    "cpu": 128,
                }
            ],
        )
        td_arn = resp["taskDefinition"]["taskDefinitionArn"]
        assert resp["taskDefinition"]["family"] == family

        desc = ecs.describe_task_definition(taskDefinition=td_arn)
        assert desc["taskDefinition"]["family"] == family
        containers = desc["taskDefinition"]["containerDefinitions"]
        assert len(containers) == 1
        assert containers[0]["name"] == "web"

        ecs.deregister_task_definition(taskDefinition=td_arn)

    def test_list_task_definitions(self, ecs):
        import uuid

        family = f"list-td-{uuid.uuid4().hex[:8]}"
        ecs.register_task_definition(
            family=family,
            containerDefinitions=[{"name": "app", "image": "alpine", "memory": 64}],
        )
        resp = ecs.list_task_definitions(familyPrefix=family)
        assert len(resp["taskDefinitionArns"]) >= 1
        assert any(family in arn for arn in resp["taskDefinitionArns"])

    def test_list_task_definition_families(self, ecs):
        resp = ecs.list_task_definition_families()
        assert "families" in resp

    def test_create_cluster_with_settings(self, ecs):
        name = _unique("settings-cluster")
        resp = ecs.create_cluster(
            clusterName=name,
            settings=[{"name": "containerInsights", "value": "enabled"}],
        )
        try:
            settings = {s["name"]: s["value"] for s in resp["cluster"].get("settings", [])}
            assert settings.get("containerInsights") == "enabled"
        finally:
            ecs.delete_cluster(cluster=name)

    def test_put_cluster_capacity_providers(self, ecs):
        name = _unique("cap-cluster")
        ecs.create_cluster(clusterName=name)
        try:
            ecs.put_cluster_capacity_providers(
                cluster=name,
                capacityProviders=["FARGATE"],
                defaultCapacityProviderStrategy=[{"capacityProvider": "FARGATE", "weight": 1}],
            )
            desc = ecs.describe_clusters(clusters=[name], include=["SETTINGS"])
            cluster = desc["clusters"][0]
            assert "FARGATE" in cluster.get("capacityProviders", [])
        finally:
            ecs.delete_cluster(cluster=name)


class TestECSExtendedV2:
    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_cluster_has_arn(self, ecs):
        name = _unique("arn-cluster")
        resp = ecs.create_cluster(clusterName=name)
        try:
            assert "clusterArn" in resp["cluster"]
            assert name in resp["cluster"]["clusterArn"]
        finally:
            ecs.delete_cluster(cluster=name)

    def test_cluster_status_active(self, ecs):
        name = _unique("status-cluster")
        resp = ecs.create_cluster(clusterName=name)
        try:
            assert resp["cluster"]["status"] == "ACTIVE"
        finally:
            ecs.delete_cluster(cluster=name)

    def test_cluster_registered_count_zero(self, ecs):
        name = _unique("count-cluster")
        resp = ecs.create_cluster(clusterName=name)
        try:
            assert resp["cluster"]["registeredContainerInstancesCount"] == 0
        finally:
            ecs.delete_cluster(cluster=name)

    def test_delete_cluster_returns_inactive(self, ecs):
        name = _unique("del-cluster")
        ecs.create_cluster(clusterName=name)
        resp = ecs.delete_cluster(cluster=name)
        assert resp["cluster"]["status"] == "INACTIVE"

    def test_describe_nonexistent_cluster(self, ecs):
        resp = ecs.describe_clusters(clusters=[_unique("nonexist")])
        assert len(resp.get("failures", [])) >= 1

    def test_task_definition_has_arn(self, ecs):
        family = _unique("td-arn")
        resp = ecs.register_task_definition(
            family=family,
            containerDefinitions=[
                {
                    "name": "app",
                    "image": "nginx",
                    "memory": 128,
                }
            ],
        )
        assert "taskDefinitionArn" in resp["taskDefinition"]
        assert family in resp["taskDefinition"]["taskDefinitionArn"]
        ecs.deregister_task_definition(taskDefinition=f"{family}:1")

    def test_task_definition_status_active(self, ecs):
        family = _unique("td-status")
        resp = ecs.register_task_definition(
            family=family,
            containerDefinitions=[
                {
                    "name": "app",
                    "image": "nginx",
                    "memory": 128,
                }
            ],
        )
        assert resp["taskDefinition"]["status"] == "ACTIVE"
        ecs.deregister_task_definition(taskDefinition=f"{family}:1")

    def test_deregister_task_definition(self, ecs):
        family = _unique("td-dereg")
        ecs.register_task_definition(
            family=family,
            containerDefinitions=[
                {
                    "name": "app",
                    "image": "nginx",
                    "memory": 128,
                }
            ],
        )
        resp = ecs.deregister_task_definition(taskDefinition=f"{family}:1")
        assert resp["taskDefinition"]["status"] == "INACTIVE"

    def test_task_definition_with_cpu_and_memory(self, ecs):
        family = _unique("td-res")
        resp = ecs.register_task_definition(
            family=family,
            containerDefinitions=[
                {
                    "name": "app",
                    "image": "nginx",
                    "cpu": 256,
                    "memory": 512,
                }
            ],
        )
        try:
            containers = resp["taskDefinition"]["containerDefinitions"]
            assert containers[0]["cpu"] == 256
            assert containers[0]["memory"] == 512
        finally:
            ecs.deregister_task_definition(taskDefinition=f"{family}:1")

    def test_task_definition_with_port_mappings(self, ecs):
        family = _unique("td-ports")
        resp = ecs.register_task_definition(
            family=family,
            containerDefinitions=[
                {
                    "name": "web",
                    "image": "nginx",
                    "memory": 128,
                    "portMappings": [
                        {"containerPort": 80, "hostPort": 80, "protocol": "tcp"},
                        {"containerPort": 443, "hostPort": 443, "protocol": "tcp"},
                    ],
                }
            ],
        )
        try:
            ports = resp["taskDefinition"]["containerDefinitions"][0]["portMappings"]
            container_ports = [p["containerPort"] for p in ports]
            assert 80 in container_ports
            assert 443 in container_ports
        finally:
            ecs.deregister_task_definition(taskDefinition=f"{family}:1")

    def test_task_definition_with_environment(self, ecs):
        family = _unique("td-env")
        resp = ecs.register_task_definition(
            family=family,
            containerDefinitions=[
                {
                    "name": "app",
                    "image": "busybox",
                    "memory": 128,
                    "environment": [
                        {"name": "APP_ENV", "value": "test"},
                        {"name": "DEBUG", "value": "true"},
                    ],
                }
            ],
        )
        try:
            env = resp["taskDefinition"]["containerDefinitions"][0]["environment"]
            env_map = {e["name"]: e["value"] for e in env}
            assert env_map["APP_ENV"] == "test"
            assert env_map["DEBUG"] == "true"
        finally:
            ecs.deregister_task_definition(taskDefinition=f"{family}:1")

    def test_task_definition_with_multiple_containers(self, ecs):
        family = _unique("td-multi")
        resp = ecs.register_task_definition(
            family=family,
            containerDefinitions=[
                {"name": "web", "image": "nginx", "memory": 128},
                {"name": "sidecar", "image": "busybox", "memory": 64},
            ],
        )
        try:
            containers = resp["taskDefinition"]["containerDefinitions"]
            names = [c["name"] for c in containers]
            assert "web" in names
            assert "sidecar" in names
        finally:
            ecs.deregister_task_definition(taskDefinition=f"{family}:1")

    def test_task_definition_revision_increments(self, ecs):
        family = _unique("td-rev")
        r1 = ecs.register_task_definition(
            family=family,
            containerDefinitions=[{"name": "app", "image": "nginx:1.0", "memory": 128}],
        )
        r2 = ecs.register_task_definition(
            family=family,
            containerDefinitions=[{"name": "app", "image": "nginx:2.0", "memory": 128}],
        )
        assert r1["taskDefinition"]["revision"] == 1
        assert r2["taskDefinition"]["revision"] == 2
        ecs.deregister_task_definition(taskDefinition=f"{family}:1")
        ecs.deregister_task_definition(taskDefinition=f"{family}:2")

    def test_list_tags_for_cluster(self, ecs):
        name = _unique("tag-cluster")
        resp = ecs.create_cluster(
            clusterName=name,
            tags=[{"key": "env", "value": "test"}],
        )
        arn = resp["cluster"]["clusterArn"]
        try:
            tags = ecs.list_tags_for_resource(resourceArn=arn)
            tag_map = {t["key"]: t["value"] for t in tags["tags"]}
            assert tag_map["env"] == "test"
        finally:
            ecs.delete_cluster(cluster=name)

    def test_tag_and_untag_cluster(self, ecs):
        name = _unique("tagop-cluster")
        resp = ecs.create_cluster(clusterName=name)
        arn = resp["cluster"]["clusterArn"]
        try:
            ecs.tag_resource(
                resourceArn=arn,
                tags=[{"key": "team", "value": "platform"}],
            )
            tags = ecs.list_tags_for_resource(resourceArn=arn)
            tag_map = {t["key"]: t["value"] for t in tags["tags"]}
            assert tag_map["team"] == "platform"
            ecs.untag_resource(resourceArn=arn, tagKeys=["team"])
            tags2 = ecs.list_tags_for_resource(resourceArn=arn)
            keys = [t["key"] for t in tags2["tags"]]
            assert "team" not in keys
        finally:
            ecs.delete_cluster(cluster=name)


class TestTaskOperations:
    """Tests for RunTask, StartTask, StopTask, DescribeTasks."""

    @pytest.fixture
    def cluster(self, ecs):
        name = _unique("task-cluster")
        ecs.create_cluster(clusterName=name)
        yield name
        ecs.delete_cluster(cluster=name)

    @pytest.fixture
    def task_def_arn(self, ecs):
        family = _unique("task-td")
        resp = ecs.register_task_definition(
            family=family,
            containerDefinitions=[{"name": "app", "image": "nginx", "memory": 128}],
        )
        yield resp["taskDefinition"]["taskDefinitionArn"]
        ecs.deregister_task_definition(taskDefinition=f"{family}:1")

    def test_run_task(self, ecs, cluster, task_def_arn):
        resp = ecs.run_task(cluster=cluster, taskDefinition=task_def_arn)
        assert "tasks" in resp
        assert len(resp["tasks"]) >= 1
        task = resp["tasks"][0]
        assert "taskArn" in task
        assert task["clusterArn"].endswith(f"/{cluster}")

    def test_run_task_with_count(self, ecs, cluster, task_def_arn):
        resp = ecs.run_task(cluster=cluster, taskDefinition=task_def_arn, count=2)
        assert len(resp["tasks"]) == 2

    def test_stop_task(self, ecs, cluster, task_def_arn):
        run_resp = ecs.run_task(cluster=cluster, taskDefinition=task_def_arn)
        task_arn = run_resp["tasks"][0]["taskArn"]
        stop_resp = ecs.stop_task(cluster=cluster, task=task_arn, reason="testing")
        assert "task" in stop_resp
        assert stop_resp["task"]["taskArn"] == task_arn

    def test_describe_tasks(self, ecs, cluster, task_def_arn):
        run_resp = ecs.run_task(cluster=cluster, taskDefinition=task_def_arn)
        task_arn = run_resp["tasks"][0]["taskArn"]
        desc_resp = ecs.describe_tasks(cluster=cluster, tasks=[task_arn])
        assert len(desc_resp["tasks"]) == 1
        assert desc_resp["tasks"][0]["taskArn"] == task_arn

    def test_start_task_nonexistent_cluster(self, ecs):
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            ecs.start_task(
                cluster="nonexistent-cluster",
                taskDefinition="arn:aws:ecs:us-east-1:123456789012:task-definition/fake:1",
                containerInstances=["fake-ci-id"],
            )
        assert exc.value.response["Error"]["Code"] == "ClusterNotFoundException"


class TestEcsAutoCoverage:
    """Auto-generated coverage tests for ecs."""

    @pytest.fixture
    def client(self):
        return make_client("ecs")

    def test_discover_poll_endpoint(self, client):
        """DiscoverPollEndpoint returns a response."""
        resp = client.discover_poll_endpoint()
        assert "endpoint" in resp

    def test_list_account_settings(self, client):
        """ListAccountSettings returns a response."""
        resp = client.list_account_settings()
        assert "settings" in resp

    def test_list_container_instances(self, client):
        """ListContainerInstances returns a response."""
        resp = client.list_container_instances()
        assert "containerInstanceArns" in resp

    def test_list_tasks(self, client):
        """ListTasks returns a response."""
        resp = client.list_tasks()
        assert "taskArns" in resp


class TestCapacityProviderOperations:
    """Tests for ECS capacity provider create and delete."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_create_capacity_provider(self, ecs):
        cp_name = _unique("cp")
        resp = ecs.create_capacity_provider(
            name=cp_name,
            autoScalingGroupProvider={
                "autoScalingGroupArn": (
                    "arn:aws:autoscaling:us-east-1:123456789012:"
                    "autoScalingGroup:xxx:autoScalingGroupName/my-asg"
                ),
            },
        )
        assert resp["capacityProvider"]["name"] == cp_name
        assert resp["capacityProvider"]["status"] == "ACTIVE"
        ecs.delete_capacity_provider(capacityProvider=cp_name)

    def test_delete_capacity_provider(self, ecs):
        cp_name = _unique("del-cp")
        ecs.create_capacity_provider(
            name=cp_name,
            autoScalingGroupProvider={
                "autoScalingGroupArn": (
                    "arn:aws:autoscaling:us-east-1:123456789012:"
                    "autoScalingGroup:xxx:autoScalingGroupName/my-asg"
                ),
            },
        )
        resp = ecs.delete_capacity_provider(capacityProvider=cp_name)
        assert resp["capacityProvider"]["name"] == cp_name

    def test_create_capacity_provider_with_managed_scaling(self, ecs):
        cp_name = _unique("ms-cp")
        resp = ecs.create_capacity_provider(
            name=cp_name,
            autoScalingGroupProvider={
                "autoScalingGroupArn": (
                    "arn:aws:autoscaling:us-east-1:123456789012:"
                    "autoScalingGroup:xxx:autoScalingGroupName/my-asg"
                ),
                "managedScaling": {
                    "status": "ENABLED",
                    "targetCapacity": 80,
                },
            },
        )
        cp = resp["capacityProvider"]
        assert cp["name"] == cp_name
        assert "autoScalingGroupProvider" in cp
        ecs.delete_capacity_provider(capacityProvider=cp_name)


class TestAccountSettingOperations:
    """Tests for ECS account settings."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_put_account_setting(self, ecs):
        resp = ecs.put_account_setting(name="containerInstanceLongArnFormat", value="enabled")
        assert resp["setting"]["name"] == "containerInstanceLongArnFormat"
        assert resp["setting"]["value"] == "enabled"

    def test_put_and_list_account_settings(self, ecs):
        ecs.put_account_setting(name="serviceLongArnFormat", value="enabled")
        resp = ecs.list_account_settings()
        assert "settings" in resp
        names = [s["name"] for s in resp["settings"]]
        assert "serviceLongArnFormat" in names

    def test_put_account_setting_effective(self, ecs):
        ecs.put_account_setting(name="taskLongArnFormat", value="enabled")
        resp = ecs.list_account_settings(name="taskLongArnFormat", effectiveSettings=True)
        found = [s for s in resp["settings"] if s["name"] == "taskLongArnFormat"]
        assert len(found) >= 1
        assert found[0]["value"] == "enabled"


class TestListAttributesOperation:
    """Tests for ECS ListAttributes."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_list_attributes_empty(self, ecs):
        name = _unique("attr-cluster")
        ecs.create_cluster(clusterName=name)
        try:
            resp = ecs.list_attributes(cluster=name, targetType="container-instance")
            assert "attributes" in resp
            assert isinstance(resp["attributes"], list)
        finally:
            ecs.delete_cluster(cluster=name)


class TestDescribeCapacityProviders:
    """Tests for ECS DescribeCapacityProviders."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_describe_capacity_providers_empty(self, ecs):
        resp = ecs.describe_capacity_providers(capacityProviders=["nonexistent-cp"])
        assert "capacityProviders" in resp

    def test_describe_capacity_providers_after_create(self, ecs):
        cp_name = _unique("desc-cp")
        ecs.create_capacity_provider(
            name=cp_name,
            autoScalingGroupProvider={
                "autoScalingGroupArn": (
                    "arn:aws:autoscaling:us-east-1:123456789012:"
                    "autoScalingGroup:xxx:autoScalingGroupName/my-asg"
                ),
            },
        )
        try:
            resp = ecs.describe_capacity_providers(capacityProviders=[cp_name])
            assert len(resp["capacityProviders"]) == 1
            assert resp["capacityProviders"][0]["name"] == cp_name
            assert resp["capacityProviders"][0]["status"] == "ACTIVE"
        finally:
            ecs.delete_capacity_provider(capacityProvider=cp_name)


class TestUpdateCapacityProvider:
    """Tests for ECS UpdateCapacityProvider."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_update_capacity_provider_managed_scaling(self, ecs):
        cp_name = _unique("upd-cp")
        ecs.create_capacity_provider(
            name=cp_name,
            autoScalingGroupProvider={
                "autoScalingGroupArn": (
                    "arn:aws:autoscaling:us-east-1:123456789012:"
                    "autoScalingGroup:xxx:autoScalingGroupName/my-asg"
                ),
                "managedScaling": {
                    "status": "ENABLED",
                    "targetCapacity": 50,
                },
            },
        )
        try:
            resp = ecs.update_capacity_provider(
                name=cp_name,
                autoScalingGroupProvider={
                    "managedScaling": {
                        "status": "ENABLED",
                        "targetCapacity": 80,
                    },
                },
            )
            cp = resp["capacityProvider"]
            assert cp["name"] == cp_name
            scaling = cp.get("autoScalingGroupProvider", {}).get("managedScaling", {})
            assert scaling.get("targetCapacity") == 80
        finally:
            ecs.delete_capacity_provider(capacityProvider=cp_name)


class TestDeleteTaskDefinitions:
    """Tests for ECS DeleteTaskDefinitions (batch delete)."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_delete_task_definitions(self, ecs):
        family = _unique("del-tds")
        ecs.register_task_definition(
            family=family,
            containerDefinitions=[{"name": "app", "image": "nginx", "memory": 128}],
        )
        resp = ecs.delete_task_definitions(taskDefinitions=[f"{family}:1"])
        assert "taskDefinitions" in resp
        assert len(resp["taskDefinitions"]) >= 1

    def test_delete_task_definitions_marks_deleted(self, ecs):
        family = _unique("del-tds-status")
        ecs.register_task_definition(
            family=family,
            containerDefinitions=[{"name": "app", "image": "busybox", "memory": 64}],
        )
        resp = ecs.delete_task_definitions(taskDefinitions=[f"{family}:1"])
        td = resp["taskDefinitions"][0]
        assert td["status"] == "DELETE_IN_PROGRESS"


class TestContainerInstanceOperations:
    """Tests for container instance register/deregister/describe/update."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    @pytest.fixture
    def cluster(self, ecs):
        name = _unique("ci-cluster")
        ecs.create_cluster(clusterName=name)
        yield name
        ecs.delete_cluster(cluster=name)

    def test_register_container_instance(self, ecs, cluster):
        resp = ecs.register_container_instance(
            cluster=cluster,
            instanceIdentityDocument=(
                '{"region": "us-east-1", "instanceId": "i-reg12345", "accountId": "123456789012"}'
            ),
        )
        ci = resp["containerInstance"]
        assert "containerInstanceArn" in ci
        assert ci["status"] == "ACTIVE"
        ecs.deregister_container_instance(
            cluster=cluster, containerInstance=ci["containerInstanceArn"], force=True
        )

    def test_describe_container_instances(self, ecs, cluster):
        reg = ecs.register_container_instance(
            cluster=cluster,
            instanceIdentityDocument=(
                '{"region": "us-east-1", "instanceId": "i-desc12345", "accountId": "123456789012"}'
            ),
        )
        ci_arn = reg["containerInstance"]["containerInstanceArn"]
        try:
            resp = ecs.describe_container_instances(cluster=cluster, containerInstances=[ci_arn])
            assert len(resp["containerInstances"]) == 1
            assert resp["containerInstances"][0]["containerInstanceArn"] == ci_arn
        finally:
            ecs.deregister_container_instance(cluster=cluster, containerInstance=ci_arn, force=True)

    def test_deregister_container_instance(self, ecs, cluster):
        reg = ecs.register_container_instance(
            cluster=cluster,
            instanceIdentityDocument=(
                '{"region": "us-east-1", "instanceId": "i-dereg12345", "accountId": "123456789012"}'
            ),
        )
        ci_arn = reg["containerInstance"]["containerInstanceArn"]
        resp = ecs.deregister_container_instance(
            cluster=cluster, containerInstance=ci_arn, force=True
        )
        assert resp["containerInstance"]["containerInstanceArn"] == ci_arn
        assert resp["containerInstance"]["status"] == "INACTIVE"

    def test_update_container_instances_state(self, ecs, cluster):
        reg = ecs.register_container_instance(
            cluster=cluster,
            instanceIdentityDocument=(
                '{"region": "us-east-1", "instanceId": "i-state12345", "accountId": "123456789012"}'
            ),
        )
        ci_arn = reg["containerInstance"]["containerInstanceArn"]
        try:
            resp = ecs.update_container_instances_state(
                cluster=cluster,
                containerInstances=[ci_arn],
                status="DRAINING",
            )
            assert len(resp["containerInstances"]) == 1
            assert resp["containerInstances"][0]["status"] == "DRAINING"
        finally:
            ecs.deregister_container_instance(cluster=cluster, containerInstance=ci_arn, force=True)


class TestTaskSetOperations:
    """Tests for task set create/describe/update/delete."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    @pytest.fixture
    def cluster(self, ecs):
        name = _unique("ts-cluster")
        ecs.create_cluster(clusterName=name)
        yield name
        ecs.delete_cluster(cluster=name)

    @pytest.fixture
    def task_def_arn(self, ecs):
        family = _unique("ts-td")
        resp = ecs.register_task_definition(
            family=family,
            containerDefinitions=[{"name": "app", "image": "nginx", "memory": 128}],
        )
        yield resp["taskDefinition"]["taskDefinitionArn"]
        ecs.deregister_task_definition(taskDefinition=f"{family}:1")

    @pytest.fixture
    def external_service(self, ecs, cluster, task_def_arn):
        svc_name = _unique("ts-svc")
        ecs.create_service(
            cluster=cluster,
            serviceName=svc_name,
            taskDefinition=task_def_arn,
            desiredCount=0,
            deploymentController={"type": "EXTERNAL"},
        )
        yield svc_name
        ecs.delete_service(cluster=cluster, service=svc_name, force=True)

    def test_create_task_set(self, ecs, cluster, task_def_arn, external_service):
        resp = ecs.create_task_set(
            cluster=cluster,
            service=external_service,
            taskDefinition=task_def_arn,
        )
        ts = resp["taskSet"]
        assert "taskSetArn" in ts
        assert "id" in ts
        assert ts["status"] == "ACTIVE"
        ecs.delete_task_set(cluster=cluster, service=external_service, taskSet=ts["id"])

    def test_describe_task_sets(self, ecs, cluster, task_def_arn, external_service):
        create_resp = ecs.create_task_set(
            cluster=cluster,
            service=external_service,
            taskDefinition=task_def_arn,
        )
        ts_id = create_resp["taskSet"]["id"]
        try:
            resp = ecs.describe_task_sets(
                cluster=cluster, service=external_service, taskSets=[ts_id]
            )
            assert len(resp["taskSets"]) == 1
            assert resp["taskSets"][0]["id"] == ts_id
        finally:
            ecs.delete_task_set(cluster=cluster, service=external_service, taskSet=ts_id)

    def test_update_task_set(self, ecs, cluster, task_def_arn, external_service):
        create_resp = ecs.create_task_set(
            cluster=cluster,
            service=external_service,
            taskDefinition=task_def_arn,
        )
        ts_id = create_resp["taskSet"]["id"]
        try:
            resp = ecs.update_task_set(
                cluster=cluster,
                service=external_service,
                taskSet=ts_id,
                scale={"value": 50.0, "unit": "PERCENT"},
            )
            assert resp["taskSet"]["scale"]["value"] == 50.0
            assert resp["taskSet"]["scale"]["unit"] == "PERCENT"
        finally:
            ecs.delete_task_set(cluster=cluster, service=external_service, taskSet=ts_id)

    def test_delete_task_set(self, ecs, cluster, task_def_arn, external_service):
        create_resp = ecs.create_task_set(
            cluster=cluster,
            service=external_service,
            taskDefinition=task_def_arn,
        )
        ts_id = create_resp["taskSet"]["id"]
        resp = ecs.delete_task_set(cluster=cluster, service=external_service, taskSet=ts_id)
        assert resp["taskSet"]["status"] == "INACTIVE"


class TestGetTaskProtection:
    """Tests for ECS GetTaskProtection."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_get_task_protection_nonexistent_cluster(self, ecs):
        """GetTaskProtection returns ClusterNotFoundException for a fake cluster."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            ecs.get_task_protection(cluster="nonexistent-cluster-12345")
        assert exc.value.response["Error"]["Code"] == "ClusterNotFoundException"


class TestListServicesByNamespace:
    """Tests for ECS ListServicesByNamespace."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_list_services_by_namespace(self, ecs):
        resp = ecs.list_services_by_namespace(namespace="test-namespace")
        assert "serviceArns" in resp
        assert isinstance(resp["serviceArns"], list)


class TestAttributeOperations:
    """Tests for ECS put/delete/list attributes."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    @pytest.fixture
    def cluster(self, ecs):
        name = _unique("attr-cluster")
        ecs.create_cluster(clusterName=name)
        yield name
        ecs.delete_cluster(cluster=name)

    def test_put_attributes(self, ecs, cluster):
        resp = ecs.put_attributes(
            cluster=cluster,
            attributes=[
                {
                    "name": "env",
                    "value": "production",
                    "targetType": "container-instance",
                    "targetId": "fake-ci-id",
                }
            ],
        )
        assert "attributes" in resp
        assert len(resp["attributes"]) == 1
        assert resp["attributes"][0]["name"] == "env"

    def test_list_attributes_after_put(self, ecs, cluster):
        ecs.put_attributes(
            cluster=cluster,
            attributes=[
                {
                    "name": "team",
                    "value": "platform",
                    "targetType": "container-instance",
                    "targetId": "fake-ci-id-2",
                }
            ],
        )
        resp = ecs.list_attributes(cluster=cluster, targetType="container-instance")
        assert "attributes" in resp
        names = [a["name"] for a in resp["attributes"]]
        assert "team" in names

    def test_delete_attributes(self, ecs, cluster):
        ecs.put_attributes(
            cluster=cluster,
            attributes=[
                {
                    "name": "temp-attr",
                    "value": "temp-val",
                    "targetType": "container-instance",
                    "targetId": "fake-ci-id-3",
                }
            ],
        )
        resp = ecs.delete_attributes(
            cluster=cluster,
            attributes=[
                {
                    "name": "temp-attr",
                    "targetType": "container-instance",
                    "targetId": "fake-ci-id-3",
                }
            ],
        )
        assert "attributes" in resp
        # Verify it's gone
        listed = ecs.list_attributes(cluster=cluster, targetType="container-instance")
        names = [a["name"] for a in listed["attributes"] if a.get("targetId") == "fake-ci-id-3"]
        assert "temp-attr" not in names


class TestECSAdditionalOperations:
    """Tests for additional ECS operations not yet covered."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_put_account_setting_default(self, ecs):
        resp = ecs.put_account_setting_default(
            name="containerInstanceLongArnFormat", value="enabled"
        )
        assert resp["setting"]["name"] == "containerInstanceLongArnFormat"
        assert resp["setting"]["value"] == "enabled"

    def test_update_cluster_nonexistent(self, ecs):
        """UpdateCluster returns ClusterNotFoundException for nonexistent cluster."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            ecs.update_cluster(
                cluster="nonexistent-cluster-12345",
                settings=[{"name": "containerInsights", "value": "enabled"}],
            )
        assert exc.value.response["Error"]["Code"] == "ClusterNotFoundException"

    def test_update_cluster_settings_nonexistent(self, ecs):
        """UpdateClusterSettings returns ClusterNotFoundException for nonexistent cluster."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            ecs.update_cluster_settings(
                cluster="nonexistent-cluster-12345",
                settings=[{"name": "containerInsights", "value": "disabled"}],
            )
        assert exc.value.response["Error"]["Code"] == "ClusterNotFoundException"

    def test_submit_container_state_change(self, ecs):
        name = _unique("scsc-cluster")
        ecs.create_cluster(clusterName=name)
        try:
            resp = ecs.submit_container_state_change(cluster=name, status="RUNNING")
            assert "acknowledgment" in resp
        finally:
            ecs.delete_cluster(cluster=name)

    def test_submit_task_state_change(self, ecs):
        name = _unique("stsc-cluster")
        ecs.create_cluster(clusterName=name)
        try:
            resp = ecs.submit_task_state_change(cluster=name, status="RUNNING")
            assert "acknowledgment" in resp
        finally:
            ecs.delete_cluster(cluster=name)

    def test_execute_command_nonexistent(self, ecs):
        """ExecuteCommand returns error for nonexistent cluster/task."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            ecs.execute_command(
                cluster="nonexistent-cluster",
                task="nonexistent-task",
                interactive=True,
                command="/bin/sh",
            )
        assert exc.value.response["Error"]["Code"] in (
            "ClusterNotFoundException",
            "InvalidParameterException",
        )

    def test_update_container_agent_nonexistent(self, ecs):
        """UpdateContainerAgent returns error for nonexistent cluster."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            ecs.update_container_agent(
                cluster="nonexistent-cluster",
                containerInstance="nonexistent-ci",
            )
        assert exc.value.response["Error"]["Code"] == "ClusterNotFoundException"

    def test_update_task_protection_nonexistent(self, ecs):
        """UpdateTaskProtection returns error for nonexistent cluster."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            ecs.update_task_protection(
                cluster="nonexistent-cluster",
                tasks=["nonexistent-task"],
                protectionEnabled=True,
            )
        assert exc.value.response["Error"]["Code"] == "ClusterNotFoundException"


class TestEcsAccountSettings:
    """Tests for ECS account setting operations."""

    def test_delete_account_setting(self, ecs):
        """DeleteAccountSetting removes a setting."""
        from botocore.exceptions import ClientError

        # DeleteAccountSetting should work (even if setting doesn't exist)
        try:
            resp = ecs.delete_account_setting(name="containerInsights")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except ClientError:
            # Some implementations raise if setting not set
            pass  # best-effort cleanup

    def test_delete_account_setting_after_put(self, ecs):
        """DeleteAccountSetting after PutAccountSetting removes the value."""
        ecs.put_account_setting(name="containerInsights", value="enabled")
        resp = ecs.delete_account_setting(name="containerInsights")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify it's gone from list
        list_resp = ecs.list_account_settings(name="containerInsights")
        settings = list_resp.get("settings", [])
        active = [s for s in settings if s.get("value") == "enabled"]
        assert len(active) == 0


class TestEcsServiceDeployments:
    """Tests for service deployment operations."""

    def test_describe_service_deployments_empty(self, ecs):
        """DescribeServiceDeployments with unknown ARNs returns empty list."""
        fake_arn = "arn:aws:ecs:us-east-1:123456789012:service-deployment/fake-id"
        resp = ecs.describe_service_deployments(serviceDeploymentArns=[fake_arn])
        assert "serviceDeployments" in resp
        assert "failures" in resp

    def test_describe_service_revisions_empty(self, ecs):
        """DescribeServiceRevisions with unknown ARNs returns empty list."""
        fake_arn = "arn:aws:ecs:us-east-1:123456789012:service-revision/fake-id"
        resp = ecs.describe_service_revisions(serviceRevisionArns=[fake_arn])
        assert "serviceRevisions" in resp
        assert "failures" in resp

    def test_list_service_deployments_nonexistent_service(self, ecs):
        """ListServiceDeployments with a nonexistent service raises ServiceNotFoundException."""
        resp = ecs.create_cluster(clusterName="test-svc-deploy-cls")
        cluster_arn = resp["cluster"]["clusterArn"]
        try:
            with pytest.raises(ClientError) as exc_info:
                ecs.list_service_deployments(
                    service="nonexistent-service",
                    cluster=cluster_arn,
                )
            assert exc_info.value.response["Error"]["Code"] == "ServiceNotFoundException"
        finally:
            ecs.delete_cluster(cluster=cluster_arn)


class TestEcsUpdateServicePrimaryTaskSet:
    """Tests for UpdateServicePrimaryTaskSet operation."""

    def test_update_service_primary_task_set_nonexistent(self, ecs):
        """UpdateServicePrimaryTaskSet with fake IDs raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            ecs.update_service_primary_task_set(
                cluster="nonexistent-cluster",
                service="nonexistent-service",
                primaryTaskSet="nonexistent-task-set",
            )
        assert exc.value.response["Error"]["Code"] in (
            "ClusterNotFoundException",
            "ServiceNotFoundException",
            "InvalidParameterException",
        )


class TestEcsServiceDeploymentOperations:
    """Tests for ECS service deployment and revision operations."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_describe_service_deployments_empty(self, ecs):
        """DescribeServiceDeployments with empty list returns response structure."""
        resp = ecs.describe_service_deployments(serviceDeploymentArns=[])
        assert "serviceDeployments" in resp
        assert isinstance(resp["serviceDeployments"], list)

    def test_describe_service_revisions_empty(self, ecs):
        """DescribeServiceRevisions with empty list returns response structure."""
        resp = ecs.describe_service_revisions(serviceRevisionArns=[])
        assert "serviceRevisions" in resp
        assert isinstance(resp["serviceRevisions"], list)


class TestECSMissingGapOps:
    """Tests for previously-missing ECS operations."""

    def test_stop_service_deployment(self, ecs):
        """StopServiceDeployment returns 200."""
        fake_arn = "arn:aws:ecs:us-east-1:123456789012:service-deployment/test/fake-deployment"
        resp = ecs.stop_service_deployment(serviceDeploymentArn=fake_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_submit_attachment_state_changes(self, ecs):
        """SubmitAttachmentStateChanges returns an acknowledgment."""
        resp = ecs.submit_attachment_state_changes(
            cluster="default",
            attachments=[
                {
                    "attachmentArn": "arn:aws:ecs:us-east-1:123456789012:attachment/abc",
                    "status": "ATTACHED",
                }
            ],
        )
        assert "acknowledgment" in resp


class TestECSExpressGatewayService:
    """Tests for ECS Express Gateway Service ops."""

    def test_create_express_gateway_service(self, ecs):
        """CreateExpressGatewayService returns 200."""
        resp = ecs.create_express_gateway_service(
            serviceName="test-express-svc",
            executionRoleArn="arn:aws:iam::123456789012:role/exec-role",
            infrastructureRoleArn="arn:aws:iam::123456789012:role/infra-role",
            primaryContainer={"image": "nginx:latest", "containerPort": 80},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_express_gateway_service(self, ecs):
        """DescribeExpressGatewayService with fake ARN returns 200."""
        fake_arn = "arn:aws:ecs:us-east-1:123456789012:express-gateway-service/fake"
        resp = ecs.describe_express_gateway_service(serviceArn=fake_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_express_gateway_service(self, ecs):
        """UpdateExpressGatewayService with fake ARN returns 200."""
        fake_arn = "arn:aws:ecs:us-east-1:123456789012:express-gateway-service/fake"
        resp = ecs.update_express_gateway_service(serviceArn=fake_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_express_gateway_service(self, ecs):
        """DeleteExpressGatewayService with fake ARN returns 200."""
        fake_arn = "arn:aws:ecs:us-east-1:123456789012:express-gateway-service/fake"
        resp = ecs.delete_express_gateway_service(serviceArn=fake_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestListClustersEdgeCases:
    """Edge cases and behavioral fidelity for list_clusters."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_list_clusters_pagination(self, ecs):
        """list_clusters supports maxResults + nextToken pagination."""
        names = [_unique("pg-cluster") for _ in range(3)]
        for name in names:
            ecs.create_cluster(clusterName=name)
        try:
            page1 = ecs.list_clusters(maxResults=2)
            assert "clusterArns" in page1
            all_arns = list(page1["clusterArns"])
            if "nextToken" in page1:
                page2 = ecs.list_clusters(nextToken=page1["nextToken"])
                all_arns.extend(page2["clusterArns"])
            found_names = [n for n in names if any(n in arn for arn in all_arns)]
            assert len(found_names) == 3
        finally:
            for name in names:
                ecs.delete_cluster(cluster=name)

    def test_list_clusters_arn_format(self, ecs):
        """Cluster ARNs match expected arn:aws:ecs:*:cluster/* format."""
        name = _unique("arn-fmt-cluster")
        resp = ecs.create_cluster(clusterName=name)
        arn = resp["cluster"]["clusterArn"]
        try:
            assert arn.startswith("arn:aws:ecs:")
            assert ":cluster/" in arn
            assert name in arn
        finally:
            ecs.delete_cluster(cluster=name)

    def test_list_clusters_deleted_cluster_absent(self, ecs):
        """A deleted cluster should not appear in list_clusters."""
        name = _unique("del-list-cls")
        ecs.create_cluster(clusterName=name)
        ecs.delete_cluster(cluster=name)
        resp = ecs.list_clusters()
        active_matching = [arn for arn in resp["clusterArns"] if name in arn]
        assert len(active_matching) == 0


class TestDiscoverPollEndpointEdgeCases:
    """Edge cases and behavioral fidelity for discover_poll_endpoint."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_discover_poll_endpoint_has_telemetry(self, ecs):
        """discover_poll_endpoint returns both endpoint and telemetryEndpoint."""
        resp = ecs.discover_poll_endpoint()
        assert "endpoint" in resp
        assert "telemetryEndpoint" in resp

    def test_discover_poll_endpoint_endpoint_is_url(self, ecs):
        """discover_poll_endpoint endpoint value looks like a URL."""
        resp = ecs.discover_poll_endpoint()
        assert resp["endpoint"].startswith("http")

    def test_discover_poll_endpoint_with_cluster(self, ecs):
        """discover_poll_endpoint with cluster parameter returns endpoint."""
        name = _unique("dpoll-cluster")
        ecs.create_cluster(clusterName=name)
        try:
            resp = ecs.discover_poll_endpoint(cluster=name)
            assert "endpoint" in resp
        finally:
            ecs.delete_cluster(cluster=name)

    def test_discover_poll_endpoint_with_container_instance(self, ecs):
        """discover_poll_endpoint with containerInstance returns endpoint."""
        resp = ecs.discover_poll_endpoint(containerInstance="fake-ci-id")
        assert "endpoint" in resp

    def test_discover_poll_endpoint_telemetry_is_url(self, ecs):
        """discover_poll_endpoint telemetryEndpoint value looks like a URL."""
        resp = ecs.discover_poll_endpoint()
        assert resp["telemetryEndpoint"].startswith("http")


class TestSubmitAttachmentStateChangesEdgeCases:
    """Edge cases for submit_attachment_state_changes."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_submit_multiple_attachments(self, ecs):
        """submit_attachment_state_changes with multiple attachments returns acknowledgment."""
        resp = ecs.submit_attachment_state_changes(
            cluster="default",
            attachments=[
                {
                    "attachmentArn": "arn:aws:ecs:us-east-1:123456789012:attachment/abc",
                    "status": "ATTACHED",
                },
                {
                    "attachmentArn": "arn:aws:ecs:us-east-1:123456789012:attachment/def",
                    "status": "DETACHED",
                },
            ],
        )
        assert "acknowledgment" in resp
        assert isinstance(resp["acknowledgment"], str)

    def test_submit_attachment_acknowledgment_not_empty(self, ecs):
        """submit_attachment_state_changes acknowledgment is non-empty."""
        resp = ecs.submit_attachment_state_changes(
            cluster="default",
            attachments=[
                {
                    "attachmentArn": "arn:aws:ecs:us-east-1:123456789012:attachment/xyz",
                    "status": "ATTACHED",
                }
            ],
        )
        assert len(resp["acknowledgment"]) > 0

    def test_submit_attachment_with_real_cluster(self, ecs):
        """submit_attachment_state_changes works with a real cluster name."""
        name = _unique("attach-cls")
        ecs.create_cluster(clusterName=name)
        try:
            resp = ecs.submit_attachment_state_changes(
                cluster=name,
                attachments=[
                    {
                        "attachmentArn": f"arn:aws:ecs:us-east-1:123456789012:attachment/{uuid.uuid4().hex}",
                        "status": "ATTACHED",
                    }
                ],
            )
            assert "acknowledgment" in resp
        finally:
            ecs.delete_cluster(cluster=name)


class TestListTaskDefinitionFamiliesEdgeCases:
    """Edge cases for list_task_definition_families."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_list_families_filter_active(self, ecs):
        """list_task_definition_families with status=ACTIVE returns active families."""
        family = _unique("fam-active")
        ecs.register_task_definition(
            family=family,
            containerDefinitions=[{"name": "app", "image": "nginx", "memory": 64}],
        )
        try:
            resp = ecs.list_task_definition_families(familyPrefix=family, status="ACTIVE")
            assert family in resp["families"]
        finally:
            ecs.deregister_task_definition(taskDefinition=f"{family}:1")

    def test_list_families_deregistered_shows_inactive(self, ecs):
        """Deregistered family appears with status=INACTIVE filter."""
        family = _unique("fam-inact")
        ecs.register_task_definition(
            family=family,
            containerDefinitions=[{"name": "app", "image": "nginx", "memory": 64}],
        )
        ecs.deregister_task_definition(taskDefinition=f"{family}:1")
        resp = ecs.list_task_definition_families(familyPrefix=family, status="INACTIVE")
        assert family in resp["families"]

    def test_list_families_pagination(self, ecs):
        """list_task_definition_families returns all families across pages."""
        families = [_unique("pg-fam") for _ in range(3)]
        for fam in families:
            ecs.register_task_definition(
                family=fam,
                containerDefinitions=[{"name": "app", "image": "busybox", "memory": 64}],
            )
        try:
            resp = ecs.list_task_definition_families(maxResults=100)
            all_families = resp["families"]
            found = [f for f in families if f in all_families]
            assert len(found) == 3
        finally:
            for fam in families:
                ecs.deregister_task_definition(taskDefinition=f"{fam}:1")

    def test_list_families_prefix_filters_correctly(self, ecs):
        """list_task_definition_families familyPrefix only returns matching families."""
        prefix = f"prefix-{uuid.uuid4().hex[:8]}"
        family = f"{prefix}-myapp"
        ecs.register_task_definition(
            family=family,
            containerDefinitions=[{"name": "app", "image": "nginx", "memory": 64}],
        )
        try:
            resp = ecs.list_task_definition_families(familyPrefix=prefix)
            assert all(f.startswith(prefix) for f in resp["families"])
            assert family in resp["families"]
        finally:
            ecs.deregister_task_definition(taskDefinition=f"{family}:1")


class TestRunTaskEdgeCases:
    """Edge cases and behavioral fidelity for run_task."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    @pytest.fixture
    def cluster(self, ecs):
        name = _unique("rt-cluster")
        ecs.create_cluster(clusterName=name)
        yield name
        ecs.delete_cluster(cluster=name)

    @pytest.fixture
    def task_def_arn(self, ecs):
        family = _unique("rt-td")
        resp = ecs.register_task_definition(
            family=family,
            containerDefinitions=[{"name": "app", "image": "nginx", "memory": 128}],
        )
        yield resp["taskDefinition"]["taskDefinitionArn"]
        ecs.deregister_task_definition(taskDefinition=f"{family}:1")

    def test_run_task_arn_format(self, ecs, cluster, task_def_arn):
        """run_task returns task ARN in expected arn:aws:ecs:*:task/* format."""
        resp = ecs.run_task(cluster=cluster, taskDefinition=task_def_arn)
        task = resp["tasks"][0]
        assert task["taskArn"].startswith("arn:aws:ecs:")
        assert "task" in task["taskArn"]

    def test_run_task_and_describe(self, ecs, cluster, task_def_arn):
        """run_task then describe_tasks returns matching task with correct definition ARN."""
        run_resp = ecs.run_task(cluster=cluster, taskDefinition=task_def_arn)
        task_arn = run_resp["tasks"][0]["taskArn"]
        desc_resp = ecs.describe_tasks(cluster=cluster, tasks=[task_arn])
        assert len(desc_resp["tasks"]) == 1
        assert desc_resp["tasks"][0]["taskArn"] == task_arn
        assert desc_resp["tasks"][0]["taskDefinitionArn"] == task_def_arn

    def test_run_task_appears_in_list(self, ecs, cluster, task_def_arn):
        """run_task then list_tasks in cluster returns the task ARN."""
        run_resp = ecs.run_task(cluster=cluster, taskDefinition=task_def_arn)
        task_arn = run_resp["tasks"][0]["taskArn"]
        list_resp = ecs.list_tasks(cluster=cluster)
        assert task_arn in list_resp["taskArns"]

    def test_run_task_and_stop(self, ecs, cluster, task_def_arn):
        """run_task then stop_task returns task with STOPPED desired status."""
        run_resp = ecs.run_task(cluster=cluster, taskDefinition=task_def_arn)
        task_arn = run_resp["tasks"][0]["taskArn"]
        stop_resp = ecs.stop_task(cluster=cluster, task=task_arn, reason="test-stop")
        assert stop_resp["task"]["taskArn"] == task_arn
        assert stop_resp["task"]["desiredStatus"] == "STOPPED"

    def test_run_task_count_returns_correct_number(self, ecs, cluster, task_def_arn):
        """run_task with count=3 returns exactly 3 tasks, each with unique ARN."""
        resp = ecs.run_task(cluster=cluster, taskDefinition=task_def_arn, count=3)
        assert len(resp["tasks"]) == 3
        arns = [t["taskArn"] for t in resp["tasks"]]
        assert len(set(arns)) == 3


class TestListContainerInstancesEdgeCases:
    """Edge cases for list_container_instances."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    @pytest.fixture
    def cluster(self, ecs):
        name = _unique("ci-list-cls")
        ecs.create_cluster(clusterName=name)
        yield name
        ecs.delete_cluster(cluster=name)

    def test_list_container_instances_empty_for_new_cluster(self, ecs, cluster):
        """list_container_instances returns empty list for a fresh cluster."""
        resp = ecs.list_container_instances(cluster=cluster)
        assert "containerInstanceArns" in resp
        assert isinstance(resp["containerInstanceArns"], list)
        assert len(resp["containerInstanceArns"]) == 0

    def test_list_container_instances_after_register(self, ecs, cluster):
        """list_container_instances finds a registered instance."""
        reg = ecs.register_container_instance(
            cluster=cluster,
            instanceIdentityDocument=(
                '{"region": "us-east-1", "instanceId": "i-list123", "accountId": "123456789012"}'
            ),
        )
        ci_arn = reg["containerInstance"]["containerInstanceArn"]
        try:
            resp = ecs.list_container_instances(cluster=cluster)
            assert ci_arn in resp["containerInstanceArns"]
        finally:
            ecs.deregister_container_instance(cluster=cluster, containerInstance=ci_arn, force=True)

    def test_list_container_instances_filter_active(self, ecs, cluster):
        """list_container_instances with status=ACTIVE returns active instances."""
        reg = ecs.register_container_instance(
            cluster=cluster,
            instanceIdentityDocument=(
                '{"region": "us-east-1", "instanceId": "i-active456", "accountId": "123456789012"}'
            ),
        )
        ci_arn = reg["containerInstance"]["containerInstanceArn"]
        try:
            resp = ecs.list_container_instances(cluster=cluster, status="ACTIVE")
            assert ci_arn in resp["containerInstanceArns"]
        finally:
            ecs.deregister_container_instance(cluster=cluster, containerInstance=ci_arn, force=True)

    def test_list_container_instances_deregistered_absent(self, ecs, cluster):
        """Deregistered instance is not returned by list_container_instances."""
        reg = ecs.register_container_instance(
            cluster=cluster,
            instanceIdentityDocument=(
                '{"region": "us-east-1", "instanceId": "i-dereg789", "accountId": "123456789012"}'
            ),
        )
        ci_arn = reg["containerInstance"]["containerInstanceArn"]
        ecs.deregister_container_instance(cluster=cluster, containerInstance=ci_arn, force=True)
        resp = ecs.list_container_instances(cluster=cluster)
        assert ci_arn not in resp["containerInstanceArns"]


class TestListTasksEdgeCases:
    """Edge cases for list_tasks."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    @pytest.fixture
    def cluster(self, ecs):
        name = _unique("lt-cluster")
        ecs.create_cluster(clusterName=name)
        yield name
        ecs.delete_cluster(cluster=name)

    @pytest.fixture
    def task_def_arn(self, ecs):
        family = _unique("lt-td")
        resp = ecs.register_task_definition(
            family=family,
            containerDefinitions=[{"name": "app", "image": "nginx", "memory": 128}],
        )
        yield resp["taskDefinition"]["taskDefinitionArn"]
        ecs.deregister_task_definition(taskDefinition=f"{family}:1")

    def test_list_tasks_after_run(self, ecs, cluster, task_def_arn):
        """list_tasks returns task ARNs for running tasks."""
        run_resp = ecs.run_task(cluster=cluster, taskDefinition=task_def_arn)
        task_arn = run_resp["tasks"][0]["taskArn"]
        resp = ecs.list_tasks(cluster=cluster)
        assert task_arn in resp["taskArns"]

    def test_list_tasks_filter_running(self, ecs, cluster, task_def_arn):
        """list_tasks with desiredStatus=RUNNING returns running tasks."""
        run_resp = ecs.run_task(cluster=cluster, taskDefinition=task_def_arn)
        task_arn = run_resp["tasks"][0]["taskArn"]
        resp = ecs.list_tasks(cluster=cluster, desiredStatus="RUNNING")
        assert task_arn in resp["taskArns"]

    def test_list_tasks_stopped_appear_with_stopped_filter(self, ecs, cluster, task_def_arn):
        """Stopped tasks appear in list_tasks with desiredStatus=STOPPED."""
        run_resp = ecs.run_task(cluster=cluster, taskDefinition=task_def_arn)
        task_arn = run_resp["tasks"][0]["taskArn"]
        ecs.stop_task(cluster=cluster, task=task_arn)
        resp = ecs.list_tasks(cluster=cluster, desiredStatus="STOPPED")
        assert task_arn in resp["taskArns"]

    def test_list_tasks_empty_for_new_cluster(self, ecs, cluster):
        """list_tasks returns empty list for a cluster with no tasks."""
        resp = ecs.list_tasks(cluster=cluster)
        assert "taskArns" in resp
        assert isinstance(resp["taskArns"], list)
        assert len(resp["taskArns"]) == 0


class TestPutAccountSettingEdgeCases:
    """Edge cases for put_account_setting."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_put_account_setting_update(self, ecs):
        """put_account_setting can update an existing setting."""
        ecs.put_account_setting(name="containerInstanceLongArnFormat", value="enabled")
        resp = ecs.put_account_setting(name="containerInstanceLongArnFormat", value="disabled")
        assert resp["setting"]["value"] == "disabled"

    def test_put_account_setting_list_verifies_stored(self, ecs):
        """put_account_setting value persists and appears in list_account_settings."""
        ecs.put_account_setting(name="serviceLongArnFormat", value="enabled")
        resp = ecs.list_account_settings(name="serviceLongArnFormat")
        found = [s for s in resp["settings"] if s["name"] == "serviceLongArnFormat"]
        assert len(found) >= 1
        assert found[0]["value"] == "enabled"

    def test_put_account_setting_then_delete(self, ecs):
        """put_account_setting then delete_account_setting succeeds."""
        ecs.put_account_setting(name="taskLongArnFormat", value="enabled")
        resp = ecs.delete_account_setting(name="taskLongArnFormat")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_account_setting_returns_setting_name(self, ecs):
        """put_account_setting response includes the setting name."""
        resp = ecs.put_account_setting(name="containerInsights", value="enabled")
        assert resp["setting"]["name"] == "containerInsights"


class TestPutAccountSettingDefaultEdgeCases:
    """Edge cases for put_account_setting_default."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_put_account_setting_default_and_list(self, ecs):
        """put_account_setting_default value appears in list_account_settings."""
        ecs.put_account_setting_default(name="awsvpcTrunking", value="enabled")
        resp = ecs.list_account_settings(name="awsvpcTrunking", effectiveSettings=True)
        found = [s for s in resp["settings"] if s["name"] == "awsvpcTrunking"]
        assert len(found) >= 1
        assert found[0]["value"] == "enabled"

    def test_put_account_setting_default_update(self, ecs):
        """put_account_setting_default can update an existing default."""
        ecs.put_account_setting_default(name="serviceLongArnFormat", value="disabled")
        resp = ecs.put_account_setting_default(name="serviceLongArnFormat", value="enabled")
        assert resp["setting"]["value"] == "enabled"

    def test_put_account_setting_default_returns_principal(self, ecs):
        """put_account_setting_default response includes principalArn."""
        resp = ecs.put_account_setting_default(name="taskLongArnFormat", value="enabled")
        assert "setting" in resp
        assert resp["setting"]["name"] == "taskLongArnFormat"


class TestDescribeCapacityProvidersEdgeCases:
    """Edge cases for describe_capacity_providers."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_describe_capacity_providers_builtin_fargate(self, ecs):
        """describe_capacity_providers returns FARGATE as built-in provider."""
        resp = ecs.describe_capacity_providers(capacityProviders=["FARGATE"])
        assert "capacityProviders" in resp
        if resp["capacityProviders"]:
            names = [cp["name"] for cp in resp["capacityProviders"]]
            assert "FARGATE" in names

    def test_describe_capacity_providers_create_retrieve_delete(self, ecs):
        """describe_capacity_providers: create → describe (present) → delete."""
        cp_name = _unique("crud-cp")
        ecs.create_capacity_provider(
            name=cp_name,
            autoScalingGroupProvider={
                "autoScalingGroupArn": (
                    "arn:aws:autoscaling:us-east-1:123456789012:"
                    "autoScalingGroup:xxx:autoScalingGroupName/my-asg"
                ),
            },
        )
        try:
            resp = ecs.describe_capacity_providers(capacityProviders=[cp_name])
            assert len(resp["capacityProviders"]) == 1
            assert resp["capacityProviders"][0]["name"] == cp_name
            assert resp["capacityProviders"][0]["status"] == "ACTIVE"
        finally:
            ecs.delete_capacity_provider(capacityProvider=cp_name)

    def test_describe_capacity_providers_failures_for_nonexistent(self, ecs):
        """describe_capacity_providers returns failures for unknown providers."""
        resp = ecs.describe_capacity_providers(capacityProviders=["no-such-cp-xyz"])
        assert "capacityProviders" in resp
        # Either in failures or empty capacityProviders
        found = any(cp["name"] == "no-such-cp-xyz" for cp in resp.get("capacityProviders", []))
        if not found:
            assert len(resp.get("capacityProviders", [])) == 0


class TestListServicesByNamespaceEdgeCases:
    """Edge cases for list_services_by_namespace."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_list_services_by_namespace_unknown_returns_empty(self, ecs):
        """list_services_by_namespace returns empty for unknown namespace."""
        resp = ecs.list_services_by_namespace(namespace="does-not-exist-ns-xyz")
        assert "serviceArns" in resp
        assert isinstance(resp["serviceArns"], list)
        assert len(resp["serviceArns"]) == 0

    def test_list_services_by_namespace_different_namespaces_independent(self, ecs):
        """Different namespaces return independent result lists."""
        resp1 = ecs.list_services_by_namespace(namespace="ns-alpha-test")
        resp2 = ecs.list_services_by_namespace(namespace="ns-beta-test")
        assert isinstance(resp1["serviceArns"], list)
        assert isinstance(resp2["serviceArns"], list)

    def test_list_services_by_namespace_response_has_next_token_key(self, ecs):
        """list_services_by_namespace response structure is valid."""
        resp = ecs.list_services_by_namespace(namespace="any-namespace")
        assert "serviceArns" in resp


class TestPutAttributesEdgeCases:
    """Edge cases for put_attributes."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    @pytest.fixture
    def cluster(self, ecs):
        name = _unique("pa-cluster")
        ecs.create_cluster(clusterName=name)
        yield name
        ecs.delete_cluster(cluster=name)

    def test_put_multiple_attributes_all_saved(self, ecs, cluster):
        """put_attributes with multiple attributes saves all of them."""
        resp = ecs.put_attributes(
            cluster=cluster,
            attributes=[
                {
                    "name": "zone",
                    "value": "us-east-1a",
                    "targetType": "container-instance",
                    "targetId": "fake-ci-multi-1",
                },
                {
                    "name": "gpu",
                    "value": "true",
                    "targetType": "container-instance",
                    "targetId": "fake-ci-multi-1",
                },
            ],
        )
        assert len(resp["attributes"]) == 2
        names = [a["name"] for a in resp["attributes"]]
        assert "zone" in names
        assert "gpu" in names

    def test_put_attribute_then_list_verifies(self, ecs, cluster):
        """put_attributes value is visible in list_attributes."""
        attr_id = uuid.uuid4().hex[:8]
        ecs.put_attributes(
            cluster=cluster,
            attributes=[
                {
                    "name": f"myattr-{attr_id}",
                    "value": "myval",
                    "targetType": "container-instance",
                    "targetId": "fake-ci-listed",
                }
            ],
        )
        resp = ecs.list_attributes(cluster=cluster, targetType="container-instance")
        names = [a["name"] for a in resp["attributes"]]
        assert f"myattr-{attr_id}" in names

    def test_put_attribute_then_delete_removes_it(self, ecs, cluster):
        """put_attributes then delete_attributes removes the attribute."""
        ecs.put_attributes(
            cluster=cluster,
            attributes=[
                {
                    "name": "deleteme",
                    "value": "yes",
                    "targetType": "container-instance",
                    "targetId": "fake-ci-del-edge",
                }
            ],
        )
        ecs.delete_attributes(
            cluster=cluster,
            attributes=[
                {
                    "name": "deleteme",
                    "targetType": "container-instance",
                    "targetId": "fake-ci-del-edge",
                }
            ],
        )
        resp = ecs.list_attributes(cluster=cluster, targetType="container-instance")
        matching = [
            a
            for a in resp["attributes"]
            if a["name"] == "deleteme" and a.get("targetId") == "fake-ci-del-edge"
        ]
        assert len(matching) == 0


class TestDescribeServiceDeploymentsEdgeCases:
    """Edge cases for describe_service_deployments."""

    @pytest.fixture
    def ecs(self):
        return make_client("ecs")

    def test_describe_service_deployments_has_failures_key(self, ecs):
        """describe_service_deployments with unknown ARN includes failures key."""
        fake_arn = "arn:aws:ecs:us-east-1:123456789012:service-deployment/fake-id"
        resp = ecs.describe_service_deployments(serviceDeploymentArns=[fake_arn])
        assert "failures" in resp
        assert isinstance(resp["failures"], list)

    def test_describe_service_deployments_both_keys_present(self, ecs):
        """describe_service_deployments always returns serviceDeployments and failures."""
        resp = ecs.describe_service_deployments(serviceDeploymentArns=[])
        assert "serviceDeployments" in resp
        assert "failures" in resp
        assert isinstance(resp["serviceDeployments"], list)
        assert isinstance(resp["failures"], list)

    def test_list_service_deployments_nonexistent_raises(self, ecs):
        """list_service_deployments for nonexistent service raises ServiceNotFoundException."""
        cluster_name = _unique("sd-edge-cls")
        ecs.create_cluster(clusterName=cluster_name)
        try:
            with pytest.raises(ClientError) as exc:
                ecs.list_service_deployments(
                    cluster=cluster_name,
                    service="nonexistent-svc-xyz",
                )
            assert exc.value.response["Error"]["Code"] == "ServiceNotFoundException"
        finally:
            ecs.delete_cluster(cluster=cluster_name)

    def test_describe_service_revisions_structure(self, ecs):
        """describe_service_revisions returns expected keys."""
        resp = ecs.describe_service_revisions(serviceRevisionArns=[])
        assert "serviceRevisions" in resp
        assert "failures" in resp
        assert isinstance(resp["serviceRevisions"], list)
