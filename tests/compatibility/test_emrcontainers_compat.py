"""EMR Containers compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def emr_containers():
    return make_client("emr-containers")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def virtual_cluster(emr_containers):
    namespace = f"ns-{uuid.uuid4().hex[:12]}"
    resp = emr_containers.create_virtual_cluster(
        name=_unique("test-vc"),
        containerProvider={
            "id": "eks-cluster-1",
            "type": "EKS",
            "info": {"eksInfo": {"namespace": namespace}},
        },
    )
    vc_id = resp["id"]
    yield resp
    try:
        emr_containers.delete_virtual_cluster(id=vc_id)
    except Exception:
        pass  # best-effort cleanup


@pytest.fixture
def virtual_cluster_for_jobs(emr_containers):
    namespace = f"ns-{uuid.uuid4().hex[:12]}"
    resp = emr_containers.create_virtual_cluster(
        name=_unique("test-vc-jobs"),
        containerProvider={
            "id": "eks-cluster-1",
            "type": "EKS",
            "info": {"eksInfo": {"namespace": namespace}},
        },
    )
    vc_id = resp["id"]
    yield resp
    try:
        emr_containers.delete_virtual_cluster(id=vc_id)
    except Exception:
        pass  # best-effort cleanup


class TestEMRContainersJobRuns:
    @pytest.fixture
    def job_run(self, emr_containers, virtual_cluster_for_jobs):
        vc_id = virtual_cluster_for_jobs["id"]
        resp = emr_containers.start_job_run(
            name=_unique("test-job"),
            virtualClusterId=vc_id,
            executionRoleArn="arn:aws:iam::123456789012:role/test-role",
            releaseLabel="emr-6.2.0-latest",
            jobDriver={
                "sparkSubmitJobDriver": {
                    "entryPoint": "s3://bucket/script.py",
                }
            },
        )
        return {"jobRunId": resp["id"], "virtualClusterId": vc_id}

    def test_start_job_run(self, emr_containers, virtual_cluster_for_jobs):
        vc_id = virtual_cluster_for_jobs["id"]
        name = _unique("start-job")
        resp = emr_containers.start_job_run(
            name=name,
            virtualClusterId=vc_id,
            executionRoleArn="arn:aws:iam::123456789012:role/test-role",
            releaseLabel="emr-6.2.0-latest",
            jobDriver={
                "sparkSubmitJobDriver": {
                    "entryPoint": "s3://bucket/script.py",
                }
            },
        )
        assert "id" in resp
        assert resp["name"] == name
        assert resp["virtualClusterId"] == vc_id

    def test_list_job_runs(self, emr_containers, virtual_cluster_for_jobs, job_run):
        vc_id = virtual_cluster_for_jobs["id"]
        resp = emr_containers.list_job_runs(virtualClusterId=vc_id)
        assert "jobRuns" in resp
        run_ids = [r["id"] for r in resp["jobRuns"]]
        assert job_run["jobRunId"] in run_ids

    def test_describe_job_run(self, emr_containers, virtual_cluster_for_jobs, job_run):
        vc_id = virtual_cluster_for_jobs["id"]
        jr_id = job_run["jobRunId"]
        resp = emr_containers.describe_job_run(virtualClusterId=vc_id, id=jr_id)
        jr = resp["jobRun"]
        assert jr["id"] == jr_id
        assert jr["virtualClusterId"] == vc_id
        assert "state" in jr
        assert "arn" in jr

    def test_cancel_job_run(self, emr_containers, virtual_cluster_for_jobs):
        vc_id = virtual_cluster_for_jobs["id"]
        start_resp = emr_containers.start_job_run(
            name=_unique("cancel-job"),
            virtualClusterId=vc_id,
            executionRoleArn="arn:aws:iam::123456789012:role/test-role",
            releaseLabel="emr-6.2.0-latest",
            jobDriver={
                "sparkSubmitJobDriver": {
                    "entryPoint": "s3://bucket/cancel-script.py",
                }
            },
        )
        jr_id = start_resp["id"]
        cancel_resp = emr_containers.cancel_job_run(virtualClusterId=vc_id, id=jr_id)
        assert cancel_resp["id"] == jr_id
        assert cancel_resp["virtualClusterId"] == vc_id


class TestEMRContainersJobTemplates:
    def test_list_job_templates_empty(self, emr_containers):
        resp = emr_containers.list_job_templates()
        assert "templates" in resp

    def test_create_job_template(self, emr_containers):
        name = _unique("test-template")
        resp = emr_containers.create_job_template(
            name=name,
            clientToken=uuid.uuid4().hex,
            jobTemplateData={
                "executionRoleArn": "arn:aws:iam::123456789012:role/EMRRole",
                "releaseLabel": "emr-6.3.0-latest",
                "jobDriver": {
                    "sparkSubmitJobDriver": {
                        "entryPoint": "s3://bucket/script.py",
                    }
                },
            },
        )
        assert "id" in resp
        assert resp["name"] == name
        assert "arn" in resp
        # Cleanup
        emr_containers.delete_job_template(id=resp["id"])

    def test_describe_job_template(self, emr_containers):
        name = _unique("describe-template")
        create_resp = emr_containers.create_job_template(
            name=name,
            clientToken=uuid.uuid4().hex,
            jobTemplateData={
                "executionRoleArn": "arn:aws:iam::123456789012:role/EMRRole",
                "releaseLabel": "emr-6.3.0-latest",
                "jobDriver": {
                    "sparkSubmitJobDriver": {
                        "entryPoint": "s3://bucket/script.py",
                    }
                },
            },
        )
        template_id = create_resp["id"]
        resp = emr_containers.describe_job_template(id=template_id)
        assert "jobTemplate" in resp
        assert resp["jobTemplate"]["id"] == template_id
        assert resp["jobTemplate"]["name"] == name
        assert "arn" in resp["jobTemplate"]
        # Cleanup
        emr_containers.delete_job_template(id=template_id)

    def test_delete_job_template(self, emr_containers):
        create_resp = emr_containers.create_job_template(
            name=_unique("delete-template"),
            clientToken=uuid.uuid4().hex,
            jobTemplateData={
                "executionRoleArn": "arn:aws:iam::123456789012:role/EMRRole",
                "releaseLabel": "emr-6.3.0-latest",
                "jobDriver": {
                    "sparkSubmitJobDriver": {
                        "entryPoint": "s3://bucket/script.py",
                    }
                },
            },
        )
        template_id = create_resp["id"]
        delete_resp = emr_containers.delete_job_template(id=template_id)
        assert delete_resp["id"] == template_id

    def test_list_job_templates_includes_created(self, emr_containers):
        name = _unique("list-template")
        create_resp = emr_containers.create_job_template(
            name=name,
            clientToken=uuid.uuid4().hex,
            jobTemplateData={
                "executionRoleArn": "arn:aws:iam::123456789012:role/EMRRole",
                "releaseLabel": "emr-6.3.0-latest",
                "jobDriver": {
                    "sparkSubmitJobDriver": {
                        "entryPoint": "s3://bucket/script.py",
                    }
                },
            },
        )
        template_id = create_resp["id"]
        resp = emr_containers.list_job_templates()
        template_ids = [t["id"] for t in resp["templates"]]
        assert template_id in template_ids
        # Cleanup
        emr_containers.delete_job_template(id=template_id)


class TestEMRContainersManagedEndpoints:
    @pytest.fixture
    def vc_for_endpoints(self, emr_containers):
        namespace = f"ns-{uuid.uuid4().hex[:12]}"
        resp = emr_containers.create_virtual_cluster(
            name=_unique("test-vc-ep"),
            containerProvider={
                "id": "eks-cluster-1",
                "type": "EKS",
                "info": {"eksInfo": {"namespace": namespace}},
            },
        )
        vc_id = resp["id"]
        yield resp
        try:
            emr_containers.delete_virtual_cluster(id=vc_id)
        except Exception:
            pass  # best-effort cleanup

    def test_list_managed_endpoints_empty(self, emr_containers, vc_for_endpoints):
        vc_id = vc_for_endpoints["id"]
        resp = emr_containers.list_managed_endpoints(virtualClusterId=vc_id)
        assert "endpoints" in resp

    def test_create_managed_endpoint(self, emr_containers, vc_for_endpoints):
        vc_id = vc_for_endpoints["id"]
        name = _unique("test-ep")
        resp = emr_containers.create_managed_endpoint(
            name=name,
            virtualClusterId=vc_id,
            type="JUPYTER_ENTERPRISE_GATEWAY",
            releaseLabel="emr-6.3.0-latest",
            executionRoleArn="arn:aws:iam::123456789012:role/EMRRole",
            clientToken=uuid.uuid4().hex,
        )
        assert "id" in resp
        assert resp["name"] == name
        assert resp["virtualClusterId"] == vc_id
        assert "arn" in resp
        # Cleanup
        emr_containers.delete_managed_endpoint(id=resp["id"], virtualClusterId=vc_id)

    def test_describe_managed_endpoint(self, emr_containers, vc_for_endpoints):
        vc_id = vc_for_endpoints["id"]
        name = _unique("describe-ep")
        create_resp = emr_containers.create_managed_endpoint(
            name=name,
            virtualClusterId=vc_id,
            type="JUPYTER_ENTERPRISE_GATEWAY",
            releaseLabel="emr-6.3.0-latest",
            executionRoleArn="arn:aws:iam::123456789012:role/EMRRole",
            clientToken=uuid.uuid4().hex,
        )
        ep_id = create_resp["id"]
        resp = emr_containers.describe_managed_endpoint(id=ep_id, virtualClusterId=vc_id)
        assert "endpoint" in resp
        assert resp["endpoint"]["id"] == ep_id
        assert resp["endpoint"]["name"] == name
        assert resp["endpoint"]["virtualClusterId"] == vc_id
        # Cleanup
        emr_containers.delete_managed_endpoint(id=ep_id, virtualClusterId=vc_id)

    def test_delete_managed_endpoint(self, emr_containers, vc_for_endpoints):
        vc_id = vc_for_endpoints["id"]
        create_resp = emr_containers.create_managed_endpoint(
            name=_unique("delete-ep"),
            virtualClusterId=vc_id,
            type="JUPYTER_ENTERPRISE_GATEWAY",
            releaseLabel="emr-6.3.0-latest",
            executionRoleArn="arn:aws:iam::123456789012:role/EMRRole",
            clientToken=uuid.uuid4().hex,
        )
        ep_id = create_resp["id"]
        delete_resp = emr_containers.delete_managed_endpoint(id=ep_id, virtualClusterId=vc_id)
        assert delete_resp["id"] == ep_id
        assert delete_resp["virtualClusterId"] == vc_id

    def test_list_managed_endpoints_includes_created(self, emr_containers, vc_for_endpoints):
        vc_id = vc_for_endpoints["id"]
        create_resp = emr_containers.create_managed_endpoint(
            name=_unique("list-ep"),
            virtualClusterId=vc_id,
            type="JUPYTER_ENTERPRISE_GATEWAY",
            releaseLabel="emr-6.3.0-latest",
            executionRoleArn="arn:aws:iam::123456789012:role/EMRRole",
            clientToken=uuid.uuid4().hex,
        )
        ep_id = create_resp["id"]
        resp = emr_containers.list_managed_endpoints(virtualClusterId=vc_id)
        ep_ids = [e["id"] for e in resp["endpoints"]]
        assert ep_id in ep_ids
        # Cleanup
        emr_containers.delete_managed_endpoint(id=ep_id, virtualClusterId=vc_id)


class TestEMRContainersSecurityConfigurations:
    def test_list_security_configurations_empty(self, emr_containers):
        resp = emr_containers.list_security_configurations()
        assert "securityConfigurations" in resp

    def test_create_security_configuration(self, emr_containers):
        name = _unique("test-sc")
        resp = emr_containers.create_security_configuration(
            name=name,
            clientToken=uuid.uuid4().hex,
            securityConfigurationData={
                "authorizationConfiguration": {
                    "lakeFormationConfiguration": {
                        "authorizedSessionTagValue": "EMR",
                        "secureNamespaceInfo": {
                            "clusterId": "my-cluster",
                            "namespace": "my-ns",
                        },
                        "queryEngineRoleArn": "arn:aws:iam::123456789012:role/LFRole",
                    }
                }
            },
        )
        assert "id" in resp
        assert resp["name"] == name
        assert "arn" in resp

    def test_describe_security_configuration(self, emr_containers):
        name = _unique("describe-sc")
        create_resp = emr_containers.create_security_configuration(
            name=name,
            clientToken=uuid.uuid4().hex,
            securityConfigurationData={
                "authorizationConfiguration": {
                    "lakeFormationConfiguration": {
                        "authorizedSessionTagValue": "EMR",
                        "secureNamespaceInfo": {
                            "clusterId": "my-cluster",
                            "namespace": "my-ns",
                        },
                        "queryEngineRoleArn": "arn:aws:iam::123456789012:role/LFRole",
                    }
                }
            },
        )
        sc_id = create_resp["id"]
        resp = emr_containers.describe_security_configuration(id=sc_id)
        assert "securityConfiguration" in resp
        assert resp["securityConfiguration"]["id"] == sc_id
        assert resp["securityConfiguration"]["name"] == name
        assert "arn" in resp["securityConfiguration"]

    def test_list_security_configurations_includes_created(self, emr_containers):
        name = _unique("list-sc")
        create_resp = emr_containers.create_security_configuration(
            name=name,
            clientToken=uuid.uuid4().hex,
            securityConfigurationData={
                "authorizationConfiguration": {
                    "lakeFormationConfiguration": {
                        "authorizedSessionTagValue": "EMR",
                        "secureNamespaceInfo": {
                            "clusterId": "my-cluster",
                            "namespace": "my-ns",
                        },
                        "queryEngineRoleArn": "arn:aws:iam::123456789012:role/LFRole",
                    }
                }
            },
        )
        sc_id = create_resp["id"]
        resp = emr_containers.list_security_configurations()
        sc_ids = [sc["id"] for sc in resp["securityConfigurations"]]
        assert sc_id in sc_ids


class TestEMRContainersVirtualCluster:
    def test_list_virtual_clusters_empty(self, emr_containers):
        resp = emr_containers.list_virtual_clusters()
        assert "virtualClusters" in resp

    def test_create_virtual_cluster(self, emr_containers):
        namespace = f"ns-{uuid.uuid4().hex[:12]}"
        name = _unique("create-vc")
        resp = emr_containers.create_virtual_cluster(
            name=name,
            containerProvider={
                "id": "eks-cluster-1",
                "type": "EKS",
                "info": {"eksInfo": {"namespace": namespace}},
            },
        )
        assert "id" in resp
        assert resp["name"] == name
        assert "arn" in resp
        # Cleanup
        emr_containers.delete_virtual_cluster(id=resp["id"])

    def test_describe_virtual_cluster(self, emr_containers, virtual_cluster):
        vc_id = virtual_cluster["id"]
        resp = emr_containers.describe_virtual_cluster(id=vc_id)
        vc = resp["virtualCluster"]
        assert vc["id"] == vc_id
        assert vc["name"] == virtual_cluster["name"]
        assert "arn" in vc
        assert "containerProvider" in vc

    def test_delete_virtual_cluster(self, emr_containers):
        namespace = f"ns-{uuid.uuid4().hex[:12]}"
        resp = emr_containers.create_virtual_cluster(
            name=_unique("delete-vc"),
            containerProvider={
                "id": "eks-cluster-1",
                "type": "EKS",
                "info": {"eksInfo": {"namespace": namespace}},
            },
        )
        vc_id = resp["id"]
        delete_resp = emr_containers.delete_virtual_cluster(id=vc_id)
        assert delete_resp["id"] == vc_id

    def test_list_virtual_clusters_includes_created(self, emr_containers, virtual_cluster):
        vc_id = virtual_cluster["id"]
        resp = emr_containers.list_virtual_clusters()
        vc_ids = [vc["id"] for vc in resp["virtualClusters"]]
        assert vc_id in vc_ids
