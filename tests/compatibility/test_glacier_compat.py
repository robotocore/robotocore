"""Glacier compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def glacier():
    return make_client("glacier")


def _uid():
    return uuid.uuid4().hex[:8]


class TestGlacierVaultOperations:
    def test_list_vaults_empty_or_populated(self, glacier):
        """list_vaults returns a VaultList."""
        response = glacier.list_vaults(accountId="-")
        assert "VaultList" in response
        assert isinstance(response["VaultList"], list)

    def test_create_vault(self, glacier):
        """create_vault returns 201."""
        name = f"test-vault-{_uid()}"
        response = glacier.create_vault(accountId="-", vaultName=name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 201
        glacier.delete_vault(accountId="-", vaultName=name)

    def test_describe_vault(self, glacier):
        """describe_vault returns vault details."""
        name = f"desc-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        response = glacier.describe_vault(accountId="-", vaultName=name)
        assert response["VaultName"] == name
        assert "VaultARN" in response
        assert "CreationDate" in response
        assert response["NumberOfArchives"] == 0
        assert response["SizeInBytes"] == 0
        glacier.delete_vault(accountId="-", vaultName=name)

    def test_describe_vault_arn_format(self, glacier):
        """describe_vault returns a properly formatted ARN."""
        name = f"arn-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        response = glacier.describe_vault(accountId="-", vaultName=name)
        arn = response["VaultARN"]
        assert arn.startswith("arn:aws:glacier:")
        assert name in arn
        glacier.delete_vault(accountId="-", vaultName=name)

    def test_delete_vault(self, glacier):
        """delete_vault returns 204 and removes the vault."""
        name = f"del-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        response = glacier.delete_vault(accountId="-", vaultName=name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 204
        # Verify it no longer appears in list
        vaults = glacier.list_vaults(accountId="-")
        names = [v["VaultName"] for v in vaults["VaultList"]]
        assert name not in names

    def test_list_vaults_includes_created_vault(self, glacier):
        """A newly created vault appears in list_vaults."""
        name = f"list-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        response = glacier.list_vaults(accountId="-")
        names = [v["VaultName"] for v in response["VaultList"]]
        assert name in names
        glacier.delete_vault(accountId="-", vaultName=name)

    def test_create_multiple_vaults(self, glacier):
        """Multiple vaults can be created and listed."""
        name_a = f"multi-a-{_uid()}"
        name_b = f"multi-b-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name_a)
        glacier.create_vault(accountId="-", vaultName=name_b)
        response = glacier.list_vaults(accountId="-")
        names = [v["VaultName"] for v in response["VaultList"]]
        assert name_a in names
        assert name_b in names
        glacier.delete_vault(accountId="-", vaultName=name_a)
        glacier.delete_vault(accountId="-", vaultName=name_b)

    def test_describe_vault_has_inventory_date(self, glacier):
        """describe_vault includes LastInventoryDate."""
        name = f"inv-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        response = glacier.describe_vault(accountId="-", vaultName=name)
        assert "LastInventoryDate" in response
        glacier.delete_vault(accountId="-", vaultName=name)

    def test_create_vault_idempotent(self, glacier):
        """Creating the same vault twice succeeds (idempotent)."""
        name = f"idem-vault-{_uid()}"
        r1 = glacier.create_vault(accountId="-", vaultName=name)
        r2 = glacier.create_vault(accountId="-", vaultName=name)
        assert r1["ResponseMetadata"]["HTTPStatusCode"] == 201
        assert r2["ResponseMetadata"]["HTTPStatusCode"] == 201
        glacier.delete_vault(accountId="-", vaultName=name)


class TestGlacierJobOperations:
    def test_list_jobs_empty(self, glacier):
        """list_jobs on a vault with no jobs returns empty list."""
        name = f"listjobs-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            response = glacier.list_jobs(accountId="-", vaultName=name)
            assert "JobList" in response
            assert isinstance(response["JobList"], list)
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)

    def test_initiate_and_describe_job(self, glacier):
        """initiate_job then describe_job returns job details."""
        name = f"descjob-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            init_resp = glacier.initiate_job(
                accountId="-",
                vaultName=name,
                jobParameters={"Type": "inventory-retrieval"},
            )
            job_id = init_resp["jobId"]
            assert job_id

            desc_resp = glacier.describe_job(accountId="-", vaultName=name, jobId=job_id)
            assert desc_resp["JobId"] == job_id
            assert desc_resp["Action"] == "InventoryRetrieval"
            assert "StatusCode" in desc_resp
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)

    def test_list_jobs_after_initiate(self, glacier):
        """list_jobs includes a job after initiate_job."""
        name = f"ljafter-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            init_resp = glacier.initiate_job(
                accountId="-",
                vaultName=name,
                jobParameters={"Type": "inventory-retrieval"},
            )
            job_id = init_resp["jobId"]

            response = glacier.list_jobs(accountId="-", vaultName=name)
            assert "JobList" in response
            job_ids = [j["JobId"] for j in response["JobList"]]
            assert job_id in job_ids
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)

    def test_upload_archive(self, glacier):
        """upload_archive stores an archive and returns metadata."""
        name = f"upload-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            response = glacier.upload_archive(
                accountId="-",
                vaultName=name,
                body=b"test archive data",
            )
            assert response["ResponseMetadata"]["HTTPStatusCode"] == 201
            assert "archiveId" in response
            assert "checksum" in response
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)

    def test_delete_archive(self, glacier):
        """delete_archive removes an archive from a vault."""
        name = f"delarch-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            upload = glacier.upload_archive(accountId="-", vaultName=name, body=b"delete me")
            archive_id = upload["archiveId"]
            resp = glacier.delete_archive(accountId="-", vaultName=name, archiveId=archive_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)


class TestGlacierTagOperations:
    def test_add_tags_to_vault(self, glacier):
        """add_tags_to_vault attaches tags to a vault."""
        name = f"tag-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            resp = glacier.add_tags_to_vault(
                accountId="-",
                vaultName=name,
                Tags={"Env": "test", "Team": "dev"},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)

    def test_list_tags_for_vault(self, glacier):
        """list_tags_for_vault returns tags."""
        name = f"ltag-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            glacier.add_tags_to_vault(
                accountId="-",
                vaultName=name,
                Tags={"Env": "test"},
            )
            resp = glacier.list_tags_for_vault(accountId="-", vaultName=name)
            assert "Tags" in resp
            assert resp["Tags"]["Env"] == "test"
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)

    def test_remove_tags_from_vault(self, glacier):
        """remove_tags_from_vault removes specified tags."""
        name = f"rmtag-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            glacier.add_tags_to_vault(
                accountId="-",
                vaultName=name,
                Tags={"Env": "test", "Team": "dev"},
            )
            resp = glacier.remove_tags_from_vault(accountId="-", vaultName=name, TagKeys=["Env"])
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204
            tags = glacier.list_tags_for_vault(accountId="-", vaultName=name)
            assert "Env" not in tags["Tags"]
            assert tags["Tags"]["Team"] == "dev"
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)


class TestGlacierDataRetrievalPolicy:
    def test_get_data_retrieval_policy(self, glacier):
        """get_data_retrieval_policy returns a policy."""
        resp = glacier.get_data_retrieval_policy(accountId="-")
        assert "Policy" in resp

    def test_set_data_retrieval_policy(self, glacier):
        """set_data_retrieval_policy sets retrieval rules."""
        resp = glacier.set_data_retrieval_policy(
            accountId="-",
            Policy={"Rules": [{"Strategy": "FreeTier"}]},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204


class TestGlacierVaultAccessPolicy:
    def test_set_vault_access_policy(self, glacier):
        """set_vault_access_policy sets an access policy on a vault."""
        name = f"ap-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            import json

            policy_doc = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "glacier:ListJobs",
                            "Resource": f"arn:aws:glacier:us-east-1:123456789012:vaults/{name}",
                        }
                    ],
                }
            )
            resp = glacier.set_vault_access_policy(
                accountId="-",
                vaultName=name,
                policy={"Policy": policy_doc},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)

    def test_get_vault_access_policy(self, glacier):
        """get_vault_access_policy retrieves a vault's access policy."""
        name = f"gap-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            import json

            policy_doc = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "glacier:ListJobs",
                            "Resource": f"arn:aws:glacier:us-east-1:123456789012:vaults/{name}",
                        }
                    ],
                }
            )
            glacier.set_vault_access_policy(
                accountId="-",
                vaultName=name,
                policy={"Policy": policy_doc},
            )
            resp = glacier.get_vault_access_policy(accountId="-", vaultName=name)
            assert "policy" in resp
            assert "Policy" in resp["policy"]
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)

    def test_delete_vault_access_policy(self, glacier):
        """delete_vault_access_policy removes a vault's access policy."""
        name = f"dap-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            import json

            policy_doc = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "glacier:ListJobs",
                            "Resource": f"arn:aws:glacier:us-east-1:123456789012:vaults/{name}",
                        }
                    ],
                }
            )
            glacier.set_vault_access_policy(
                accountId="-",
                vaultName=name,
                policy={"Policy": policy_doc},
            )
            resp = glacier.delete_vault_access_policy(accountId="-", vaultName=name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)


class TestGlacierVaultNotifications:
    def test_set_vault_notifications(self, glacier):
        """set_vault_notifications configures notifications for a vault."""
        name = f"notif-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            resp = glacier.set_vault_notifications(
                accountId="-",
                vaultName=name,
                vaultNotificationConfig={
                    "SNSTopic": "arn:aws:sns:us-east-1:123456789012:glacier-notif",
                    "Events": [
                        "ArchiveRetrievalCompleted",
                        "InventoryRetrievalCompleted",
                    ],
                },
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)

    def test_get_vault_notifications(self, glacier):
        """get_vault_notifications returns notification config."""
        name = f"gnotif-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            glacier.set_vault_notifications(
                accountId="-",
                vaultName=name,
                vaultNotificationConfig={
                    "SNSTopic": "arn:aws:sns:us-east-1:123456789012:glacier-notif",
                    "Events": ["ArchiveRetrievalCompleted"],
                },
            )
            resp = glacier.get_vault_notifications(accountId="-", vaultName=name)
            assert "vaultNotificationConfig" in resp
            assert "SNSTopic" in resp["vaultNotificationConfig"]
            assert "Events" in resp["vaultNotificationConfig"]
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)

    def test_delete_vault_notifications(self, glacier):
        """delete_vault_notifications removes notification config."""
        name = f"dnotif-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            glacier.set_vault_notifications(
                accountId="-",
                vaultName=name,
                vaultNotificationConfig={
                    "SNSTopic": "arn:aws:sns:us-east-1:123456789012:glacier-notif",
                    "Events": ["ArchiveRetrievalCompleted"],
                },
            )
            resp = glacier.delete_vault_notifications(accountId="-", vaultName=name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)


class TestGlacierVaultLock:
    def test_initiate_vault_lock(self, glacier):
        """initiate_vault_lock starts the lock process."""
        name = f"lock-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            import json

            policy_doc = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Deny",
                            "Principal": "*",
                            "Action": "glacier:DeleteArchive",
                            "Resource": f"arn:aws:glacier:us-east-1:123456789012:vaults/{name}",
                            "Condition": {"NumericLessThan": {"glacier:ArchiveAgeinDays": "365"}},
                        }
                    ],
                }
            )
            resp = glacier.initiate_vault_lock(
                accountId="-",
                vaultName=name,
                policy={"Policy": policy_doc},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201
            assert "lockId" in resp
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)

    def test_get_vault_lock(self, glacier):
        """get_vault_lock returns lock state."""
        name = f"glock-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            import json

            policy_doc = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Deny",
                            "Principal": "*",
                            "Action": "glacier:DeleteArchive",
                            "Resource": f"arn:aws:glacier:us-east-1:123456789012:vaults/{name}",
                        }
                    ],
                }
            )
            glacier.initiate_vault_lock(
                accountId="-",
                vaultName=name,
                policy={"Policy": policy_doc},
            )
            resp = glacier.get_vault_lock(accountId="-", vaultName=name)
            assert "Policy" in resp
            assert "State" in resp
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)

    def test_abort_vault_lock(self, glacier):
        """abort_vault_lock cancels a pending lock."""
        name = f"alock-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            import json

            policy_doc = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Deny",
                            "Principal": "*",
                            "Action": "glacier:DeleteArchive",
                            "Resource": f"arn:aws:glacier:us-east-1:123456789012:vaults/{name}",
                        }
                    ],
                }
            )
            glacier.initiate_vault_lock(
                accountId="-",
                vaultName=name,
                policy={"Policy": policy_doc},
            )
            resp = glacier.abort_vault_lock(accountId="-", vaultName=name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)

    def test_complete_vault_lock(self, glacier):
        """complete_vault_lock finalizes a vault lock."""
        name = f"clock-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            import json

            policy_doc = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Deny",
                            "Principal": "*",
                            "Action": "glacier:DeleteArchive",
                            "Resource": f"arn:aws:glacier:us-east-1:123456789012:vaults/{name}",
                        }
                    ],
                }
            )
            init_resp = glacier.initiate_vault_lock(
                accountId="-",
                vaultName=name,
                policy={"Policy": policy_doc},
            )
            lock_id = init_resp["lockId"]
            resp = glacier.complete_vault_lock(accountId="-", vaultName=name, lockId=lock_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)


class TestGlacierMultipartUpload:
    def test_initiate_multipart_upload(self, glacier):
        """initiate_multipart_upload starts a multipart upload."""
        name = f"mpu-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            resp = glacier.initiate_multipart_upload(
                accountId="-",
                vaultName=name,
                partSize="1048576",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201
            assert "uploadId" in resp
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)

    def test_list_multipart_uploads(self, glacier):
        """list_multipart_uploads returns upload list."""
        name = f"lmpu-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            glacier.initiate_multipart_upload(
                accountId="-",
                vaultName=name,
                partSize="1048576",
            )
            resp = glacier.list_multipart_uploads(accountId="-", vaultName=name)
            assert "UploadsList" in resp
            assert len(resp["UploadsList"]) >= 1
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)

    def test_abort_multipart_upload(self, glacier):
        """abort_multipart_upload cancels an in-progress upload."""
        name = f"ampu-vault-{_uid()}"
        glacier.create_vault(accountId="-", vaultName=name)
        try:
            init = glacier.initiate_multipart_upload(
                accountId="-",
                vaultName=name,
                partSize="1048576",
            )
            upload_id = init["uploadId"]
            resp = glacier.abort_multipart_upload(accountId="-", vaultName=name, uploadId=upload_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204
        finally:
            glacier.delete_vault(accountId="-", vaultName=name)

    def test_list_provisioned_capacity(self, glacier):
        """list_provisioned_capacity returns capacity list."""
        resp = glacier.list_provisioned_capacity(accountId="-")
        assert "ProvisionedCapacityList" in resp
        assert isinstance(resp["ProvisionedCapacityList"], list)
