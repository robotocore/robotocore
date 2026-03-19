"""Compatibility tests for Amazon Textract."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def textract():
    return make_client("textract")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestTextractDocumentTextDetection:
    """Tests for synchronous and asynchronous text detection."""

    def test_detect_document_text(self, textract):
        """detect_document_text returns blocks and metadata."""
        resp = textract.detect_document_text(
            Document={"S3Object": {"Bucket": "test-bucket", "Name": "test.pdf"}}
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "DocumentMetadata" in resp
        assert "Blocks" in resp
        assert isinstance(resp["Blocks"], list)

    def test_start_document_text_detection(self, textract):
        """start_document_text_detection returns a job ID."""
        resp = textract.start_document_text_detection(
            DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "test.pdf"}}
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobId" in resp
        assert len(resp["JobId"]) > 0

    def test_get_document_text_detection(self, textract):
        """get_document_text_detection retrieves results for a started job."""
        start_resp = textract.start_document_text_detection(
            DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "test.pdf"}}
        )
        job_id = start_resp["JobId"]

        resp = textract.get_document_text_detection(JobId=job_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobStatus" in resp
        assert "DocumentMetadata" in resp
        assert "Blocks" in resp

    def test_get_document_text_detection_invalid_job_id(self, textract):
        """get_document_text_detection with invalid job ID raises error."""
        with pytest.raises(ClientError) as exc:
            textract.get_document_text_detection(JobId="00000000-0000-0000-0000-000000000000")
        assert "InvalidJobIdException" in str(exc.value)

    def test_detect_document_text_with_bytes(self, textract):
        """detect_document_text accepts Document with Bytes input."""
        resp = textract.detect_document_text(Document={"Bytes": b"%PDF-1.4 fake content"})
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "DocumentMetadata" in resp
        assert isinstance(resp["Blocks"], list)

    def test_get_document_text_detection_job_status(self, textract):
        """get_document_text_detection returns SUCCEEDED status for completed job."""
        start_resp = textract.start_document_text_detection(
            DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "test.pdf"}}
        )
        job_id = start_resp["JobId"]

        resp = textract.get_document_text_detection(JobId=job_id)
        assert resp["JobStatus"] == "SUCCEEDED"
        assert isinstance(resp["Blocks"], list)


class TestTextractDocumentAnalysis:
    """Tests for asynchronous document analysis."""

    def test_start_document_analysis(self, textract):
        """start_document_analysis returns a job ID."""
        resp = textract.start_document_analysis(
            DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "test.pdf"}},
            FeatureTypes=["TABLES"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobId" in resp
        assert len(resp["JobId"]) > 0

    def test_get_document_analysis(self, textract):
        """get_document_analysis retrieves results for a started analysis job."""
        start_resp = textract.start_document_analysis(
            DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "test.pdf"}},
            FeatureTypes=["TABLES"],
        )
        job_id = start_resp["JobId"]

        resp = textract.get_document_analysis(JobId=job_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobStatus" in resp
        assert "DocumentMetadata" in resp
        assert "Blocks" in resp

    def test_get_document_analysis_invalid_job_id(self, textract):
        """get_document_analysis with invalid job ID raises error."""
        with pytest.raises(ClientError) as exc:
            textract.get_document_analysis(JobId="00000000-0000-0000-0000-000000000000")
        assert "InvalidJobIdException" in str(exc.value)

    def test_start_document_analysis_with_forms(self, textract):
        """start_document_analysis with FORMS feature type."""
        resp = textract.start_document_analysis(
            DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "test.pdf"}},
            FeatureTypes=["FORMS"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobId" in resp

    def test_start_document_analysis_multiple_features(self, textract):
        """start_document_analysis with multiple feature types."""
        resp = textract.start_document_analysis(
            DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "test.pdf"}},
            FeatureTypes=["TABLES", "FORMS"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobId" in resp

    def test_start_document_analysis_with_queries(self, textract):
        """start_document_analysis with QUERIES feature type and config."""
        resp = textract.start_document_analysis(
            DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "test.pdf"}},
            FeatureTypes=["QUERIES"],
            QueriesConfig={"Queries": [{"Text": "What is the total?"}]},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobId" in resp
        assert len(resp["JobId"]) > 0

    def test_get_document_analysis_job_status(self, textract):
        """get_document_analysis returns SUCCEEDED status for completed job."""
        start_resp = textract.start_document_analysis(
            DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "test.pdf"}},
            FeatureTypes=["TABLES"],
        )
        job_id = start_resp["JobId"]

        resp = textract.get_document_analysis(JobId=job_id)
        assert resp["JobStatus"] == "SUCCEEDED"
        assert isinstance(resp["Blocks"], list)


class TestTextractAnalyzeDocument:
    """Tests for synchronous AnalyzeDocument."""

    def test_analyze_document_tables(self, textract):
        """analyze_document with TABLES feature returns blocks and metadata."""
        resp = textract.analyze_document(
            Document={"S3Object": {"Bucket": "test-bucket", "Name": "test.pdf"}},
            FeatureTypes=["TABLES"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "DocumentMetadata" in resp
        assert "Blocks" in resp
        assert "AnalyzeDocumentModelVersion" in resp

    def test_analyze_document_forms(self, textract):
        """analyze_document with FORMS feature returns blocks."""
        resp = textract.analyze_document(
            Document={"S3Object": {"Bucket": "test-bucket", "Name": "test.pdf"}},
            FeatureTypes=["FORMS"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Blocks" in resp

    def test_analyze_document_multiple_features(self, textract):
        """analyze_document with multiple features."""
        resp = textract.analyze_document(
            Document={"S3Object": {"Bucket": "test-bucket", "Name": "test.pdf"}},
            FeatureTypes=["TABLES", "FORMS"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "DocumentMetadata" in resp

    def test_analyze_document_with_bytes(self, textract):
        """analyze_document accepts raw bytes input."""
        resp = textract.analyze_document(
            Document={"Bytes": b"%PDF-1.4 fake content"},
            FeatureTypes=["TABLES"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Blocks" in resp


class TestTextractAnalyzeExpense:
    """Tests for synchronous AnalyzeExpense."""

    def test_analyze_expense_s3(self, textract):
        """analyze_expense returns expense documents and metadata."""
        resp = textract.analyze_expense(
            Document={"S3Object": {"Bucket": "test-bucket", "Name": "invoice.pdf"}}
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "DocumentMetadata" in resp
        assert "ExpenseDocuments" in resp
        assert isinstance(resp["ExpenseDocuments"], list)

    def test_analyze_expense_with_bytes(self, textract):
        """analyze_expense accepts Document with Bytes input."""
        resp = textract.analyze_expense(Document={"Bytes": b"%PDF-1.4 fake invoice"})
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ExpenseDocuments" in resp


class TestTextractAnalyzeID:
    """Tests for synchronous AnalyzeID."""

    def test_analyze_id_s3(self, textract):
        """analyze_id returns identity documents and metadata."""
        resp = textract.analyze_id(
            DocumentPages=[{"S3Object": {"Bucket": "test-bucket", "Name": "id-front.pdf"}}]
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "DocumentMetadata" in resp
        assert "IdentityDocuments" in resp
        assert "AnalyzeIDModelVersion" in resp
        assert isinstance(resp["IdentityDocuments"], list)

    def test_analyze_id_multiple_pages(self, textract):
        """analyze_id can accept multiple document pages."""
        resp = textract.analyze_id(
            DocumentPages=[
                {"S3Object": {"Bucket": "test-bucket", "Name": "id-front.pdf"}},
                {"S3Object": {"Bucket": "test-bucket", "Name": "id-back.pdf"}},
            ]
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "IdentityDocuments" in resp


class TestTextractExpenseAnalysis:
    """Tests for asynchronous expense analysis."""

    def test_start_expense_analysis(self, textract):
        """start_expense_analysis returns a job ID."""
        resp = textract.start_expense_analysis(
            DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "invoice.pdf"}}
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobId" in resp
        assert len(resp["JobId"]) > 0

    def test_get_expense_analysis_succeeded(self, textract):
        """get_expense_analysis retrieves results for a started job."""
        start_resp = textract.start_expense_analysis(
            DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "invoice.pdf"}}
        )
        job_id = start_resp["JobId"]

        resp = textract.get_expense_analysis(JobId=job_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["JobStatus"] == "SUCCEEDED"
        assert "DocumentMetadata" in resp
        assert "ExpenseDocuments" in resp

    def test_get_expense_analysis_invalid_job_id(self, textract):
        """get_expense_analysis with invalid job ID raises InvalidJobIdException."""
        with pytest.raises(ClientError) as exc:
            textract.get_expense_analysis(JobId="00000000000000000000000000000000")
        assert "InvalidJobIdException" in str(exc.value)


class TestTextractLendingAnalysis:
    """Tests for asynchronous lending analysis."""

    def test_start_lending_analysis(self, textract):
        """start_lending_analysis returns a job ID."""
        resp = textract.start_lending_analysis(
            DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "loan-app.pdf"}}
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobId" in resp
        assert len(resp["JobId"]) > 0

    def test_get_lending_analysis_succeeded(self, textract):
        """get_lending_analysis retrieves results for a started job."""
        start_resp = textract.start_lending_analysis(
            DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "loan-app.pdf"}}
        )
        job_id = start_resp["JobId"]

        resp = textract.get_lending_analysis(JobId=job_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["JobStatus"] == "SUCCEEDED"
        assert "DocumentMetadata" in resp
        assert "Results" in resp

    def test_get_lending_analysis_invalid_job_id(self, textract):
        """get_lending_analysis with invalid job ID raises InvalidJobIdException."""
        with pytest.raises(ClientError) as exc:
            textract.get_lending_analysis(JobId="00000000000000000000000000000000")
        assert "InvalidJobIdException" in str(exc.value)

    def test_get_lending_analysis_summary(self, textract):
        """get_lending_analysis_summary retrieves summary for a started job."""
        start_resp = textract.start_lending_analysis(
            DocumentLocation={"S3Object": {"Bucket": "test-bucket", "Name": "loan-app.pdf"}}
        )
        job_id = start_resp["JobId"]

        resp = textract.get_lending_analysis_summary(JobId=job_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["JobStatus"] == "SUCCEEDED"
        assert "DocumentMetadata" in resp

    def test_get_lending_analysis_summary_invalid_job_id(self, textract):
        """get_lending_analysis_summary with invalid job ID raises error."""
        with pytest.raises(ClientError) as exc:
            textract.get_lending_analysis_summary(JobId="00000000000000000000000000000000")
        assert "InvalidJobIdException" in str(exc.value)


class TestTextractAdapters:
    """Tests for Adapter CRUD operations."""

    def test_create_adapter(self, textract):
        """create_adapter returns an adapter ID."""
        name = _unique("adapter")
        resp = textract.create_adapter(AdapterName=name, FeatureTypes=["TABLES"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AdapterId" in resp
        assert len(resp["AdapterId"]) > 0
        # cleanup
        textract.delete_adapter(AdapterId=resp["AdapterId"])

    def test_create_adapter_with_description(self, textract):
        """create_adapter accepts optional description and auto-update setting."""
        name = _unique("adapter")
        resp = textract.create_adapter(
            AdapterName=name,
            FeatureTypes=["TABLES"],
            Description="Test adapter for unit coverage",
            AutoUpdate="DISABLED",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        adapter_id = resp["AdapterId"]
        # cleanup
        textract.delete_adapter(AdapterId=adapter_id)

    def test_get_adapter(self, textract):
        """get_adapter retrieves adapter details."""
        name = _unique("adapter")
        create_resp = textract.create_adapter(AdapterName=name, FeatureTypes=["FORMS"])
        adapter_id = create_resp["AdapterId"]

        resp = textract.get_adapter(AdapterId=adapter_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["AdapterId"] == adapter_id
        assert resp["AdapterName"] == name
        assert "FORMS" in resp["FeatureTypes"]
        assert "CreationTime" in resp
        # cleanup
        textract.delete_adapter(AdapterId=adapter_id)

    def test_get_adapter_not_found(self, textract):
        """get_adapter with nonexistent ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            textract.get_adapter(AdapterId="nonexistent-adapter-id")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "InvalidParameterException",
        )

    def test_update_adapter(self, textract):
        """update_adapter modifies adapter name."""
        name = _unique("adapter")
        create_resp = textract.create_adapter(AdapterName=name, FeatureTypes=["TABLES"])
        adapter_id = create_resp["AdapterId"]

        new_name = _unique("updated")
        resp = textract.update_adapter(AdapterId=adapter_id, AdapterName=new_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AdapterId" in resp

        # Verify update
        get_resp = textract.get_adapter(AdapterId=adapter_id)
        assert get_resp["AdapterName"] == new_name
        # cleanup
        textract.delete_adapter(AdapterId=adapter_id)

    def test_delete_adapter(self, textract):
        """delete_adapter removes the adapter."""
        name = _unique("adapter")
        create_resp = textract.create_adapter(AdapterName=name, FeatureTypes=["TABLES"])
        adapter_id = create_resp["AdapterId"]

        resp = textract.delete_adapter(AdapterId=adapter_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify it's gone
        with pytest.raises(ClientError):
            textract.get_adapter(AdapterId=adapter_id)

    def test_list_adapters(self, textract):
        """list_adapters returns all created adapters."""
        name = _unique("adapter")
        create_resp = textract.create_adapter(AdapterName=name, FeatureTypes=["TABLES"])
        adapter_id = create_resp["AdapterId"]

        resp = textract.list_adapters()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Adapters" in resp
        assert isinstance(resp["Adapters"], list)
        adapter_ids = [a["AdapterId"] for a in resp["Adapters"]]
        assert adapter_id in adapter_ids
        # cleanup
        textract.delete_adapter(AdapterId=adapter_id)


class TestTextractAdapterVersions:
    """Tests for AdapterVersion CRUD operations."""

    @pytest.fixture
    def adapter_id(self, textract):
        """Create and yield an adapter, then clean up."""
        resp = textract.create_adapter(AdapterName=_unique("av-test"), FeatureTypes=["TABLES"])
        aid = resp["AdapterId"]
        yield aid
        try:
            textract.delete_adapter(AdapterId=aid)
        except ClientError:
            pass

    def test_create_adapter_version(self, textract, adapter_id):
        """create_adapter_version returns adapter ID and version."""
        resp = textract.create_adapter_version(
            AdapterId=adapter_id,
            DatasetConfig={"ManifestS3Object": {"Bucket": "test-bucket", "Name": "manifest.json"}},
            OutputConfig={"S3Bucket": "test-output", "S3Prefix": "versions/"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AdapterId" in resp
        assert "AdapterVersion" in resp

    def test_get_adapter_version(self, textract, adapter_id):
        """get_adapter_version retrieves details about a version."""
        create_resp = textract.create_adapter_version(
            AdapterId=adapter_id,
            DatasetConfig={"ManifestS3Object": {"Bucket": "test-bucket", "Name": "manifest.json"}},
            OutputConfig={"S3Bucket": "test-output", "S3Prefix": "versions/"},
        )
        version = create_resp["AdapterVersion"]

        resp = textract.get_adapter_version(AdapterId=adapter_id, AdapterVersion=version)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["AdapterId"] == adapter_id
        assert resp["AdapterVersion"] == version
        assert "Status" in resp

    def test_list_adapter_versions(self, textract, adapter_id):
        """list_adapter_versions returns versions for a given adapter."""
        textract.create_adapter_version(
            AdapterId=adapter_id,
            DatasetConfig={"ManifestS3Object": {"Bucket": "test-bucket", "Name": "manifest.json"}},
            OutputConfig={"S3Bucket": "test-output", "S3Prefix": "versions/"},
        )

        resp = textract.list_adapter_versions(AdapterId=adapter_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AdapterVersions" in resp
        assert isinstance(resp["AdapterVersions"], list)
        assert len(resp["AdapterVersions"]) >= 1

    def test_delete_adapter_version(self, textract, adapter_id):
        """delete_adapter_version removes a version."""
        create_resp = textract.create_adapter_version(
            AdapterId=adapter_id,
            DatasetConfig={"ManifestS3Object": {"Bucket": "test-bucket", "Name": "manifest.json"}},
            OutputConfig={"S3Bucket": "test-output", "S3Prefix": "versions/"},
        )
        version = create_resp["AdapterVersion"]

        resp = textract.delete_adapter_version(AdapterId=adapter_id, AdapterVersion=version)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify it's gone
        with pytest.raises(ClientError):
            textract.get_adapter_version(AdapterId=adapter_id, AdapterVersion=version)


class TestTextractTagging:
    """Tests for resource tagging on Textract adapters."""

    @pytest.fixture
    def adapter_arn(self, textract):
        """Create an adapter and return its ARN."""
        resp = textract.create_adapter(AdapterName=_unique("tag-test"), FeatureTypes=["TABLES"])
        adapter_id = resp["AdapterId"]
        arn = f"arn:aws:textract:us-east-1:123456789012:adapter/{adapter_id}"
        yield arn
        try:
            textract.delete_adapter(AdapterId=adapter_id)
        except ClientError:
            pass

    def test_tag_resource(self, textract, adapter_arn):
        """tag_resource adds tags to a resource."""
        resp = textract.tag_resource(
            ResourceARN=adapter_arn,
            Tags={"env": "test", "project": "robotocore"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_tags_for_resource(self, textract, adapter_arn):
        """list_tags_for_resource returns tags for a resource."""
        textract.tag_resource(
            ResourceARN=adapter_arn,
            Tags={"env": "test", "project": "robotocore"},
        )
        resp = textract.list_tags_for_resource(ResourceARN=adapter_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Tags" in resp
        assert resp["Tags"]["env"] == "test"
        assert resp["Tags"]["project"] == "robotocore"

    def test_untag_resource(self, textract, adapter_arn):
        """untag_resource removes specific tags."""
        textract.tag_resource(
            ResourceARN=adapter_arn,
            Tags={"env": "test", "project": "robotocore", "owner": "jack"},
        )
        textract.untag_resource(ResourceARN=adapter_arn, TagKeys=["env", "owner"])

        resp = textract.list_tags_for_resource(ResourceARN=adapter_arn)
        assert "env" not in resp["Tags"]
        assert "owner" not in resp["Tags"]
        assert resp["Tags"].get("project") == "robotocore"

    def test_create_adapter_with_tags(self, textract):
        """create_adapter with Tags propagates tags to the resource."""
        name = _unique("tagged")
        resp = textract.create_adapter(
            AdapterName=name,
            FeatureTypes=["TABLES"],
            Tags={"created-by": "test"},
        )
        adapter_id = resp["AdapterId"]
        arn = f"arn:aws:textract:us-east-1:123456789012:adapter/{adapter_id}"

        tags_resp = textract.list_tags_for_resource(ResourceARN=arn)
        assert tags_resp["Tags"].get("created-by") == "test"
        # cleanup
        textract.delete_adapter(AdapterId=adapter_id)
