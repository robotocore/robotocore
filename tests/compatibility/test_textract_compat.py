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
