"""Compatibility tests for Amazon Textract.

Operations recorded `not_implemented` (501) by probes/textract.json are
outside the moto-era surface; their classes were removed per the G2 rule:
AnalyzeDocument, AnalyzeExpense, the expense/lending analysis pairs and
summaries, adapter CRUD plus adapter versions, and resource tagging.
AnalyzeID stays: its probe could not auto-fill `DocumentPages`
(needs_params — unknown, not proven absent) and moto implements the
handler.
"""

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def textract():
    return make_client("textract")


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

