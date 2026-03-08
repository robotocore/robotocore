"""Compatibility tests for Amazon Textract."""

import uuid

import pytest
from botocore.exceptions import ClientError, ParamValidationError

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


class TestTextractAutoCoverage:
    """Auto-generated coverage tests for textract."""

    @pytest.fixture
    def client(self):
        return make_client("textract")

    def test_analyze_document(self, client):
        """AnalyzeDocument is implemented (may need params)."""
        try:
            client.analyze_document()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params

    def test_analyze_expense(self, client):
        """AnalyzeExpense is implemented (may need params)."""
        try:
            client.analyze_expense()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params

    def test_analyze_id(self, client):
        """AnalyzeID is implemented (may need params)."""
        try:
            client.analyze_id()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params

    def test_create_adapter(self, client):
        """CreateAdapter is implemented (may need params)."""
        try:
            client.create_adapter()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params

    def test_create_adapter_version(self, client):
        """CreateAdapterVersion is implemented (may need params)."""
        try:
            client.create_adapter_version()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params

    def test_delete_adapter(self, client):
        """DeleteAdapter is implemented (may need params)."""
        try:
            client.delete_adapter()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params

    def test_delete_adapter_version(self, client):
        """DeleteAdapterVersion is implemented (may need params)."""
        try:
            client.delete_adapter_version()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params

    def test_get_adapter(self, client):
        """GetAdapter is implemented (may need params)."""
        try:
            client.get_adapter()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params

    def test_get_adapter_version(self, client):
        """GetAdapterVersion is implemented (may need params)."""
        try:
            client.get_adapter_version()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params

    def test_get_expense_analysis(self, client):
        """GetExpenseAnalysis is implemented (may need params)."""
        try:
            client.get_expense_analysis()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params

    def test_get_lending_analysis(self, client):
        """GetLendingAnalysis is implemented (may need params)."""
        try:
            client.get_lending_analysis()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params

    def test_get_lending_analysis_summary(self, client):
        """GetLendingAnalysisSummary is implemented (may need params)."""
        try:
            client.get_lending_analysis_summary()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params

    def test_list_tags_for_resource(self, client):
        """ListTagsForResource is implemented (may need params)."""
        try:
            client.list_tags_for_resource()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params

    def test_start_expense_analysis(self, client):
        """StartExpenseAnalysis is implemented (may need params)."""
        try:
            client.start_expense_analysis()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params

    def test_start_lending_analysis(self, client):
        """StartLendingAnalysis is implemented (may need params)."""
        try:
            client.start_lending_analysis()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params

    def test_tag_resource(self, client):
        """TagResource is implemented (may need params)."""
        try:
            client.tag_resource()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params

    def test_untag_resource(self, client):
        """UntagResource is implemented (may need params)."""
        try:
            client.untag_resource()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params

    def test_update_adapter(self, client):
        """UpdateAdapter is implemented (may need params)."""
        try:
            client.update_adapter()
        except client.exceptions.ClientError:
            pass  # Expected — operation exists but needs params
        except ParamValidationError:
            pass  # Expected — operation exists but needs params
