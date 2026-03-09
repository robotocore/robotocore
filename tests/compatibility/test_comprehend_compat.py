"""Comprehend compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def comprehend():
    return make_client("comprehend")


def _uid():
    return uuid.uuid4().hex[:8]


class TestComprehendOperations:
    def test_list_document_classifiers(self, comprehend):
        response = comprehend.list_document_classifiers()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(response["DocumentClassifierPropertiesList"], list)

    def test_list_entity_recognizers(self, comprehend):
        response = comprehend.list_entity_recognizers()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(response["EntityRecognizerPropertiesList"], list)

    def test_list_key_phrases_detection_jobs(self, comprehend):
        response = comprehend.list_key_phrases_detection_jobs()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(response["KeyPhrasesDetectionJobPropertiesList"], list)

    def test_detect_key_phrases(self, comprehend):
        response = comprehend.detect_key_phrases(
            Text="It is raining today in Seattle",
            LanguageCode="en",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(response["KeyPhrases"], list)
        for phrase in response["KeyPhrases"]:
            assert "Score" in phrase
            assert "BeginOffset" in phrase
            assert "EndOffset" in phrase

    def test_detect_sentiment(self, comprehend):
        response = comprehend.detect_sentiment(
            Text="I am very happy",
            LanguageCode="en",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Sentiment" in response
        assert response["Sentiment"] in ("POSITIVE", "NEGATIVE", "NEUTRAL", "MIXED")
        score = response["SentimentScore"]
        assert "Positive" in score
        assert "Negative" in score
        assert "Neutral" in score
        assert "Mixed" in score

    def test_list_sentiment_detection_jobs(self, comprehend):
        response = comprehend.list_sentiment_detection_jobs()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(response["SentimentDetectionJobPropertiesList"], list)

    def test_list_topics_detection_jobs(self, comprehend):
        response = comprehend.list_topics_detection_jobs()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(response["TopicsDetectionJobPropertiesList"], list)


class TestComprehendAutoCoverage:
    """Auto-generated coverage tests for comprehend."""

    @pytest.fixture
    def client(self):
        return make_client("comprehend")

    def test_list_document_classification_jobs(self, client):
        """ListDocumentClassificationJobs returns a response."""
        resp = client.list_document_classification_jobs()
        assert "DocumentClassificationJobPropertiesList" in resp

    def test_list_dominant_language_detection_jobs(self, client):
        """ListDominantLanguageDetectionJobs returns a response."""
        resp = client.list_dominant_language_detection_jobs()
        assert "DominantLanguageDetectionJobPropertiesList" in resp

    def test_list_endpoints(self, client):
        """ListEndpoints returns a response."""
        resp = client.list_endpoints()
        assert "EndpointPropertiesList" in resp

    def test_list_entities_detection_jobs(self, client):
        """ListEntitiesDetectionJobs returns a response."""
        resp = client.list_entities_detection_jobs()
        assert "EntitiesDetectionJobPropertiesList" in resp

    def test_list_events_detection_jobs(self, client):
        """ListEventsDetectionJobs returns a response."""
        resp = client.list_events_detection_jobs()
        assert "EventsDetectionJobPropertiesList" in resp

    def test_list_flywheels(self, client):
        """ListFlywheels returns a response."""
        resp = client.list_flywheels()
        assert "FlywheelSummaryList" in resp

    def test_list_pii_entities_detection_jobs(self, client):
        """ListPiiEntitiesDetectionJobs returns a response."""
        resp = client.list_pii_entities_detection_jobs()
        assert "PiiEntitiesDetectionJobPropertiesList" in resp

    def test_list_targeted_sentiment_detection_jobs(self, client):
        """ListTargetedSentimentDetectionJobs returns a response."""
        resp = client.list_targeted_sentiment_detection_jobs()
        assert "TargetedSentimentDetectionJobPropertiesList" in resp

    def test_create_and_describe_endpoint(self, client):
        """CreateEndpoint + DescribeEndpoint lifecycle."""
        name = f"test-ep-{_uid()}"
        model_arn = "arn:aws:comprehend:us-east-1:123456789012:document-classifier/test-cls"
        create_resp = client.create_endpoint(
            EndpointName=name,
            ModelArn=model_arn,
            DesiredInferenceUnits=1,
        )
        endpoint_arn = create_resp["EndpointArn"]
        assert endpoint_arn
        try:
            desc = client.describe_endpoint(EndpointArn=endpoint_arn)
            props = desc["EndpointProperties"]
            assert props["EndpointArn"] == endpoint_arn
            assert props["Status"] == "IN_SERVICE"
            assert props["DesiredInferenceUnits"] == 1
        finally:
            client.delete_endpoint(EndpointArn=endpoint_arn)

    def test_update_endpoint(self, client):
        """UpdateEndpoint accepts valid request and returns 200."""
        name = f"test-ep-{_uid()}"
        model_arn = "arn:aws:comprehend:us-east-1:123456789012:document-classifier/test-cls"
        create_resp = client.create_endpoint(
            EndpointName=name,
            ModelArn=model_arn,
            DesiredInferenceUnits=1,
        )
        endpoint_arn = create_resp["EndpointArn"]
        try:
            update_resp = client.update_endpoint(
                EndpointArn=endpoint_arn,
                DesiredInferenceUnits=2,
            )
            assert update_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            client.delete_endpoint(EndpointArn=endpoint_arn)

    def test_delete_endpoint(self, client):
        """DeleteEndpoint removes an endpoint."""
        name = f"test-ep-{_uid()}"
        model_arn = "arn:aws:comprehend:us-east-1:123456789012:document-classifier/test-cls"
        create_resp = client.create_endpoint(
            EndpointName=name,
            ModelArn=model_arn,
            DesiredInferenceUnits=1,
        )
        endpoint_arn = create_resp["EndpointArn"]
        del_resp = client.delete_endpoint(EndpointArn=endpoint_arn)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        with pytest.raises(ClientError) as exc:
            client.describe_endpoint(EndpointArn=endpoint_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_endpoint_nonexistent(self, client):
        """DescribeEndpoint with nonexistent ARN raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.describe_endpoint(
                EndpointArn="arn:aws:comprehend:us-east-1:123456789012:endpoint/nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_document_classification_job(self, client):
        """DescribeDocumentClassificationJob with fake ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.describe_document_classification_job(JobId="fake-job-id-12345")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_document_classifier(self, client):
        """DescribeDocumentClassifier with fake ARN raises ResourceNotFoundException."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:document-classifier/fake-cls"
        with pytest.raises(ClientError) as exc:
            client.describe_document_classifier(DocumentClassifierArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_dominant_language_detection_job(self, client):
        """DescribeDominantLanguageDetectionJob with fake ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.describe_dominant_language_detection_job(JobId="fake-job-id-12345")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_entities_detection_job(self, client):
        """DescribeEntitiesDetectionJob with fake ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.describe_entities_detection_job(JobId="fake-job-id-12345")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_entity_recognizer(self, client):
        """DescribeEntityRecognizer with fake ARN raises ResourceNotFoundException."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:entity-recognizer/fake-rec"
        with pytest.raises(ClientError) as exc:
            client.describe_entity_recognizer(EntityRecognizerArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_events_detection_job(self, client):
        """DescribeEventsDetectionJob with fake ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.describe_events_detection_job(JobId="fake-job-id-12345")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_flywheel(self, client):
        """DescribeFlywheel with fake ARN raises ResourceNotFoundException."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:flywheel/fake-flywheel"
        with pytest.raises(ClientError) as exc:
            client.describe_flywheel(FlywheelArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_key_phrases_detection_job(self, client):
        """DescribeKeyPhrasesDetectionJob with fake ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.describe_key_phrases_detection_job(JobId="fake-job-id-12345")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_pii_entities_detection_job(self, client):
        """DescribePiiEntitiesDetectionJob with fake ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.describe_pii_entities_detection_job(JobId="fake-job-id-12345")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_resource_policy(self, client):
        """DescribeResourcePolicy with fake ARN raises ResourceNotFoundException."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:document-classifier/fake-policy"
        with pytest.raises(ClientError) as exc:
            client.describe_resource_policy(ResourceArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_sentiment_detection_job(self, client):
        """DescribeSentimentDetectionJob with fake ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.describe_sentiment_detection_job(JobId="fake-job-id-12345")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_targeted_sentiment_detection_job(self, client):
        """DescribeTargetedSentimentDetectionJob with fake ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.describe_targeted_sentiment_detection_job(JobId="fake-job-id-12345")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_topics_detection_job(self, client):
        """DescribeTopicsDetectionJob with fake ID raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.describe_topics_detection_job(JobId="fake-job-id-12345")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_tags_for_resource(self, client):
        """ListTagsForResource with fake ARN returns empty tags list."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:document-classifier/fake-tags"
        resp = client.list_tags_for_resource(ResourceArn=fake_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp.get("Tags", []), list)

    def test_detect_pii_entities(self, client):
        """DetectPiiEntities returns PII entity list."""
        resp = client.detect_pii_entities(
            Text="My SSN is 123-45-6789 and my email is test@example.com",
            LanguageCode="en",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp["Entities"], list)
