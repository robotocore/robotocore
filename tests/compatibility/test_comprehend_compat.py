"""Comprehend compatibility tests."""

import uuid

import pytest

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
