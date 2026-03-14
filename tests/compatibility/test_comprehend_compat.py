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

    def test_stop_training_document_classifier_nonexistent(self, client):
        """StopTrainingDocumentClassifier with nonexistent ARN raises ResourceNotFoundException."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:document-classifier/nonexistent-cls"
        with pytest.raises(ClientError) as exc:
            client.stop_training_document_classifier(DocumentClassifierArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestComprehendEndpointTagging:
    """Test tag operations on Comprehend endpoints."""

    @pytest.fixture
    def client(self):
        return make_client("comprehend")

    @pytest.fixture
    def endpoint_arn(self, client):
        """Create an endpoint for tagging tests, clean up after."""
        name = f"test-ep-{_uid()}"
        model_arn = "arn:aws:comprehend:us-east-1:123456789012:document-classifier/test-cls"
        resp = client.create_endpoint(
            EndpointName=name,
            ModelArn=model_arn,
            DesiredInferenceUnits=1,
        )
        arn = resp["EndpointArn"]
        yield arn
        try:
            client.delete_endpoint(EndpointArn=arn)
        except ClientError:
            pass

    def test_tag_and_list_tags(self, client, endpoint_arn):
        """TagResource adds tags visible via ListTagsForResource."""
        client.tag_resource(
            ResourceArn=endpoint_arn,
            Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "ml"}],
        )
        resp = client.list_tags_for_resource(ResourceArn=endpoint_arn)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags["env"] == "test"
        assert tags["team"] == "ml"

    def test_untag_resource(self, client, endpoint_arn):
        """UntagResource removes specified tag keys."""
        client.tag_resource(
            ResourceArn=endpoint_arn,
            Tags=[{"Key": "a", "Value": "1"}, {"Key": "b", "Value": "2"}],
        )
        client.untag_resource(ResourceArn=endpoint_arn, TagKeys=["a"])
        resp = client.list_tags_for_resource(ResourceArn=endpoint_arn)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert "a" not in tags
        assert tags["b"] == "2"

    def test_tag_overwrite(self, client, endpoint_arn):
        """TagResource overwrites existing tag values."""
        client.tag_resource(
            ResourceArn=endpoint_arn,
            Tags=[{"Key": "env", "Value": "dev"}],
        )
        client.tag_resource(
            ResourceArn=endpoint_arn,
            Tags=[{"Key": "env", "Value": "prod"}],
        )
        resp = client.list_tags_for_resource(ResourceArn=endpoint_arn)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags["env"] == "prod"


class TestComprehendAdditionalOperations:
    """Additional Comprehend operation coverage."""

    @pytest.fixture
    def client(self):
        return make_client("comprehend")

    def test_stop_training_entity_recognizer_nonexistent(self, client):
        """StopTrainingEntityRecognizer with nonexistent ARN raises ResourceNotFoundException."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:entity-recognizer/nonexistent-rec"
        with pytest.raises(ClientError) as exc:
            client.stop_training_entity_recognizer(EntityRecognizerArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_endpoints_finds_created(self, client):
        """ListEndpoints returns an endpoint that was just created."""
        name = f"test-list-{_uid()}"
        model_arn = "arn:aws:comprehend:us-east-1:123456789012:document-classifier/test-cls"
        resp = client.create_endpoint(
            EndpointName=name,
            ModelArn=model_arn,
            DesiredInferenceUnits=1,
        )
        ep_arn = resp["EndpointArn"]
        try:
            listed = client.list_endpoints()
            arns = [e["EndpointArn"] for e in listed["EndpointPropertiesList"]]
            assert ep_arn in arns
        finally:
            client.delete_endpoint(EndpointArn=ep_arn)

    def test_endpoint_lifecycle_create_describe_update_delete(self, client):
        """Full endpoint CRUD lifecycle."""
        name = f"test-crud-{_uid()}"
        model_arn = "arn:aws:comprehend:us-east-1:123456789012:document-classifier/test-cls"

        # Create
        create_resp = client.create_endpoint(
            EndpointName=name,
            ModelArn=model_arn,
            DesiredInferenceUnits=1,
        )
        ep_arn = create_resp["EndpointArn"]
        assert ep_arn

        try:
            # Describe
            desc = client.describe_endpoint(EndpointArn=ep_arn)
            assert desc["EndpointProperties"]["Status"] == "IN_SERVICE"

            # Update
            upd = client.update_endpoint(EndpointArn=ep_arn, DesiredInferenceUnits=2)
            assert upd["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            # Delete
            client.delete_endpoint(EndpointArn=ep_arn)

        # Verify deleted
        with pytest.raises(ClientError) as exc:
            client.describe_endpoint(EndpointArn=ep_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestComprehendFlywheelAndDatasetOps:
    """Tests for flywheel and dataset operations."""

    @pytest.fixture
    def client(self):
        return make_client("comprehend")

    def test_describe_dataset_nonexistent(self, client):
        """DescribeDataset with fake ARN raises ResourceNotFoundException."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:flywheel/fw/dataset/ds-fake"
        with pytest.raises(ClientError) as exc:
            client.describe_dataset(DatasetArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_flywheel_iteration_nonexistent(self, client):
        """DescribeFlywheelIteration with fake flywheel raises ResourceNotFoundException."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:flywheel/fake-fw"
        with pytest.raises(ClientError) as exc:
            client.describe_flywheel_iteration(
                FlywheelArn=fake_arn,
                FlywheelIterationId="iter-fake",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_datasets_nonexistent_flywheel(self, client):
        """ListDatasets with fake flywheel ARN raises ResourceNotFoundException."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:flywheel/fake-fw"
        with pytest.raises(ClientError) as exc:
            client.list_datasets(FlywheelArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_document_classifier_summaries(self, client):
        """ListDocumentClassifierSummaries returns a list."""
        resp = client.list_document_classifier_summaries()
        assert "DocumentClassifierSummariesList" in resp
        assert isinstance(resp["DocumentClassifierSummariesList"], list)

    def test_list_entity_recognizer_summaries(self, client):
        """ListEntityRecognizerSummaries returns a list."""
        resp = client.list_entity_recognizer_summaries()
        assert "EntityRecognizerSummariesList" in resp
        assert isinstance(resp["EntityRecognizerSummariesList"], list)

    def test_list_flywheel_iteration_history_nonexistent(self, client):
        """ListFlywheelIterationHistory with fake ARN raises ResourceNotFoundException."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:flywheel/fake-fw"
        with pytest.raises(ClientError) as exc:
            client.list_flywheel_iteration_history(FlywheelArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestComprehendDetectionOps:
    """Tests for Comprehend detection operations."""

    @pytest.fixture
    def client(self):
        return make_client("comprehend")

    def test_detect_dominant_language(self, client):
        """DetectDominantLanguage returns language list."""
        resp = client.detect_dominant_language(Text="Hello, how are you today?")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp["Languages"], list)
        for lang in resp["Languages"]:
            assert "LanguageCode" in lang
            assert "Score" in lang

    def test_detect_entities(self, client):
        """DetectEntities returns entity list."""
        resp = client.detect_entities(
            Text="John Smith lives in Seattle and works at Amazon",
            LanguageCode="en",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp["Entities"], list)
        for ent in resp["Entities"]:
            assert "Type" in ent
            assert "Text" in ent
            assert "Score" in ent

    def test_detect_syntax(self, client):
        """DetectSyntax returns syntax tokens."""
        resp = client.detect_syntax(
            Text="The quick brown fox jumps over the lazy dog.",
            LanguageCode="en",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp["SyntaxTokens"], list)
        for token in resp["SyntaxTokens"]:
            assert "TokenId" in token
            assert "Text" in token
            assert "PartOfSpeech" in token

    def test_detect_targeted_sentiment(self, client):
        """DetectTargetedSentiment returns entities with sentiment."""
        resp = client.detect_targeted_sentiment(
            Text="I love the food but hate the service",
            LanguageCode="en",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp["Entities"], list)

    def test_contains_pii_entities(self, client):
        """ContainsPiiEntities returns labels list."""
        resp = client.contains_pii_entities(
            Text="My SSN is 123-45-6789",
            LanguageCode="en",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp["Labels"], list)

    def test_classify_document(self, client):
        """ClassifyDocument returns classification results."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:document-classifier-endpoint/fake"
        resp = client.classify_document(Text="test text", EndpointArn=fake_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Classes" in resp or "Labels" in resp


class TestComprehendResourcePolicyOps:
    """Tests for Comprehend resource policy operations."""

    @pytest.fixture
    def client(self):
        return make_client("comprehend")

    def test_delete_resource_policy_nonexistent(self, client):
        """DeleteResourcePolicy with fake ARN raises ResourceNotFoundException."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:document-classifier/no-policy"
        with pytest.raises(ClientError) as exc:
            client.delete_resource_policy(ResourceArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_put_resource_policy(self, client):
        """PutResourcePolicy accepts a policy and returns a revision ID."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:document-classifier/fake-cls"
        policy = (
            '{"Version":"2012-10-17","Statement":'
            '[{"Effect":"Allow","Principal":{"AWS":"123456789012"},'
            '"Action":"comprehend:DescribeDocumentClassifier",'
            '"Resource":"*"}]}'
        )
        resp = client.put_resource_policy(ResourceArn=fake_arn, ResourcePolicy=policy)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "PolicyRevisionId" in resp


class TestComprehendJobOps:
    """Tests for Comprehend job start/stop operations."""

    @pytest.fixture
    def client(self):
        return make_client("comprehend")

    def test_stop_dominant_language_detection_job(self, client):
        """StopDominantLanguageDetectionJob with fake job raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.stop_dominant_language_detection_job(JobId="fake-job-id-00000")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_stop_entities_detection_job(self, client):
        """StopEntitiesDetectionJob with fake job raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.stop_entities_detection_job(JobId="fake-job-id-00000")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_stop_events_detection_job(self, client):
        """StopEventsDetectionJob with fake job raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.stop_events_detection_job(JobId="fake-job-id-00000")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_stop_key_phrases_detection_job(self, client):
        """StopKeyPhrasesDetectionJob with fake job raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.stop_key_phrases_detection_job(JobId="fake-job-id-00000")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_stop_pii_entities_detection_job(self, client):
        """StopPiiEntitiesDetectionJob with fake job raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.stop_pii_entities_detection_job(JobId="fake-job-id-00000")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_stop_sentiment_detection_job(self, client):
        """StopSentimentDetectionJob with fake job raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.stop_sentiment_detection_job(JobId="fake-job-id-00000")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_stop_targeted_sentiment_detection_job(self, client):
        """StopTargetedSentimentDetectionJob with fake job raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.stop_targeted_sentiment_detection_job(JobId="fake-job-id-00000")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_document_classifier_nonexistent(self, client):
        """DeleteDocumentClassifier with fake ARN returns 200."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:document-classifier/fake-cls"
        resp = client.delete_document_classifier(DocumentClassifierArn=fake_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_entity_recognizer_nonexistent(self, client):
        """DeleteEntityRecognizer with fake ARN returns 200."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:entity-recognizer/fake-rec"
        resp = client.delete_entity_recognizer(EntityRecognizerArn=fake_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_flywheel_nonexistent(self, client):
        """DeleteFlywheel with fake ARN returns 200."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:flywheel/fake-fw"
        resp = client.delete_flywheel(FlywheelArn=fake_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_start_flywheel_iteration_nonexistent(self, client):
        """StartFlywheelIteration with fake ARN raises ResourceNotFoundException."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:flywheel/fake-fw"
        with pytest.raises(ClientError) as exc:
            client.start_flywheel_iteration(FlywheelArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_flywheel_nonexistent(self, client):
        """UpdateFlywheel with fake ARN raises ResourceNotFoundException."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:flywheel/fake-fw"
        with pytest.raises(ClientError) as exc:
            client.update_flywheel(FlywheelArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_dataset_nonexistent_flywheel(self, client):
        """CreateDataset with nonexistent flywheel raises ResourceNotFoundException."""
        fake_arn = "arn:aws:comprehend:us-east-1:123456789012:flywheel/fake-fw"
        with pytest.raises(ClientError) as exc:
            client.create_dataset(
                FlywheelArn=fake_arn,
                DatasetName=f"test-ds-{_uid()}",
                DatasetType="TRAIN",
                InputDataConfig={
                    "DataFormat": "COMPREHEND_CSV",
                    "DocumentClassifierInputDataConfig": {
                        "S3Uri": "s3://fake-bucket/data.csv",
                    },
                },
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_document_classifier(self, client):
        """CreateDocumentClassifier returns a classifier ARN."""
        name = f"test-cls-{_uid()}"
        resp = client.create_document_classifier(
            DocumentClassifierName=name,
            DataAccessRoleArn="arn:aws:iam::123456789012:role/comprehend-role",
            InputDataConfig={
                "DataFormat": "COMPREHEND_CSV",
                "S3Uri": "s3://fake-bucket/training-data.csv",
            },
            LanguageCode="en",
        )
        assert "DocumentClassifierArn" in resp
        assert name in resp["DocumentClassifierArn"]

    def test_create_entity_recognizer(self, client):
        """CreateEntityRecognizer returns an entity recognizer ARN."""
        name = f"test-rec-{_uid()}"
        resp = client.create_entity_recognizer(
            RecognizerName=name,
            DataAccessRoleArn="arn:aws:iam::123456789012:role/comprehend-role",
            InputDataConfig={
                "DataFormat": "COMPREHEND_CSV",
                "EntityTypes": [{"Type": "PERSON"}],
                "Documents": {"S3Uri": "s3://fake-bucket/docs.csv"},
                "EntityList": {"S3Uri": "s3://fake-bucket/entities.csv"},
            },
            LanguageCode="en",
        )
        assert "EntityRecognizerArn" in resp
        assert name in resp["EntityRecognizerArn"]

    def test_create_flywheel(self, client):
        """CreateFlywheel returns a flywheel ARN."""
        name = f"test-fw-{_uid()}"
        resp = client.create_flywheel(
            FlywheelName=name,
            DataAccessRoleArn="arn:aws:iam::123456789012:role/comprehend-role",
            DataLakeS3Uri="s3://fake-bucket/datalake/",
        )
        assert "FlywheelArn" in resp
        assert name in resp["FlywheelArn"]

    def test_import_model(self, client):
        """ImportModel accepts a source model ARN."""
        source_arn = "arn:aws:comprehend:us-east-1:123456789012:document-classifier/test-cls"
        resp = client.import_model(SourceModelArn=source_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ModelArn" in resp

    def test_start_dominant_language_detection_job(self, client):
        """StartDominantLanguageDetectionJob returns a job ID."""
        resp = client.start_dominant_language_detection_job(
            InputDataConfig={
                "S3Uri": "s3://fake-bucket/input/",
                "InputFormat": "ONE_DOC_PER_LINE",
            },
            OutputDataConfig={"S3Uri": "s3://fake-bucket/output/"},
            DataAccessRoleArn="arn:aws:iam::123456789012:role/comprehend-role",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobId" in resp
        assert "JobStatus" in resp

    def test_start_entities_detection_job(self, client):
        """StartEntitiesDetectionJob returns a job ID."""
        resp = client.start_entities_detection_job(
            InputDataConfig={
                "S3Uri": "s3://fake-bucket/input/",
                "InputFormat": "ONE_DOC_PER_LINE",
            },
            OutputDataConfig={"S3Uri": "s3://fake-bucket/output/"},
            DataAccessRoleArn="arn:aws:iam::123456789012:role/comprehend-role",
            LanguageCode="en",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobId" in resp
        assert "JobStatus" in resp

    def test_start_key_phrases_detection_job(self, client):
        """StartKeyPhrasesDetectionJob returns a job ID."""
        resp = client.start_key_phrases_detection_job(
            InputDataConfig={
                "S3Uri": "s3://fake-bucket/input/",
                "InputFormat": "ONE_DOC_PER_LINE",
            },
            OutputDataConfig={"S3Uri": "s3://fake-bucket/output/"},
            DataAccessRoleArn="arn:aws:iam::123456789012:role/comprehend-role",
            LanguageCode="en",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobId" in resp
        assert "JobStatus" in resp

    def test_start_pii_entities_detection_job(self, client):
        """StartPiiEntitiesDetectionJob returns a job ID."""
        resp = client.start_pii_entities_detection_job(
            InputDataConfig={
                "S3Uri": "s3://fake-bucket/input/",
                "InputFormat": "ONE_DOC_PER_LINE",
            },
            OutputDataConfig={"S3Uri": "s3://fake-bucket/output/"},
            Mode="ONLY_OFFSETS",
            DataAccessRoleArn="arn:aws:iam::123456789012:role/comprehend-role",
            LanguageCode="en",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobId" in resp
        assert "JobStatus" in resp

    def test_start_sentiment_detection_job(self, client):
        """StartSentimentDetectionJob returns a job ID."""
        resp = client.start_sentiment_detection_job(
            InputDataConfig={
                "S3Uri": "s3://fake-bucket/input/",
                "InputFormat": "ONE_DOC_PER_LINE",
            },
            OutputDataConfig={"S3Uri": "s3://fake-bucket/output/"},
            DataAccessRoleArn="arn:aws:iam::123456789012:role/comprehend-role",
            LanguageCode="en",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobId" in resp
        assert "JobStatus" in resp

    def test_start_targeted_sentiment_detection_job(self, client):
        """StartTargetedSentimentDetectionJob returns a job ID."""
        resp = client.start_targeted_sentiment_detection_job(
            InputDataConfig={
                "S3Uri": "s3://fake-bucket/input/",
                "InputFormat": "ONE_DOC_PER_LINE",
            },
            OutputDataConfig={"S3Uri": "s3://fake-bucket/output/"},
            DataAccessRoleArn="arn:aws:iam::123456789012:role/comprehend-role",
            LanguageCode="en",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobId" in resp
        assert "JobStatus" in resp

    def test_start_topics_detection_job(self, client):
        """StartTopicsDetectionJob returns a job ID."""
        resp = client.start_topics_detection_job(
            InputDataConfig={
                "S3Uri": "s3://fake-bucket/input/",
                "InputFormat": "ONE_DOC_PER_LINE",
            },
            OutputDataConfig={"S3Uri": "s3://fake-bucket/output/"},
            DataAccessRoleArn="arn:aws:iam::123456789012:role/comprehend-role",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobId" in resp
        assert "JobStatus" in resp

    def test_start_document_classification_job(self, client):
        """StartDocumentClassificationJob returns a job ID."""
        resp = client.start_document_classification_job(
            DocumentClassifierArn=(
                "arn:aws:comprehend:us-east-1:123456789012:document-classifier/test-cls"
            ),
            InputDataConfig={
                "S3Uri": "s3://fake-bucket/input/",
                "InputFormat": "ONE_DOC_PER_LINE",
            },
            OutputDataConfig={"S3Uri": "s3://fake-bucket/output/"},
            DataAccessRoleArn="arn:aws:iam::123456789012:role/comprehend-role",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobId" in resp
        assert "JobStatus" in resp

    def test_start_events_detection_job(self, client):
        """StartEventsDetectionJob returns a job ID and status."""
        resp = client.start_events_detection_job(
            InputDataConfig={
                "S3Uri": "s3://fake-bucket/input/",
                "InputFormat": "ONE_DOC_PER_LINE",
            },
            OutputDataConfig={"S3Uri": "s3://fake-bucket/output/"},
            DataAccessRoleArn="arn:aws:iam::123456789012:role/comprehend-role",
            LanguageCode="en",
            TargetEventTypes=["BANKRUPTCY", "EMPLOYMENT"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "JobId" in resp
        assert "JobStatus" in resp
