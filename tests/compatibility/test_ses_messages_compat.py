"""Live SES inspection tests.

Run against LocalStack with ENDPOINT_URL set to its endpoint and
SES_MESSAGES_PATH=/_aws/ses. Robotocore uses /_robotocore/ses/messages.
"""

import os
import uuid

import pytest
import requests
from botocore.exceptions import ClientError

from tests.compatibility.conftest import ENDPOINT_URL, make_client


class SesMessagesDriver:
    def __init__(self, url: str):
        self.url = url

    def read_messages(
        self, *, message_id: str | None = None, email: str | None = None
    ) -> requests.Response:
        return requests.get(self.url, params={"id": message_id, "email": email}, timeout=10)

    def delete_message(self, message_id: str) -> requests.Response:
        return requests.delete(self.url, params={"id": message_id}, timeout=10)


class SesMessagesDsl:
    def __init__(self, driver: SesMessagesDriver):
        self.driver = driver

    def verify_messages(
        self,
        expected: list[dict[str, object]],
        *,
        message_id: str | None = None,
        email: str | None = None,
    ) -> None:
        response = self.driver.read_messages(message_id=message_id, email=email)
        assert response.status_code == 200, response.text
        messages = response.json()["messages"]
        assert len(messages) == len(expected), messages
        by_id = {message["Id"]: message for message in messages}
        for expected_message in expected:
            assert expected_message["Id"] in by_id, messages
            actual = by_id[expected_message["Id"]]
            for key, value in expected_message.items():
                assert actual[key] == value, actual

    def delete_message(self, message_id: str) -> None:
        response = self.driver.delete_message(message_id)
        assert response.status_code == 204, response.text


@pytest.fixture
def ses():
    client = make_client("ses", aws_access_key_id=f"{uuid.uuid4().int % 10**12:012d}")
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def ses_messages():
    path = os.environ.get("SES_MESSAGES_PATH", "/_robotocore/ses/messages")
    return SesMessagesDsl(SesMessagesDriver(f"{ENDPOINT_URL.rstrip('/')}{path}"))


@pytest.fixture
def sender(ses, ses_messages):
    source = f"inspection-{uuid.uuid4().hex[:8]}@example.com"
    ses.verify_email_identity(EmailAddress=source)
    try:
        yield source
    finally:
        try:
            response = ses_messages.driver.read_messages(email=source)
            response.raise_for_status()
            for message in response.json()["messages"]:
                if message["Source"] == source:
                    ses_messages.delete_message(message["Id"])
        finally:
            ses.delete_identity(Identity=source)


class TestSESSentMessages:
    def test_send_raw_email_is_inspectable_without_explicit_source(self, ses, ses_messages, sender):
        raw_message = (
            f"From: {sender}\r\n"
            "To: recipient@example.com\r\n"
            "Subject: Raw inspection\r\n"
            "MIME-Version: 1.0\r\n"
            "Content-Type: text/plain; charset=utf-8\r\n"
            "\r\n"
            "Raw message body.\r\n"
        )
        response = ses.send_raw_email(RawMessage={"Data": raw_message})
        ses_messages.verify_messages(
            [
                {
                    "Id": response["MessageId"],
                    "Source": sender,
                    "Region": ses.meta.region_name,
                    "RawData": raw_message,
                }
            ],
            email=sender,
        )

    @pytest.mark.parametrize(
        "body",
        [
            {"Text": {"Data": "Plain text"}},
            {"Html": {"Data": "<p>HTML</p>"}},
            {"Text": {"Data": "Plain text"}, "Html": {"Data": "<p>HTML</p>"}},
        ],
        ids=["text", "html", "multipart"],
    )
    def test_send_email_records_structured_fields(self, ses, ses_messages, sender, body):
        destination = {"ToAddresses": ["recipient@example.com"]}
        response = ses.send_email(
            Source=sender,
            Destination=destination,
            Message={"Subject": {"Data": "Structured inspection"}, "Body": body},
        )
        ses_messages.verify_messages(
            [
                {
                    "Id": response["MessageId"],
                    "Source": sender,
                    "Region": ses.meta.region_name,
                    "Destination": destination,
                    "Subject": "Structured inspection",
                    "Body": {
                        "text_part": body.get("Text", {}).get("Data"),
                        "html_part": body.get("Html", {}).get("Data"),
                    },
                }
            ],
            email=sender,
        )

    def test_send_templated_email_records_template_fields(self, ses, ses_messages, sender):
        template = f"inspection-{uuid.uuid4().hex[:8]}"
        destination = {"ToAddresses": ["recipient@example.com"]}
        template_data = '{"name": "World"}'
        ses.create_template(
            Template={
                "TemplateName": template,
                "SubjectPart": "Hello {{name}}",
                "TextPart": "Hello {{name}}",
                "HtmlPart": "<p>Hello {{name}}</p>",
            }
        )
        try:
            response = ses.send_templated_email(
                Source=sender,
                Destination=destination,
                Template=template,
                TemplateData=template_data,
            )
            ses_messages.verify_messages(
                [
                    {
                        "Id": response["MessageId"],
                        "Source": sender,
                        "Region": ses.meta.region_name,
                        "Destination": destination,
                        "Template": template,
                        "TemplateData": template_data,
                    }
                ],
                email=sender,
            )
        finally:
            ses.delete_template(TemplateName=template)

    def test_filters_and_deletion_preserve_other_messages(self, ses, ses_messages, sender):
        expected = []
        for subject in ("First", "Second"):
            response = ses.send_email(
                Source=sender,
                Destination={"ToAddresses": ["recipient@example.com"]},
                Message={
                    "Subject": {"Data": subject},
                    "Body": {"Text": {"Data": "Inspection body"}},
                },
            )
            expected.append({"Id": response["MessageId"], "Source": sender, "Subject": subject})

        message_id = expected[0]["Id"]
        ses_messages.verify_messages(expected, email=sender)
        ses_messages.verify_messages(expected, message_id="", email=sender)
        ses_messages.verify_messages([expected[0]], message_id=message_id, email=sender)
        ses_messages.verify_messages([expected[0]], message_id=message_id, email="")
        ses_messages.verify_messages([], message_id=uuid.uuid4().hex, email=sender)
        ses_messages.verify_messages(
            [], message_id=message_id, email=f"other-{uuid.uuid4().hex[:8]}@example.com"
        )

        ses_messages.delete_message(message_id)
        ses_messages.verify_messages([], message_id=message_id, email=sender)
        ses_messages.verify_messages([expected[1]], email=sender)

    def test_rejected_email_is_not_recorded(self, ses, ses_messages):
        source = f"sender@unverified-{uuid.uuid4().hex[:8]}.example.com"
        with pytest.raises(ClientError) as error:
            ses.send_email(
                Source=source,
                Destination={"ToAddresses": ["recipient@example.com"]},
                Message={
                    "Subject": {"Data": "Rejected inspection"},
                    "Body": {"Text": {"Data": "Must not be recorded"}},
                },
            )
        assert error.value.response["Error"]["Code"] == "MessageRejected"
        ses_messages.verify_messages([], email=source)
