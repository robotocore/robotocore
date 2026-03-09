"""SQS event processor for event pipeline scenario."""

import json
import logging

logger = logging.getLogger(__name__)


def process(event, context):
    for record in event.get("Records", []):
        body = json.loads(record["body"])
        logger.info("Processing event: %s", body)
    return {"statusCode": 200, "processed": len(event.get("Records", []))}
