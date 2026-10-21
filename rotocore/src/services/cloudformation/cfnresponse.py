# Sourced from https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-lambda-function-code-cfnresponsemodule.html
# 01/Nov/2021

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# Deviation from the vendored AWS/moto module: urllib3 is unavailable in the
# robotocore lambda shim's host python, so fall back to the stdlib client
# for the single PUT this module performs.


import json
from typing import Any

try:
    import urllib3

    http = urllib3.PoolManager()
except ImportError:
    from urllib import request as _stdlib_request

    class _StdlibPool:
        def request(self, method: Any, url: Any, headers: Any = None, body: Any = None):
            data = body.encode() if isinstance(body, str) else body
            req = _stdlib_request.Request(
                url, data=data, headers=headers or {}, method=method
            )
            return _stdlib_request.urlopen(req)

    http = _StdlibPool()

SUCCESS = "SUCCESS"
FAILED = "FAILED"


def send(
    event: Any,
    context: Any,
    responseStatus: Any,
    responseData: Any,
    physicalResourceId: Any = None,
    noEcho: bool = False,
    reason: Any = None,
) -> None:
    responseUrl = event["ResponseURL"]

    print(responseUrl)  # noqa: T201

    responseBody = {
        "Status": responseStatus,
        "Reason": reason
        or f"See the details in CloudWatch Log Stream: {context.log_stream_name}",
        "PhysicalResourceId": physicalResourceId or context.log_stream_name,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "NoEcho": noEcho,
        "Data": responseData,
    }

    json_responseBody = json.dumps(responseBody)

    print("Response body:")  # noqa: T201
    print(json_responseBody)  # noqa: T201

    headers = {"content-type": "", "content-length": str(len(json_responseBody))}

    try:
        response = http.request(  # type: ignore
            "PUT", responseUrl, headers=headers, body=json_responseBody
        )
        print("Status code:", response.status)  # noqa: T201

    except Exception as e:
        print("send(..) failed executing http.request(..):", e)  # noqa: T201
